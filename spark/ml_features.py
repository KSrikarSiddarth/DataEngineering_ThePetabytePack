"""M6 feature tables for Spark ML (`silver.ml_features`) and the columns each model may use.

Two tables, both built from the batch store:
- zone-hour: trips per pickup zone and hour (from the gold `zone_od_daily` mart) on a complete zone x hour grid,
  with the demand 1, 24 and 168 hours earlier (lag features) and calendar columns. For the demand forecast.
- trips: a deterministic sample of clean Silver trips (no `dq_*` flag) with calendar columns, zone profile and the
  labels of the trip-level models. Rows are kept by a hash of `trip_id`, so the same data and seed give the same sample.

Leakage (O4): columns known only after the trip, or after the request for HVFHV, are in POST_TRIP and may never be
a feature. `FEATURES` lists exactly what each model reads; tests/test_ml.py checks it against POST_TRIP and the label.
"""
from functools import reduce

import pyspark.sql.functions as F

from transform import DQ_FLAGS

LAGS = (1, 24, 168)                       # hours; 168 = "same hour last week", the demand baseline
HASH_BUCKETS = 1_000_000

# Known only after the trip is over (or the passenger is picked up): never a feature of any model.
POST_TRIP = ["dropoff_ts", "duration_sec", "total_amount", "tip_amount", "driver_pay", "on_scene_ts"]

CALENDAR = ["hour", "day_of_week", "is_weekend", "is_holiday"]
TRIP = CALENDAR + ["pu_location_id", "do_location_id", "distance_miles", "ratecode_id", "passenger_count",
                   "service_code", "pu_zone_demand", "do_zone_demand"]
REQUEST = CALENDAR + ["pu_location_id", "company", "pu_zone_demand"]   # what is known when a ride is requested

FEATURES = {
    "demand_forecast": [f"lag_{h}h" for h in LAGS] + CALENDAR + ["pu_location_id"],
    "fare": TRIP,
    "duration": TRIP,
    "tip": TRIP + ["base_fare"],           # the fare is known before the tip is given; total_amount is not usable
    "wait": REQUEST,
    "shared_match": REQUEST + ["do_location_id", "do_zone_demand"],
}
LABELS = {"demand_forecast": "trips", "fare": "base_fare", "duration": "duration_min", "tip": "tip20",
          "wait": "wait_sec", "shared_match": "shared_match"}

MAX_MILES, MAX_FARE, MAX_MINUTES = 100, 500, 180   # plausibility caps: beyond these a trip is an outlier, not training data
MAX_WAIT_SEC = 3600                       # longer waits are treated as data errors, not training targets

# Rows each trip-level model trains on (SQL over the trips table; service_code 1 yellow, 2 green, 3 fhvhv).
ROWS = {
    "fare": f"service_code IN (1, 2) AND base_fare > 0 AND base_fare <= {MAX_FARE} AND distance_miles <= {MAX_MILES}",
    "duration": f"service_code IN (1, 2, 3) AND duration_min > 0 AND duration_min <= {MAX_MINUTES} "
                f"AND distance_miles <= {MAX_MILES}",
    "tip": f"tip20 IS NOT NULL AND base_fare <= {MAX_FARE} AND distance_miles <= {MAX_MILES}",
    "wait": "wait_sec IS NOT NULL",
    "shared_match": "shared_match IS NOT NULL",
}
TIP_RATE = 0.2                            # the "generous tip" class: tip of at least 20% of the base fare


def calendar_cols(ts, holidays):
    """hour, day_of_week (1 = Sunday), is_weekend and is_holiday of timestamp column `ts`."""
    dow = F.dayofweek(ts)
    return [F.hour(ts).alias("hour"), dow.alias("day_of_week"), dow.isin(1, 7).cast("int").alias("is_weekend"),
            F.date_format(ts, "yyyy-MM-dd").isin(list(holidays)).cast("int").alias("is_holiday")]


def file_month(year="year", month="month"):
    """'YYYY-MM' of the source file (the partition columns `year` and `month`, D31), the month used by every ML
    table. A file holds a few trips just outside its month; they must not make a month of their own."""
    return F.format_string("%04d-%02d", F.col(year), F.col(month))


def zone_hour(od, holidays):
    """Zone-hour table from the zone_od mart `od`: trips on a full grid of the zones and the days present (0 for an
    hour without trips), lag features (null where the earlier day is not loaded; those rows are dropped), calendar
    columns and `ym`. Hours outside the loaded file months are ignored. Returns (table, zone_demand) where
    zone_demand = mean trips per hour of each zone."""
    spark = od.sparkSession
    loaded = od.select(file_month().alias("ym")).distinct()
    hourly = (od.where(F.col("pu_location_id").isNotNull())
              .groupBy("pu_location_id", "hour_start").agg(F.sum("trips").alias("trips"))
              .withColumn("ym", F.date_format("hour_start", "yyyy-MM")).join(loaded, "ym", "left_semi").drop("ym"))
    days = hourly.select(F.to_date("hour_start").alias("day")).distinct()
    grid = (hourly.select("pu_location_id").distinct().crossJoin(days).crossJoin(spark.range(24).toDF("h"))
            .select("pu_location_id",
                    (F.unix_timestamp(F.col("day").cast("timestamp")) + F.col("h") * 3600).cast("timestamp")
                    .alias("hour_start")))
    full = grid.join(hourly, ["pu_location_id", "hour_start"], "left").fillna(0, ["trips"])

    lagged = full
    for h in LAGS:
        earlier = full.select("pu_location_id", (F.col("hour_start") + F.expr(f"INTERVAL {h} HOURS")).alias("hour_start"),
                              F.col("trips").alias(f"lag_{h}h"))
        lagged = lagged.join(earlier, ["pu_location_id", "hour_start"], "left")
    table = lagged.dropna(subset=[f"lag_{h}h" for h in LAGS]).select(
        "pu_location_id", "hour_start", F.date_format("hour_start", "yyyy-MM").alias("ym"), "trips",
        *[f"lag_{h}h" for h in LAGS], *calendar_cols("hour_start", holidays))
    return table, full.groupBy("pu_location_id").agg(F.avg("trips").alias("zone_demand"))


def trip_features(silver, zone_demand, holidays, sample_rows, seed):
    """Trips table: a sample of at most `sample_rows` clean trips per service (yellow, green, fhvhv; FHV has no
    distance or fare) with calendar columns, zone profile (mean hourly demand of the pickup and dropoff zones) and
    the labels: base_fare, duration_min, tip20 (card trips: tip >= 20% of fare), wait_sec (pickup - request),
    shared_match (shared requests only). A label is null where the model does not apply to the trip."""
    clean = reduce(lambda df, flag: df.where(~F.col(flag)), DQ_FLAGS,
                   silver.where(F.col("service_type").isin("yellow", "green", "fhvhv")))
    counts = {r["service_type"]: r["count"] for r in clean.groupBy("service_type").count().collect()}
    fraction = F.coalesce(*[F.when(F.col("service_type") == s, F.lit(min(1.0, sample_rows / n)))
                            for s, n in counts.items()], F.lit(0.0))
    keep = F.pmod(F.hash(F.lit(seed), "trip_id"), F.lit(HASH_BUCKETS)) < fraction * HASH_BUCKETS

    svc = F.col("service_type")
    wait = F.unix_timestamp("pickup_ts") - F.unix_timestamp("request_ts")
    card = (F.col("payment_type") == 1) & (F.col("base_fare") > 0)
    pu = zone_demand.withColumnRenamed("zone_demand", "pu_zone_demand")
    do = (zone_demand.withColumnRenamed("pu_location_id", "do_location_id")
          .withColumnRenamed("zone_demand", "do_zone_demand"))
    return (clean.where(keep)
            .join(pu, "pu_location_id", "left").join(do, "do_location_id", "left")
            .select("trip_id", "service_type", "pickup_ts", file_month().alias("ym"),
                    *calendar_cols("pickup_ts", holidays),
                    "pu_location_id", "do_location_id", "distance_miles",
                    F.coalesce("ratecode_id", F.lit(0)).alias("ratecode_id"),
                    F.coalesce("passenger_count", F.lit(0)).alias("passenger_count"),
                    F.when(svc == "yellow", 1).when(svc == "green", 2).otherwise(3).alias("service_code"),
                    F.coalesce(F.regexp_extract("hvfhs_license_num", r"(\d+)$", 1).cast("int"), F.lit(0)).alias("company"),
                    F.coalesce("pu_zone_demand", F.lit(0.0)).alias("pu_zone_demand"),
                    F.coalesce("do_zone_demand", F.lit(0.0)).alias("do_zone_demand"),
                    "base_fare", (F.col("duration_sec") / 60).alias("duration_min"),
                    F.when(card, (F.col("tip_amount") / F.col("base_fare") >= TIP_RATE).cast("int")).alias("tip20"),
                    F.when((svc == "fhvhv") & (wait >= 0) & (wait <= MAX_WAIT_SEC), wait).alias("wait_sec"),
                    F.when((svc == "fhvhv") & (F.col("shared_request_flag") == "Y"),
                           (F.col("shared_match_flag") == "Y").cast("int")).alias("shared_match")))
