"""Gold daily marts (M4). One function per mart: Silver DataFrame -> mart DataFrame.

Design rules (see specs/gold-schema.md):
- Hourly grain. Every mart is grouped by the one-hour window of `pickup_ts`, so the same function works
  on a streaming Silver (event-time windows + watermark, append mode) and on a batch Silver (M5).
  The "daily" in the names is the roll-up: sum the measures by `pickup_date`.
- Additive measures only (counts and sums, never averages or ratios). Hourly rows then roll up to days
  exactly, stream and batch marts can be unioned (D28), and a KPI ratio is computed at query time,
  e.g. average fare = sum_total_amount / trips. `scripts/queries/` and the evaluation use these formulas.
- Each mart filters Silver with the `dq_*` flags (D27) according to the KPI's scope in specs/3_KPI.html.
"""
import pyspark.sql.functions as F

GRAIN = "1 hour"


def _count(cond=None):
    return (F.count(F.lit(1)) if cond is None else F.sum(F.when(cond, 1).otherwise(0))).cast("long")


def _sum(col, cond=None):
    c = F.col(col) if cond is None else F.when(cond, F.col(col))
    return F.coalesce(F.sum(c), F.lit(0.0)).cast("double")


def _hourly(df, keys, aggs):
    """Group by the pickup hour and `keys`; output hour_start, pickup_date, pickup_hour, keys, measures."""
    grouped = df.groupBy(F.window("pickup_ts", GRAIN).alias("w"), *keys).agg(*aggs)
    measures = grouped.columns[1 + len(keys):]
    return grouped.select(F.col("w.start").alias("hour_start"),
                          F.to_date(F.col("w.start")).alias("pickup_date"),
                          F.hour(F.col("w.start")).alias("pickup_hour"),
                          *keys, *measures)


def _in_period(df):
    return df.where(~F.col("dq_pickup_out_of_range"))


def _valid_time(df):
    return df.where(~F.col("dq_dropoff_before_pickup") & ~F.col("dq_trip_too_long")
                    & F.col("duration_sec").isNotNull())


def trips_daily(silver):
    """Demand: trips by service type. Share of trips = trips / total trips of the day."""
    return _hourly(_in_period(silver), ["service_type"], [_count().alias("trips")])


def demand_hourly(silver):
    """Demand by hour of day and day of week (day_of_week: 1 = Sunday ... 7 = Saturday)."""
    out = _hourly(_in_period(silver), ["service_type"], [_count().alias("trips")])
    return out.withColumn("day_of_week", F.dayofweek("pickup_date"))


def zone_od_daily(silver):
    """Trips and distance by pickup/dropoff zone pair (average distance = sum_distance_miles / distance_trips)."""
    has_dist = F.col("distance_miles") > 0
    return _hourly(_in_period(silver), ["service_type", "pu_location_id", "do_location_id"], [
        _count().alias("trips"),
        _count(has_dist).alias("distance_trips"),
        _sum("distance_miles", has_dist).alias("sum_distance_miles"),
    ])


def revenue_daily(silver):
    """Yellow and Green revenue. Fare per mile = sum_total_amount_dist / sum_distance_miles;
    tip rate (card) = sum_tip_amount / sum_base_fare where payment_type = 1."""
    df = _in_period(silver).where(F.col("service_type").isin("yellow", "green") & ~F.col("dq_negative_fare"))
    has_dist = F.col("distance_miles") > 0
    return _hourly(df, ["service_type", "pu_location_id", "pu_borough", "payment_type", "payment_label"], [
        _count().alias("trips"),
        _sum("total_amount").alias("sum_total_amount"),
        _sum("base_fare").alias("sum_base_fare"),
        _sum("tip_amount").alias("sum_tip_amount"),
        _count(has_dist).alias("distance_trips"),
        _sum("distance_miles", has_dist).alias("sum_distance_miles"),
        _sum("total_amount", has_dist).alias("sum_total_amount_dist"),
    ])


def trip_efficiency_daily(silver):
    """Duration and speed for trips with valid timestamps.
    Average duration = sum_duration_sec / trips; speed = sum_distance_miles / (sum_duration_sec_dist / 3600)."""
    df = _valid_time(_in_period(silver))
    ok = (F.col("distance_miles") > 0) & (F.col("duration_sec") > 0)
    return _hourly(df, ["service_type"], [
        _count().alias("trips"),
        _sum("duration_sec").alias("sum_duration_sec"),
        _count(ok).alias("speed_trips"),
        _sum("distance_miles", ok).alias("sum_distance_miles"),
        _sum("duration_sec", ok).alias("sum_duration_sec_dist"),
    ])


def hvfhv_economics_daily(silver):
    """HVFHV driver pay share = sum_driver_pay / sum_base_fare_with_pay; fee share of passenger cost =
    sum_fees / sum_passenger_cost. Passenger cost = base fare + tolls + bcf + sales tax + congestion
    surcharge + airport fee + CBD fee (tips excluded)."""
    df = _in_period(silver).where((F.col("service_type") == "fhvhv") & ~F.col("dq_negative_fare"))
    fees = sum(F.coalesce(F.col(c), F.lit(0.0)) for c in
               ("tolls", "bcf", "sales_tax", "congestion_surcharge", "airport_fee", "cbd_congestion_fee"))
    df = df.withColumn("fees", fees).withColumn("passenger_cost", F.coalesce(F.col("base_fare"), F.lit(0.0)) + fees)
    paid = F.col("driver_pay").isNotNull()
    return _hourly(df, ["hvfhs_license_num"], [
        _count().alias("trips"),
        _sum("base_fare").alias("sum_base_fare"),
        _count(paid).alias("trips_with_pay"),
        _sum("driver_pay", paid).alias("sum_driver_pay"),
        _sum("base_fare", paid).alias("sum_base_fare_with_pay"),
        _sum("fees").alias("sum_fees"),
        _sum("passenger_cost").alias("sum_passenger_cost"),
    ])


def hvfhv_service_daily(silver):
    """HVFHV wait (pickup - request), response (on scene - request) and shared-ride match.
    Wait = sum_wait_sec / wait_trips; match rate = shared_matches / shared_requests."""
    df = _in_period(silver).where(F.col("service_type") == "fhvhv")
    req = F.col("request_ts")
    wait = F.unix_timestamp("pickup_ts") - F.unix_timestamp(req)
    resp = F.unix_timestamp("on_scene_ts") - F.unix_timestamp(req)
    df = df.withColumn("wait_sec", F.when(wait >= 0, wait)).withColumn("response_sec", F.when(resp >= 0, resp))
    asked = F.col("shared_request_flag") == "Y"
    return _hourly(df, ["hvfhs_license_num"], [
        _count().alias("trips"),
        _count(F.col("wait_sec").isNotNull()).alias("wait_trips"),
        _sum("wait_sec").alias("sum_wait_sec"),
        _count(F.col("response_sec").isNotNull()).alias("response_trips"),
        _sum("response_sec").alias("sum_response_sec"),
        _count(asked).alias("shared_requests"),
        _count(asked & (F.col("shared_match_flag") == "Y")).alias("shared_matches"),
    ])


def data_quality_daily(silver):
    """Row count and one counter per dq flag, for every Silver row (no scope filter).
    Rate = counter / rows; unmatched-zone share = (unmatched_pu + unmatched_do) / (2 * rows)."""
    from transform import DQ_FLAGS
    return _hourly(silver, ["service_type"],
                   [_count().alias("rows")] + [_count(F.col(f)).alias(f.replace("dq_", "n_", 1)) for f in DQ_FLAGS])


MARTS = {
    "trips_daily": trips_daily,
    "demand_hourly": demand_hourly,
    "zone_od_daily": zone_od_daily,
    "revenue_daily": revenue_daily,
    "trip_efficiency_daily": trip_efficiency_daily,
    "hvfhv_economics_daily": hvfhv_economics_daily,
    "hvfhv_service_daily": hvfhv_service_daily,
    "data_quality_daily": data_quality_daily,
}
