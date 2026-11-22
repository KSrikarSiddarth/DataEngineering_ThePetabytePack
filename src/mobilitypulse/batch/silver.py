"""Steps 4-5 - BRONZE -> SILVER clean trips (+ QUARANTINE + DQ report) and hourly weather."""
from __future__ import annotations

from datetime import datetime

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from ..common.config import Config
from ..common.utils import (get_logger, month_range, read_parquet, record_measurements, write_json,
                            write_parquet)
from .quality import apply_quality

log = get_logger(__name__)

NULL_CHECK_COLUMNS = ["pickup_ts", "dropoff_ts", "pickup_zone_id", "dropoff_zone_id", "passenger_count",
                      "trip_distance", "total_amount", "payment_type", "ratecode_id"]


def iso_dow(col) -> F.Column:
    """1 = Monday ... 7 = Sunday."""
    return F.weekday(col) + 1


def hour_band(hour_col) -> F.Column:
    return (F.when(hour_col.between(7, 9), "am_peak").when(hour_col.between(16, 19), "pm_peak")
            .when(hour_col.between(0, 5), "night").otherwise("off_peak"))


def enrich_trips(valid: DataFrame, zones: DataFrame, broadcast_zones: bool = True) -> DataFrame:
    pu = zones.select(F.col("zone_id").alias("pickup_zone_id"), F.col("borough").alias("pickup_borough"),
                      F.col("zone_name").alias("pickup_zone_name"))
    do = zones.select(F.col("zone_id").alias("dropoff_zone_id"), F.col("borough").alias("dropoff_borough"))
    if broadcast_zones:
        pu, do = F.broadcast(pu), F.broadcast(do)
    return (
        valid.join(pu, "pickup_zone_id", "left").join(do, "dropoff_zone_id", "left")
        .withColumn("pickup_hour", F.date_trunc("hour", "pickup_ts"))
        .withColumn("dropoff_hour", F.date_trunc("hour", "dropoff_ts"))
        .withColumn("hour_of_day", F.hour("pickup_ts"))
        .withColumn("day_of_week", iso_dow("pickup_ts"))
        .withColumn("pickup_date", F.to_date("pickup_ts"))
    )


def dq_report(flagged: DataFrame, months: list[str]) -> dict[str, dict]:
    by_reason = flagged.groupBy("source_month", "reject_reason").count().collect()
    nulls = flagged.groupBy("source_month").agg(
        F.count(F.lit(1)).alias("input_rows"),
        *[F.sum(F.col(c).isNull().cast("long")).alias(c) for c in NULL_CHECK_COLUMNS],
    ).collect()
    reports: dict[str, dict] = {m: {"month": m, "rejects_by_reason": {}} for m in months}
    for r in by_reason:
        rep = reports[r["source_month"]]
        if r["reject_reason"] is None:
            rep["valid_rows"] = r["count"]
        else:
            rep["rejects_by_reason"][r["reject_reason"]] = r["count"]
    for r in nulls:
        rep = reports[r["source_month"]]
        rep["input_rows"] = r["input_rows"]
        rep["null_counts"] = {c: r[c] for c in NULL_CHECK_COLUMNS}
    for rep in reports.values():
        rep.setdefault("valid_rows", 0)
        rep["rejected_rows"] = sum(rep["rejects_by_reason"].values())
        rep["duplicates"] = rep["rejects_by_reason"].get("DUPLICATE", 0)
    return reports


def build_silver_trips(spark: SparkSession, cfg: Config, months: list[str], broadcast_zones: bool = True,
                       report: bool = True) -> dict:
    bronze = read_parquet(spark, cfg.path("bronze") / "trips", "bronze").where(F.col("source_month").isin(months))
    zones = read_parquet(spark, cfg.path("bronze") / "taxi_zones", "bronze")
    flagged = apply_quality(bronze, cfg.section("quality")).persist(StorageLevel.MEMORY_AND_DISK)

    valid = enrich_trips(flagged.where(F.col("reject_reason").isNull()).drop("reject_reasons", "reject_reason"),
                         zones, broadcast_zones)
    silver_path = cfg.path("silver") / "trips_clean"
    # one file per day avoids the small-files problem of partitionBy after a 32-way shuffle
    write_parquet(valid.repartition("pickup_date"), silver_path, partition_by=["pickup_date"])
    rejected = flagged.where(F.col("reject_reason").isNotNull()).withColumn("rejected_at", F.current_timestamp())
    q_path = cfg.path("quarantine") / "trips_rejected"
    write_parquet(rejected.coalesce(4), q_path, partition_by=["source_month"])

    reports = dq_report(flagged, months) if report else {}
    if report:
        from .dq_daily import write_dq_daily  # local import: dq_daily imports NULL_CHECK_COLUMNS from this module
        write_dq_daily(flagged, cfg, months)
    flagged.unpersist()
    if not report:
        return {}

    # reconciliation re-reads what was actually written, so it also verifies the writes
    start, end = month_range(months)
    silver = spark.read.parquet(str(silver_path)).where(
        (F.col("pickup_date") >= F.lit(start.date())) & (F.col("pickup_date") < F.lit(end.date())))
    written_valid = {r["source_month"]: r["count"] for r in silver.groupBy("source_month").count().collect()}
    written_rej = {r["source_month"]: r["count"] for r in
                   spark.read.parquet(str(q_path)).where(F.col("source_month").isin(months))
                   .groupBy("source_month").count().collect()}
    silver_dups = silver.groupBy("record_id").count().where("count > 1").count()

    max_diff, max_rate = 0, 0.0
    for m, rep in reports.items():
        rep["written_valid_rows"] = written_valid.get(m, 0)
        rep["written_rejected_rows"] = written_rej.get(m, 0)
        rep["reconciliation_diff"] = rep["input_rows"] - rep["written_valid_rows"] - rep["written_rejected_rows"]
        rep["reject_rate_pct"] = round(100.0 * rep["rejected_rows"] / max(rep["input_rows"], 1), 3)
        write_json(cfg.path("results") / "dq" / f"dq_report_{m}.json", rep)
        max_diff = max(max_diff, abs(rep["reconciliation_diff"]))
        max_rate = max(max_rate, rep["reject_rate_pct"])
        log.info("DQ %s input=%d valid=%d rejected=%d diff=%d rate=%.2f%%", m, rep["input_rows"],
                 rep["written_valid_rows"], rep["written_rejected_rows"], rep["reconciliation_diff"],
                 rep["reject_rate_pct"])
    record_measurements(cfg, {"dq.max_reconciliation_diff": max_diff, "dq.max_reject_rate_pct": max_rate,
                              "dq.silver_duplicates": silver_dups}, {"months": months})
    return reports


def hour_calendar(spark: SparkSession, start: datetime, end: datetime, col: str = "window_start") -> DataFrame:
    return spark.sql(
        f"SELECT explode(sequence(timestamp'{start:%Y-%m-%d %H:%M:%S}', "
        f"timestamp'{end:%Y-%m-%d %H:%M:%S}' - INTERVAL 1 HOUR, INTERVAL 1 HOUR)) AS {col}"
    )


def weather_condition(temp, precip, vis) -> F.Column:
    return (F.when((precip > 0) & (temp <= 0), "snow_or_ice").when(precip > 2.5, "heavy_rain")
            .when(precip > 0, "light_rain").when(vis < 1600, "low_visibility").otherwise("dry"))


def build_weather_hourly(spark: SparkSession, cfg: Config, months: list[str]) -> int:
    start, end = month_range(months)
    obs = read_parquet(spark, cfg.path("bronze") / "weather_obs", "bronze") \
        .withColumn("weather_hour", F.date_trunc("hour", "obs_ts_local")) \
        .where((F.col("weather_hour") >= F.lit(start)) & (F.col("weather_hour") < F.lit(end)))

    def prefer_routine(c: str) -> F.Column:
        return F.coalesce(F.avg(F.when(F.col("report_type") == "FM-15", F.col(c))), F.avg(c)).alias(c)

    one_hour_precip = F.when(F.col("precip_period_h") == 1, F.col("precip_mm"))
    hourly = obs.groupBy("weather_hour").agg(
        *[prefer_routine(c) for c in ["temp_c", "dew_point_c", "wind_mps", "visibility_m"]],
        F.coalesce(F.max(F.when(F.col("report_type") == "FM-15", one_hour_precip)), F.max(one_hour_precip)).alias("precip_mm"),
    )
    cal = hour_calendar(spark, start, end, "weather_hour")
    full = cal.join(hourly, "weather_hour", "left")

    w = Window.orderBy("weather_hour").rowsBetween(-3, 0)
    cols = ["temp_c", "dew_point_c", "wind_mps", "visibility_m", "precip_mm"]
    full = full.withColumn("weather_imputed", F.col("temp_c").isNull() | F.col("precip_mm").isNull())
    for c in cols:
        full = full.withColumn(c, F.last(c, ignorenulls=True).over(w))
    full = (
        full.withColumn("is_raining", F.coalesce(F.col("precip_mm") > 0, F.lit(False)))
        .withColumn("weather_condition", weather_condition(F.col("temp_c"), F.coalesce("precip_mm", F.lit(0.0)),
                                                           F.coalesce("visibility_m", F.lit(16093.0))))
        .withColumn("month", F.date_format("weather_hour", "yyyy-MM"))
    )
    write_parquet(full.coalesce(1), cfg.path("silver") / "weather_hourly", partition_by=["month"])
    return full.count()


def run_silver(spark: SparkSession, cfg: Config, months: list[str]) -> dict:
    return {"weather_hours": build_weather_hourly(spark, cfg, months),
            "dq": build_silver_trips(spark, cfg, months)}
