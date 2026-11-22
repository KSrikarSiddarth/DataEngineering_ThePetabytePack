"""Step 6 - SILVER zone-hour demand: dense (zone x hour) grid with trip stats and weather."""
from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from ..common.config import Config
from ..common.utils import get_logger, month_range, read_parquet, record_measurements, write_parquet
from .silver import hour_calendar

log = get_logger(__name__)


def build_zone_hour_demand(spark: SparkSession, cfg: Config, months: list[str], report: bool = True) -> int:
    start, end = month_range(months)
    trips = read_parquet(spark, cfg.path("silver") / "trips_clean", "silver").where(
        (F.col("pickup_date") >= F.lit(start.date())) & (F.col("pickup_date") < F.lit(end.date())))
    zones = read_parquet(spark, cfg.path("bronze") / "taxi_zones", "bronze").where("is_known_zone")
    weather = read_parquet(spark, cfg.path("silver") / "weather_hourly", "silver").where(F.col("month").isin(months))

    pickups = trips.groupBy(F.col("pickup_zone_id").alias("zone_id"), F.col("pickup_hour").alias("window_start")).agg(
        F.count(F.lit(1)).alias("trip_count"),
        F.avg("trip_distance").alias("avg_trip_distance"),
        F.avg("duration_min").alias("avg_duration_min"),
        F.avg("total_amount").alias("avg_fare"),
        F.sum("total_amount").alias("total_revenue"),
    )
    dropoffs = (
        trips.where((F.col("dropoff_hour") >= F.lit(start)) & (F.col("dropoff_hour") < F.lit(end)))
        .groupBy(F.col("dropoff_zone_id").alias("zone_id"), F.col("dropoff_hour").alias("window_start"))
        .agg(F.count(F.lit(1)).alias("dropoff_count"))
    )
    # densify: an hour with no pickups is a real observation (demand = 0), and lags need a gap-free series
    grid = zones.select("zone_id", "zone_name", "borough", "service_zone").crossJoin(hour_calendar(spark, start, end))
    demand = (
        grid.join(pickups, ["zone_id", "window_start"], "left")
        .join(dropoffs, ["zone_id", "window_start"], "left")
        .fillna(0, subset=["trip_count", "dropoff_count", "total_revenue"])
        .join(F.broadcast(weather.select(
            F.col("weather_hour").alias("window_start"), "temp_c", "precip_mm", "wind_mps", "visibility_m",
            "is_raining", "weather_condition")), "window_start", "left")
        .withColumn("month", F.date_format("window_start", "yyyy-MM"))
    )
    out = cfg.path("silver") / "zone_hour_demand"
    write_parquet(demand.repartition("month"), out, partition_by=["month"])

    written = spark.read.parquet(str(out)).where(F.col("month").isin(months))
    rows = written.count()
    if report:
        total = written.agg(F.sum("trip_count")).first()[0] or 0
        diff = int(total) - trips.count()
        record_measurements(cfg, {"dq.demand_reconciliation_diff": abs(diff)}, {"months": months})
        log.info("zone_hour_demand: %d rows, trip reconciliation diff=%d", rows, diff)
    return rows
