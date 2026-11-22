"""Step 7 - SILVER zone-hour features: lags, rolling means, calendar, weather and next-hour label."""
from __future__ import annotations

from datetime import timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from ..common.config import Config
from ..common.utils import get_logger, read_parquet, us_holidays, write_parquet
from .silver import iso_dow

log = get_logger(__name__)

SEASONAL_LAG = 167  # value at (t+1) - 168h, i.e. the target hour one week earlier


def add_time_features(df: DataFrame, lags: list[int], windows: list[int], holidays: list[str]) -> DataFrame:
    w = Window.partitionBy("zone_id").orderBy("window_start")
    for n in lags:
        df = df.withColumn(f"lag_{n}", F.lag("trip_count", n).over(w))
    df = df.withColumn("target_hour_last_week", F.lag("trip_count", SEASONAL_LAG).over(w))
    df = df.withColumn("target_hour_yesterday", F.lag("trip_count", 23).over(w))
    two_weeks = F.lag("trip_count", SEASONAL_LAG + 168).over(w)
    df = df.withColumn("target_hour_avg_2w", F.when(two_weeks.isNull(), F.col("target_hour_last_week"))
                       .otherwise((F.col("target_hour_last_week") + two_weeks) / 2.0))
    for k in windows:
        df = df.withColumn(f"roll_mean_{k}", F.avg("trip_count").over(w.rowsBetween(-(k - 1), 0)))
    df = df.withColumn("diff_day", F.col("trip_count") - F.lag("trip_count", 24).over(w))
    target = F.col("window_start") + F.expr("INTERVAL 1 HOUR")
    return (
        df.withColumn("label_next_hour", F.lead("trip_count", 1).over(w))
        .withColumn("target_hour", target)
        .withColumn("hour_of_day", F.hour("window_start"))
        .withColumn("day_of_week", iso_dow("window_start"))
        .withColumn("target_hour_of_day", F.hour(target))
        .withColumn("target_day_of_week", iso_dow(target))
        .withColumn("is_weekend", (F.col("day_of_week") >= 6).cast("int"))
        .withColumn("is_holiday", F.date_format("window_start", "yyyy-MM-dd").isin(holidays).cast("int"))
        .withColumn("month_of_year", F.month("window_start"))
        .withColumn("is_raining_int", F.coalesce(F.col("is_raining").cast("int"), F.lit(0)))
    )


def assign_split(df: DataFrame, max_ts, test_days: int, validation_days: int) -> DataFrame:
    test_start = max_ts - timedelta(days=test_days)
    val_start = test_start - timedelta(days=validation_days)
    return df.withColumn(
        "split",
        F.when(F.col("window_start") >= F.lit(test_start), "test")
        .when(F.col("window_start") >= F.lit(val_start), "validation").otherwise("train"),
    )


def build_features(spark: SparkSession, cfg: Config, months: list[str]) -> dict:
    fcfg, mcfg = cfg.section("features"), cfg.section("ml")
    demand = read_parquet(spark, cfg.path("silver") / "zone_hour_demand", "demand").where(F.col("month").isin(months))

    eligible = demand.groupBy("zone_id").agg(F.sum("trip_count").alias("total_trips")) \
        .where(F.col("total_trips") >= fcfg["min_zone_trips"])
    write_parquet(eligible.coalesce(1), cfg.path("silver") / "eligible_zones")

    years = sorted({int(m[:4]) for m in months})
    holidays = [d.isoformat() for d in us_holidays(range(years[0], years[-1] + 1))]
    feats = add_time_features(demand.join(F.broadcast(eligible.select("zone_id")), "zone_id"),
                              fcfg["lags"], fcfg["rolling_windows"], holidays)
    needed = [f"lag_{max(fcfg['lags'])}", "target_hour_last_week", "label_next_hour"]
    feats = feats.dropna(subset=needed)

    max_ts = feats.agg(F.max("window_start")).first()[0]
    if max_ts is None:
        raise ValueError("No feature rows: need more than 168 hours of history for eligible zones")
    feats = assign_split(feats, max_ts, mcfg["test_days"], mcfg["validation_days"])
    out = cfg.path("silver") / "zone_hour_features"
    write_parquet(feats.repartition("split"), out, partition_by=["split"], replace_all=True)

    counts = {r["split"]: r["count"] for r in spark.read.parquet(str(out)).groupBy("split").count().collect()}
    log.info("features: eligible zones=%d rows by split=%s", eligible.count(), counts)
    return counts
