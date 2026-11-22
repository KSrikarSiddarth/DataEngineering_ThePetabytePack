"""Batch experiments E1-E5 (Plan section 7): data scaling, shuffle partitions, join strategy, caching, file format."""
from __future__ import annotations

import argparse
import shutil

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from ..batch.demand import build_zone_hour_demand
from ..batch.features import build_features
from ..batch.quality import apply_quality
from ..batch.silver import build_silver_trips, build_weather_hourly, enrich_trips
from ..common.config import Config, load_config
from ..common.spark import get_spark
from ..common.utils import Timer, get_logger, record_measurements
from .harness import median_seconds, run_benchmark

log = get_logger(__name__)


def bench_cfg(cfg: Config, name: str) -> Config:
    root = cfg.path("bench") / name
    return cfg.with_overrides({"paths": {"silver": str(root / "silver"), "gold": str(root / "gold"),
                                         "quarantine": str(root / "quarantine")}})


def bronze_rows(spark: SparkSession, cfg: Config, months: list[str]) -> int:
    return spark.read.parquet(str(cfg.path("bronze") / "trips")).where(F.col("source_month").isin(months)).count()


def batch_chain(spark: SparkSession, cfg: Config, months: list[str]) -> dict:
    """bronze -> silver trips -> zone-hour demand -> features, timing each step."""
    steps = {}
    with Timer() as t:
        build_weather_hourly(spark, cfg, months)
        build_silver_trips(spark, cfg, months, report=False)
    steps["silver_s"] = t.seconds
    with Timer() as t:
        build_zone_hour_demand(spark, cfg, months, report=False)
    steps["demand_s"] = t.seconds
    with Timer() as t:
        build_features(spark, cfg, months)
    steps["features_s"] = t.seconds
    return steps


# ------------------------------------------------------------------ E1 data scaling
def e1_scaling(spark: SparkSession, cfg: Config) -> dict:
    sizes = ["small", "medium", "large"]
    medians, rows = {}, {}
    for size in sizes:
        months = cfg.section("datasets")[size]
        rows[size] = bronze_rows(spark, cfg, months)
        bcfg = bench_cfg(cfg, f"e1_{size}")
        df = run_benchmark(spark, cfg, "e1_scaling", size, lambda: batch_chain(spark, bcfg, months),
                           extra={"rows": rows[size], "months": len(months)})
        medians[size] = {"total": median_seconds(df), "silver": float(df["silver_s"].median()),
                         "features": float(df["features_s"].median())}
    eff = []
    for a, b in [("small", "medium"), ("medium", "large")]:
        k = rows[b] / rows[a]
        eff.append((medians[a]["total"] * k) / medians[b]["total"])
    values = {
        "batch.small_e2e_min": medians["small"]["total"] / 60,
        "batch.bronze_to_silver_rps": rows["medium"] / medians["medium"]["silver"],
        "batch.scaling_efficiency_min": min(eff),
        "features.build_min": medians["medium"]["features"] / 60,
    }
    record_measurements(cfg, values, {"rows": rows, "efficiencies": eff})
    return values


# ------------------------------------------------------------------ E2 shuffle partitions
def e2_partitions(spark: SparkSession, cfg: Config) -> dict:
    months = cfg.section("datasets")["medium"]
    bcfg = bench_cfg(cfg, "e2")
    original = spark.conf.get("spark.sql.shuffle.partitions")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")
    results = {}
    try:
        build_weather_hourly(spark, bcfg, months)
        for n in cfg.section("benchmark")["partition_values"]:
            spark.conf.set("spark.sql.shuffle.partitions", str(n))

            def work():
                build_silver_trips(spark, bcfg, months, report=False)
                build_zone_hour_demand(spark, bcfg, months, report=False)

            results[n] = median_seconds(run_benchmark(spark, cfg, "e2_partitions", str(n), work,
                                                      extra={"partitions": n}))
    finally:
        spark.conf.set("spark.sql.shuffle.partitions", original)
        spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    best = min(results, key=results.get)
    record_measurements(cfg, {"batch.best_partitions": best}, {"median_seconds": results})
    return {"best_partitions": best, "median_seconds": results}


# ------------------------------------------------------------------ E3 broadcast vs sort-merge join
def e3_join(spark: SparkSession, cfg: Config) -> dict:
    months = cfg.section("datasets")["medium"]
    bronze = spark.read.parquet(str(cfg.path("bronze") / "trips")).where(F.col("source_month").isin(months))
    zones = spark.read.parquet(str(cfg.path("bronze") / "taxi_zones"))
    q = cfg.section("quality")
    original = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")

    def work(broadcast: bool):
        def run():
            valid = apply_quality(bronze, q).where(F.col("reject_reason").isNull())
            enrich_trips(valid, zones, broadcast_zones=broadcast).write.format("noop").mode("overwrite").save()
        return run

    try:
        b = run_benchmark(spark, cfg, "e3_join", "broadcast", work(True))
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
        s = run_benchmark(spark, cfg, "e3_join", "sort_merge", work(False))
    finally:
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", original)
    speedup = median_seconds(s) / median_seconds(b)
    record_measurements(cfg, {"batch.broadcast_speedup": speedup},
                        {"shuffle_bytes_broadcast": float(b.get("shuffle_write_bytes", pd.Series([0])).median()),
                         "shuffle_bytes_sort_merge": float(s.get("shuffle_write_bytes", pd.Series([0])).median())})
    return {"broadcast_speedup": speedup}


# ------------------------------------------------------------------ E4 caching a reused DataFrame
def e4_cache(spark: SparkSession, cfg: Config) -> dict:
    months = cfg.section("datasets")["medium"]
    trips = spark.read.parquet(str(cfg.path("silver") / "trips_clean")).where(F.col("source_month").isin(months))

    def queries(df):
        df.groupBy("pickup_zone_id").count().collect()
        df.groupBy("pickup_borough").agg(F.avg("total_amount")).collect()
        df.groupBy("hour_of_day").agg(F.avg("duration_min")).collect()
        df.groupBy("payment_type").count().collect()
        df.groupBy("pickup_borough", "dropoff_borough").count().collect()

    def work(cache: bool):
        def run():
            base = trips.where("trip_distance > 0.5").withColumn("fare_per_mile", F.col("total_amount") / F.col("trip_distance"))
            if cache:
                base = base.cache()
                base.count()
            queries(base)
            if cache:
                base.unpersist()
        return run

    u = run_benchmark(spark, cfg, "e4_cache", "uncached", work(False))
    c = run_benchmark(spark, cfg, "e4_cache", "cached", work(True))
    speedup = median_seconds(u) / median_seconds(c)
    record_measurements(cfg, {"batch.cache_speedup": speedup})
    return {"cache_speedup": speedup}


# ------------------------------------------------------------------ E5 Parquet vs CSV
def e5_format(spark: SparkSession, cfg: Config) -> dict:
    month = cfg.section("datasets")["small"][0]
    src = cfg.path("raw") / "tlc" / f"{cfg.section('sources')['tlc_dataset']}_{month}.parquet"
    csv_dir = cfg.path("bench") / "e5_csv"
    if csv_dir.exists():
        shutil.rmtree(csv_dir)
    spark.read.parquet(str(src)).write.option("header", True).csv(str(csv_dir))

    def query(df):
        df.groupBy("PULocationID").agg(F.count(F.lit(1)), F.avg(F.col("total_amount").cast("double"))).collect()

    p = run_benchmark(spark, cfg, "e5_format", "parquet", lambda: query(spark.read.parquet(str(src))))
    c = run_benchmark(spark, cfg, "e5_format", "csv",
                      lambda: query(spark.read.option("header", True).option("inferSchema", False).csv(str(csv_dir))))
    csv_bytes = sum(f.stat().st_size for f in csv_dir.glob("*.csv"))
    speedup = median_seconds(c) / median_seconds(p)
    record_measurements(cfg, {"batch.parquet_vs_csv_speedup": speedup},
                        {"parquet_bytes": src.stat().st_size, "csv_bytes": csv_bytes})
    return {"parquet_vs_csv_speedup": speedup, "size_ratio_csv_over_parquet": csv_bytes / src.stat().st_size}


EXPERIMENTS = {"e1": e1_scaling, "e2": e2_partitions, "e3": e3_join, "e4": e4_cache, "e5": e5_format}


def main() -> None:
    ap = argparse.ArgumentParser(description="Run batch benchmark experiments")
    # no `choices`: Python 3.11 argparse rejects an empty nargs='*' positional when choices is set
    ap.add_argument("experiments", nargs="*", help=f"subset of {list(EXPERIMENTS)} (default: all)")
    a = ap.parse_args()
    if unknown := set(a.experiments) - set(EXPERIMENTS):
        ap.error(f"unknown experiments {sorted(unknown)}; choose from {list(EXPERIMENTS)}")
    cfg = load_config()
    spark = get_spark(cfg, "bench")
    for name in a.experiments or list(EXPERIMENTS):
        log.info("=== %s ===", name)
        log.info("%s -> %s", name, EXPERIMENTS[name](spark, cfg))
    spark.stop()


if __name__ == "__main__":
    main()
