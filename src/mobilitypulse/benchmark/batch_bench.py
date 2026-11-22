"""Batch experiments E1-E5 (Plan section 7): data scaling, shuffle partitions, join strategy, caching, file format."""
from __future__ import annotations

import argparse
import shutil

import pandas as pd
from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from ..batch.demand import build_zone_hour_demand
from ..batch.features import build_features
from ..batch.quality import apply_quality
from ..batch.silver import build_silver_trips, build_weather_hourly, dq_report, enrich_trips
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
SCAN_QUERY_COLUMNS = ["pickup_zone_id", "pickup_borough", "dropoff_borough", "total_amount", "duration_min",
                      "hour_of_day", "payment_type"]


def e4_cache(spark: SparkSession, cfg: Config) -> dict:
    """Does caching pay off? Two scenarios, because the answer depends on how expensive recomputation is.

    1. pipeline reuse (scored, B5): the quality-flagged bronze frame that build_silver_trips really reuses - one
       window-based dedupe (a shuffle) feeding three consumers (silver output, quarantine output, DQ report).
       Without a cache every consumer recomputes the dedupe.
    2. scan-only reuse (informational): five aggregations over silver Parquet. Recomputing is a cheap, column-pruned
       scan, so caching the *wide* table is slower than not caching; caching only the needed columns is the fair
       comparison. This was the original B5 benchmark, which cached all columns and so read ~34x more data.
    """
    months = cfg.section("datasets")["medium"]
    bronze = spark.read.parquet(str(cfg.path("bronze") / "trips")).where(F.col("source_month").isin(months))
    quality = cfg.section("quality")

    def pipeline_reuse(cache: bool):
        def run():
            flagged = apply_quality(bronze, quality)
            if cache:
                flagged = flagged.persist(StorageLevel.MEMORY_AND_DISK)
                flagged.count()  # materialise once, as the silver build does
            flagged.where(F.col("reject_reason").isNull()).write.format("noop").mode("overwrite").save()
            flagged.where(F.col("reject_reason").isNotNull()).write.format("noop").mode("overwrite").save()
            dq_report(flagged, months)
            if cache:
                flagged.unpersist()
        return run

    u = run_benchmark(spark, cfg, "e4_cache", "pipeline_uncached", pipeline_reuse(False))
    c = run_benchmark(spark, cfg, "e4_cache", "pipeline_cached", pipeline_reuse(True))
    speedup = median_seconds(u) / median_seconds(c)

    trips = spark.read.parquet(str(cfg.path("silver") / "trips_clean")).where(F.col("source_month").isin(months))

    def queries(df):
        df.groupBy("pickup_zone_id").count().collect()
        df.groupBy("pickup_borough").agg(F.avg("total_amount")).collect()
        df.groupBy("hour_of_day").agg(F.avg("duration_min")).collect()
        df.groupBy("payment_type").count().collect()
        df.groupBy("pickup_borough", "dropoff_borough").count().collect()

    def scan_reuse(mode: str):
        def run():
            base = trips.where("trip_distance > 0.5")
            if mode == "wide":
                base = base.cache()
                base.count()
            elif mode == "narrow":
                base = base.select(*SCAN_QUERY_COLUMNS).cache()
                base.count()
            queries(base)
            if mode != "none":
                base.unpersist()
        return run

    scan = {m: median_seconds(run_benchmark(spark, cfg, "e4_cache", f"scan_{m}", scan_reuse(m)))
            for m in ["none", "wide", "narrow"]}
    record_measurements(cfg, {"batch.cache_speedup": speedup,
                              "batch.cache_speedup_scan_wide": scan["none"] / scan["wide"],
                              "batch.cache_speedup_scan_narrow": scan["none"] / scan["narrow"]},
                        {"scored_scenario": "pipeline_reuse", "scan_median_seconds": scan})
    return {"cache_speedup": speedup, "scan_median_seconds": scan,
            "scan_speedup_wide": scan["none"] / scan["wide"], "scan_speedup_narrow": scan["none"] / scan["narrow"]}


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
