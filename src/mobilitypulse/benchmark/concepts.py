"""Spark internals evidence pack: small experiments that show *why* Spark behaves as it does.

Each experiment writes its evidence (physical plans, CSV rows) under results/concepts/.
  c1  lazy evaluation and the DAG   - transformations build a plan, only an action runs jobs
  c2  narrow vs wide                - a shuffle splits a job into stages; narrow steps stay in one stage
  c3  data skew                     - one hot key makes one task slow; AQE skew join and salting fix it
  c4  silver scan vs gold lookup    - why a precomputed gold table is cheaper than re-aggregating silver
"""
from __future__ import annotations

import argparse
import time
from pathlib import Path

import pandas as pd
import requests
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ..common.config import Config, load_config
from ..common.spark import get_spark
from ..common.utils import Timer, get_logger

log = get_logger(__name__)


def out_dir(cfg: Config) -> Path:
    d = cfg.path("results") / "concepts"
    d.mkdir(parents=True, exist_ok=True)
    return d


def save_plan(cfg: Config, name: str, df: DataFrame, mode: str = "formatted") -> Path:
    """Write df.explain(mode) to results/concepts/<name>.txt and return the path."""
    path = out_dir(cfg) / f"{name}.txt"
    path.write_text(df._jdf.queryExecution().explainString(
        spark_explain_mode(df.sparkSession, mode)), encoding="utf-8")
    return path


def spark_explain_mode(spark: SparkSession, mode: str):
    return spark._jvm.org.apache.spark.sql.execution.ExplainMode.fromString(mode)


def write_rows(cfg: Config, name: str, rows: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    df.to_csv(out_dir(cfg) / f"{name}.csv", index=False)
    return df


def jobs_in_group(spark: SparkSession, group: str) -> int:
    return len(spark.sparkContext.statusTracker().getJobIdsForGroup(group))


# ------------------------------------------------------------------ C1 lazy evaluation and the DAG
def c1_lazy(spark: SparkSession, cfg: Config) -> dict:
    """Show that transformations only build a plan; jobs run when an action is called."""
    sc = spark.sparkContext
    trips = spark.read.parquet(str(cfg.path("silver") / "trips_clean"))
    zones = spark.read.parquet(str(cfg.path("bronze") / "taxi_zones"))
    rows = []

    def step(label: str, kind: str, fn) -> object:
        group = f"c1:{label}:{time.time_ns()}"
        sc.setJobGroup(group, label)
        with Timer() as t:
            result = fn()
        sc.setLocalProperty("spark.jobGroup.id", None)
        time.sleep(0.3)  # let the listener bus register the jobs
        rows.append({"step": label, "kind": kind, "seconds": round(t.seconds, 4),
                     "spark_jobs": jobs_in_group(spark, group)})
        return result

    # five chained transformations: no job should run for any of them
    q = step("1 filter", "transformation", lambda: trips.where("trip_distance > 0.5"))
    q = step("2 withColumn", "transformation",
             lambda: q.withColumn("fare_per_mile", F.col("total_amount") / F.col("trip_distance")))
    q = step("3 join zones", "transformation",
             lambda: q.join(F.broadcast(zones.select(F.col("zone_id").alias("pickup_zone_id"),
                                                     F.col("borough").alias("zone_borough"))),
                            "pickup_zone_id", "left"))
    q = step("4 groupBy", "transformation",
             lambda: q.groupBy("zone_borough", "hour_of_day").agg(F.avg("fare_per_mile").alias("avg_fpm"),
                                                                  F.count(F.lit(1)).alias("trips")))
    q = step("5 orderBy", "transformation", lambda: q.orderBy(F.desc("trips")))

    # explain() is not an action: it compiles the plan but does not execute it
    plan = step("6 explain (plan only)", "plan", lambda: save_plan(cfg, "c1_lazy_plan", q))

    # the action triggers the whole chain
    step("7 count() ACTION", "action", lambda: q.count())
    step("8 collect() ACTION", "action", lambda: q.collect())
    # a second action recomputes everything, since nothing was cached
    step("9 count() again (no cache)", "action", lambda: q.count())

    df = write_rows(cfg, "c1_lazy", rows)
    log.info("C1 lazy evaluation:\n%s", df.to_string(index=False))
    log.info("physical plan saved to %s", plan)
    return {"rows": rows, "plan": str(plan)}


def stage_details(spark: SparkSession, group: str) -> list[dict]:
    """Per-stage metrics (tasks, shuffle, input) for the jobs of one job group, from the Spark UI REST API."""
    sc = spark.sparkContext
    if not sc.uiWebUrl:  # UI disabled (e.g. in tests): no REST API, so no stage metrics
        return []
    base = f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}"
    time.sleep(0.5)  # the listener bus updates the REST view asynchronously
    jobs = [j for j in requests.get(f"{base}/jobs", timeout=10).json() if j.get("jobGroup") == group]
    out = []
    for sid in sorted({s for j in jobs for s in j.get("stageIds", [])}):
        for st in requests.get(f"{base}/stages/{sid}", timeout=10).json():
            if st.get("status") != "COMPLETE":  # skipped stages (reused shuffle output) did no work
                continue
            out.append({"stage_id": sid, "tasks": st.get("numCompleteTasks", 0),
                        "input_mb": round(st.get("inputBytes", 0) / 1e6, 2),
                        "shuffle_write_mb": round(st.get("shuffleWriteBytes", 0) / 1e6, 2),
                        "shuffle_read_mb": round(st.get("shuffleReadBytes", 0) / 1e6, 2)})
    return out


# ------------------------------------------------------------------ C2 narrow vs wide transformations
def c2_narrow_wide(spark: SparkSession, cfg: Config) -> dict:
    """Narrow steps stay inside a partition (one stage); wide steps need a shuffle and split the job into stages.

    Every query is run with a `noop` write instead of count(): count() itself adds a shuffle and would blur the result.
    AQE is switched off so that the stage and task structure is the classic one (tasks = shuffle partitions).
    """
    sc = spark.sparkContext
    trips = spark.read.parquet(str(cfg.path("silver") / "trips_clean"))

    queries = {
        # narrow only: filter + derived column + projection, each row handled independently
        "narrow_only": trips.where("trip_distance > 0.5")
                            .withColumn("fare_per_mile", F.col("total_amount") / F.col("trip_distance"))
                            .select("pickup_zone_id", "fare_per_mile"),
        # wide x1: the real zone-hour pickup aggregation from batch/demand.py
        "wide_groupby": trips.groupBy(F.col("pickup_zone_id").alias("zone_id"), F.col("pickup_hour").alias("window_start"))
                             .agg(F.count(F.lit(1)).alias("trip_count"), F.avg("trip_distance").alias("avg_trip_distance"),
                                  F.avg("duration_min").alias("avg_duration_min"), F.avg("total_amount").alias("avg_fare"),
                                  F.sum("total_amount").alias("total_revenue")),
        # wide x2: groupBy then a global sort (two shuffles)
        "wide_groupby_sort": trips.groupBy("pickup_zone_id", "pickup_hour").count().orderBy(F.desc("count")),
        # wide: repartition is a pure shuffle with no computation
        "wide_repartition": trips.repartition(32, "pickup_zone_id"),
    }
    original_aqe = spark.conf.get("spark.sql.adaptive.enabled")
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    rows = []
    try:
        for name, df in queries.items():
            save_plan(cfg, f"c2_{name}_plan", df)
            group = f"c2:{name}:{time.time_ns()}"
            sc.setJobGroup(group, name)
            with Timer() as t:
                df.write.format("noop").mode("overwrite").save()
            sc.setLocalProperty("spark.jobGroup.id", None)
            stages = stage_details(spark, group)
            exchanges = df._jdf.queryExecution().executedPlan().toString().count("Exchange")
            rows.append({"query": name, "seconds": round(t.seconds, 3), "exchanges_in_plan": exchanges,
                         "stages": len(stages), "tasks": sum(s["tasks"] for s in stages),
                         "shuffle_write_mb": round(sum(s["shuffle_write_mb"] for s in stages), 2),
                         "shuffle_read_mb": round(sum(s["shuffle_read_mb"] for s in stages), 2),
                         "type": "wide" if exchanges else "narrow", "per_stage": stages})
    finally:
        spark.conf.set("spark.sql.adaptive.enabled", original_aqe)

    detail = [{"query": r["query"], "order": i + 1, **s} for r in rows for i, s in enumerate(r["per_stage"])]
    summary = write_rows(cfg, "c2_narrow_wide", [{k: v for k, v in r.items() if k != "per_stage"} for r in rows])
    write_rows(cfg, "c2_narrow_wide_stages", detail)
    log.info("C2 narrow vs wide:\n%s", summary.to_string(index=False))
    return {"rows": rows}


def reduce_stage_task_stats(spark: SparkSession, group: str) -> dict:
    """Task-level spread of the stage that read the most shuffle data (the join/aggregation stage)."""
    sc = spark.sparkContext
    if not sc.uiWebUrl:
        return {}
    base = f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}"
    time.sleep(0.5)
    jobs = [j for j in requests.get(f"{base}/jobs", timeout=10).json() if j.get("jobGroup") == group]
    best = None
    for sid in {s for j in jobs for s in j.get("stageIds", [])}:
        for st in requests.get(f"{base}/stages/{sid}", timeout=10).json():
            if st.get("status") == "COMPLETE" and (best is None or st.get("shuffleReadBytes", 0) > best[1]):
                best = (sid, st.get("shuffleReadBytes", 0), st.get("attemptId", 0))
    if best is None or best[1] == 0:
        return {}
    tasks = requests.get(f"{base}/stages/{best[0]}/{best[2]}/taskList", params={"length": 100000}, timeout=30).json()
    tasks = [t for t in tasks if t.get("status") == "SUCCESS" and t.get("taskMetrics")]
    runtimes = sorted(t["taskMetrics"]["executorRunTime"] for t in tasks)
    reads = sorted(t["taskMetrics"]["shuffleReadMetrics"]["localBytesRead"]
                   + t["taskMetrics"]["shuffleReadMetrics"]["remoteBytesRead"] for t in tasks)
    med = lambda xs: xs[len(xs) // 2]
    return {"join_stage_id": best[0], "join_stage_tasks": len(tasks),
            "task_read_median_mb": round(med(reads) / 1e6, 3), "task_read_max_mb": round(reads[-1] / 1e6, 3),
            "read_skew_ratio": round(reads[-1] / max(med(reads), 1), 1),
            "task_ms_median": med(runtimes), "task_ms_max": runtimes[-1],
            "time_skew_ratio": round(runtimes[-1] / max(med(runtimes), 1), 1)}


# ------------------------------------------------------------------ C3 data skew
def c3_skew(spark: SparkSession, cfg: Config, hot_pct: int = 50, hot_zone: int = 161, copies: int = 10,
            salts: int = 8) -> dict:
    """One hot key makes one task do most of the work. Compare: no handling, AQE skew join, manual salting.

    The sample data is only mildly skewed, so a hot zone is injected on purpose (hot_pct % of trips are remapped
    to hot_zone, deterministically) and a natural-data control is run too. The zone table is small enough to be
    broadcast, which would hide skew, so broadcasting is disabled to force a shuffle (sort-merge) join.
    """
    trips = spark.read.parquet(str(cfg.path("silver") / "trips_clean"))
    zones = spark.read.parquet(str(cfg.path("bronze") / "taxi_zones")).select("zone_id", "borough")
    amplify = F.explode(F.sequence(F.lit(1), F.lit(copies)))  # more rows per key so tasks are measurable

    natural = trips.select("pickup_zone_id", "total_amount").withColumn("copy", amplify).drop("copy")
    hot_bucket = F.pmod(F.xxhash64("pickup_ts", "dropoff_ts", "total_amount", "trip_distance"), F.lit(100))
    skewed = (trips.select(F.when(hot_bucket < hot_pct, F.lit(hot_zone)).otherwise(F.col("pickup_zone_id"))
                           .alias("pickup_zone_id"), "total_amount")
              .withColumn("copy", amplify).drop("copy"))

    # NOTE: the aggregation groups by `borough`, not by the join key. Grouping by zone_id would reuse the join's
    # partitioning, and AQE then refuses to split the skewed partition (it would need an extra shuffle).
    def plain_join(facts):
        return (facts.join(zones, facts.pickup_zone_id == zones.zone_id)
                .groupBy("borough").agg(F.sum("total_amount").alias("revenue")))

    def salted_join(facts):
        f = facts.withColumn("salt", (F.rand(7) * salts).cast("int"))
        z = zones.withColumn("salt", F.explode(F.array(*[F.lit(i) for i in range(salts)])))
        return (f.join(z, (f.pickup_zone_id == z.zone_id) & (f.salt == z.salt))
                .groupBy("borough").agg(F.sum("total_amount").alias("revenue")))

    # key-skew of the data itself (the cause), before any join
    for label, facts in [("natural", natural), ("skewed", skewed)]:
        c = sorted(r["count"] for r in facts.groupBy("pickup_zone_id").count().collect())
        log.info("C3 key distribution [%s]: zones=%d max=%d median=%d max/median=%.1f", label, len(c), c[-1],
                 c[len(c) // 2], c[-1] / c[len(c) // 2])

    variants = [
        ("natural_data_aqe_off", plain_join(natural), False),
        ("skewed_aqe_off", plain_join(skewed), False),
        ("skewed_aqe_skew_join", plain_join(skewed), True),
        ("skewed_manual_salting", salted_join(skewed), False),
    ]
    conf_keys = ["spark.sql.adaptive.enabled", "spark.sql.adaptive.skewJoin.enabled",
                 "spark.sql.adaptive.coalescePartitions.enabled", "spark.sql.autoBroadcastJoinThreshold",
                 "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes",
                 "spark.sql.adaptive.advisoryPartitionSizeInBytes", "spark.sql.join.preferSortMergeJoin"]
    saved = {k: spark.conf.get(k, None) for k in conf_keys}
    sc = spark.sparkContext
    rows = []
    try:
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
        spark.conf.set("spark.sql.join.preferSortMergeJoin", "true")
        spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")
        # thresholds lowered from 256 MB / 64 MB defaults because the sample data is small
        spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", str(512 * 1024))
        spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", str(512 * 1024))
        for name, df, aqe in variants:
            spark.conf.set("spark.sql.adaptive.enabled", str(aqe).lower())
            spark.conf.set("spark.sql.adaptive.skewJoin.enabled", str(aqe).lower())
            group = f"c3:{name}:{time.time_ns()}"
            sc.setJobGroup(group, name)
            with Timer() as t:
                df.write.format("noop").mode("overwrite").save()
            sc.setLocalProperty("spark.jobGroup.id", None)
            rows.append({"variant": name, "aqe_skew_join": aqe, "seconds": round(t.seconds, 3),
                         **reduce_stage_task_stats(spark, group)})
    finally:
        for k, v in saved.items():
            if v is None:  # was never set explicitly: remove our override so the Spark default applies again
                spark.conf.unset(k)
            else:
                spark.conf.set(k, v)
    df = write_rows(cfg, "c3_skew", rows)
    log.info("C3 data skew (hot zone injected: %d%% of trips -> zone %d):\n%s", hot_pct, hot_zone, df.to_string(index=False))
    return {"rows": rows}


# ------------------------------------------------------------------ C4 silver scan vs gold lookup
def c4_silver_vs_gold(spark: SparkSession, cfg: Config, reps: int | None = None) -> dict:
    """Same question answered by scanning/aggregating silver trips vs reading a precomputed gold table."""
    sc = spark.sparkContext
    reps = reps or cfg.section("benchmark")["repetitions"]
    silver = spark.read.parquet(str(cfg.path("silver") / "trips_clean"))
    gold = cfg.path("gold") / "analytics"
    top_daily = spark.read.parquet(str(gold / "top_zones_daily"))
    borough = spark.read.parquet(str(gold / "borough_summary"))
    day = silver.agg(F.max("pickup_date")).first()[0]

    questions = {
        # Q1: ten busiest pickup zones on the latest day
        "top10_zones_one_day": (
            lambda: silver.where(F.col("pickup_date") == F.lit(day)).groupBy("pickup_zone_id").count()
            .orderBy(F.desc("count"), "pickup_zone_id").limit(10).collect(),
            # gold is built on the dense zone x hour grid, so it can list zero-trip zones that silver cannot see
            lambda: top_daily.where((F.col("date") == F.lit(day)) & (F.col("trips") > 0)).orderBy("rank")
            .select("zone_id", "trips").collect(),
            lambda silver_rows, gold_rows: sorted(r["count"] for r in silver_rows) == sorted(r["trips"] for r in gold_rows),
        ),
        # Q2: trips per borough and month
        "trips_by_borough_month": (
            lambda: silver.groupBy("pickup_borough", F.date_format("pickup_ts", "yyyy-MM").alias("month")).count().collect(),
            lambda: borough.select("borough", "month", "trips").collect(),
            lambda silver_rows, gold_rows: (sorted((r["pickup_borough"], r["month"], r["count"]) for r in silver_rows)
                                            == sorted((r["borough"], r["month"], r["trips"]) for r in gold_rows)),
        ),
    }
    from .harness import job_group_metrics  # reads the Spark UI REST API

    rows = []
    for qname, (from_silver, from_gold, same) in questions.items():
        results = {}
        for layer, fn in [("silver_scan", from_silver), ("gold_lookup", from_gold)]:
            results[layer] = fn()  # warm-up: file listing, JIT; not timed
            for rep in range(1, reps + 1):
                group = f"c4:{qname}:{layer}:{rep}:{time.time_ns()}"
                sc.setJobGroup(group, f"{qname} {layer}")
                with Timer() as t:
                    fn()
                sc.setLocalProperty("spark.jobGroup.id", None)
                m = job_group_metrics(spark, group)
                rows.append({"question": qname, "layer": layer, "rep": rep, "seconds": round(t.seconds, 4),
                             "stages": m.get("stages", 0), "tasks": m.get("tasks", 0),
                             "input_mb": round(m.get("input_bytes", 0) / 1e6, 3),
                             "shuffle_mb": round(m.get("shuffle_write_bytes", 0) / 1e6, 3)})
        match = same(results["silver_scan"], results["gold_lookup"])
        for r in rows:
            if r["question"] == qname:
                r["answers_match"] = bool(match)
        if not match:
            log.warning("C4 %s: silver and gold answers differ - the comparison is not like-for-like", qname)

    raw = write_rows(cfg, "c4_silver_vs_gold_runs", rows)
    summary = (raw.groupby(["question", "layer"]).agg(seconds_median=("seconds", "median"), stages=("stages", "median"),
                                                       tasks=("tasks", "median"), input_mb=("input_mb", "median"),
                                                       shuffle_mb=("shuffle_mb", "median"),
                                                       answers_match=("answers_match", "all")).reset_index())
    speed = summary.pivot(index="question", columns="layer", values="seconds_median")
    summary = summary.merge((speed["silver_scan"] / speed["gold_lookup"]).rename("silver_over_gold_time").reset_index(),
                            on="question")
    summary.to_csv(out_dir(cfg) / "c4_silver_vs_gold.csv", index=False)
    log.info("C4 silver scan vs gold lookup (day=%s):\n%s", day, summary.to_string(index=False))
    return {"rows": summary.to_dict("records")}


EXPERIMENTS = {"c1": c1_lazy, "c2": c2_narrow_wide, "c3": c3_skew, "c4": c4_silver_vs_gold}


def main() -> None:
    ap = argparse.ArgumentParser(description="Run the Spark internals evidence pack")
    ap.add_argument("experiments", nargs="*", help=f"subset of {list(EXPERIMENTS)} (default: all)")
    a = ap.parse_args()
    if unknown := set(a.experiments) - set(EXPERIMENTS):
        ap.error(f"unknown experiments {sorted(unknown)}; choose from {list(EXPERIMENTS)}")
    cfg = load_config()
    spark = get_spark(cfg, "concepts")
    for name in a.experiments or list(EXPERIMENTS):
        log.info("=== %s ===", name)
        EXPERIMENTS[name](spark, cfg)
    spark.stop()


if __name__ == "__main__":
    main()
