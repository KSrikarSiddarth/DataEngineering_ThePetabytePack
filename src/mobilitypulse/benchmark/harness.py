"""Benchmark harness: repeated timed runs + Spark shuffle/spill/task metrics from the Spark UI REST API."""
from __future__ import annotations

import statistics
import time
from pathlib import Path
from typing import Callable

import pandas as pd
import requests
from pyspark.sql import SparkSession

from ..common.config import Config
from ..common.utils import Timer, get_logger

log = get_logger(__name__)


def job_group_metrics(spark: SparkSession, group: str) -> dict:
    """Sum stage metrics for all jobs tagged with `group` (empty dict if the UI is disabled)."""
    sc = spark.sparkContext
    url = sc.uiWebUrl
    if not url:
        return {}
    base = f"{url}/api/v1/applications/{sc.applicationId}"
    try:
        time.sleep(0.5)  # the listener bus updates the REST view asynchronously
        jobs = [j for j in requests.get(f"{base}/jobs", timeout=10).json() if j.get("jobGroup") == group]
        stage_ids = {s for j in jobs for s in j.get("stageIds", [])}
        totals = {"jobs": len(jobs), "stages": 0, "tasks": 0, "shuffle_read_bytes": 0, "shuffle_write_bytes": 0,
                  "spill_bytes": 0, "input_bytes": 0}
        for sid in stage_ids:
            for attempt in requests.get(f"{base}/stages/{sid}", timeout=10).json():
                if attempt.get("status") != "COMPLETE":
                    continue
                totals["stages"] += 1
                totals["tasks"] += attempt.get("numCompleteTasks", 0)
                totals["shuffle_read_bytes"] += attempt.get("shuffleReadBytes", 0)
                totals["shuffle_write_bytes"] += attempt.get("shuffleWriteBytes", 0)
                totals["spill_bytes"] += attempt.get("memoryBytesSpilled", 0) + attempt.get("diskBytesSpilled", 0)
                totals["input_bytes"] += attempt.get("inputBytes", 0)
        return totals
    except (requests.RequestException, ValueError) as exc:
        log.warning("could not read Spark REST metrics: %s", exc)
        return {}


def run_benchmark(spark: SparkSession, cfg: Config, experiment: str, variant: str, fn: Callable[[], dict | None],
                  reps: int | None = None, extra: dict | None = None) -> pd.DataFrame:
    """Run fn `reps` times; append one row per repetition to results/benchmarks/<experiment>.csv."""
    reps = reps or cfg.section("benchmark")["repetitions"]
    rows = []
    for rep in range(1, reps + 1):
        spark.catalog.clearCache()
        group = f"{experiment}:{variant}:{rep}:{time.time_ns()}"
        spark.sparkContext.setJobGroup(group, f"{experiment} {variant} rep {rep}")
        with Timer() as t:
            out = fn() or {}
        spark.sparkContext.setLocalProperty("spark.jobGroup.id", None)
        row = {"experiment": experiment, "variant": variant, "rep": rep, "seconds": round(t.seconds, 3),
               **(extra or {}), **out, **job_group_metrics(spark, group)}
        rows.append(row)
        log.info("%s/%s rep %d: %.2fs", experiment, variant, rep, t.seconds)
    df = pd.DataFrame(rows)
    path = cfg.path("results") / "benchmarks" / f"{experiment}.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, mode="a", header=not path.exists(), index=False)
    return df


def median_seconds(df: pd.DataFrame) -> float:
    return float(statistics.median(df["seconds"]))


def load_results(cfg: Config, experiment: str) -> pd.DataFrame:
    path: Path = cfg.path("results") / "benchmarks" / f"{experiment}.csv"
    return pd.read_csv(path) if path.exists() else pd.DataFrame()
