"""Streaming experiments E6-E9 and targets S1-S11.

Each trial: (re)create a Kafka topic, start the stream job as a subprocess with an isolated tag/checkpoint/output,
replay events at a fixed rate, stop, then summarise results/stream/*.csv for that tag.
"""
from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

import numpy as np
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from ..common.config import Config, load_config
from ..common.spark import get_spark
from ..common.utils import get_logger, record_measurements
from ..streaming.producer import MALFORMED_KEY_PREFIX, ensure_topic, replay

log = get_logger(__name__)


def _read(cfg: Config, name: str, tag: str) -> pd.DataFrame:
    p = cfg.path("results") / "stream" / f"{name}.csv"
    if not p.exists():
        return pd.DataFrame()
    df = pd.read_csv(p)
    return df[df["tag"] == tag]


def trigger_seconds(cfg: Config) -> float:
    return float(str(cfg.section("streaming")["trigger"]).split()[0])


def summarize(cfg: Config, tag: str, warmup_s: float | None = None) -> dict:
    warmup_s = cfg.section("benchmark")["stream_warmup_s"] if warmup_s is None else warmup_s
    batches, samples, progress = _read(cfg, "batches", tag), _read(cfg, "latency_samples", tag), _read(cfg, "progress", tag)
    if batches.empty or progress.empty:
        return {}
    start_ms = batches["commit_ms"].min() + warmup_s * 1000
    steady = batches[batches["commit_ms"] >= start_ms]
    lat = samples[samples["batch_id"].isin(steady["batch_id"])]["latency_ms"].to_numpy() / 1000.0

    progress = progress.assign(ts=pd.to_datetime(progress["timestamp"], utc=True))
    progress = progress[progress["ts"] >= pd.to_datetime(start_ms, unit="ms", utc=True)]
    active = progress[progress["num_input_rows"] > 0]
    trig_ms = trigger_seconds(cfg) * 1000
    # span of batches that received input; the idle drain after the producer stops is not processing time
    elapsed = (active["ts"].max() - active["ts"].min()).total_seconds() if len(active) > 1 else np.nan
    rows_in_span = active["num_input_rows"].iloc[1:].sum() if len(active) > 1 else 0
    lag = progress["lag_events"].dropna().to_numpy()
    third = max(len(lag) // 3, 1)
    return {
        "tag": tag,
        "latency_p50_s": float(np.percentile(lat, 50)) if len(lat) else None,
        "latency_p95_s": float(np.percentile(lat, 95)) if len(lat) else None,
        "latency_p99_s": float(np.percentile(lat, 99)) if len(lat) else None,
        "batches_within_trigger_pct": 100.0 * float((active["trigger_ms"] <= trig_ms).mean()) if len(active) else None,
        "processed_eps": float(rows_in_span / elapsed) if elapsed and elapsed > 0 else None,
        "mean_input_eps": float(active["input_rows_per_s"].mean()) if len(active) else None,
        "lag_p95_events": float(np.percentile(lag, 95)) if len(lag) else None,
        "lag_growth_events": float(lag[-third:].mean() - lag[:third].mean()) if len(lag) else None,
        "batches": int(len(active)),
    }


class Trial:
    """Runs the stream job in a subprocess with isolated topic/checkpoint/outputs for one tag."""

    def __init__(self, cfg: Config, tag: str, partitions: int):
        self.tag, self.partitions = tag, partitions
        root = cfg.path("bench") / "stream" / tag
        self.topic = f"mp-bench-{tag}"
        self.env = {**os.environ, "MP__KAFKA__TOPIC": self.topic,
                    "MP__PATHS__STREAM_OUT": str(root / "metrics"), "MP__PATHS__STREAM_DLQ": str(root / "dlq"),
                    "MP__STREAMING__STARTING_OFFSETS": "earliest", "MP__SPARK__UI_PORT": "4050"}
        self.cfg = cfg.with_overrides({"kafka": {"topic": self.topic},
                                       "paths": {"stream_out": str(root / "metrics"), "stream_dlq": str(root / "dlq")}})
        self.proc: subprocess.Popen | None = None

    def start_job(self, reset: bool) -> None:
        cmd = [sys.executable, "-m", "mobilitypulse.streaming.stream_job", "--tag", self.tag]
        self.proc = subprocess.Popen(cmd + (["--reset"] if reset else []), env=self.env)

    def wait_ready(self, timeout: float = 300, after_ms: float = 0) -> float:
        """Block until the job reports a progress row (newer than after_ms); returns that time in ms."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            p = _read(self.cfg, "progress", self.tag)
            if not p.empty:
                ts = pd.to_datetime(p["timestamp"], utc=True).astype("int64") // 10**6
                newer = ts[ts > after_ms]
                if len(newer):
                    return float(newer.min())
            if self.proc and self.proc.poll() is not None:
                raise RuntimeError(f"stream job exited early with code {self.proc.returncode}")
            time.sleep(2)
        raise TimeoutError("stream job did not become ready")

    def stop_job(self, kill: bool = False) -> None:
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGKILL if kill else signal.SIGTERM)
            try:
                self.proc.wait(timeout=90)
            except subprocess.TimeoutExpired:
                self.proc.kill()

    def run(self, rate: float, duration_s: float, drain_s: float = 20, max_events: int = 0) -> dict:
        kcfg = self.cfg.section("kafka")
        ensure_topic(kcfg["bootstrap_servers"], self.topic, self.partitions, recreate=True)
        self.start_job(reset=True)
        try:
            self.wait_ready()
            prod = replay(self.cfg, rate=rate, max_events=max_events, duration_s=duration_s, topic=self.topic, tag=self.tag)
            time.sleep(drain_s)
        finally:
            self.stop_job()
        return {"producer": prod, **summarize(self.cfg, self.tag)}


# ------------------------------------------------------------------ correctness checks
def latest_windows(spark, path: Path, tag: str):
    df = spark.read.parquet(str(path)).where(F.col("run_tag") == tag)
    w = Window.partitionBy("zone_id", "window_start").orderBy(F.col("batch_id").desc())
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def reconcile_with_batch(cfg: Config, trial_cfg: Config, tag: str) -> float:
    """% of (zone, closed hourly window) cells where the stream count equals the batch zone_hour_demand count."""
    spark = get_spark(cfg, "stream-reconcile")
    hourly = latest_windows(spark, trial_cfg.path("stream_out"), tag).where(F.minute("window_start") == 0)
    bounds = hourly.agg(F.min("window_start"), F.max("window_start")).first()
    if bounds[0] is None:
        return 0.0
    lo, hi = bounds[0], bounds[1]  # the newest hour may be incomplete
    stream = hourly.where(F.col("window_start") < F.lit(hi)).select("zone_id", "window_start",
                                                                       F.col("trip_count").alias("stream_count"))
    batch = spark.read.parquet(str(cfg.path("silver") / "zone_hour_demand")) \
        .where((F.col("window_start") >= F.lit(lo)) & (F.col("window_start") < F.lit(hi))) \
        .select("zone_id", "window_start", F.col("trip_count").alias("batch_count"))
    joined = stream.join(batch, ["zone_id", "window_start"], "full_outer").fillna(0, ["stream_count", "batch_count"]) \
        .where("stream_count > 0 OR batch_count > 0")
    r = joined.agg(F.count(F.lit(1)).alias("n"), F.sum((F.col("stream_count") == F.col("batch_count")).cast("long")).alias("eq")).first()
    return 100.0 * (r["eq"] or 0) / max(r["n"], 1)


def dlq_accounting(cfg: Config, trial_cfg: Config, tag: str, producer: dict) -> dict:
    spark = get_spark(cfg, "stream-reconcile")
    dlq = spark.read.parquet(str(trial_cfg.path("stream_dlq"))).where(F.col("run_tag") == tag)
    injected = dlq.where(F.col("kafka_key").startswith(MALFORMED_KEY_PREFIX)).select("kafka_partition", "kafka_offset").distinct().count()
    dlq_total = dlq.select("kafka_partition", "kafka_offset").distinct().count()
    hourly = latest_windows(spark, trial_cfg.path("stream_out"), tag).where(F.minute("window_start") == 0)
    processed = hourly.agg(F.sum("trip_count")).first()[0] or 0
    unaccounted = producer["events_sent"] - processed - dlq_total - producer.get("duplicates_sent", 0)
    return {"dlq_routed_pct": 100.0 * injected / max(producer["malformed_sent"], 1),
            "events_unaccounted": abs(int(unaccounted)), "dlq_total": dlq_total, "processed": int(processed)}


# ------------------------------------------------------------------ experiments
def main_trial(cfg: Config) -> dict:
    b = cfg.section("benchmark")
    trial = Trial(cfg, "main", cfg.section("kafka")["partitions"])
    res = trial.run(rate=cfg.section("producer")["events_per_second"], duration_s=b["stream_trial_s"])
    acct = dlq_accounting(cfg, trial.cfg, "main", res["producer"])
    agreement = reconcile_with_batch(cfg, trial.cfg, "main")
    record_measurements(cfg, {
        "stream.latency_p50_s": res.get("latency_p50_s"), "stream.latency_p95_s": res.get("latency_p95_s"),
        "stream.latency_p99_s": res.get("latency_p99_s"),
        "stream.batches_within_trigger_pct": res.get("batches_within_trigger_pct"),
        "stream.lag_p95_events": res.get("lag_p95_events"), "stream.dlq_routed_pct": acct["dlq_routed_pct"],
        "stream.events_unaccounted": acct["events_unaccounted"], "stream.batch_agreement_pct": agreement,
    }, {"trial": "main"})
    return {**res, **acct, "batch_agreement_pct": agreement}


def rate_sweep(cfg: Config) -> dict:
    b = cfg.section("benchmark")
    trig = trigger_seconds(cfg)
    rows, sustained = [], 0.0
    for rate in b["stream_rates"]:
        res = Trial(cfg, f"rate{rate}", cfg.section("kafka")["partitions"]).run(rate, b["stream_sweep_trial_s"])
        res.pop("producer", None)
        ok = (res.get("processed_eps") or 0) >= 0.95 * rate and (res.get("lag_growth_events") or 0) <= rate * trig
        rows.append({"rate": rate, "sustained": ok, **res})
        if ok:
            sustained = max(sustained, rate)
    _save(cfg, "stream_rate_sweep", rows)
    record_measurements(cfg, {"stream.max_sustained_eps": sustained})
    return {"max_sustained_eps": sustained}


def partition_sweep(cfg: Config) -> dict:
    b = cfg.section("benchmark")
    rows, p95 = [], {}
    for parts in b["stream_partitions"]:
        res = Trial(cfg, f"part{parts}", parts).run(b["partition_trial_rate"], b["stream_sweep_trial_s"])
        res.pop("producer", None)
        rows.append({"partitions": parts, **res})
        p95[parts] = res.get("latency_p95_s")
    _save(cfg, "stream_partition_sweep", rows)
    if p95.get(1) and p95.get(4):
        record_measurements(cfg, {"stream.p95_reduction_4v1_pct": 100.0 * (p95[1] - p95[4]) / p95[1]})
    return {"p95_by_partitions": p95}


def recovery_trial(cfg: Config, kill_after_s: float = 60) -> dict:
    trial = Trial(cfg, "recovery", cfg.section("kafka")["partitions"])
    kcfg = trial.cfg.section("kafka")
    ensure_topic(kcfg["bootstrap_servers"], trial.topic, trial.partitions, recreate=True)
    trial.start_job(reset=True)
    trial.wait_ready()
    rate = cfg.section("producer")["events_per_second"]
    producer: dict = {}
    t = threading.Thread(target=lambda: producer.update(
        replay(trial.cfg, rate=rate, max_events=0, duration_s=kill_after_s * 3, topic=trial.topic, tag="recovery")))
    t.start()
    time.sleep(kill_after_s)
    kill_ms = time.time() * 1000
    trial.stop_job(kill=True)
    trial.start_job(reset=False)
    deadline = time.time() + 300
    recovery_s = None
    while time.time() < deadline and recovery_s is None:
        b = _read(trial.cfg, "batches", "recovery")
        after = b[(b["commit_ms"] > kill_ms) & (b["rows_updated"] > 0)]
        if not after.empty:
            recovery_s = (after["commit_ms"].min() - kill_ms) / 1000.0
        time.sleep(1)
    t.join()
    time.sleep(20)
    trial.stop_job()
    agreement = reconcile_with_batch(cfg, trial.cfg, "recovery")
    record_measurements(cfg, {"stream.recovery_s": recovery_s, "stream.recovery_match_pct": agreement},
                        {"kill_after_s": kill_after_s})
    return {"recovery_s": recovery_s, "recovery_match_pct": agreement, "producer": producer}


def _save(cfg: Config, name: str, rows: list[dict]) -> None:
    p = cfg.path("results") / "benchmarks" / f"{name}.csv"
    p.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(p, index=False)


EXPERIMENTS = {"main": main_trial, "rates": rate_sweep, "partitions": partition_sweep, "recovery": recovery_trial}


def main() -> None:
    ap = argparse.ArgumentParser(description="Streaming benchmarks (requires Kafka on localhost:9092)")
    # no `choices`: Python 3.11 argparse rejects an empty nargs='*' positional when choices is set
    ap.add_argument("experiments", nargs="*", help=f"subset of {list(EXPERIMENTS)} (default: all)")
    a = ap.parse_args()
    if unknown := set(a.experiments) - set(EXPERIMENTS):
        ap.error(f"unknown experiments {sorted(unknown)}; choose from {list(EXPERIMENTS)}")
    cfg = load_config()
    for name in a.experiments or list(EXPERIMENTS):
        log.info("=== stream %s ===", name)
        log.info("%s -> %s", name, EXPERIMENTS[name](cfg))


if __name__ == "__main__":
    main()
