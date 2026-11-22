"""Spark Structured Streaming job: Kafka -> validate (DLQ) -> watermark + sliding windows -> checkpointed sink.

Two queries share the Kafka source:
  * metrics : valid events -> dedup within watermark -> 1h/15min windows per zone (update mode) -> parquet
  * dlq     : invalid events + reason -> parquet quarantine and Kafka dead-letter topic
Per micro-batch latency samples (sink commit time - Kafka append time of the newest event in each updated
window) and StreamingQueryProgress rows are written to results/stream/*.csv for the evaluation.
"""
from __future__ import annotations

import argparse
import csv
import json
import signal
import threading
import time
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

from ..batch.quality import add_derived, add_reject_reasons
from ..common.config import Config, load_config
from ..common.spark import get_spark
from ..common.storage import rmtree
from ..common.utils import get_logger

log = get_logger(__name__)

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("pickup_ts", StringType()),
    StructField("dropoff_ts", StringType()),
    StructField("pickup_zone_id", LongType()),
    StructField("dropoff_zone_id", LongType()),
    StructField("trip_distance", DoubleType()),
    StructField("total_amount", DoubleType()),
    StructField("passenger_count", DoubleType()),
    StructField("source_month", StringType()),
    StructField("produced_at_ms", LongType()),
])

_csv_lock = threading.Lock()


def append_rows(path: Path, rows: list[dict]) -> None:
    if not rows:
        return
    with _csv_lock:
        path.parent.mkdir(parents=True, exist_ok=True)
        new = not path.exists()
        with open(path, "a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0]))
            if new:
                w.writeheader()
            w.writerows(rows)


def parse_events(raw: DataFrame) -> DataFrame:
    """raw has Kafka columns key, value, partition, offset, timestamp (works for batch DataFrames too)."""
    e = F.from_json(F.col("value").cast("string"), EVENT_SCHEMA)
    return raw.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("payload"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_ts"),
        e.alias("e"),
    ).select(
        "kafka_key", "payload", "kafka_partition", "kafka_offset", "kafka_ts",
        F.col("e.event_id").alias("event_id"),
        F.to_timestamp("e.pickup_ts").alias("pickup_ts"),
        F.to_timestamp("e.dropoff_ts").alias("dropoff_ts"),
        F.col("e.pickup_zone_id").cast("int").alias("pickup_zone_id"),
        F.col("e.dropoff_zone_id").cast("int").alias("dropoff_zone_id"),
        F.col("e.trip_distance").alias("trip_distance"),
        F.col("e.total_amount").alias("total_amount"),
        F.col("e.passenger_count").alias("passenger_count"),
        F.col("e.source_month").alias("source_month"),
        F.col("e.produced_at_ms").alias("produced_at_ms"),
        (F.col("e").isNull() | F.col("e.event_id").isNull()).alias("parse_error"),
    )


def validate(parsed: DataFrame, q: dict) -> DataFrame:
    """Same rules as the batch silver layer (except global dedup, done with dropDuplicatesWithinWatermark)."""
    checked = add_reject_reasons(add_derived(parsed), q)
    return checked.withColumn(
        "reject_reasons", F.when(F.col("parse_error"), F.array(F.lit("PARSE_ERROR"))).otherwise(F.col("reject_reasons")))


def windowed_metrics(valid: DataFrame, scfg: dict) -> DataFrame:
    return (
        valid.withWatermark("pickup_ts", scfg["watermark"])
        .dropDuplicatesWithinWatermark(["event_id"])
        .groupBy(F.window("pickup_ts", scfg["window"], scfg["slide"]).alias("w"), F.col("pickup_zone_id").alias("zone_id"))
        .agg(F.count(F.lit(1)).alias("trip_count"), F.avg("total_amount").alias("avg_fare"),
             F.avg("trip_distance").alias("avg_distance"), F.max("kafka_ts").alias("max_kafka_ts"))
    )


def _percentile(sorted_vals: list[float], p: float) -> float:
    if not sorted_vals:
        return float("nan")
    k = (len(sorted_vals) - 1) * p
    lo, hi = int(k), min(int(k) + 1, len(sorted_vals) - 1)
    return sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * (k - lo)


class StreamJob:
    def __init__(self, spark: SparkSession, cfg: Config, tag: str, dlq_to_kafka: bool = True):
        self.spark, self.cfg, self.tag, self.dlq_to_kafka = spark, cfg, tag, dlq_to_kafka
        self.scfg, self.kcfg = cfg.section("streaming"), cfg.section("kafka")
        self.results = cfg.path("results") / "stream"
        self.checkpoint = cfg.path("checkpoints") / tag
        self.zones = spark.read.parquet(str(cfg.path("bronze") / "taxi_zones")).select("zone_id", "borough", "zone_name")
        self.zones.cache().count()

    def source(self) -> DataFrame:
        return (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.kcfg["bootstrap_servers"])
            .option("subscribe", self.kcfg["topic"])
            .option("startingOffsets", self.scfg["starting_offsets"])
            .option("maxOffsetsPerTrigger", str(self.scfg["max_offsets_per_trigger"]))
            .option("failOnDataLoss", "false")
            .load()
        )

    # ------------------------------------------------------------ sinks
    def write_metrics(self, batch_df: DataFrame, batch_id: int) -> None:
        out = (
            batch_df.select(F.col("w.start").alias("window_start"), F.col("w.end").alias("window_end"), "zone_id",
                            "trip_count", "avg_fare", "avg_distance", "max_kafka_ts")
            .join(F.broadcast(self.zones), "zone_id", "left")
            .withColumn("batch_id", F.lit(batch_id).cast("long"))
            .withColumn("run_tag", F.lit(self.tag))
            .withColumn("updated_at", F.current_timestamp())
        ).persist()
        out.write.mode("append").parquet(str(self.cfg.path("stream_out")))
        commit_ms = time.time() * 1000
        kafka_ms = [r[0] for r in out.select(F.unix_millis("max_kafka_ts")).collect() if r[0] is not None]
        out.unpersist()
        lat = sorted(commit_ms - k for k in kafka_ms)
        append_rows(self.results / "latency_samples.csv",
                    [{"tag": self.tag, "batch_id": batch_id, "latency_ms": round(v, 1)} for v in lat])
        append_rows(self.results / "batches.csv", [{
            "tag": self.tag, "batch_id": batch_id, "commit_ms": int(commit_ms), "rows_updated": len(lat),
            "lat_p50_ms": round(_percentile(lat, 0.5), 1), "lat_p95_ms": round(_percentile(lat, 0.95), 1),
            "lat_max_ms": round(lat[-1], 1) if lat else None}])

    def write_dlq(self, batch_df: DataFrame, batch_id: int) -> None:
        rows = batch_df.select(
            "kafka_key", "payload", F.element_at("reject_reasons", 1).alias("error_reason"),
            "kafka_partition", "kafka_offset", "kafka_ts", F.lit(batch_id).cast("long").alias("batch_id"),
            F.lit(self.tag).alias("run_tag"), F.current_timestamp().alias("detected_at")).persist()
        rows.write.mode("append").parquet(str(self.cfg.path("stream_dlq")))
        if self.dlq_to_kafka:
            rows.select(F.col("kafka_key").alias("key"),
                        F.to_json(F.struct("payload", "error_reason", "kafka_partition", "kafka_offset",
                                           "detected_at")).alias("value")) \
                .write.format("kafka").option("kafka.bootstrap.servers", self.kcfg["bootstrap_servers"]) \
                .option("topic", self.kcfg["dlq_topic"]).save()
        rows.unpersist()

    # ------------------------------------------------------------ run
    def start(self):
        checked = validate(parse_events(self.source()), self.cfg.section("quality"))
        valid = checked.where(F.size("reject_reasons") == 0)
        invalid = checked.where(F.size("reject_reasons") > 0)
        trigger = {"processingTime": self.scfg["trigger"]}
        self.metrics_q = (
            windowed_metrics(valid, self.scfg).writeStream.queryName(f"metrics-{self.tag}")
            .outputMode("update").foreachBatch(self.write_metrics).trigger(**trigger)
            .option("checkpointLocation", str(self.checkpoint / "metrics")).start()
        )
        self.dlq_q = (
            invalid.writeStream.queryName(f"dlq-{self.tag}").foreachBatch(self.write_dlq).trigger(**trigger)
            .option("checkpointLocation", str(self.checkpoint / "dlq")).start()
        )
        return self

    def progress_row(self, p: dict) -> dict:
        src = (p.get("sources") or [{}])[0]
        end, latest = src.get("endOffset"), src.get("latestOffset")
        end = json.loads(end) if isinstance(end, str) else end
        latest = json.loads(latest) if isinstance(latest, str) else latest
        lag = None
        if isinstance(end, dict) and isinstance(latest, dict):
            lag = sum(int(off) - int(end.get(t, {}).get(part, 0))
                      for t, parts in latest.items() for part, off in parts.items())
        dur = p.get("durationMs", {})
        return {"tag": self.tag, "batch_id": p["batchId"], "timestamp": p["timestamp"],
                "num_input_rows": p.get("numInputRows"), "input_rows_per_s": p.get("inputRowsPerSecond"),
                "processed_rows_per_s": p.get("processedRowsPerSecond"),
                "trigger_ms": dur.get("triggerExecution"), "add_batch_ms": dur.get("addBatch"), "lag_events": lag}

    def monitor(self, duration_s: float | None = None) -> None:
        stop = threading.Event()
        # SIGBREAK: Windows Ctrl+Break / CTRL_BREAK_EVENT sent by the stream benchmark for a graceful stop
        for sig in (signal.SIGINT, signal.SIGTERM, getattr(signal, "SIGBREAK", None)):
            if sig is not None:
                signal.signal(sig, lambda *_: stop.set())
        seen: set[int] = set()
        t0 = time.time()
        while not stop.is_set() and self.metrics_q.isActive and self.dlq_q.isActive:
            time.sleep(1)
            rows = [self.progress_row(p) for p in self.metrics_q.recentProgress if p["batchId"] not in seen]
            rows = [r for r in rows if r["num_input_rows"] is not None]
            seen.update(r["batch_id"] for r in rows)
            append_rows(self.results / "progress.csv", rows)
            if duration_s and time.time() - t0 >= duration_s:
                break
        for q in (self.metrics_q, self.dlq_q):
            if q.isActive:
                q.stop()
        for q in (self.metrics_q, self.dlq_q):
            if q.exception():
                raise RuntimeError(f"query {q.name} failed: {q.exception()}")


def reset_outputs(cfg: Config, tag: str) -> None:
    for p in (cfg.path("checkpoints") / tag, cfg.path("stream_out"), cfg.path("stream_dlq")):
        if p.exists():
            rmtree(p)
    # batch ids restart after a reset, so stale rows for the tag would be mixed into the new run's metrics
    for name in ("batches", "latency_samples", "progress"):
        path = cfg.path("results") / "stream" / f"{name}.csv"
        if path.exists():
            with open(path, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                fields, rows = reader.fieldnames, [r for r in reader if r["tag"] != tag]
            with open(path, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=fields)
                w.writeheader()
                w.writerows(rows)


def main() -> None:
    ap = argparse.ArgumentParser(description="MobilityPulse Structured Streaming job")
    ap.add_argument("--duration", type=float, help="stop after N seconds (default: run until Ctrl+C)")
    ap.add_argument("--tag", help="run tag (separates checkpoints and result rows)")
    ap.add_argument("--reset", action="store_true", help="delete this tag's checkpoint and the stream outputs first")
    a = ap.parse_args()
    cfg = load_config()
    tag = a.tag or cfg.section("streaming").get("run_tag", "default")
    if a.reset:
        reset_outputs(cfg, tag)
    spark = get_spark(cfg, f"stream-{tag}", with_kafka=True, conf={
        "spark.sql.shuffle.partitions": cfg.section("streaming").get("shuffle_partitions", 8)})
    job = StreamJob(spark, cfg, tag).start()
    log.info("streaming started (tag=%s, topic=%s); Spark UI %s", tag, cfg.section("kafka")["topic"],
             spark.sparkContext.uiWebUrl)
    job.monitor(a.duration)


if __name__ == "__main__":
    main()
