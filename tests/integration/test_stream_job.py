"""Runs the real StreamJob (watermark, dedup, sliding windows, foreachBatch sinks, checkpoints) on a file source
standing in for Kafka, then checks the outputs and the latency/progress logs used by the evaluation."""
import json

import pandas as pd
import pytest
from pyspark.sql import functions as F

from mobilitypulse.streaming.producer import make_malformed, to_event
from mobilitypulse.streaming.stream_job import StreamJob

pytestmark = pytest.mark.integration


class FileSourceJob(StreamJob):
    def __init__(self, spark, cfg, tag, src):
        super().__init__(spark, cfg, tag, dlq_to_kafka=False)
        self.src = src

    def source(self):
        schema = "key string, value string, partition int, offset long, timestamp timestamp"
        return self.spark.readStream.schema(schema).option("maxFilesPerTrigger", 1).json(str(self.src))


def _write_batch(src, name, events):
    lines = [json.dumps({"key": k, "value": v, "partition": 0, "offset": i, "timestamp": "2026-11-07T10:00:00"})
             for i, (k, v) in events]
    (src / name).write_text("\n".join(lines), encoding="utf-8")


def test_stream_job_end_to_end(spark, base_cfg, tmp_path):
    cfg = base_cfg.with_overrides({"paths": {"stream_out": str(tmp_path / "out"), "stream_dlq": str(tmp_path / "dlq"),
                                             "checkpoints": str(tmp_path / "chk"), "results": str(tmp_path / "res"),
                                             "bronze": str(tmp_path / "bronze")}})
    zones = cfg.path("bronze") / "taxi_zones"
    spark.createDataFrame([(161, "Manhattan", "Midtown"), (237, "Manhattan", "UES")],
                          "zone_id int, borough string, zone_name string").write.mode("overwrite").parquet(str(zones))
    src = tmp_path / "src"
    src.mkdir()
    base = {"record_id": "x", "pickup_ts": pd.Timestamp("2024-01-15 08:04:00"), "dropoff_ts": pd.Timestamp("2024-01-15 08:20:00"),
            "pickup_zone_id": 161, "dropoff_zone_id": 237, "trip_distance": 2.0, "total_amount": 15.0,
            "passenger_count": 1.0, "source_month": "2024-01"}
    good = [("161", json.dumps(to_event({**base, "record_id": f"r{i}"}, 1))) for i in range(5)]
    dup = [("161", json.dumps(to_event({**base, "record_id": "r0"}, 1)))]
    bad = [("__malformed__:bad_zone", make_malformed(to_event(base, 1), "bad_zone").decode()),
           ("__malformed__:invalid_json", make_malformed(to_event(base, 1), "invalid_json").decode())]
    _write_batch(src, "b1.json", list(enumerate(good + dup + bad)))

    job = FileSourceJob(spark, cfg, "t", src).start()
    job.metrics_q.processAllAvailable()
    job.dlq_q.processAllAvailable()
    job.metrics_q.stop()
    job.dlq_q.stop()

    out = spark.read.parquet(str(tmp_path / "out"))
    hourly = out.where(F.minute("window_start") == 0).collect()
    assert len(hourly) == 1 and hourly[0]["trip_count"] == 5 and hourly[0]["borough"] == "Manhattan"
    assert out.select("window_start").distinct().count() == 4
    dlq = spark.read.parquet(str(tmp_path / "dlq"))
    assert sorted(r["error_reason"] for r in dlq.collect()) == ["INVALID_ZONE", "PARSE_ERROR"]
    batches = pd.read_csv(tmp_path / "res" / "stream" / "batches.csv")
    assert batches["rows_updated"].sum() == 4 and (batches["tag"] == "t").all()
    assert (pd.read_csv(tmp_path / "res" / "stream" / "latency_samples.csv")["latency_ms"] > 0).all()
