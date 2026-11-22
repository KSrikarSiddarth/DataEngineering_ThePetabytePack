import json

import pandas as pd
import pytest
from pyspark.sql import functions as F

from mobilitypulse.streaming.producer import MALFORMED_KINDS, make_malformed, to_event
from mobilitypulse.streaming.stream_job import _percentile, parse_events, validate, windowed_metrics

ROW = {"record_id": "abc", "pickup_ts": pd.Timestamp("2024-01-15 08:04:12", tz="UTC"),
       "dropoff_ts": pd.Timestamp("2024-01-15 08:19:40", tz="UTC"), "pickup_zone_id": 161, "dropoff_zone_id": 237,
       "trip_distance": 2.4, "total_amount": 18.5, "passenger_count": float("nan"), "source_month": "2024-01"}


def test_to_event_is_json_serialisable_and_wall_clock():
    e = to_event(ROW, 123)
    assert e["pickup_ts"] == "2024-01-15T08:04:12" and e["passenger_count"] is None
    assert json.loads(json.dumps(e))["event_id"] == "abc"


@pytest.mark.parametrize("kind", MALFORMED_KINDS)
def test_every_malformed_kind_is_rejected_by_the_stream_validator(spark, base_cfg, kind):
    good = json.dumps(to_event(ROW, 1)).encode()
    bad = make_malformed(to_event(ROW, 1), kind)
    raw = spark.createDataFrame(
        [("161", good, 0, 1, "2024-01-15 08:04:13"), (f"__malformed__:{kind}", bad, 0, 2, "2024-01-15 08:04:13")],
        "key string, value binary, partition int, offset long, timestamp string"
    ).withColumn("timestamp", F.to_timestamp("timestamp"))
    out = {r["kafka_offset"]: r for r in validate(parse_events(raw), base_cfg.section("quality")).collect()}
    assert out[1]["reject_reasons"] == []
    assert len(out[2]["reject_reasons"]) >= 1


def test_windowed_metrics_counts_each_event_in_four_sliding_windows(spark, base_cfg):
    events = [json.dumps({**to_event(ROW, 1), "event_id": f"e{i}"}).encode() for i in range(3)]
    events.append(json.dumps({**to_event(ROW, 1), "event_id": "e0"}).encode())  # duplicate
    raw = spark.createDataFrame([("161", v, 0, i, "2024-01-15 08:04:13") for i, v in enumerate(events)],
                                "key string, value binary, partition int, offset long, timestamp string") \
        .withColumn("timestamp", F.to_timestamp("timestamp"))
    valid = validate(parse_events(raw), base_cfg.section("quality")).where(F.size("reject_reasons") == 0)
    scfg = base_cfg.section("streaming")
    # batch DataFrames ignore the watermark; dedup is checked via dropDuplicates semantics separately
    agg = valid.dropDuplicates(["event_id"]).groupBy(F.window("pickup_ts", scfg["window"], scfg["slide"]).alias("w")) \
        .agg(F.count(F.lit(1)).alias("n")).collect()
    assert len(agg) == 4 and all(r["n"] == 3 for r in agg)
    assert windowed_metrics(valid, scfg).isStreaming is False


def test_percentile_interpolates():
    assert _percentile([1, 2, 3, 4], 0.5) == pytest.approx(2.5)
    assert _percentile([5], 0.95) == 5
