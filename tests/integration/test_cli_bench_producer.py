"""Exercises the CLI, the batch benchmark experiments and the Kafka producer (with a fake client) on synthetic data."""
import json
import sys

import pandas as pd
import pytest

from mobilitypulse import cli
from mobilitypulse.benchmark import batch_bench
from mobilitypulse.benchmark.scorecard import build_scorecard
from mobilitypulse.common.utils import load_measurements
from mobilitypulse.streaming import producer

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def bench_env(spark, base_cfg, tmp_path_factory):
    root = tmp_path_factory.mktemp("bench")
    cfg = base_cfg.with_overrides({
        "paths": {"data_dir": str(root / "data"), "results": str(root / "results")},
        "datasets": {"small": ["2024-01"], "medium": ["2024-01"], "large": ["2024-01"], "default": ["2024-01"]},
        "benchmark": {"repetitions": 1, "partition_values": [2, 4]},
    })
    cli.run_many(["sample", "bronze", "silver"], cfg, ["2024-01"])
    return cfg


def test_batch_experiments_record_measurements(spark, bench_env):
    for name in ["e1", "e2", "e3", "e4", "e5"]:
        batch_bench.EXPERIMENTS[name](spark, bench_env)
    m = load_measurements(bench_env)
    for key in ["batch.small_e2e_min", "batch.bronze_to_silver_rps", "batch.scaling_efficiency_min",
                "batch.best_partitions", "batch.broadcast_speedup", "batch.cache_speedup",
                "batch.parquet_vs_csv_speedup"]:
        assert key in m, key
    e3 = pd.read_csv(bench_env.path("results") / "benchmarks" / "e3_join.csv")
    assert set(e3["variant"]) == {"broadcast", "sort_merge"}
    assert spark.conf.get("spark.sql.autoBroadcastJoinThreshold") != "-1"


def test_scorecard_file(bench_env):
    rows = build_scorecard(bench_env)
    text = (bench_env.path("results") / "evaluation_scorecard.md").read_text()
    assert "| B4 |" in text and any(r["status"] in ("PASS", "FAIL") for r in rows)


class FakeProducer:
    sent: list = []

    def __init__(self, conf):
        FakeProducer.sent = []

    def produce(self, topic, key, value):
        FakeProducer.sent.append((topic, key.decode(), value))

    def poll(self, t):
        return 0

    def flush(self, t):
        return 0


def test_producer_replay_with_fake_kafka(bench_env, monkeypatch):
    import confluent_kafka

    monkeypatch.setattr(confluent_kafka, "Producer", FakeProducer)
    monkeypatch.setattr(producer, "ensure_topic", lambda *a, **k: None)
    cfg = bench_env.with_overrides({"producer": {"replay_month": "2024-01", "malformed_fraction": 0.05}})
    summary = producer.replay(cfg, rate=1e9, max_events=2000, tag="unit")
    assert summary["events_sent"] == len(FakeProducer.sent) == 2000 + summary["malformed_sent"]
    assert summary["malformed_sent"] > 0
    keys = [k for _, k, _ in FakeProducer.sent]
    assert sum(k.startswith(producer.MALFORMED_KEY_PREFIX) for k in keys) == summary["malformed_sent"]
    first_valid = json.loads(next(v for _, k, v in FakeProducer.sent if not k.startswith("__")))
    assert first_valid["source_month"] == "2024-01" and "T" in first_valid["pickup_ts"]
    runs = pd.read_csv(cfg.path("results") / "stream" / "producer_runs.csv")
    assert runs.iloc[-1]["tag"] == "unit"


def test_cli_main_single_stage(bench_env, monkeypatch):
    monkeypatch.setenv("MOBILITYPULSE_DATA_DIR", str(bench_env.data_dir))
    monkeypatch.setenv("MP__PATHS__RESULTS", str(bench_env.path("results")))
    monkeypatch.setattr(sys, "argv", ["mp", "--stage", "scorecard"])
    cli.main()
    assert (bench_env.path("results") / "evaluation_scorecard.md").exists()
