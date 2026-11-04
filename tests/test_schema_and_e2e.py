"""Schema tests (one per dataset) against the real parquet files, plus a dry-run end-to-end."""
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from conftest import ROOT, write_green_parquet
from config import load_settings, resolve_rates
from producer import NdjsonSink, run

DATA = ROOT / "data"
DATASETS = ["yellow", "green", "fhv", "hvfhv"]
KIND = {"int": pa.types.is_integer, "float": pa.types.is_floating,
        "string": lambda t: pa.types.is_string(t) or pa.types.is_large_string(t),
        "timestamp": pa.types.is_timestamp}


def real_file(spec):
    return DATA / f"{spec['file_prefix']}_2026-01.parquet"


@pytest.mark.parametrize("name", DATASETS)
def test_spec_matches_real_parquet(specs, name):
    spec = specs[name]
    path = real_file(spec)
    if not path.exists():
        pytest.skip(f"{path.name} not downloaded (run scripts/download_data.py)")
    schema = pq.ParquetFile(path).schema_arrow
    assert list(schema.names) == list(spec["columns"]), "column names/order differ from datasets.yaml"
    for field in schema:
        assert KIND[spec["columns"][field.name]](field.type), f"{field.name}: {field.type}"


@pytest.mark.parametrize("name", DATASETS)
def test_spec_internal_consistency(specs, name):
    spec = specs[name]
    cols = set(spec["columns"])
    for ref in [spec["pickup_col"], spec["dropoff_col"], spec["key_col"], *spec["location_cols"], *spec["amount_cols"]]:
        assert ref in cols, f"{ref} not in columns"
    if spec["distance_col"]:
        assert spec["distance_col"] in cols
    assert spec["columns"][spec["pickup_col"]] == "timestamp"


def test_config_topics_match_dataset_specs(stream_cfg, specs):
    topics = {t["name"] for t in stream_cfg["topics"]}
    assert {s["topic"] for s in specs.values()} <= topics and "taxi.trips.invalid" in topics
    for name, spec in specs.items():
        assert spec["topic"] == f"taxi.{name}.raw"


def test_config_profiles_are_valid(stream_cfg):
    q = stream_cfg["data_quality"]
    for profile in ("clean", "realistic", "messy", "custom"):
        resolve_rates(q, profile)
    assert q["active_profile"] in q["profiles"]
    assert abs(sum(q["invalid_injection"]["types"].values()) - 1.0) < 1e-9
    eligible = q["null_injection"]["eligible_fields"]
    import yaml
    specs = yaml.safe_load((ROOT / "producer" / "datasets.yaml").read_text())["datasets"]
    for name, fields in eligible.items():
        assert set(fields) <= set(specs[name]["columns"]), f"{name}: unknown eligible field"


def _env(tmp_path, **extra):
    env = {"DATA_DIR": str(tmp_path), "MONTHS": "2026-01", "REPLAY_SPEEDUP": "0",
           "CONFIG_PATH": str(ROOT / "stream-config.yaml")}
    env.update(extra)
    return env


def test_dry_run_end_to_end_synthetic(tmp_path):
    write_green_parquet(tmp_path / "green_tripdata_2026-01.parquet", 300)
    settings = load_settings("green", _env(tmp_path, MAX_RECORDS="200", QUALITY_PROFILE="clean"))
    out = tmp_path / "out.ndjson"
    stats = run(settings, NdjsonSink(out))
    lines = [json.loads(l) for l in out.read_text().splitlines()]
    assert stats["source_records"] == stats["messages_sent"] == len(lines) == 200
    assert all(l["key"] is not None for l in lines)
    msgs = [json.loads(l["value"]) for l in lines]
    assert all(list(m) == list(settings.spec["columns"]) for m in msgs)
    times = [m["lpep_pickup_datetime"] for m in msgs]
    assert times == sorted(times)


def test_dry_run_messy_profile_produces_tagged_faults(tmp_path):
    write_green_parquet(tmp_path / "green_tripdata_2026-01.parquet", 400)
    settings = load_settings("green", _env(tmp_path, QUALITY_PROFILE="messy"))
    out = tmp_path / "out.ndjson"
    stats = run(settings, NdjsonSink(out))
    assert stats["messages_sent"] > stats["source_records"]       # duplicates added
    assert {"null_fields", "duplicate"} <= set(stats["faults"])
    lines = [json.loads(l) for l in out.read_text().splitlines()]
    assert any("fault" in l["headers"] for l in lines)


@pytest.mark.parametrize("name", DATASETS)
def test_dry_run_real_data_1000_messages(tmp_path, specs, name):
    if not real_file(specs[name]).exists():
        pytest.skip("real data not downloaded")
    env = _env(tmp_path, DATA_DIR=str(DATA), MAX_RECORDS="1000", QUALITY_PROFILE="clean")
    settings = load_settings(name, env)
    out = tmp_path / f"{name}.ndjson"
    stats = run(settings, NdjsonSink(out))
    lines = [json.loads(l) for l in out.read_text().splitlines()]
    assert stats["messages_sent"] == len(lines) == 1000
    msgs = [json.loads(l["value"]) for l in lines]
    assert all(set(m) == set(settings.spec["columns"]) for m in msgs)
    pickups = [m[settings.spec["pickup_col"]] for m in msgs]
    assert pickups == sorted(pickups)
    key_col = settings.spec["key_col"]
    assert all(l["key"] == str(m[key_col]) for l, m in zip(lines, msgs))
