"""M3 tests: the Spark transformations on batch DataFrames (same code as the streaming job)."""
import csv
import json
import os
import random
import shutil
import sys
from collections import Counter, defaultdict
from datetime import datetime

import pytest
import yaml

pyspark = pytest.importorskip("pyspark")
if shutil.which("java") is None:
    pytest.skip("Java not found on PATH (Spark needs Java 8, 11 or 17)", allow_module_level=True)

from conftest import ROOT  # noqa: E402
from config import load_settings  # noqa: E402
from producer import NdjsonSink, run  # noqa: E402
from transform import CLEAN_FIELDS, INVALID_FIELDS, process  # noqa: E402

DS = yaml.safe_load((ROOT / "producer" / "datasets.yaml").read_text(encoding="utf-8"))
SPECS, NORMS = DS["datasets"], DS["normalize"]
CFG = yaml.safe_load((ROOT / "stream-config.yaml").read_text(encoding="utf-8"))
VCFG = CFG["processing"]["validation"]
DATA = ROOT / "data"
RAW_COLS = ["topic", "partition", "offset", "value"]


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession
    # A machine-wide SPARK_HOME (e.g. another Spark version) would override the pip-installed
    # pyspark and break the JVM/Python match, so point both at this environment for the test run.
    os.environ["SPARK_HOME"] = os.path.dirname(pyspark.__file__)
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    session = (SparkSession.builder.master("local[2]").appName("m3-tests")
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.shuffle.partitions", "2")
               .config("spark.ui.enabled", "false")
               .config("spark.driver.bindAddress", "127.0.0.1")
               .getOrCreate())
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def zones(spark):
    path = DATA / "taxi_zones" / "taxi_zone_lookup.csv"
    if not path.exists():
        pytest.skip("zone lookup not in data/taxi_zones/")
    with open(path, newline="", encoding="utf-8") as fh:
        rows = [(r["LocationID"], r["Borough"], r["Zone"], r["service_zone"]) for r in csv.DictReader(fh)]
    return spark.createDataFrame(rows, ["LocationID", "Borough", "Zone", "service_zone"])


def msg(dataset, **over):
    """A valid JSON message for `dataset`, with overrides; unspecified columns are null."""
    spec = SPECS[dataset]
    row = {c: None for c in spec["columns"]}
    row.update({
        "yellow": dict(tpep_pickup_datetime="2026-01-10T10:00:00", tpep_dropoff_datetime="2026-01-10T10:20:00",
                       PULocationID=161, DOLocationID=237, trip_distance=3.2, fare_amount=15.0,
                       tip_amount=3.0, total_amount=21.0, passenger_count=1, payment_type=1),
        "green": dict(lpep_pickup_datetime="2026-01-10T10:00:00", lpep_dropoff_datetime="2026-01-10T10:20:00",
                      PULocationID=74, DOLocationID=75, trip_distance=1.1, fare_amount=8.0,
                      tip_amount=1.0, total_amount=10.0, passenger_count=1, payment_type=2),
        "fhv": dict(dispatching_base_num="B00013", pickup_datetime="2026-01-10T10:00:00",
                    dropOff_datetime="2026-01-10T10:30:00", PUlocationID=None, DOlocationID=100),
        "hvfhv": dict(hvfhs_license_num="HV0003", pickup_datetime="2026-01-10T10:00:00",
                      dropoff_datetime="2026-01-10T10:25:00", PULocationID=132, DOLocationID=230,
                      trip_miles=4.5, base_passenger_fare=22.5, tips=2.0, driver_pay=17.0),
    }[dataset])
    row.update(over)
    return json.dumps(row)


def build_raw(spark, items):
    """items: [(dataset, value)] -> Kafka-shaped DataFrame; offset = position, so rows can be tracked."""
    rows = [(SPECS[ds]["topic"], 0, i, value) for i, (ds, value) in enumerate(items)]
    return spark.createDataFrame(rows, RAW_COLS)


def run_process(spark, zones, items):
    clean, invalid = process(build_raw(spark, items), SPECS, NORMS, zones, VCFG)
    return {r["kafka_offset"]: r for r in clean.collect()}, {r["kafka_offset"]: r for r in invalid.collect()}


# ---- rule-by-rule -------------------------------------------------------------

CASES = [
    # (name, dataset, value, expected reject reasons ('' = valid))
    ("valid yellow", "yellow", msg("yellow"), ""),
    ("valid green", "green", msg("green"), ""),
    ("valid hvfhv", "hvfhv", msg("hvfhv"), ""),
    ("fhv missing pickup zone is flagged, not rejected", "fhv", msg("fhv"), ""),
    ("dropoff before pickup", "yellow", msg("yellow", tpep_dropoff_datetime="2026-01-10T09:00:00"), "dropoff_before_pickup"),
    ("negative fare", "yellow", msg("yellow", fare_amount=-5.0), "negative_amount"),
    ("negative total", "green", msg("green", total_amount=-1.0), "negative_amount"),
    ("negative hvfhv tip", "hvfhv", msg("hvfhv", tips=-5.0), "negative_amount"),
    ("negative distance", "yellow", msg("yellow", trip_distance=-1.0), "negative_distance"),
    ("negative hvfhv miles", "hvfhv", msg("hvfhv", trip_miles=-2.0), "negative_distance"),
    ("pickup before month", "yellow", msg("yellow", tpep_pickup_datetime="2025-10-01T10:00:00",
                                          tpep_dropoff_datetime="2025-10-01T10:20:00"), "pickup_out_of_range"),
    ("pickup after month", "green", msg("green", lpep_pickup_datetime="2026-03-01T10:00:00",
                                        lpep_dropoff_datetime="2026-03-01T10:20:00"), "pickup_out_of_range"),
    ("one-day tolerance accepts Dec 31", "yellow", msg("yellow", tpep_pickup_datetime="2025-12-31T23:57:29",
                                                       tpep_dropoff_datetime="2026-01-01T00:10:00"), ""),
    ("unknown pickup zone", "yellow", msg("yellow", PULocationID=999), "unknown_pickup_location"),
    ("unknown dropoff zone", "hvfhv", msg("hvfhv", DOLocationID=999), "unknown_dropoff_location"),
    ("null pickup zone rejects yellow", "yellow", msg("yellow", PULocationID=None), "null_pickup_location"),
    ("null dropoff zone rejects hvfhv", "hvfhv", msg("hvfhv", DOLocationID=None), "null_dropoff_location"),
    ("trip longer than 24h", "yellow", msg("yellow", tpep_dropoff_datetime="2026-01-12T10:00:00"), "trip_too_long"),
    ("null pickup time", "green", msg("green", lpep_pickup_datetime=None), "null_pickup_time"),
    ("unexpected extra field is ignored", "yellow", msg("yellow")[:-1] + ',"_unexpected_field":3}', ""),
    ("natural nulls (passenger count, rate code) are fine", "yellow", msg("yellow", passenger_count=None, RatecodeID=None), ""),
    ("truncated JSON", "yellow", msg("yellow")[:60], "malformed_json"),
    ("empty value", "green", "", "malformed_json"),
    ("not JSON at all", "fhv", "hello", "malformed_json"),
]


def test_each_rule(spark, zones):
    items = [(ds, value) for _, ds, value, _ in CASES]
    clean, invalid = run_process(spark, zones, items)
    assert len(clean) + len(invalid) == len(CASES), "every message must land in exactly one output"
    assert not (set(clean) & set(invalid))
    problems = []
    for i, (name, _, _, expected) in enumerate(CASES):
        if expected:
            got = invalid[i]["reject_reason"] if i in invalid else "<clean>"
            if got != expected:
                problems.append(f"{name}: expected {expected!r}, got {got!r}")
        elif i not in clean:
            problems.append(f"{name}: expected clean, got invalid {invalid[i]['reject_reason']!r}")
    assert not problems, "\n".join(problems)


def test_unified_mapping_and_enrichment(spark, zones):
    clean, _ = run_process(spark, zones, [("yellow", msg("yellow")), ("hvfhv", msg("hvfhv")), ("fhv", msg("fhv"))])
    y, h, f = clean[0], clean[1], clean[2]
    assert y["service"] == "yellow" and y["trip_distance"] == 3.2 and y["payment_type"] == 1
    assert y["PUZone"] and y["PUBorough"] and y["DOZone"] and y["DOBorough"]
    assert y["pickup_ts"] == datetime(2026, 1, 10, 10, 0, 0)
    # HVFHV: trip_miles -> trip_distance, base_passenger_fare -> fare_amount, tips -> tip_amount
    assert (h["trip_distance"], h["fare_amount"], h["tip_amount"]) == (4.5, 22.5, 2.0)
    assert h["total_amount"] is None and h["passenger_count"] is None
    assert json.loads(h["extras"])["driver_pay"] == 17.0
    assert "trip_miles" not in json.loads(h["extras"])
    # FHV: no money fields, pickup zone missing -> flagged
    assert f["fare_amount"] is None and f["PUZone"] is None and f["DOZone"]
    assert f["dq_flags"] == "missing_pu_location"


def test_output_columns_match_declared_layout(spark, zones):
    clean, invalid = process(build_raw(spark, [("yellow", msg("yellow")), ("yellow", "x")]),
                             SPECS, NORMS, zones, VCFG)
    assert clean.columns == [n for n, _ in CLEAN_FIELDS]
    assert invalid.columns == [n for n, _ in INVALID_FIELDS]
    assert dict(clean.dtypes)["pickup_ts"] == "timestamp" and dict(clean.dtypes)["kafka_offset"] == "bigint"


def test_malformed_keeps_raw_payload(spark, zones):
    _, invalid = run_process(spark, zones, [("green", "{truncated")])
    row = invalid[0]
    assert row["raw_value"] == "{truncated" and row["service"] == "green" and row["pickup_ts"] is None


# ---- reconciliation on real data with injected faults -----------------------------

EXPECT = {  # injected fault tag -> reject reasons that must include it
    "dropoff_before_pickup": {"dropoff_before_pickup"},
    "negative_amount": {"negative_amount"},
    "negative_distance": {"negative_distance"},
    "pickup_out_of_range": {"pickup_out_of_range"},
    "unknown_location_id": {"unknown_pickup_location", "unknown_dropoff_location"},
}


@pytest.fixture(scope="module")
def real_stream(tmp_path_factory):
    """Run the real producer (dry-run, messy profile, 1000 records/dataset) and collect its messages."""
    if not all((DATA / f"{s['file_prefix']}_2026-01.parquet").exists() for s in SPECS.values()):
        pytest.skip("real data not downloaded")
    tmp = tmp_path_factory.mktemp("stream")
    items, tags, sent = [], {}, {}
    for name in SPECS:
        env = {"DATA_DIR": str(DATA), "MONTHS": "2026-01", "REPLAY_SPEEDUP": "0", "MAX_RECORDS": "1000",
               "QUALITY_PROFILE": "messy", "CONFIG_PATH": str(ROOT / "stream-config.yaml")}
        out = tmp / f"{name}.ndjson"
        sent[name] = run(load_settings(name, env), NdjsonSink(out))["messages_sent"]
        for line in out.read_text(encoding="utf-8").splitlines():
            rec = json.loads(line)
            tags[len(items)] = (name, rec["headers"].get("fault", ""))
            items.append((name, rec["value"]))
    return items, tags, sent


@pytest.fixture(scope="module")
def real_result(spark, zones, real_stream):
    items, tags, sent = real_stream
    clean, invalid = run_process(spark, zones, items)
    return items, tags, sent, clean, invalid


def test_real_data_every_message_accounted_for(real_result):
    items, tags, sent, clean, invalid = real_result
    assert len(clean) + len(invalid) == len(items)
    by_service = Counter(r["service"] for r in clean.values()) + Counter(r["service"] for r in invalid.values())
    assert dict(by_service) == sent


def test_real_data_injected_faults_are_caught(real_result):
    items, tags, sent, clean, invalid = real_result
    misses, seen = [], Counter()
    for off, (ds, fault_header) in tags.items():
        for tag in filter(None, fault_header.split(",")):
            if tag == "malformed_json":
                seen[tag] += 1
                if off not in invalid or invalid[off]["reject_reason"] != "malformed_json":
                    misses.append((ds, off, tag, invalid.get(off, {}) and invalid[off]["reject_reason"]))
            elif tag.startswith("invalid:") and "malformed_json" not in fault_header:
                kind = tag.split(":", 1)[1]
                seen[kind] += 1
                got = set(invalid[off]["reject_reason"].split(",")) if off in invalid else set()
                if not (got & EXPECT[kind]):
                    misses.append((ds, off, kind, sorted(got)))
    assert sum(seen.values()) > 50, f"too few injected faults to be meaningful: {seen}"
    assert not misses, f"{len(misses)} injected faults not rejected, e.g. {misses[:5]}"


def test_real_data_clean_rows_satisfy_every_rule(real_result):
    _, _, _, clean, _ = real_result
    lo, hi = datetime(2025, 12, 31), datetime(2026, 2, 2)
    for r in clean.values():
        assert lo <= r["pickup_ts"] < hi and r["dropoff_ts"] >= r["pickup_ts"]
        assert (r["trip_distance"] or 0) >= 0 and (r["fare_amount"] or 0) >= 0 and (r["total_amount"] or 0) >= 0
        if r["service"] != "fhv":
            assert r["PUZone"] and r["DOZone"]
        elif r["PULocationID"] is not None:
            assert r["PUZone"]


def test_real_data_unfaulted_rows_are_not_wrongly_rejected(real_result):
    """A row with no injected fault may only be rejected for a problem already in the TLC data."""
    items, tags, sent, clean, invalid = real_result
    natural = defaultdict(Counter)
    for off, row in invalid.items():
        ds, fault_header = tags[off]
        if not fault_header or fault_header in {"duplicate", "late", "out_of_order"}:
            natural[ds][row["reject_reason"]] += 1
    allowed = {"pickup_out_of_range", "negative_amount", "negative_distance", "trip_too_long",
               "dropoff_before_pickup", "unknown_pickup_location", "unknown_dropoff_location"}
    for ds, counts in natural.items():
        assert set(counts) <= allowed, f"{ds}: unexpected natural reject reasons {dict(counts)}"
        assert sum(counts.values()) < 0.1 * 1000, f"{ds}: {dict(counts)}"
