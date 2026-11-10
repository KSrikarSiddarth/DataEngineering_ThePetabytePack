"""M3/M4 tests: the Spark transformations on batch DataFrames (same code as the streaming job)."""
import csv
import json
import os
import shutil
import sys
from collections import Counter
from datetime import datetime

import pytest
import yaml

pyspark = pytest.importorskip("pyspark")
if shutil.which("java") is None:
    pytest.skip("Java not found on PATH (Spark needs Java 8, 11 or 17)", allow_module_level=True)

from conftest import ROOT  # noqa: E402
from config import load_settings  # noqa: E402
from producer import NdjsonSink, run  # noqa: E402
from transform import DQ_FLAGS, INVALID_FIELDS, SILVER_FIELDS, process, service_type  # noqa: E402

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
    session = (SparkSession.builder.master("local[2]").appName("m4-tests")
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
    silver, invalid = process(build_raw(spark, items), SPECS, NORMS, zones, VCFG)
    return {r["kafka_offset"]: r for r in silver.collect()}, {r["kafka_offset"]: r for r in invalid.collect()}


def flags_of(row):
    return {f for f in DQ_FLAGS if row[f]}


# ---- rule-by-rule -------------------------------------------------------------
# D27: only malformed JSON and a null pickup time are rejected; every other problem stays in Silver
# with a dq_* flag.

OK = ("silver", set())
CASES = [
    # (name, dataset, value, expected outcome: ("silver", {flags that are true}) or ("invalid", reason))
    ("valid yellow", "yellow", msg("yellow"), OK),
    ("valid green", "green", msg("green"), OK),
    ("valid hvfhv", "hvfhv", msg("hvfhv"), OK),
    ("fhv missing pickup zone is flagged", "fhv", msg("fhv"), ("silver", {"dq_null_pu_location"})),
    ("dropoff before pickup", "yellow", msg("yellow", tpep_dropoff_datetime="2026-01-10T09:00:00"),
     ("silver", {"dq_dropoff_before_pickup"})),
    ("negative fare", "yellow", msg("yellow", fare_amount=-5.0), ("silver", {"dq_negative_fare"})),
    ("negative total", "green", msg("green", total_amount=-1.0), ("silver", {"dq_negative_fare"})),
    ("negative hvfhv tip", "hvfhv", msg("hvfhv", tips=-5.0), ("silver", {"dq_negative_fare"})),
    ("negative distance", "yellow", msg("yellow", trip_distance=-1.0), ("silver", {"dq_zero_distance"})),
    ("negative hvfhv miles", "hvfhv", msg("hvfhv", trip_miles=-2.0), ("silver", {"dq_zero_distance"})),
    ("zero distance", "green", msg("green", trip_distance=0.0), ("silver", {"dq_zero_distance"})),
    ("pickup before month", "yellow", msg("yellow", tpep_pickup_datetime="2025-10-01T10:00:00",
                                          tpep_dropoff_datetime="2025-10-01T10:20:00"),
     ("silver", {"dq_pickup_out_of_range"})),
    ("pickup after month", "green", msg("green", lpep_pickup_datetime="2026-03-01T10:00:00",
                                        lpep_dropoff_datetime="2026-03-01T10:20:00"),
     ("silver", {"dq_pickup_out_of_range"})),
    ("one-day tolerance accepts Dec 31", "yellow", msg("yellow", tpep_pickup_datetime="2025-12-31T23:57:29",
                                                       tpep_dropoff_datetime="2026-01-01T00:10:00"), OK),
    ("unknown pickup zone", "yellow", msg("yellow", PULocationID=999), ("silver", {"dq_unmatched_pu_zone"})),
    ("unknown dropoff zone", "hvfhv", msg("hvfhv", DOLocationID=999), ("silver", {"dq_unmatched_do_zone"})),
    ("null pickup zone", "yellow", msg("yellow", PULocationID=None), ("silver", {"dq_null_pu_location"})),
    ("null dropoff zone", "hvfhv", msg("hvfhv", DOLocationID=None), ("silver", {"dq_null_do_location"})),
    ("null dropoff time", "green", msg("green", lpep_dropoff_datetime=None), ("silver", {"dq_null_dropoff_ts"})),
    ("trip longer than 24h", "yellow", msg("yellow", tpep_dropoff_datetime="2026-01-12T10:00:00"),
     ("silver", {"dq_trip_too_long"})),
    ("null pickup time is rejected", "green", msg("green", lpep_pickup_datetime=None),
     ("invalid", "null_pickup_time")),
    ("unexpected extra field is ignored", "yellow", msg("yellow")[:-1] + ',"_unexpected_field":3}', OK),
    ("natural nulls (passenger count, rate code) are fine", "yellow",
     msg("yellow", passenger_count=None, RatecodeID=None), OK),
    ("truncated JSON", "yellow", msg("yellow")[:60], ("invalid", "malformed_json")),
    ("empty value", "green", "", ("invalid", "malformed_json")),
    ("not JSON at all", "fhv", "hello", ("invalid", "malformed_json")),
]


def test_each_rule(spark, zones):
    items = [(ds, value) for _, ds, value, _ in CASES]
    silver, invalid = run_process(spark, zones, items)
    assert len(silver) + len(invalid) == len(CASES), "every message must land in exactly one output"
    assert not (set(silver) & set(invalid))
    problems = []
    for i, (name, _, _, (where, expected)) in enumerate(CASES):
        if where == "invalid":
            got = invalid[i]["reject_reason"] if i in invalid else "<silver>"
            if got != expected:
                problems.append(f"{name}: expected rejected as {expected!r}, got {got!r}")
        elif i not in silver:
            problems.append(f"{name}: expected silver, got rejected {invalid[i]['reject_reason']!r}")
        elif flags_of(silver[i]) != expected:
            problems.append(f"{name}: expected flags {sorted(expected)}, got {sorted(flags_of(silver[i]))}")
    assert not problems, "\n".join(problems)


def test_unified_mapping_and_enrichment(spark, zones):
    silver, _ = run_process(spark, zones, [("yellow", msg("yellow")), ("hvfhv", msg("hvfhv")), ("fhv", msg("fhv"))])
    y, h, f = silver[0], silver[1], silver[2]
    assert y["service_type"] == "yellow" and y["distance_miles"] == 3.2 and y["payment_type"] == 1
    assert y["payment_label"] == "Credit card"
    assert y["pu_zone"] and y["pu_borough"] and y["do_zone"] and y["do_borough"]
    assert y["pickup_ts"] == datetime(2026, 1, 10, 10, 0, 0) and y["pickup_date"].isoformat() == "2026-01-10"
    assert y["pickup_hour"] == 10 and y["duration_sec"] == 1200 and y["base_fare"] == 15.0
    assert y["trip_id"] and len(y["trip_id"]) == 32
    # HVFHV is `fhvhv` in Silver; miles/fare/tip/driver pay/licence are typed columns, not extras
    assert h["service_type"] == "fhvhv" and h["hvfhs_license_num"] == "HV0003"
    assert (h["distance_miles"], h["base_fare"], h["tip_amount"], h["driver_pay"]) == (4.5, 22.5, 2.0, 17.0)
    assert h["total_amount"] is None and h["passenger_count"] is None and h["payment_label"] is None
    assert "driver_pay" not in json.loads(h["extras"]) and "trip_miles" not in json.loads(h["extras"])
    # FHV: no money fields, pickup zone missing -> flagged
    assert f["service_type"] == "fhv" and f["base_fare"] is None and f["pu_zone"] is None and f["do_zone"]
    assert flags_of(f) == {"dq_null_pu_location"}
    for r in (y, h, f):
        assert r["silver_write_ts"] is not None and r["kafka_timestamp"] is None  # no Kafka time in this test input


def test_fee_columns_are_typed(spark, zones):
    silver, _ = run_process(spark, zones, [
        ("yellow", msg("yellow", tolls_amount=6.94, Airport_fee=1.75, congestion_surcharge=2.5,
                       cbd_congestion_fee=0.75)),
        ("hvfhv", msg("hvfhv", tolls=1.0, bcf=0.5, sales_tax=1.2, congestion_surcharge=2.75, airport_fee=2.5,
                      cbd_congestion_fee=1.5, shared_request_flag="Y", shared_match_flag="N",
                      request_datetime="2026-01-10T09:55:00", on_scene_datetime="2026-01-10T09:58:00"))])
    y, h = silver[0], silver[1]
    assert (y["tolls"], y["airport_fee"], y["congestion_surcharge"], y["cbd_congestion_fee"]) == (6.94, 1.75, 2.5, 0.75)
    assert y["bcf"] is None
    assert (h["tolls"], h["bcf"], h["sales_tax"], h["congestion_surcharge"], h["airport_fee"],
            h["cbd_congestion_fee"]) == (1.0, 0.5, 1.2, 2.75, 2.5, 1.5)
    assert (h["shared_request_flag"], h["shared_match_flag"]) == ("Y", "N")
    assert h["request_ts"] == datetime(2026, 1, 10, 9, 55) and h["on_scene_ts"] == datetime(2026, 1, 10, 9, 58)


def test_output_columns_match_declared_layout(spark, zones):
    silver, invalid = process(build_raw(spark, [("yellow", msg("yellow")), ("yellow", "x")]),
                              SPECS, NORMS, zones, VCFG)
    assert silver.columns == [n for n, _ in SILVER_FIELDS]
    assert invalid.columns == [n for n, _ in INVALID_FIELDS]
    dtypes = dict(silver.dtypes)
    assert dtypes["pickup_ts"] == "timestamp" and dtypes["kafka_offset"] == "bigint"
    assert dtypes["pickup_date"] == "date" and dtypes["duration_sec"] == "bigint"
    assert all(dtypes[f] == "boolean" for f in DQ_FLAGS)


def test_trip_id_is_deterministic_and_discriminating(spark, zones):
    same = msg("yellow")
    silver, _ = run_process(spark, zones, [("yellow", same), ("yellow", same),
                                           ("yellow", msg("yellow", tip_amount=4.0)),
                                           ("yellow", msg("yellow", PULocationID=162)),
                                           ("green", msg("green"))])
    ids = [silver[i]["trip_id"] for i in range(5)]
    assert ids[0] == ids[1], "an exact duplicate must get the same trip_id"
    assert len(set(ids)) == 4, "different trips (amount, zone, service) must get different trip_ids"


def test_malformed_keeps_raw_payload(spark, zones):
    _, invalid = run_process(spark, zones, [("green", "{truncated")])
    row = invalid[0]
    assert row["raw_value"] == "{truncated" and row["service_type"] == "green"
    assert row["reject_reason"] == "malformed_json"


# ---- reconciliation on real data with injected faults -----------------------------

EXPECT = {  # injected fault tag -> Silver flags, at least one of which must be set (D27: flagged, not rejected)
    "dropoff_before_pickup": {"dq_dropoff_before_pickup"},
    "negative_amount": {"dq_negative_fare"},
    "negative_distance": {"dq_zero_distance"},
    "pickup_out_of_range": {"dq_pickup_out_of_range"},
    "unknown_location_id": {"dq_unmatched_pu_zone", "dq_unmatched_do_zone"},
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
    silver, invalid = run_process(spark, zones, items)
    return items, tags, sent, silver, invalid


def test_real_data_every_message_accounted_for(real_result):
    items, tags, sent, silver, invalid = real_result
    assert len(silver) + len(invalid) == len(items)
    by_service = (Counter(r["service_type"] for r in silver.values())
                  + Counter(r["service_type"] for r in invalid.values()))
    assert dict(by_service) == {service_type(k): v for k, v in sent.items()}


def test_real_data_injected_faults_are_flagged_or_rejected(real_result):
    items, tags, sent, silver, invalid = real_result
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
                got = flags_of(silver[off]) if off in silver else set()
                if not (got & EXPECT[kind]):
                    misses.append((ds, off, kind, sorted(got)))
    assert sum(seen.values()) > 50, f"too few injected faults to be meaningful: {seen}"
    assert not misses, f"{len(misses)} injected faults not flagged, e.g. {misses[:5]}"


def test_real_data_only_unusable_rows_are_rejected(real_result):
    """Without injected malformed JSON, only a null pickup time may reject a row."""
    items, tags, sent, silver, invalid = real_result
    for off, row in invalid.items():
        if "malformed_json" not in tags[off][1]:
            assert row["reject_reason"] == "null_pickup_time", row["reject_reason"]


def test_real_data_flags_are_never_null(real_result):
    _, _, _, silver, _ = real_result
    for r in silver.values():
        assert all(r[f] is not None for f in DQ_FLAGS)
