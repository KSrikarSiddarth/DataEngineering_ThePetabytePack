"""M4 test: the whole streaming job (Silver -> deduplication -> Gold) run end to end on NDJSON input files.

Kafka is replaced by the job's `--source files` dev source, so this needs Spark and Java but no Docker.
"""
import json
import shutil
import tempfile
from pathlib import Path

import pytest

from conftest import ROOT
from gold import MARTS
from stream_job import main
from test_spark_transform import DATA, msg, spark, zones  # noqa: F401


@pytest.fixture(scope="module")
def run_dirs(spark):  # noqa: F811
    """Run the job twice on the same input and checkpoints; yield (output dir, counts after run 1 and 2)."""
    base = Path(tempfile.mkdtemp(prefix="m4"))          # short path: Windows' 260-character limit
    inp, out, ckpt = base / "in", base / "out", base / "ckpt"
    inp.mkdir()
    jan20 = dict(tpep_pickup_datetime="2026-01-20T10:00:00", tpep_dropoff_datetime="2026-01-20T10:10:00")
    yellow = [msg("yellow"), msg("yellow"),             # an exact duplicate, to be removed
              msg("yellow", tpep_pickup_datetime="2026-01-10T10:30:00", tpep_dropoff_datetime="2026-01-10T10:50:00"),
              msg("yellow", **jan20),                    # later in event time: moves the watermark past Jan 10
              "{truncated"]                              # malformed: goes to silver_invalid
    lines = {"yellow": yellow, "hvfhv": [msg("hvfhv")]}
    for name, values in lines.items():
        (inp / f"{name}.ndjson").write_text(
            "\n".join(json.dumps({"key": "k", "headers": {}, "value": v}) for v in values), encoding="utf-8")

    env = {"ZONE_LOOKUP": str(DATA / "taxi_zones" / "taxi_zone_lookup.csv"), "OUTPUT_DIR": str(out),
           "CHECKPOINT_DIR": str(ckpt)}
    try:
        with pytest.MonkeyPatch.context() as mp:
            for key, value in env.items():
                mp.setenv(key, value)
            assert main(["--source", "files", "--input-dir", str(inp)]) == 0
            first = counts(spark, out)
            assert main(["--source", "files", "--input-dir", str(inp)]) == 0   # restart from the checkpoints
            second = counts(spark, out)
        yield out, first, second
    finally:
        shutil.rmtree(base, ignore_errors=True)


def counts(spark, out):  # noqa: F811
    """Row counts of Silver, invalid and every Gold mart under `out`."""
    def n(path):
        return spark.read.parquet(str(path)).count() if Path(path).exists() else 0
    return {"silver": n(out / "silver" / "trips"), "invalid": n(out / "silver_invalid"),
            **{name: n(out / "gold" / name) for name in MARTS}}


def test_duplicates_are_removed_and_bad_rows_split(spark, run_dirs):  # noqa: F811
    out, first, _ = run_dirs
    assert first["silver"] == 4          # 4 yellow + 1 hvfhv usable rows sent, minus 1 duplicate
    assert first["invalid"] == 1         # the truncated JSON
    silver = spark.read.parquet(str(out / "silver" / "trips"))
    assert silver.select("trip_id").distinct().count() == silver.count()


def test_silver_is_partitioned_by_service_type(run_dirs):
    out, _, _ = run_dirs
    parts = {p.name for p in (out / "silver" / "trips").glob("service_type=*")}
    assert parts == {"service_type=yellow", "service_type=fhvhv"}


def test_dimension_tables_are_written(run_dirs):
    out, _, _ = run_dirs
    for name in ("dim_zone", "dim_payment_type", "dim_ratecode", "dim_hvfhs_company"):
        assert list((out / "silver" / name).glob("*.parquet")), name


def test_gold_marts_are_written_for_closed_windows(spark, run_dirs):  # noqa: F811
    out, first, _ = run_dirs
    trips = spark.read.parquet(str(out / "gold" / "trips_daily"))
    jan10 = trips.where("pickup_date = '2026-01-10' and service_type = 'yellow'").agg({"trips": "sum"}).collect()[0][0]
    assert jan10 == 2                     # the two Jan 10 yellow trips; the duplicate is not counted twice
    assert first["data_quality_daily"] > 0 and first["revenue_daily"] > 0


def test_restart_does_not_duplicate_output(run_dirs):
    _, first, second = run_dirs
    assert first == second
