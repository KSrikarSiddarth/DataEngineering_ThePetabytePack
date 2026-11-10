"""M5 tests: the batch job end to end on small synthetic monthly files, the KPI queries on DuckDB and Spark SQL,
and the stream+batch combined views. Needs Spark and Java, no Docker."""
import json
import shutil
import sys
import tempfile
from datetime import date, datetime, timedelta
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from conftest import ROOT
from test_spark_transform import DATA, SPECS, msg, spark  # noqa: F401

sys.path.insert(0, str(ROOT / "scripts"))
from batch_job import main as run_batch_job  # noqa: E402
from combined_views import check_disjoint, duckdb_views, spark_views  # noqa: E402
from reconcile import reconcile_batch, rows_under  # noqa: E402

DATASETS = ("yellow", "green", "hvfhv")
CUTOVER = "2026-01-01"
PA_TYPES = {"int": pa.int64(), "float": pa.float64(), "string": pa.string(), "timestamp": pa.timestamp("us")}
QUERIES = sorted((ROOT / "scripts" / "queries").glob("*.sql"))


def trip(dataset, month, minute=0, **over):
    """One source row for `dataset` picked up in `month` at 10:<minute> and dropped off 20 minutes later."""
    spec = SPECS[dataset]
    pickup = datetime.fromisoformat(f"{month}-10T10:{minute:02d}:00")
    fields = {spec["pickup_col"]: pickup.isoformat(), spec["dropoff_col"]: (pickup + timedelta(minutes=20)).isoformat()}
    fields.update(over)
    row = json.loads(msg(dataset, **fields))
    return {c: (datetime.fromisoformat(row[c]) if t == "timestamp" and row[c] else row[c])
            for c, t in spec["columns"].items()}


def write_file(data_dir, dataset, month, rows, drop_columns=()):
    """Write `rows` as the TLC-style monthly Parquet file of `dataset`, without the `drop_columns`."""
    spec = SPECS[dataset]
    cols = {c: t for c, t in spec["columns"].items() if c not in drop_columns}
    table = pa.table({c: pa.array([r[c] for r in rows], type=PA_TYPES[t]) for c, t in cols.items()})
    pq.write_table(table, Path(data_dir) / f"{spec['file_prefix']}_{month}.parquet")


def write_month(data_dir, month, yellow_b=True, drop_cbd=False):
    """The three files of a month. Yellow has: A, B (optional), an exact copy of A, a row without pickup time
    and a row picked up in 2019 (out of range), so bronze 5, invalid 1, duplicates 1, silver 3."""
    a = trip("yellow", month)
    yellow = [a, a, trip("yellow", month, 30, tip_amount=1.0),
              trip("yellow", month, 40, tpep_pickup_datetime=None),
              trip("yellow", month, 50, tpep_pickup_datetime="2019-01-01T10:00:00",
                   tpep_dropoff_datetime="2019-01-01T10:20:00")]
    if not yellow_b:
        del yellow[2]
    drop = ("cbd_congestion_fee",) if drop_cbd else ()
    write_file(data_dir, "yellow", month, yellow, drop)
    write_file(data_dir, "green", month, [trip("green", month), trip("green", month, 5)], drop)
    write_file(data_dir, "hvfhv", month, [trip("hvfhv", month), trip("hvfhv", month, 5)], drop)


class Run:
    """The batch store and what was observed at each stage of the scenario."""

    def __init__(self, base):
        self.data, self.out = base / "data", base / "out"
        self.data.mkdir()
        self.seen = {}

    def batch(self, months):
        """Run the batch job for `months` on the three test datasets; returns its exit code."""
        return self.batch_datasets(months, DATASETS)

    def batch_datasets(self, months, datasets):
        """Run the batch job for `months` on the given datasets; returns its exit code."""
        return run_batch_job(["--months", *months, "--datasets", *datasets, "--input-dir", str(self.data),
                              "--output-dir", str(self.out / "batch")])

    def silver_rows(self, dataset, month):
        """Silver rows written for one file."""
        svc = "fhvhv" if dataset == "hvfhv" else dataset
        return rows_under(self.out / "batch" / "silver" / "trips" / f"service_type={svc}" /
                          f"year={month[:4]}" / f"month={int(month[5:])}")

    def log(self):
        """Every batch_run_log row as a dict."""
        files = (self.out / "batch" / "silver" / "batch_run_log").glob("*.parquet")
        return [r for f in files for r in pq.read_table(f).to_pylist()]


@pytest.fixture(scope="module")
def run(spark):  # noqa: F811
    """Run the scenario once: two months, a rerun, a corrected file and a missing month."""
    base = Path(tempfile.mkdtemp(prefix="m5"))   # short path: Windows' 260-character limit
    r = Run(base)
    with pytest.MonkeyPatch.context() as mp:
        mp.setenv("ZONE_LOOKUP", str(DATA / "taxi_zones" / "taxi_zone_lookup.csv"))
        write_month(r.data, "2025-11", drop_cbd=True)     # an older file without the CBD fee column
        write_month(r.data, "2025-12")
        r.seen["first_exit"] = r.batch(["2025-11", "2025-12"])
        r.seen["after_first"] = {(d, m): r.silver_rows(d, m) for d in DATASETS for m in ("2025-11", "2025-12")}
        r.seen["reconcile_first"] = reconcile_batch(r.out, r.data)

        r.seen["rerun_exit"] = r.batch(["2025-12"])        # same input again
        r.seen["after_rerun"] = {(d, m): r.silver_rows(d, m) for d in DATASETS for m in ("2025-11", "2025-12")}

        write_month(r.data, "2025-12", yellow_b=False)     # TLC "corrects" the file: row B is gone
        r.seen["correction_exit"] = r.batch(["2025-12"])
        r.seen["after_correction"] = r.silver_rows("yellow", "2025-12")
        r.seen["reconcile_last"] = reconcile_batch(r.out, r.data)

        r.seen["missing_exit"] = r.batch(["2025-10"])      # no files for this month

        write_file(r.data, "hvfhv", "2025-09", [trip("hvfhv", "2025-09")])   # a month with only HVFHV: no yellow/green marts
        r.seen["hvfhv_only_exit"] = r.batch_datasets(["2025-09"], ["hvfhv"])
        yield r
    shutil.rmtree(base, ignore_errors=True)


def test_silver_rows_flags_and_reconciliation(run):
    assert run.seen["first_exit"] == 0
    for month in ("2025-11", "2025-12"):
        assert run.seen["after_first"][("yellow", month)] == 3   # A, B and the out-of-range row; copy of A removed
        assert run.seen["after_first"][("green", month)] == 2
        assert run.seen["after_first"][("hvfhv", month)] == 2
    assert run.seen["reconcile_first"] == 0                      # source rows = silver + invalid + duplicates


def test_log_has_one_row_per_load(run):
    first = [r for r in run.log() if r["source_file"] == "yellow_tripdata_2025-11.parquet"]
    assert len(first) == 1
    r = first[0]
    assert (r["bronze_rows"], r["invalid_rows"], r["silver_rows"], r["duplicates_removed"]) == (5, 1, 3, 1)
    assert r["status"] == "success" and r["gold_available_ts"] and r["schema_hash"] and r["late_correction_rows"] == 0


def test_rerun_is_idempotent_and_adds_a_log_row(run):
    assert run.seen["rerun_exit"] == 0 and run.seen["after_rerun"] == run.seen["after_first"]
    december = [r for r in run.log() if r["source_file"] == "yellow_tripdata_2025-12.parquet"]
    assert len(december) == 3 and len([r for r in run.log() if r["source_file"] == "yellow_tripdata_2025-11.parquet"]) == 1


def test_corrected_file_replaces_only_its_partition_and_counts_late_corrections(run):
    assert run.seen["after_correction"] == 2                    # row B is gone from Silver
    assert run.silver_rows("yellow", "2025-11") == 3            # other months untouched
    latest = max((r for r in run.log() if r["source_file"] == "yellow_tripdata_2025-12.parquet"),
                 key=lambda r: r["start_ts"])
    assert latest["late_correction_rows"] == 1
    assert run.seen["reconcile_last"] == 0


def test_missing_file_is_logged_as_failed(run):
    assert run.seen["missing_exit"] == 1
    failed = [r for r in run.log() if r["source_month"] == "2025-10"]
    assert len(failed) == 3 and all(r["status"] == "failed" and "not found" in r["error"] for r in failed)


def test_month_without_yellow_or_green_still_builds_gold(run):
    assert run.seen["hvfhv_only_exit"] == 0
    gold = run.out / "batch" / "gold"
    assert (gold / "hvfhv_economics_monthly" / "year=2025" / "month=9").exists()
    assert not (gold / "revenue_monthly" / "year=2025" / "month=9").exists()   # nothing to roll up, nothing stale


def test_gold_marts_are_partitioned_and_monthly_equals_daily(run):
    gold = run.out / "batch" / "gold"
    for mart in ("trips_daily", "revenue_daily", "hvfhv_economics_daily", "trips_monthly", "data_quality_monthly"):
        assert (gold / mart / "year=2025" / "month=12").exists(), mart
    con = duckdb.connect()
    duckdb_views(con, run.out / "batch", run.out / "stream", CUTOVER)
    monthly = con.sql("SELECT service_type, month, trips FROM trips_monthly ORDER BY 1, 2").fetchall()
    daily = con.sql("SELECT service_type, month, SUM(trips) FROM trips_daily GROUP BY 1, 2 ORDER BY 1, 2").fetchall()
    assert monthly == daily
    assert dict(((s, m), t) for s, m, t in monthly)[("yellow", 11)] == 2   # A and B; the 2019 row is out of period
    dq = con.sql("SELECT SUM(n_pickup_out_of_range), SUM(duplicates_removed) FROM data_quality_monthly "
                 "WHERE service_type = 'yellow' AND month = 11").fetchone()
    assert dq == (1, 1)                                         # flagged row counted; the removed duplicate recorded


def test_schema_drift_is_measured(run):
    con = duckdb.connect()
    duckdb_views(con, run.out / "batch", run.out / "stream", CUTOVER)
    rows = con.sql("SELECT source_month, MAX(columns_added), MAX(columns_removed), MAX(ABS(reconciliation_diff)) "
                   "FROM batch_run_metrics WHERE dataset = 'yellow' AND status = 'success' "
                   "GROUP BY source_month ORDER BY source_month").fetchall()
    assert rows == [("2025-11", 0, 0, 0),                       # first file: nothing to compare with
                    ("2025-12", 1, 0, 0)]                       # December added cbd_congestion_fee


def run_queries(execute):
    """Run every KPI query (except the combined one) with `execute(sql) -> rows`; returns {name: rows}."""
    return {p.stem: execute(p.read_text(encoding="utf-8").replace("{cutover}", CUTOVER))
            for p in QUERIES if p.stem != "live_vs_baseline"}


def test_kpi_queries_answer_on_duckdb_and_spark_sql(run, spark):  # noqa: F811
    con = duckdb.connect()
    duckdb_views(con, run.out / "batch", run.out / "stream", CUTOVER)
    on_duckdb = run_queries(lambda sql: con.sql(sql).fetchall())
    spark_views(spark, run.out / "batch", run.out / "stream", CUTOVER)
    on_spark = run_queries(lambda sql: [tuple(r) for r in spark.sql(sql).collect()])
    for name, rows in on_duckdb.items():
        assert rows, f"{name} returned nothing"
        assert len(rows) == len(on_spark[name]), f"{name}: DuckDB and Spark SQL disagree on the row count"

    # two months are not enough for year-over-year or a 12-month average: reported, not computed
    growth = on_duckdb["trips_growth"]
    assert all(r[5] is None and r[6] is None and r[8] == "not enough history" for r in growth)
    assert any(r[4] is not None for r in growth)                # month-over-month exists for December
    fees = {(r[0], r[1]): r[3] for r in on_duckdb["fee_impact"]}
    assert fees[("airport_fee", "before")] == "not enough history" and fees[("cbd_congestion_fee", "after")] == "ok"
    health = on_duckdb["batch_health"][0]
    assert health[3] == 0 and health[1] < 1 and health[7] == pytest.approx(3 / 24)   # reconciled; a failure; 3 of 24 months


def test_combined_views_use_each_date_from_one_side(run):
    """Stream rows before the cutover and batch rows from it on are left out; the rest add up."""
    stream = run.out / "stream" / "gold" / "demand_hourly"
    stream.mkdir(parents=True)
    rows = [(date(2025, 12, 31), 5), (date(2026, 1, 5), 7), (date(2026, 1, 6), 9)]   # first one is before the cutover
    pq.write_table(pa.table({
        "hour_start": pa.array([datetime(d.year, d.month, d.day, 10) for d, _ in rows], pa.timestamp("us")),
        "pickup_date": pa.array([d for d, _ in rows], pa.date32()), "pickup_hour": pa.array([10] * 3, pa.int32()),
        "service_type": ["yellow"] * 3, "trips": pa.array([t for _, t in rows], pa.int64()),
        "day_of_week": pa.array([4, 2, 3], pa.int32())}), stream / "part-0.parquet")

    con = duckdb.connect()
    duckdb_views(con, run.out / "batch", run.out / "stream", CUTOVER)
    counts = {name: (b, s, t) for name, b, s, t in check_disjoint(con, CUTOVER)}
    batch_rows, stream_rows, total = counts["demand_hourly"]
    assert stream_rows == 2 and batch_rows > 0 and total == batch_rows + stream_rows
    assert counts["trips_daily"][1] == 0                        # no stream side for this mart: batch only
    live_sql = (ROOT / "scripts" / "queries" / "live_vs_baseline.sql").read_text(encoding="utf-8")
    live = con.sql(live_sql.replace("{cutover}", CUTOVER)).fetchall()
    assert len(live) == 2 and live[0][3] == 7 and live[1][3] == 9
