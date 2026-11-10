"""Time the KPI queries in scripts/queries on DuckDB and Spark SQL, and compare a partition-pruned Silver query
with a full scan (M5).

    python scripts/benchmark_queries.py [--output-dir output] [--repeats 3] [--no-spark]

Both engines read the same Parquet files in place (no load step) through the views of combined_views.py.
A query whose tables do not exist yet (for example live_vs_baseline before the stream has run) is skipped.
Spark SQL needs Java; a machine-wide SPARK_HOME for another Spark version must point at pip's pyspark
(see installation.md). Timings are the median of --repeats runs.
"""
import argparse
import statistics
import sys
import time
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))
from combined_views import default_cutover, duckdb_views, spark_views  # noqa: E402

QUERY_DIR = Path(__file__).resolve().parent / "queries"


def timed(run, repeats):
    """Median milliseconds over `repeats` calls of `run()`, and the last result."""
    times, result = [], None
    for _ in range(repeats):
        start = time.perf_counter()
        result = run()
        times.append((time.perf_counter() - start) * 1000)
    return statistics.median(times), result


def fmt(ms):
    """A timing as a right-aligned whole number of milliseconds, or '-' when the engine was not run."""
    return f"{'-' if ms is None else format(ms, '.0f'):>10}"


def parquet_files(folder):
    """Number of Parquet files under a folder."""
    return sum(1 for _ in Path(folder).rglob("*.parquet"))


def pruning_report(con, spark, out, repeats):
    """Print timings of a partition-pruned Silver query and a full scan, with the files each one needs.
    Pruned: filter on service_type, year and month (the partition columns) and read two columns.
    Full: the same aggregate with a predicate on a regular column, so every file must be read."""
    trips = Path(out) / "batch" / "silver" / "trips"
    if not any(trips.rglob("*.parquet")):
        print("\n(no batch Silver: pruning comparison skipped)")
        return
    year, month = con.execute("SELECT year, month FROM silver_trips ORDER BY year DESC, month DESC LIMIT 1").fetchone()
    agg = "SELECT COUNT(*), AVG(distance_miles) FROM silver_trips"
    pruned = f"{agg} WHERE service_type = 'yellow' AND year = {year} AND month = {month}"
    full = f"{agg} WHERE pickup_ts IS NOT NULL"
    scoped = parquet_files(trips / "service_type=yellow" / f"year={year}" / f"month={month}")
    print(f"\nPartition pruning (yellow, {year}-{month:02d}): {scoped} of {parquet_files(trips)} Silver files are needed")
    print(f"{'query':14} {'DuckDB ms':>10} {'Spark ms':>10}")
    for label, sql in (("pruned", pruned), ("full scan", full)):
        d_ms, _ = timed(lambda: con.execute(sql).fetchall(), repeats)
        s_ms = timed(lambda: spark.sql(sql).collect(), repeats)[0] if spark else None
        print(f"{label:14} {fmt(d_ms)} {fmt(s_ms)}")


def main():
    """Run every query on DuckDB and (optionally) Spark SQL and print the timing table."""
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-dir", default="output")
    ap.add_argument("--repeats", type=int, default=3)
    ap.add_argument("--no-spark", action="store_true")
    ap.add_argument("--cutover", default=default_cutover())
    args = ap.parse_args()
    out = Path(args.output_dir)

    con = duckdb.connect()
    if not duckdb_views(con, out / "batch", out / "stream", args.cutover):
        sys.exit(f"no Gold tables under {out}: run the batch job first")
    spark = None
    if not args.no_spark:
        sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "spark"))
        from stream_job import build_spark
        spark = build_spark({"shuffle_partitions": 8})
        spark.sparkContext.setLogLevel("ERROR")
        spark_views(spark, out / "batch", out / "stream", args.cutover)

    print(f"{'query':26} {'rows':>7} {'DuckDB ms':>10} {'Spark ms':>10}")
    for path in sorted(QUERY_DIR.glob("*.sql")):
        sql = path.read_text(encoding="utf-8").replace("{cutover}", args.cutover)
        try:
            d_ms, rows = timed(lambda: con.execute(sql).fetchall(), args.repeats)
        except duckdb.CatalogException:
            print(f"{path.stem:26} skipped (its tables do not exist yet)")
            continue
        s_ms = timed(lambda: spark.sql(sql).collect(), args.repeats)[0] if spark else None
        print(f"{path.stem:26} {len(rows):7,} {fmt(d_ms)} {fmt(s_ms)}")
    pruning_report(con, spark, out, args.repeats)
    return 0


if __name__ == "__main__":
    sys.exit(main())
