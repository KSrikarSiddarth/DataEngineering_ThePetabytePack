"""Views for the KPI queries: the batch tables, and the stream+batch combination at query time (D28).

    python scripts/combined_views.py [--output-dir output] [--db output/combined.duckdb] [--cutover 2026-01-01]

Nothing is copied. For every daily mart, `combined_<mart>` = the batch rows dated before the cutover plus
the stream rows dated on or after it, so each date comes from exactly one side. The batch Gold and Silver
tables are registered under their own names (`trips_monthly`, `silver_trips`, `batch_run_metrics`, ...) for
`scripts/queries/*.sql`. The same views exist as DuckDB views (`duckdb_views`) and Spark temp views
(`spark_views`). Open the written database with `duckdb output/combined.duckdb`.
"""
import argparse
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "spark"))
from gold import MARTS, MONTHLY  # noqa: E402

DAILY = list(MARTS)
PARTITIONED = DAILY + list(MONTHLY)          # batch Gold tables written as year=/month= partitions


def default_cutover():
    """combined.cutover_date from stream-config.yaml."""
    cfg = yaml.safe_load((ROOT / "stream-config.yaml").read_text(encoding="utf-8"))
    return str(cfg["combined"]["cutover_date"])


def has_parquet(folder):
    """True when `folder` exists and holds at least one Parquet file (a mart with no rows is an empty folder)."""
    return Path(folder).exists() and any(Path(folder).rglob("*.parquet"))


def duckdb_views(con, batch_dir, stream_dir, cutover):
    """Create the batch table views and the combined_<mart> views in a DuckDB connection.
    Returns the names of the views that were created."""
    batch, stream = Path(batch_dir).resolve(), Path(stream_dir).resolve()
    made = []

    def create(name, sql):
        con.execute(f"CREATE OR REPLACE VIEW {name} AS {sql}")
        made.append(name)

    for name in PARTITIONED:
        if has_parquet(batch / "gold" / name):
            create(name, f"SELECT * FROM read_parquet('{(batch / 'gold' / name).as_posix()}/*/*/*.parquet', "
                         "hive_partitioning=true)")
    for name, sub in (("batch_run_metrics", "gold/batch_run_metrics"), ("batch_run_log", "silver/batch_run_log")):
        if has_parquet(batch / sub):
            create(name, f"SELECT * FROM read_parquet('{(batch / sub).as_posix()}/*.parquet')")
    if has_parquet(batch / "silver" / "trips"):
        create("silver_trips", f"SELECT * FROM read_parquet('{(batch / 'silver' / 'trips').as_posix()}/*/*/*/*.parquet', "
                               "hive_partitioning=true)")

    for name in DAILY:
        parts = []
        if has_parquet(batch / "gold" / name):
            parts.append(f"SELECT * FROM read_parquet('{(batch / 'gold' / name).as_posix()}/*/*/*.parquet') "
                         f"WHERE pickup_date < DATE '{cutover}'")
        if has_parquet(stream / "gold" / name):
            parts.append(f"SELECT * FROM read_parquet('{(stream / 'gold' / name).as_posix()}/*.parquet') "
                         f"WHERE pickup_date >= DATE '{cutover}'")
        if parts:
            create(f"combined_{name}", " UNION ALL BY NAME ".join(parts))
    return made


def spark_views(spark, batch_dir, stream_dir, cutover):
    """The same views as Spark temp views (for Spark SQL); returns their names."""
    import pyspark.sql.functions as F
    from pyspark.errors import AnalysisException

    batch, stream = Path(batch_dir), Path(stream_dir)
    made = []

    def read(folder):
        """The Parquet table in `folder`, or None when it has no data (no files, or only empty ones)."""
        try:
            return spark.read.parquet(str(folder)) if has_parquet(folder) else None
        except AnalysisException:   # Spark cannot infer a schema from the empty files of a stream mart
            return None

    def register(name, df):
        if df is not None:
            df.createOrReplaceTempView(name)
            made.append(name)

    for name in PARTITIONED:
        register(name, read(batch / "gold" / name))
    for name, sub in (("batch_run_metrics", "gold/batch_run_metrics"), ("batch_run_log", "silver/batch_run_log")):
        register(name, read(batch / sub))
    register("silver_trips", read(batch / "silver" / "trips"))

    cut = F.lit(cutover).cast("date")
    for name in DAILY:
        b, s = read(batch / "gold" / name), read(stream / "gold" / name)
        parts = ([b.drop("year", "month").where(F.col("pickup_date") < cut)] if b is not None else []) +                 ([s.where(F.col("pickup_date") >= cut)] if s is not None else [])
        if parts:
            register(f"combined_{name}", parts[0] if len(parts) == 1 else parts[0].unionByName(parts[1]))
    return made


def check_disjoint(con, cutover):
    """For each combined mart: (name, rows dated before the cutover, rows from it on, all rows).
    Each date comes from one side only, so the first two must add up to the third."""
    rows = []
    for name in DAILY:
        try:
            batch, stream, total = con.execute(f"""
                SELECT count(*) FILTER (WHERE pickup_date < DATE '{cutover}'),
                       count(*) FILTER (WHERE pickup_date >= DATE '{cutover}'), count(*)
                FROM combined_{name}""").fetchone()
        except Exception:   # a mart that exists on neither side has no view
            continue
        rows.append((name, batch, stream, total))
    return rows


def main():
    """Create the views in a DuckDB file and print the per-mart row counts of the combination."""
    import duckdb

    ap = argparse.ArgumentParser()
    ap.add_argument("--output-dir", default="output")
    ap.add_argument("--db", help="DuckDB file to write the views to (default: <output-dir>/combined.duckdb)")
    ap.add_argument("--cutover", default=default_cutover())
    args = ap.parse_args()
    out = Path(args.output_dir)
    db = Path(args.db) if args.db else out / "combined.duckdb"

    con = duckdb.connect(str(db))
    made = duckdb_views(con, out / "batch", out / "stream", args.cutover)
    if not made:
        sys.exit(f"no batch or stream Gold tables under {out}: run the batch job and the stream job first")
    print(f"{len(made)} views in {db} (cutover {args.cutover})")
    print(f"{'combined mart':28} {'batch (< cutover)':>18} {'stream (>= cutover)':>20} {'combined':>10}")
    for name, batch, stream, total in check_disjoint(con, args.cutover):
        print(f"{name:28} {batch:18,} {stream:20,} {total:10,}" + ("" if total == batch + stream else "  MISMATCH"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
