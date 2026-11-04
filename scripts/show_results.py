"""Print summaries of the Spark output with DuckDB.

    python scripts/show_results.py [--output-dir output]

Needs `pip install duckdb`. Add your own queries with --sql "SELECT ... FROM clean" (tables: clean, invalid).
"""
import argparse
import sys
from pathlib import Path

import duckdb

QUERIES = {
    "Trips per service (clean)": """
        SELECT service, count(*) AS trips, round(avg(trip_distance), 2) AS avg_distance,
               round(avg(fare_amount), 2) AS avg_fare, round(sum(total_amount), 2) AS total_revenue
        FROM clean GROUP BY service ORDER BY trips DESC""",
    "Top pickup boroughs (clean)": """
        SELECT PUBorough, count(*) AS trips FROM clean GROUP BY ALL ORDER BY trips DESC LIMIT 8""",
    "Busiest pickup zones (clean)": """
        SELECT PUZone, PUBorough, count(*) AS trips FROM clean WHERE PUZone IS NOT NULL
        GROUP BY ALL ORDER BY trips DESC LIMIT 8""",
    "Rejected rows by service and reason": """
        SELECT service, reject_reason, count(*) AS n FROM invalid GROUP BY ALL ORDER BY n DESC LIMIT 15""",
    "Clean rows flagged (not rejected)": """
        SELECT service, dq_flags, count(*) AS n FROM clean WHERE dq_flags IS NOT NULL GROUP BY ALL ORDER BY n DESC""",
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-dir", default="output")
    ap.add_argument("--sql", help="run one custom query against tables `clean` and `invalid`")
    args = ap.parse_args()
    out = Path(args.output_dir)

    con = duckdb.connect()
    for table, name in (("clean", "trips_clean"), ("invalid", "invalid")):
        if not list((out / name).glob("service=*/*.parquet")):
            sys.exit(f"no Parquet files under {out / name}: run the Spark job first")
        # `*/*.parquet` skips Spark's _spark_metadata folder and .crc files
        pattern = (out / name / "*" / "*.parquet").as_posix()
        con.execute(f"CREATE VIEW {table} AS SELECT * FROM read_parquet('{pattern}', hive_partitioning=true)")

    if args.sql:
        print(con.sql(args.sql))
        return
    for title, sql in QUERIES.items():
        print(f"\n== {title}")
        print(con.sql(sql))


if __name__ == "__main__":
    main()
