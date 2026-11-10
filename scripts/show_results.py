"""Print summaries of the stream output (Silver and Gold) with DuckDB.

    python scripts/show_results.py [--output-dir output]

Needs `pip install duckdb`. Add your own query with --sql "SELECT ... FROM silver"
(tables: silver, invalid, and one per Gold mart such as trips_daily, revenue_daily).
Gold marts hold hourly additive measures, so daily figures are sums by pickup_date (see specs/gold-schema.md).
"""
import argparse
import sys
from pathlib import Path

import duckdb

MARTS = ["trips_daily", "demand_hourly", "zone_od_daily", "revenue_daily", "trip_efficiency_daily",
         "hvfhv_economics_daily", "hvfhv_service_daily", "data_quality_daily"]

QUERIES = {
    "Trips per service (Silver)": """
        SELECT service_type, count(*) AS trips, round(avg(distance_miles), 2) AS avg_distance,
               round(avg(base_fare), 2) AS avg_fare FROM silver GROUP BY ALL ORDER BY trips DESC""",
    "Busiest pickup zones (Silver)": """
        SELECT pu_zone, pu_borough, count(*) AS trips FROM silver WHERE pu_zone IS NOT NULL
        GROUP BY ALL ORDER BY trips DESC LIMIT 8""",
    "Rejected rows by service and reason": """
        SELECT service_type, reject_reason, count(*) AS n FROM invalid GROUP BY ALL ORDER BY n DESC""",
    "Silver rows flagged (kept, not rejected)": """
        SELECT service_type, sum(dq_dropoff_before_pickup::INT) AS dropoff_before_pickup,
               sum(dq_zero_distance::INT) AS zero_distance, sum(dq_negative_fare::INT) AS negative_fare,
               sum(dq_unmatched_pu_zone::INT + dq_unmatched_do_zone::INT) AS unmatched_zone,
               sum(dq_pickup_out_of_range::INT) AS out_of_range FROM silver GROUP BY ALL""",
    "Gold trips_daily (hourly rows summed by day)": """
        SELECT pickup_date, service_type, sum(trips) AS trips FROM trips_daily
        GROUP BY ALL ORDER BY pickup_date, service_type LIMIT 12""",
    "Gold revenue_daily: average fare and tip rate (card)": """
        SELECT service_type, round(sum(sum_total_amount) / sum(trips), 2) AS avg_fare,
               round(sum(sum_tip_amount) FILTER (payment_type = 1)
                     / sum(sum_base_fare) FILTER (payment_type = 1), 3) AS card_tip_rate
        FROM revenue_daily GROUP BY ALL""",
    "Gold hvfhv_economics_daily: driver pay share by company": """
        SELECT hvfhs_license_num, round(sum(sum_driver_pay) / sum(sum_base_fare_with_pay), 3) AS driver_pay_share
        FROM hvfhv_economics_daily GROUP BY ALL""",
}


def main():
    """Create one DuckDB view per output table, then run the summary queries (or --sql)."""
    sys.stdout.reconfigure(encoding="utf-8")   # DuckDB draws tables with box characters; Windows consoles default to cp1252
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-dir", default="output")
    ap.add_argument("--sql", help="run one custom query instead of the summaries")
    args = ap.parse_args()
    out = Path(args.output_dir) / "stream"

    # name -> (folder, glob below it); `*/*.parquet` skips Spark's _spark_metadata folder and .crc files
    tables = {"silver": (out / "silver" / "trips", "*/*.parquet"), "invalid": (out / "silver_invalid", "*/*.parquet"),
              **{m: (out / "gold" / m, "*.parquet") for m in MARTS}}
    con = duckdb.connect()
    for name, (folder, pattern) in tables.items():
        if not list(folder.glob(pattern)):
            print(f"(no Parquet files under {folder}: {name} skipped)", file=sys.stderr)
            continue
        con.execute(f"CREATE VIEW {name} AS SELECT * FROM read_parquet('{(folder / pattern).as_posix()}', "
                    "hive_partitioning=true)")

    if args.sql:
        print(con.sql(args.sql))
        return
    for title, sql in QUERIES.items():
        print(f"\n== {title}")
        try:
            print(con.sql(sql))
        except duckdb.Error as e:
            print(f"(skipped: {str(e).splitlines()[0]})")


if __name__ == "__main__":
    main()
