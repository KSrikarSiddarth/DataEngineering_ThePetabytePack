"""Step 8 - GOLD descriptive analytics expressed in Spark SQL (optimised by Catalyst)."""
from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from ..common.config import Config
from ..common.utils import get_logger, month_range, read_parquet, write_parquet

log = get_logger(__name__)

HOUR_BAND_SQL = """CASE WHEN {h} BETWEEN 7 AND 9 THEN 'am_peak' WHEN {h} BETWEEN 16 AND 19 THEN 'pm_peak'
                        WHEN {h} BETWEEN 0 AND 5 THEN 'night' ELSE 'off_peak' END"""

QUERIES: dict[str, str] = {
    # When is each zone busy? (dashboard heatmap)
    "demand_profile": """
        SELECT zone_id, zone_name, borough, weekday(window_start) + 1 AS day_of_week, hour(window_start) AS hour_of_day,
               AVG(trip_count) AS avg_pickups,
               percentile_approx(trip_count, 0.5) AS p50_pickups,
               percentile_approx(trip_count, 0.95) AS p95_pickups,
               COUNT(*) AS n_hours
        FROM demand GROUP BY zone_id, zone_name, borough, weekday(window_start) + 1, hour(window_start)
    """,
    # Busiest zones each day
    "top_zones_daily": """
        WITH daily AS (
            SELECT to_date(window_start) AS date, zone_id, zone_name, borough, SUM(trip_count) AS trips
            FROM demand GROUP BY to_date(window_start), zone_id, zone_name, borough),
        ranked AS (
            SELECT *, ROUND(100 * trips / SUM(trips) OVER (PARTITION BY date), 2) AS share_pct,
                   ROW_NUMBER() OVER (PARTITION BY date ORDER BY trips DESC) AS rank
            FROM daily)
        SELECT * FROM ranked WHERE rank <= 10
    """,
    # How do rain/snow change demand and travel time per borough and hour band?
    "weather_impact": f"""
        WITH borough_hour AS (
            SELECT borough, window_start, {HOUR_BAND_SQL.format(h='hour(window_start)')} AS hour_band,
                   weather_condition, SUM(trip_count) AS pickups
            FROM demand GROUP BY borough, window_start, weather_condition),
        demand_stats AS (
            SELECT borough, hour_band, weather_condition, AVG(pickups) AS avg_pickups_per_hour, COUNT(*) AS n_hours
            FROM borough_hour GROUP BY borough, hour_band, weather_condition),
        trip_stats AS (
            SELECT t.pickup_borough AS borough, {HOUR_BAND_SQL.format(h='t.hour_of_day')} AS hour_band,
                   w.weather_condition, AVG(t.duration_min) AS avg_duration_min, AVG(t.speed_mph) AS avg_speed_mph
            FROM trips t JOIN weather w ON t.pickup_hour = w.weather_hour
            GROUP BY t.pickup_borough, {HOUR_BAND_SQL.format(h='t.hour_of_day')}, w.weather_condition),
        joined AS (
            SELECT d.*, s.avg_duration_min, s.avg_speed_mph FROM demand_stats d
            LEFT JOIN trip_stats s USING (borough, hour_band, weather_condition))
        SELECT j.*, ROUND(100 * (j.avg_pickups_per_hour / dry.avg_pickups_per_hour - 1), 2) AS pct_change_vs_dry
        FROM joined j LEFT JOIN (SELECT borough, hour_band, avg_pickups_per_hour FROM joined
                                 WHERE weather_condition = 'dry') dry USING (borough, hour_band)
    """,
    # Fare / distance by borough and month
    "borough_summary": """
        SELECT pickup_borough AS borough, date_format(pickup_ts, 'yyyy-MM') AS month, COUNT(*) AS trips,
               AVG(total_amount) AS avg_fare, AVG(trip_distance) AS avg_distance, SUM(total_amount) AS revenue,
               AVG(CASE WHEN payment_type = 1 THEN 1D ELSE 0D END) AS card_share
        FROM trips GROUP BY pickup_borough, date_format(pickup_ts, 'yyyy-MM')
    """,
    # Which zones are unpredictable?
    "zone_volatility": """
        SELECT zone_id, zone_name, borough, AVG(trip_count) AS mean_pickups, STDDEV(trip_count) AS std_pickups,
               STDDEV(trip_count) / NULLIF(AVG(trip_count), 0) AS coef_of_variation,
               percentile_approx(trip_count, 0.5) AS p50, percentile_approx(trip_count, 0.99) AS p99,
               percentile_approx(trip_count, 0.99) / NULLIF(percentile_approx(trip_count, 0.5), 0) AS p99_p50_ratio
        FROM demand GROUP BY zone_id, zone_name, borough
    """,
    # Where do trips go?
    "od_flows": f"""
        SELECT pickup_borough, dropoff_borough, {HOUR_BAND_SQL.format(h='hour_of_day')} AS hour_band, COUNT(*) AS trips
        FROM trips GROUP BY pickup_borough, dropoff_borough, {HOUR_BAND_SQL.format(h='hour_of_day')}
    """,
}


def run_analytics(spark: SparkSession, cfg: Config, months: list[str]) -> dict[str, int]:
    start, end = month_range(months)
    read_parquet(spark, cfg.path("silver") / "trips_clean", "silver").where(
        (F.col("pickup_date") >= F.lit(start.date())) & (F.col("pickup_date") < F.lit(end.date()))
    ).createOrReplaceTempView("trips")
    read_parquet(spark, cfg.path("silver") / "zone_hour_demand", "demand") \
        .where(F.col("month").isin(months)).createOrReplaceTempView("demand")
    read_parquet(spark, cfg.path("silver") / "weather_hourly", "silver") \
        .where(F.col("month").isin(months)).createOrReplaceTempView("weather")

    counts = {}
    for name, sql in QUERIES.items():
        df = spark.sql(sql)
        write_parquet(df.coalesce(1), cfg.path("gold") / "analytics" / name, replace_all=True)
        counts[name] = spark.read.parquet(str(cfg.path("gold") / "analytics" / name)).count()
        log.info("gold/analytics/%s: %d rows", name, counts[name])
    return counts
