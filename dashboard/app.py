"""Dashboard: Streamlit over the batch and stream Gold marts and the Spark ML results.

    streamlit run dashboard/app.py                  # output folder: $DASHBOARD_OUTPUT, else ./output

Nothing is copied: DuckDB reads the Parquet files in place through the views of scripts/combined_views.py, and every
KPI page runs one of the SQL files in scripts/queries/ unchanged. A page whose data does not exist yet says so.
"""
import os
import sys
from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
from combined_views import default_cutover, duckdb_views, has_parquet  # noqa: E402
from evaluate_models import evaluate, latest_metrics  # noqa: E402

DIMS = ("dim_zone", "dim_hvfhs_company")
# ML tables with a numeric label: model -> (table, label column)
PREDICTIONS = {"demand_forecast": ("ml_demand_forecast", "trips"), "fare": ("ml_fare_predictions", "base_fare"),
               "duration": ("ml_duration_predictions", "duration_min"), "wait": ("ml_wait_predictions", "wait_sec")}


@st.cache_resource
def connection(out, cutover):
    """One in-memory DuckDB with the batch tables, the combined_<mart> views and the dimension tables."""
    con = duckdb.connect()
    duckdb_views(con, Path(out) / "batch", Path(out) / "stream", cutover)
    for dim in DIMS:
        folder = Path(out) / "batch" / "silver" / dim
        if has_parquet(folder):
            con.execute(f"CREATE VIEW {dim} AS SELECT * FROM read_parquet('{folder.resolve().as_posix()}/*.parquet')")
    return con


def sql(out, cutover, text):
    """Run SQL on the views and return a DataFrame, or None when a table it needs does not exist yet."""
    try:
        return connection(out, cutover).cursor().execute(text).df()
    except duckdb.Error:
        return None


@st.cache_data(show_spinner=False)
def kpi(out, cutover, name):
    """Result of scripts/queries/<name>.sql (the streamed-day cutover date is filled in)."""
    text = (ROOT / "scripts" / "queries" / f"{name}.sql").read_text(encoding="utf-8")
    return sql(out, cutover, text.replace("{cutover}", cutover))


def ml_table(out, table):
    """SQL fragment reading a gold ML table (None when it has not been written)."""
    folder = Path(out) / "batch" / "gold" / table
    return f"read_parquet('{folder.resolve().as_posix()}/*.parquet')" if has_parquet(folder) else None


def has_rows(df, message="No data for this page yet. Run the batch job (and the stream job for the live page)."):
    """True when `df` has rows; otherwise show `message`."""
    if df is None or df.empty:
        st.info(message)
        return False
    return True


def by_month(df):
    """Add a month_start date column from year and month."""
    return df.assign(month_start=pd.to_datetime(dict(year=df["year"], month=df["month"], day=1)))


def pick(df, column, label):
    """A selectbox over the distinct values of a column; returns the rows with that value."""
    value = st.selectbox(label, sorted(df[column].dropna().unique()))
    return df[df[column] == value]


def trends_page(out, cut):
    """Trips by month, service share by year and the growth table (trips_growth, service_share_shift)."""
    growth, share = kpi(out, cut, "trips_growth"), kpi(out, cut, "service_share_shift")
    if has_rows(growth):
        st.subheader("Trips per month")
        st.line_chart(by_month(growth), x="month_start", y="trips", color="service_type")
        st.dataframe(growth, hide_index=True)
    if has_rows(share, "No share data."):
        st.subheader("Share of trips per year")
        st.bar_chart(share, x="year", y="share_of_year", color="service_type")


def revenue_page(out, cut):
    """Fare, fare per mile and card share by month, and the fee-impact comparison (revenue_trends, fee_impact)."""
    rev, fees = kpi(out, cut, "revenue_trends"), kpi(out, cut, "fee_impact")
    if has_rows(rev):
        rev = by_month(rev)
        for col, title in (("avg_fare", "Average fare ($)"), ("fare_per_mile", "Fare per mile ($)"),
                           ("card_share", "Card share of trips"), ("card_tip_rate", "Card tip rate")):
            st.subheader(title)
            st.line_chart(rev, x="month_start", y=col, color="service_type")
    if has_rows(fees, "No fee-impact data."):
        st.subheader("Fee impact (before and after a policy)")
        st.dataframe(fees, hide_index=True)


def efficiency_page(out, cut):
    """Speed and duration by hour of day, and the peak-hour shift for one service (efficiency_trends, peak_hour_shift)."""
    eff, peak = kpi(out, cut, "efficiency_trends"), kpi(out, cut, "peak_hour_shift")
    if not has_rows(eff):
        return
    service = st.selectbox("Service", sorted(eff["service_type"].unique()))
    for df, col, title in ((eff, "avg_speed_mph", "Average speed by hour (mph)"),
                           (eff, "avg_duration_sec", "Average duration by hour (s)"),
                           (peak, "share", "Share of the day's trips by hour")):
        if df is not None:
            st.subheader(title)
            st.line_chart(df[df["service_type"] == service].assign(year=lambda d: d["year"].astype(str)),
                          x="pickup_hour", y=col, color="year")


def zones_page(out, cut):
    """Zones with the largest change in trips between years (zone_growth), with names from dim_zone."""
    zones = kpi(out, cut, "zone_growth")
    if not has_rows(zones):
        return
    names = sql(out, cut, "SELECT location_id AS pu_location_id, borough, zone FROM dim_zone")
    if names is not None:
        zones = zones.merge(names, on="pu_location_id", how="left")
    service = pick(zones, "service_type", "Service")
    year = pick(service, "year", "Year").dropna(subset=["change"])
    st.subheader("Largest changes in trips versus the prior year")
    cols = [c for c in ("zone", "borough", "pu_location_id", "trips", "change", "change_pct") if c in year]
    st.dataframe(year.reindex(year["change"].abs().sort_values(ascending=False).index)[cols].head(25), hide_index=True)


def hvfhv_page(out, cut):
    """Driver pay share by company and month (driver_pay_trend)."""
    pay = kpi(out, cut, "driver_pay_trend")
    if has_rows(pay, "No HVFHV data loaded."):
        st.subheader("Driver pay as a share of the base fare")
        st.line_chart(by_month(pay), x="month_start", y="driver_pay_share", color="hvfhs_license_num")


def health_page(out, cut):
    """Pipeline health, data-quality drift and the per-file load times (batch_health, data_quality_drift)."""
    health, quality = kpi(out, cut, "batch_health"), kpi(out, cut, "data_quality_drift")
    if has_rows(health):
        row = health.iloc[0]
        cols = st.columns(4)
        cols[0].metric("Success rate", f"{row['success_rate']:.0%}")
        cols[1].metric("Backfill complete", f"{row['backfill_completeness']:.0%}")
        cols[2].metric("Reconciliation diff (should be 0)", f"{row['reconciliation_abs_diff']:,.0f}")
        cols[3].metric("Rows per second", f"{row['rows_per_sec']:,.0f}")
    runs = sql(out, cut, "SELECT source_file, status, bronze_rows, ROUND(runtime_sec) AS runtime_sec, "
                         "ROUND(rows_per_sec) AS rows_per_sec FROM batch_run_metrics ORDER BY runtime_sec DESC")
    if has_rows(runs, "No load metrics."):
        st.subheader("Slowest file loads first")
        st.bar_chart(runs, x="source_file", y="runtime_sec")
        st.dataframe(runs, hide_index=True)
    if has_rows(quality, "No data-quality data."):
        st.subheader("Data-quality drift by month")
        st.dataframe(quality, hide_index=True)


def live_page(out, cut):
    """Streamed hours against the batch baseline for one service and day (live_vs_baseline)."""
    live = kpi(out, cut, "live_vs_baseline")
    if not has_rows(live, "No streamed days yet. Run the stream job; the batch range must end before the cutover date."):
        return
    day = pick(pick(live, "service_type", "Service"), "pickup_date", "Day")
    st.line_chart(day, x="pickup_hour", y=["live_trips", "baseline_trips"])


def models_page(out, cut):
    """Model targets, lift over baseline, error by month, predictions, clusters and anomalies of the latest ML run."""
    if not has_parquet(Path(out) / "batch" / "gold" / "ml_model_metrics"):
        st.info("No ML results yet. Run spark/ml_job.py on the batch store.")
        return
    metrics, run = latest_metrics(Path(out) / "batch")
    st.caption(f"Latest run {run}; targets from evaluation.md")
    results = pd.DataFrame([{"target": n, "measured": v, "status": "n/a" if ok is None else "PASS" if ok else "FAIL"}
                            for n, v, ok in evaluate(metrics)])
    st.dataframe(results, hide_index=True, use_container_width=True)

    lift = pd.DataFrame([{"model": m, "lift": v} for (m, k, s), v in metrics.items() if k == "lift" and s == "test"])
    if not lift.empty:
        st.subheader("Lift over the baseline (positive = better than the simple rule)")
        st.bar_chart(lift, x="model", y="lift")

    st.subheader("Error by month and predictions")
    model = st.selectbox("Model", list(PREDICTIONS))
    table, label = PREDICTIONS[model]
    source = ml_table(out, table)
    if source:
        months = sql(out, cut, f"SELECT ym, AVG(ABS(error)) AS model_mae, AVG(ABS(baseline - {label})) AS baseline_mae "
                               f"FROM {source} GROUP BY ym ORDER BY ym")
        st.bar_chart(months, x="ym", y=["model_mae", "baseline_mae"], stack=False)
        sample = sql(out, cut, f"SELECT {label} AS actual, prediction FROM {source} USING SAMPLE 2000 ROWS")
        st.scatter_chart(sample, x="actual", y="prediction")

    zones, patterns, anomalies = (ml_table(out, t) for t in ("ml_zone_clusters", "ml_demand_pattern_clusters",
                                                              "ml_trip_anomalies"))
    if zones:
        st.subheader("Zone clusters")
        st.dataframe(sql(out, cut, f"SELECT cluster, COUNT(*) AS zones, ROUND(AVG(avg_distance), 2) AS avg_distance, "
                                   f"ROUND(AVG(avg_fare), 2) AS avg_fare FROM {zones} GROUP BY cluster ORDER BY cluster"),
                     hide_index=True)
    if patterns:
        st.subheader("Daily demand shapes: days per cluster and weekday (1 = Sunday)")
        st.dataframe(sql(out, cut, f"SELECT cluster, day_of_week, COUNT(*) AS days FROM {patterns} "
                                   f"GROUP BY ALL ORDER BY cluster, day_of_week").pivot(
                                       index="cluster", columns="day_of_week", values="days").fillna(0).astype(int))
    if anomalies:
        st.subheader("Largest fare anomalies flagged in real trips")
        st.dataframe(sql(out, cut, f"SELECT * FROM {anomalies} ORDER BY ABS(residual) DESC LIMIT 20"), hide_index=True)


PAGES = {"Trends": trends_page, "Revenue": revenue_page, "Efficiency": efficiency_page, "Zones": zones_page,
         "HVFHV": hvfhv_page, "Quality and health": health_page, "Live vs baseline": live_page,
         "ML models": models_page}


def main():
    """Page chrome: the output folder and cutover in the sidebar, then the selected page."""
    st.set_page_config(page_title="NYC Mobility", layout="wide")
    st.title("NYC taxi mobility")
    out = st.sidebar.text_input("Output folder", os.environ.get("DASHBOARD_OUTPUT", "output"))
    cut = st.sidebar.text_input("Cutover date (batch before, stream from)", default_cutover())
    if st.sidebar.button("Reload data"):
        st.cache_data.clear()
        st.cache_resource.clear()
    page = st.sidebar.radio("Page", list(PAGES))
    PAGES[page](out, cut)


main()
