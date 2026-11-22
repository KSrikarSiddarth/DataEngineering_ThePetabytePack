"""Streamlit dashboard. Run: streamlit run src/mobilitypulse/serving/dashboard.py"""
from __future__ import annotations

import json

import pandas as pd
import plotly.express as px
import streamlit as st

from mobilitypulse.common.config import load_config
from mobilitypulse.serving.queries import Queries

DAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

st.set_page_config(page_title="NYC MobilityPulse", layout="wide")
cfg = load_config()
db = cfg.path("serving_db")
results = cfg.path("results")

st.title("NYC MobilityPulse")
st.caption("Zone-level taxi demand, next-hour forecasts, anomalies and live stream metrics - The Petabyte Pack")

if not db.exists():
    st.error(f"Serving DB not found at {db}. Run `make export` first.")
    st.stop()

q = Queries(db)
tables = q.tables()
zones = pd.DataFrame(q.zones())
zone_label = {r.zone_id: f"{r.zone_id} - {r.zone_name} ({r.borough})" for r in zones.itertuples()}

page = st.sidebar.radio("View", ["Overview", "Demand heatmap", "Forecast vs actual", "Anomalies", "Weather impact",
                                 "Live stream", "Benchmarks & scorecard"])


def pick_zone(key: str, candidates: list[int]) -> int:
    return st.sidebar.selectbox("Zone", candidates, format_func=lambda z: zone_label.get(z, str(z)), key=key)


if page == "Overview":
    c1, c2, c3 = st.columns(3)
    metrics = pd.DataFrame(q.model_metrics("test")) if "model_metrics" in tables else pd.DataFrame()
    if not metrics.empty:
        best = metrics.iloc[0]
        c1.metric("Best model (test MAE)", f"{best['model']}", f"{best['mae']:.2f} trips/zone-hour")
        c2.metric("WAPE", f"{best['wape_pct']:.1f}%")
        c3.metric("Anomalies flagged", tables.get("anomalies", 0))
        st.subheader("Forecast models on the held-out test period")
        st.dataframe(metrics[["model", "n_rows", "mae", "rmse", "wape_pct", "mape_pct", "r2", "train_seconds"]],
                     width="stretch")
    if "top_zones_daily" in tables:
        top = pd.DataFrame(q.analytics("top_zones_daily", 5000))
        day = st.selectbox("Day", sorted(top["date"].unique(), reverse=True))
        st.subheader(f"Top zones on {day}")
        st.bar_chart(top[top["date"] == day].set_index("zone_name")["trips"])

elif page == "Demand heatmap":
    z = pick_zone("heat", q.busiest_zones())
    prof = pd.DataFrame(q.demand_profile(z))
    grid = prof.pivot_table(index="day_of_week", columns="hour_of_day", values="avg_pickups")
    grid.index = [DAYS[i - 1] for i in grid.index]
    st.plotly_chart(px.imshow(grid, aspect="auto", labels=dict(x="Hour of day", y="Day", color="Avg pickups"),
                              title=f"Average pickups - {zone_label.get(z)}"), width="stretch")

elif page == "Forecast vs actual":
    fz = q.forecast_zones() if "forecasts" in tables else []
    if not fz:
        st.info("Run the ML stage first.")
    else:
        z = pick_zone("fc", fz)
        fc = pd.DataFrame(q.forecast(z, None, None, 5000))
        fc["target_hour"] = pd.to_datetime(fc["target_hour"])
        st.plotly_chart(px.line(fc, x="target_hour", y=["actual", "predicted"],
                                title=f"Next-hour forecast ({fc['model'].iloc[0]}) - {zone_label.get(z)}"),
                        width="stretch")
        st.caption(f"MAE for this zone: {fc['residual'].abs().mean():.2f}")

elif page == "Anomalies":
    sev = st.sidebar.multiselect("Severity", ["high", "medium", "low"], default=["high", "medium"])
    an = pd.DataFrame(q.anomalies(limit=5000))
    if an.empty:
        st.info("No anomalies yet - run the anomaly stage.")
    else:
        an = an[an["severity"].isin(sev)]
        st.metric("Anomalous zone-hours", len(an))
        st.plotly_chart(px.scatter(an, x="window_start", y="residual_z", color="severity", hover_data=[
            "zone_name", "actual", "expected", "detectors_fired", "weather_condition"]), width="stretch")
        st.dataframe(an, width="stretch")
    am = pd.DataFrame(q.anomaly_metrics())
    if not am.empty:
        st.subheader("Detector evaluation")
        st.dataframe(am, width="stretch")

elif page == "Weather impact":
    wi = pd.DataFrame(q.analytics("weather_impact", 5000))
    st.plotly_chart(px.bar(wi, x="hour_band", y="avg_pickups_per_hour", color="weather_condition", barmode="group",
                           facet_col="borough", facet_col_wrap=3, title="Pickups per hour by weather"),
                    width="stretch")
    st.dataframe(wi, width="stretch")

elif page == "Live stream":
    st.button("Refresh")
    live = pd.DataFrame(q.stream_latest())
    if live.empty:
        st.info("No streaming output exported yet. Start Kafka + stream job + producer, then `make export`.")
    else:
        newest = live["window_start"].max()
        st.subheader(f"Latest window starting {newest}")
        cur = live[live["window_start"] == newest].sort_values("trip_count", ascending=False).head(25)
        st.bar_chart(cur.set_index("zone_name")["trip_count"])
        st.dataframe(live, width="stretch")
    lat = results / "stream" / "batches.csv"
    if lat.exists():
        b = pd.read_csv(lat)
        st.subheader("Micro-batch latency")
        st.line_chart(b.set_index("batch_id")[["lat_p50_ms", "lat_p95_ms"]].tail(300))

elif page == "Benchmarks & scorecard":
    sc = results / "evaluation_scorecard.md"
    if sc.exists():
        st.markdown(sc.read_text())
    bench = results / "benchmarks"
    for f in sorted(bench.glob("*.csv")) if bench.exists() else []:
        df = pd.read_csv(f)
        st.subheader(f.stem)
        if {"variant", "seconds"} <= set(df.columns):
            st.plotly_chart(px.box(df, x="variant", y="seconds", points="all"), width="stretch")
        st.dataframe(df, width="stretch")
    m = results / "measurements.json"
    if m.exists():
        with st.expander("Raw measurements"):
            st.json(json.loads(m.read_text()))
