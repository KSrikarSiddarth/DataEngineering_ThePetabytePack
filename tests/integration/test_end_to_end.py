"""End-to-end: synthetic raw data -> bronze -> silver -> demand -> features -> analytics -> ML -> anomalies -> SQLite -> API."""
import json

import pytest
from fastapi.testclient import TestClient
from pyspark.sql import functions as F

from mobilitypulse.batch.analytics import run_analytics
from mobilitypulse.batch.bronze import run_bronze
from mobilitypulse.batch.demand import build_zone_hour_demand
from mobilitypulse.batch.features import build_features
from mobilitypulse.batch.silver import run_silver
from mobilitypulse.common.utils import load_measurements
from mobilitypulse.ingestion.sample import generate_sample
from mobilitypulse.ml.anomaly import run_anomaly
from mobilitypulse.ml.train import train_and_evaluate
from mobilitypulse.serving.api import create_app
from mobilitypulse.serving.export import export_serving_db

pytestmark = pytest.mark.integration
MONTHS = ["2024-01"]


@pytest.fixture(scope="module")
def pipeline(spark, base_cfg):
    cfg = base_cfg
    summary = generate_sample(cfg, MONTHS)
    bronze = run_bronze(spark, cfg, MONTHS)
    silver = run_silver(spark, cfg, MONTHS)
    demand_rows = build_zone_hour_demand(spark, cfg, MONTHS)
    splits = build_features(spark, cfg, MONTHS)
    analytics = run_analytics(spark, cfg, MONTHS)
    ml = train_and_evaluate(spark, cfg)
    anomaly = run_anomaly(spark, cfg)
    counts = export_serving_db(cfg)
    return dict(cfg=cfg, summary=summary, bronze=bronze, silver=silver, demand_rows=demand_rows, splits=splits,
                analytics=analytics, ml=ml, anomaly=anomaly, counts=counts)


def test_bronze_keeps_every_raw_row(pipeline):
    assert pipeline["bronze"]["trips"] == pipeline["summary"]["trips"]["2024-01"]
    assert pipeline["bronze"]["zones"] == 265


def test_reconciliation_and_quality(pipeline):
    rep = pipeline["silver"]["dq"]["2024-01"]
    assert rep["reconciliation_diff"] == 0
    assert rep["written_valid_rows"] + rep["written_rejected_rows"] == rep["input_rows"]
    for reason in ["FARE_OUT_OF_RANGE", "INVALID_ZONE", "DISTANCE_OUT_OF_RANGE", "NEGATIVE_DURATION",
                   "OUTSIDE_SOURCE_MONTH", "DURATION_OUT_OF_RANGE", "DUPLICATE"]:
        assert rep["rejects_by_reason"].get(reason, 0) > 0, reason
    m = load_measurements(pipeline["cfg"])
    assert m["dq.max_reconciliation_diff"]["value"] == 0
    assert m["dq.silver_duplicates"]["value"] == 0
    assert m["dq.demand_reconciliation_diff"]["value"] == 0


def test_dense_zone_hour_grid(pipeline, spark):
    assert pipeline["demand_rows"] == 263 * 31 * 24
    w = spark.read.parquet(str(pipeline["cfg"].path("silver") / "weather_hourly"))
    assert w.count() == 31 * 24 and w.where(F.col("temp_c").isNull()).count() <= 2


def test_feature_splits_are_time_ordered(pipeline, spark):
    assert set(pipeline["splits"]) == {"train", "validation", "test"}
    f = spark.read.parquet(str(pipeline["cfg"].path("silver") / "zone_hour_features"))
    b = {r["split"]: (r["lo"], r["hi"]) for r in
         f.groupBy("split").agg(F.min("window_start").alias("lo"), F.max("window_start").alias("hi")).collect()}
    assert b["train"][1] < b["validation"][0] <= b["validation"][1] < b["test"][0]


def test_models_beat_naive_baselines(pipeline):
    ml = pipeline["ml"]
    assert ml["best_model"] in ("linear_regression", "gbt", "ensemble")
    assert ml["ml.mae_improvement_vs_persistence_pct"] > 0
    assert ml["ml.best_r2"] > 0.3


def test_anomaly_detector_finds_injected_events(pipeline):
    inj = pipeline["anomaly"]["injected:residual_flag"]
    assert inj["tp"] + inj["fn"] > 0
    assert inj["recall"] > 0.5


def test_serving_db_and_api(pipeline):
    counts = pipeline["counts"]
    for table in ["zones", "demand_profile", "forecasts", "anomalies", "model_metrics", "zone_hour_demand_recent"]:
        assert counts.get(table, 0) > 0, table
    client = TestClient(create_app(pipeline["cfg"].path("serving_db")))
    zone = client.get("/forecast", params={"zone_id": 1}).json()
    assert isinstance(zone, list)
    metrics = client.get("/metrics/latest").json()
    assert {m["model"] for m in metrics["forecast"]} >= {"persistence", "seasonal_naive", "gbt"}
    assert json.loads((pipeline["cfg"].path("models") / "best_model.json").read_text())["model"]
