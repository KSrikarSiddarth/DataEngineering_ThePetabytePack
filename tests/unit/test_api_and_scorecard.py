import sqlite3

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from mobilitypulse.benchmark.scorecard import evaluate, render
from mobilitypulse.serving.api import create_app


@pytest.fixture()
def client(tmp_path):
    db = tmp_path / "serving.db"
    with sqlite3.connect(db) as con:
        pd.DataFrame({"zone_id": [161, 237], "borough": ["Manhattan"] * 2, "zone_name": ["Midtown", "UES"],
                      "service_zone": ["Yellow Zone"] * 2}).to_sql("zones", con, index=False)
        pd.DataFrame({"zone_id": [161] * 3, "zone_name": ["Midtown"] * 3, "borough": ["Manhattan"] * 3,
                      "window_start": ["2024-01-15 07:00:00", "2024-01-15 08:00:00", "2024-01-15 09:00:00"],
                      "trip_count": [100, 300, 250], "dropoff_count": [90, 200, 260], "avg_fare": [18.0] * 3,
                      "avg_trip_distance": [2.0] * 3, "avg_duration_min": [12.0] * 3, "temp_c": [1.0] * 3,
                      "precip_mm": [0.0] * 3, "weather_condition": ["dry"] * 3}).to_sql("zone_hour_demand_recent", con, index=False)
        pd.DataFrame({"zone_id": [161], "borough": ["Manhattan"], "target_hour": ["2024-01-15 09:00:00"],
                      "actual": [250.0], "predicted": [240.0], "residual": [10.0], "model": ["gbt"]}).to_sql("forecasts", con, index=False)
        pd.DataFrame({"zone_id": [161, 237], "window_start": ["2024-01-15 09:00:00", "2024-01-16 03:00:00"],
                      "residual_z": [7.0, -3.5], "severity": ["high", "low"]}).to_sql("anomalies", con, index=False)
        pd.DataFrame({"model": ["gbt"], "split": ["test"], "segment": ["overall"], "mae": [4.2]}).to_sql("model_metrics", con, index=False)
    return TestClient(create_app(db))


def test_health_lists_tables(client):
    body = client.get("/health").json()
    assert body["status"] == "ok" and body["tables"]["zones"] == 2


def test_demand_window_filter(client):
    rows = client.get("/demand", params={"zone_id": 161, "start": "2024-01-15T08:00:00", "end": "2024-01-15T09:00:00"}).json()
    assert [r["trip_count"] for r in rows] == [300]


def test_anomalies_filter_and_order(client):
    rows = client.get("/anomalies").json()
    assert [r["zone_id"] for r in rows] == [161, 237]
    assert [r["zone_id"] for r in client.get("/anomalies", params={"severity": "low"}).json()] == [237]


def test_input_validation_rejects_bad_params(client):
    assert client.get("/demand", params={"zone_id": 999}).status_code == 422
    assert client.get("/demand", params={"zone_id": "1; DROP TABLE zones"}).status_code == 422
    assert client.get("/anomalies", params={"severity": "critical"}).status_code == 422
    assert client.get("/analytics/zones").status_code == 404


def test_forecast_and_metrics(client):
    assert client.get("/forecast", params={"zone_id": 161}).json()[0]["predicted"] == 240.0
    assert client.get("/metrics/latest").json()["forecast"][0]["mae"] == 4.2


def test_missing_db_returns_503(tmp_path):
    c = TestClient(create_app(tmp_path / "nope.db"))
    assert c.get("/health").json()["status"] == "no_db"
    assert c.get("/zones").status_code == 503


def test_scorecard_evaluation():
    targets = [
        {"id": "X1", "area": "a", "metric": "m", "key": "k1", "op": "<=", "threshold": 5, "unit": "s"},
        {"id": "X2", "area": "a", "metric": "m", "key": "k2", "op": ">=", "threshold": 0.9, "unit": ""},
        {"id": "X3", "area": "a", "metric": "m", "key": "k3", "op": "==", "threshold": 0, "unit": ""},
        {"id": "X4", "area": "a", "metric": "m", "key": "missing", "op": "<=", "threshold": 1, "unit": ""},
        {"id": "X5", "area": "a", "metric": "m", "key": "k1", "op": "info", "threshold": None, "unit": ""},
    ]
    meas = {"k1": {"value": 4.0}, "k2": {"value": 0.8}, "k3": {"value": 0.0}}
    status = [r["status"] for r in evaluate(targets, meas)]
    assert status == ["PASS", "FAIL", "PASS", "NOT RUN", "INFO"]
    assert "PASS 2 | FAIL 1 | NOT RUN 1 | INFO 1" in render(evaluate(targets, meas))
