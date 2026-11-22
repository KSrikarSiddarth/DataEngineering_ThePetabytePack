"""Read-only, parameterised SQLite queries shared by the FastAPI service and the Streamlit dashboard."""
from __future__ import annotations

import sqlite3
from contextlib import closing
from datetime import datetime
from pathlib import Path
from typing import Any

ANALYTICS_TABLES = {"demand_profile", "top_zones_daily", "weather_impact", "borough_summary", "zone_volatility",
                    "od_flows"}
MAX_ROWS = 5000


def connect(db_path: Path) -> sqlite3.Connection:
    con = sqlite3.connect(f"{Path(db_path).resolve().as_uri()}?mode=ro", uri=True, check_same_thread=False)
    con.row_factory = sqlite3.Row
    return con


def ts(value: datetime | str | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        value = datetime.fromisoformat(value)
    return value.strftime("%Y-%m-%d %H:%M:%S")


class Queries:
    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)

    def _rows(self, sql: str, params: tuple = ()) -> list[dict[str, Any]]:
        with closing(connect(self.db_path)) as con:
            return [dict(r) for r in con.execute(sql, params).fetchall()]

    def tables(self) -> dict[str, int]:
        names = [r["name"] for r in self._rows("SELECT name FROM sqlite_master WHERE type = 'table'")]
        return {n: self._rows(f'SELECT COUNT(*) AS n FROM "{n}"')[0]["n"] for n in names}

    def has_table(self, name: str) -> bool:
        return bool(self._rows("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (name,)))

    def zones(self, borough: str | None = None) -> list[dict]:
        if borough:
            return self._rows("SELECT * FROM zones WHERE borough = ? ORDER BY zone_id", (borough,))
        return self._rows("SELECT * FROM zones ORDER BY zone_id")

    def demand(self, zone_id: int, start: str | None, end: str | None, limit: int = MAX_ROWS) -> list[dict]:
        return self._rows(
            "SELECT zone_id, zone_name, borough, window_start, trip_count, dropoff_count, avg_fare, "
            "avg_trip_distance, avg_duration_min, temp_c, precip_mm, weather_condition "
            "FROM zone_hour_demand_recent WHERE zone_id = ? AND window_start >= COALESCE(?, '') "
            "AND window_start < COALESCE(?, '9999') ORDER BY window_start LIMIT ?",
            (zone_id, start, end, min(limit, MAX_ROWS)))

    def forecast(self, zone_id: int, start: str | None, end: str | None, limit: int = MAX_ROWS) -> list[dict]:
        return self._rows(
            "SELECT zone_id, borough, target_hour, actual, predicted, residual, model FROM forecasts "
            "WHERE zone_id = ? AND target_hour >= COALESCE(?, '') AND target_hour < COALESCE(?, '9999') "
            "ORDER BY target_hour LIMIT ?", (zone_id, start, end, min(limit, MAX_ROWS)))

    def anomalies(self, start: str | None = None, end: str | None = None, severity: str | None = None,
                  zone_id: int | None = None, limit: int = 500) -> list[dict]:
        return self._rows(
            "SELECT * FROM anomalies WHERE window_start >= COALESCE(?, '') AND window_start < COALESCE(?, '9999') "
            "AND (? IS NULL OR severity = ?) AND (? IS NULL OR zone_id = ?) "
            "ORDER BY ABS(residual_z) DESC LIMIT ?",
            (start, end, severity, severity, zone_id, zone_id, min(limit, MAX_ROWS)))

    def model_metrics(self, split: str = "test", segment: str = "overall") -> list[dict]:
        return self._rows("SELECT * FROM model_metrics WHERE split = ? AND segment = ? ORDER BY mae",
                          (split, segment))

    def anomaly_metrics(self) -> list[dict]:
        return self._rows("SELECT * FROM anomaly_metrics") if self.has_table("anomaly_metrics") else []

    def stream_latest(self, zone_id: int | None = None) -> list[dict]:
        if not self.has_table("stream_latest"):
            return []
        return self._rows(
            "SELECT zone_id, zone_name, borough, window_start, window_end, trip_count, avg_fare, avg_distance, "
            "updated_at FROM stream_latest WHERE (? IS NULL OR zone_id = ?) ORDER BY window_start DESC, trip_count DESC "
            "LIMIT ?", (zone_id, zone_id, MAX_ROWS))

    def analytics(self, name: str, limit: int = 1000) -> list[dict]:
        if name not in ANALYTICS_TABLES:
            raise KeyError(name)
        return self._rows(f'SELECT * FROM "{name}" LIMIT ?', (min(limit, MAX_ROWS),))

    def demand_profile(self, zone_id: int) -> list[dict]:
        return self._rows("SELECT * FROM demand_profile WHERE zone_id = ?", (zone_id,))

    def busiest_zones(self, limit: int = 100) -> list[int]:
        return [r["zone_id"] for r in self._rows(
            "SELECT zone_id FROM demand_profile GROUP BY zone_id ORDER BY AVG(avg_pickups) DESC LIMIT ?", (limit,))]

    def forecast_zones(self) -> list[int]:
        return [r["zone_id"] for r in self._rows("SELECT DISTINCT zone_id FROM forecasts ORDER BY zone_id")]
