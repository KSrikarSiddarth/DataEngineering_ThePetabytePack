"""FastAPI service over the serving SQLite DB. Run: uvicorn mobilitypulse.serving.api:app --port 8000"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Literal

from fastapi import FastAPI, HTTPException, Query

from ..common.config import load_config
from .queries import ANALYTICS_TABLES, Queries, ts


def create_app(db_path: Path | None = None) -> FastAPI:
    db = Path(db_path or load_config().path("serving_db"))
    q = Queries(db)
    app = FastAPI(title="NYC MobilityPulse API", version="0.1.0",
                  description="Zone-level taxi demand, next-hour forecasts, anomalies and live streaming metrics.")

    def require_db() -> None:
        if not db.exists():
            raise HTTPException(503, "Serving DB not built yet - run `make export`")

    @app.get("/health")
    def health():
        return {"status": "ok" if db.exists() else "no_db", "tables": q.tables() if db.exists() else {}}

    @app.get("/zones")
    def zones(borough: str | None = Query(None, max_length=40)):
        require_db()
        return q.zones(borough)

    @app.get("/demand")
    def demand(zone_id: int = Query(..., ge=1, le=265), start: datetime | None = None, end: datetime | None = None,
               limit: int = Query(1000, ge=1, le=5000)):
        require_db()
        return q.demand(zone_id, ts(start), ts(end), limit)

    @app.get("/forecast")
    def forecast(zone_id: int = Query(..., ge=1, le=265), start: datetime | None = None, end: datetime | None = None,
                 limit: int = Query(1000, ge=1, le=5000)):
        require_db()
        return q.forecast(zone_id, ts(start), ts(end), limit)

    @app.get("/anomalies")
    def anomalies(start: datetime | None = None, end: datetime | None = None,
                  severity: Literal["low", "medium", "high"] | None = None,
                  zone_id: int | None = Query(None, ge=1, le=265), limit: int = Query(200, ge=1, le=5000)):
        require_db()
        return q.anomalies(ts(start), ts(end), severity, zone_id, limit)

    @app.get("/metrics/latest")
    def metrics(split: Literal["validation", "test"] = "test"):
        require_db()
        return {"forecast": q.model_metrics(split), "anomaly": q.anomaly_metrics()}

    @app.get("/stream/latest")
    def stream_latest(zone_id: int | None = Query(None, ge=1, le=265)):
        require_db()
        return q.stream_latest(zone_id)

    @app.get("/analytics/{name}")
    def analytics(name: str, limit: int = Query(500, ge=1, le=5000)):
        require_db()
        if name not in ANALYTICS_TABLES:
            raise HTTPException(404, f"unknown analytics table; choose one of {sorted(ANALYTICS_TABLES)}")
        return q.analytics(name, limit)

    return app


app = create_app()
