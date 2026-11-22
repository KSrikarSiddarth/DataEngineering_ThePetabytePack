"""GOLD -> SQLite serving database (small, read-optimised copy for the API and dashboard; no Spark at request time)."""
from __future__ import annotations

import json
import os
import sqlite3
import time
from contextlib import closing
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

import pandas as pd
import pyarrow.dataset as ds

from ..common.config import Config
from ..common.storage import AnyPath, arrow_location, read_parquet_pandas
from ..common.utils import get_logger

log = get_logger(__name__)

ANALYTICS = ["demand_profile", "top_zones_daily", "weather_impact", "borough_summary", "zone_volatility", "od_flows"]
INDEXES = {
    "zone_hour_demand_recent": ["zone_id", "window_start"],
    "forecasts": ["zone_id", "target_hour"],
    "anomalies": ["window_start"],
    "demand_profile": ["zone_id"],
    "top_zones_daily": ["date"],
    "stream_latest": ["zone_id"],
    "data_quality_daily": ["date"],
}


def _clean(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in df.columns:
        s = df[col]
        if isinstance(s.dtype, pd.CategoricalDtype):
            df[col] = s.astype(str)
        elif isinstance(s.dtype, pd.DatetimeTZDtype):
            df[col] = s.dt.tz_convert("UTC").dt.tz_localize(None).dt.strftime("%Y-%m-%d %H:%M:%S")
        elif pd.api.types.is_datetime64_any_dtype(s):
            df[col] = s.dt.strftime("%Y-%m-%d %H:%M:%S")
        elif s.dtype == object and len(s) and s.map(lambda v: isinstance(v, Decimal)).any():
            df[col] = s.map(lambda v: float(v) if isinstance(v, Decimal) else v)
        elif s.dtype == object and len(s) and s.map(lambda v: hasattr(v, "__len__") and not isinstance(v, str)).any():
            df[col] = s.map(lambda v: ",".join(map(str, v)) if v is not None and not isinstance(v, str) else v)
        elif s.dtype == object and len(s) and s.map(lambda v: hasattr(v, "isoformat")).any():
            df[col] = s.map(lambda v: v.isoformat() if hasattr(v, "isoformat") else v)
    return df


def _read(path: AnyPath, filter_expr=None) -> pd.DataFrame | None:
    if not path.exists():
        return None
    fs, location = arrow_location(path)  # fs is None for a local folder, the MinIO filesystem for s3a://
    dataset = ds.dataset(location, filesystem=fs, format="parquet", partitioning="hive",
                         exclude_invalid_files=True, ignore_prefixes=["_", "."])
    return _clean(dataset.to_table(filter=filter_expr).to_pandas())


def _latest_stream(path: Path) -> pd.DataFrame | None:
    df = _read(path)
    if df is None or df.empty:
        return df
    df = df.sort_values("batch_id").drop_duplicates(["run_tag", "zone_id", "window_start"], keep="last")
    newest = df["window_start"].max()
    cutoff = (pd.Timestamp(newest) - pd.Timedelta(hours=6)).strftime("%Y-%m-%d %H:%M:%S")
    return df[df["window_start"] >= cutoff].drop(columns=["max_kafka_ts"], errors="ignore")


def build_tables(cfg: Config) -> dict[str, pd.DataFrame]:
    gold, silver = cfg.path("gold"), cfg.path("silver")
    tables: dict[str, pd.DataFrame | None] = {"zones": _read(cfg.path("bronze") / "taxi_zones")}
    for name in ANALYTICS:
        tables[name] = _read(gold / "analytics" / name)

    demand_path = silver / "zone_hour_demand"
    if demand_path.exists():
        months = sorted(p.name.split("=", 1)[1] for p in demand_path.glob("month=*"))
        recent = _read(demand_path, ds.field("month").isin(months[-2:]))
        days = cfg.section("serving").get("recent_days", 30)
        cutoff = (pd.Timestamp(recent["window_start"].max()) - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
        tables["zone_hour_demand_recent"] = recent[recent["window_start"] >= cutoff]

    best_file = cfg.path("models") / "best_model.json"
    if best_file.exists():
        best = json.loads(best_file.read_text(encoding="utf-8"))["model"]
        tables["forecasts"] = _read(gold / "forecasts", (ds.field("model") == best) & (ds.field("split") == "test"))
    for name in ["model_metrics", "anomaly_metrics"]:
        f = gold / name / f"{name}.parquet"
        tables[name] = _clean(read_parquet_pandas(f)) if f.exists() else None
    tables["anomalies"] = _read(gold / "anomalies")
    tables["data_quality_daily"] = _read(gold / "data_quality_daily")
    tables["stream_latest"] = _latest_stream(cfg.path("stream_out"))
    return {k: v for k, v in tables.items() if v is not None}


def _atomic_replace(src: Path, dst: Path, attempts: int = 10) -> None:
    """os.replace with retries: on Windows the target cannot be replaced while a reader has it open."""
    for i in range(attempts):
        try:
            os.replace(src, dst)
            return
        except PermissionError:
            if i == attempts - 1:
                raise PermissionError(f"cannot replace {dst}: it is open in another process "
                                      "(stop the API / dashboard and run the export again)") from None
            time.sleep(0.5)


def export_serving_db(cfg: Config) -> dict[str, int]:
    db = cfg.path("serving_db")
    db.parent.mkdir(parents=True, exist_ok=True)
    tmp = db.with_suffix(".tmp")
    if tmp.exists():
        tmp.unlink()
    tables = build_tables(cfg)
    # closing(): sqlite3's own context manager only commits; the file must be closed before os.replace (Windows)
    with closing(sqlite3.connect(tmp)) as con, con:
        for name, df in tables.items():
            df.to_sql(name, con, index=False, if_exists="replace")
            for col in INDEXES.get(name, []):
                if col in df.columns:
                    con.execute(f'CREATE INDEX IF NOT EXISTS "ix_{name}_{col}" ON "{name}" ("{col}")')
    _atomic_replace(tmp, db)  # atomic swap so readers never see a half-written DB
    counts = {k: len(v) for k, v in tables.items()}
    log.info("serving DB %s: %s", db, counts)
    return counts
