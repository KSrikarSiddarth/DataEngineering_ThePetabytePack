"""Incremental, idempotent month-by-month processing driven by a state table.

State table (`<state>/pipeline_state.parquet`): one row per (month, stage) with the fingerprint of the raw input the
stage was run against. A month needs work when any month-level stage (bronze, silver, demand) is missing, did not
finish, or ran against a different raw file than the one on disk now. Each stage is committed on its own, so a crash
resumes at the stage that failed instead of redoing the month.

The watermark is the latest month whose month-level stages are all done. Months older than the watermark that are
missing (a backfill) or whose input changed (a restatement) are picked up as well.

Idempotency comes from the writers: bronze, silver, quarantine, demand and data_quality_daily overwrite only the
partitions of the months they are given, and record ids are deterministic, so re-running a month reproduces the same
rows instead of appending duplicates.
"""
from __future__ import annotations

import hashlib
import os
import tempfile
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from pyspark.sql import SparkSession

from ..common.config import Config
from ..common.utils import get_logger

log = get_logger(__name__)

MONTH_STAGES = ["bronze", "silver", "demand"]
DOWNSTREAM_STAGES = ["features", "analytics"]  # rebuilt over every finished month when something new arrived
STATE_COLUMNS = ["month", "stage", "status", "input_fingerprint", "rows_out", "run_id", "processed_at"]
GLOBAL_MONTH = "ALL"  # state key for stages that cover every month at once


# ---------------------------------------------------------------- state table
def state_path(cfg: Config) -> Path:
    return cfg.path("state") / "pipeline_state.parquet"


def read_state(cfg: Config) -> pd.DataFrame:
    p = state_path(cfg)
    if not p.exists():
        return pd.DataFrame({c: pd.Series(dtype="object") for c in STATE_COLUMNS})
    return pd.read_parquet(p)


def write_state(cfg: Config, df: pd.DataFrame) -> None:
    """Atomic replace, so a crash mid-write never leaves a half-written state table."""
    p = state_path(cfg)
    p.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=p.parent, suffix=".parquet")
    os.close(fd)
    df[STATE_COLUMNS].to_parquet(tmp, index=False)
    os.replace(tmp, p)


def mark(cfg: Config, month: str, stage: str, status: str, fingerprint: str, rows_out: int | None, run_id: str) -> None:
    state = read_state(cfg)
    state = state[~((state["month"] == month) & (state["stage"] == stage))]
    row = {"month": month, "stage": stage, "status": status, "input_fingerprint": fingerprint,
           "rows_out": rows_out, "run_id": run_id,
           "processed_at": datetime.now(timezone.utc).isoformat(timespec="seconds")}
    write_state(cfg, pd.DataFrame([*state.to_dict("records"), row]))


# ---------------------------------------------------------------- inputs and what is pending
def raw_trip_file(cfg: Config, month: str) -> Path:
    return cfg.path("raw") / "tlc" / f"{cfg.section('sources')['tlc_dataset']}_{month}.parquet"


def fingerprint(cfg: Config, month: str) -> str:
    """SHA-256 of the month's raw TLC file: changes if and only if the input content changes."""
    h = hashlib.sha256()
    with open(raw_trip_file(cfg, month), "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def pending_stages(cfg: Config, month: str, current_fp: str, force: bool = False) -> list[str]:
    """Month-level stages that must run: all of them after the first stage that is missing/stale (they chain)."""
    if force:
        return list(MONTH_STAGES)
    state = read_state(cfg)
    for i, stage in enumerate(MONTH_STAGES):
        row = state[(state["month"] == month) & (state["stage"] == stage)]
        fresh = (not row.empty and row.iloc[0]["status"] == "done" and row.iloc[0]["input_fingerprint"] == current_fp)
        if not fresh:
            return MONTH_STAGES[i:]
    return []


def watermark(cfg: Config) -> str | None:
    """Latest month whose bronze, silver and demand stages are all done (None if nothing finished yet)."""
    state = read_state(cfg)
    done = state[(state["status"] == "done") & state["stage"].isin(MONTH_STAGES) & (state["month"] != GLOBAL_MONTH)]
    complete = [m for m, g in done.groupby("month") if set(MONTH_STAGES) <= set(g["stage"])]
    return max(complete) if complete else None


def finished_months(cfg: Config) -> list[str]:
    state = read_state(cfg)
    done = state[(state["status"] == "done") & state["stage"].isin(MONTH_STAGES) & (state["month"] != GLOBAL_MONTH)]
    return sorted(m for m, g in done.groupby("month") if set(MONTH_STAGES) <= set(g["stage"]))


# ---------------------------------------------------------------- the runner
def _run_month_stage(spark: SparkSession, cfg: Config, month: str, stage: str) -> int:
    if stage == "bronze":
        from .bronze import run_bronze
        return int(run_bronze(spark, cfg, [month])["trips"])
    if stage == "silver":
        from .silver import run_silver
        return int(run_silver(spark, cfg, [month])["dq"][month]["written_valid_rows"])
    if stage == "demand":
        from .demand import build_zone_hour_demand
        return int(build_zone_hour_demand(spark, cfg, [month]))
    raise ValueError(stage)


def run_incremental(spark: SparkSession, cfg: Config, months: list[str], force: bool = False,
                    downstream: bool = True) -> dict:
    """Process only the requested months that are new, unfinished or changed. Safe to call repeatedly."""
    t0 = time.perf_counter()
    missing = [m for m in months if not raw_trip_file(cfg, m).exists()]
    if missing:
        raise FileNotFoundError(f"raw data missing for {missing} - run stage 'ingest' (or 'sample') first")
    run_id = uuid.uuid4().hex[:12]
    before = watermark(cfg)

    plan = {}
    fps = {m: fingerprint(cfg, m) for m in months}
    for m in months:
        todo = pending_stages(cfg, m, fps[m], force)
        if todo:
            plan[m] = todo
    skipped = [m for m in months if m not in plan]
    log.info("incremental: watermark=%s, to process=%s, up to date=%s", before, plan or "nothing", skipped)

    processed: dict[str, dict[str, int]] = {}
    for month in sorted(plan):
        for stage in plan[month]:
            # a changed or forced month invalidates its later stages until they are re-run
            mark(cfg, month, stage, "running", fps[month], None, run_id)
            try:
                rows = _run_month_stage(spark, cfg, month, stage)
            except Exception:
                mark(cfg, month, stage, "failed", fps[month], None, run_id)
                raise
            mark(cfg, month, stage, "done", fps[month], rows, run_id)
            processed.setdefault(month, {})[stage] = rows
            log.info("incremental: %s/%s done (%d rows)", month, stage, rows)

    rebuilt: list[str] = []
    if processed and downstream:
        done_months = finished_months(cfg)
        from .analytics import run_analytics
        from .features import build_features
        build_features(spark, cfg, done_months)
        run_analytics(spark, cfg, done_months)
        rebuilt = list(DOWNSTREAM_STAGES)
        fp = hashlib.sha256(",".join(f"{m}:{fps.get(m, '')}" for m in done_months).encode()).hexdigest()
        for stage in rebuilt:
            mark(cfg, GLOBAL_MONTH, stage, "done", fp, len(done_months), run_id)

    return {"run_id": run_id, "watermark_before": before, "watermark_after": watermark(cfg),
            "processed": processed, "skipped_up_to_date": skipped, "downstream_rebuilt": rebuilt,
            "seconds": round(time.perf_counter() - t0, 2)}
