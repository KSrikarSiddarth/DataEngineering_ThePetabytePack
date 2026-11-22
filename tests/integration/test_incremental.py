"""Incremental, idempotent processing: re-running a month gives identical output, only new work is done."""
import pytest
from pyspark.sql import functions as F

from mobilitypulse.batch import incremental
from mobilitypulse.batch.incremental import (finished_months, pending_stages, read_state, run_incremental,
                                             watermark)
from mobilitypulse.ingestion.sample import generate_sample

pytestmark = pytest.mark.integration

# columns that legitimately differ between two runs (wall-clock lineage stamps), excluded from content hashes
VOLATILE = {"ingested_at", "rejected_at", "computed_at", "ingest_lag_h", "ingested_at_ts"}
OUTPUTS = {"bronze": ("bronze", "trips"), "silver": ("silver", "trips_clean"), "quarantine": ("quarantine", "trips_rejected"),
           "demand": ("silver", "zone_hour_demand"), "dq_daily": ("gold", "data_quality_daily")}


@pytest.fixture()
def env(spark, base_cfg, tmp_path):
    """A fresh data dir per test, with raw data for two months so we can add one later."""
    cfg = base_cfg.with_overrides({"paths": {"data_dir": str(tmp_path / "data"), "results": str(tmp_path / "results")}})
    generate_sample(cfg, ["2024-01", "2024-02"])
    return cfg


def content_fingerprint(spark, cfg, month_filter: str) -> dict:
    """Order-independent content hash + row count of every month-level output (volatile stamp columns excluded)."""
    out = {}
    for name, (layer, table) in OUTPUTS.items():
        df = spark.read.parquet(str(cfg.path(layer) / table))
        month_col = "month" if "month" in df.columns else "source_month"
        df = df.where(F.col(month_col) == month_filter)
        cols = [c for c in df.columns if c not in VOLATILE]
        row = df.agg(F.count(F.lit(1)).alias("n"),
                     F.sum(F.xxhash64(*[F.col(c) for c in cols]).cast("decimal(38,0)")).alias("h")).first()
        out[name] = (row["n"], row["h"])
    return out


def state_rows(cfg, month):
    s = read_state(cfg)
    return {r["stage"]: r for r in s[s["month"] == month].to_dict("records")}


def test_first_run_processes_the_month_and_sets_the_watermark(spark, env):
    assert watermark(env) is None
    res = run_incremental(spark, env, ["2024-01"])
    assert list(res["processed"]) == ["2024-01"] and set(res["processed"]["2024-01"]) == {"bronze", "silver", "demand"}
    assert res["watermark_before"] is None and res["watermark_after"] == "2024-01"
    assert res["downstream_rebuilt"] == ["features", "analytics"]
    st = state_rows(env, "2024-01")
    assert all(st[s]["status"] == "done" and st[s]["rows_out"] > 0 for s in ["bronze", "silver", "demand"])
    assert {"features", "analytics"} <= set(state_rows(env, "ALL"))


def test_second_run_with_nothing_new_does_no_work(spark, env):
    run_incremental(spark, env, ["2024-01"])
    before = read_state(env)
    res = run_incremental(spark, env, ["2024-01"])
    assert res["processed"] == {} and res["skipped_up_to_date"] == ["2024-01"] and res["downstream_rebuilt"] == []
    assert read_state(env).equals(before)  # not even a timestamp changed: nothing ran


def test_rerunning_a_month_reproduces_identical_output(spark, env):
    run_incremental(spark, env, ["2024-01"])
    first = content_fingerprint(spark, env, "2024-01")
    res = run_incremental(spark, env, ["2024-01"], force=True)
    assert set(res["processed"]["2024-01"]) == {"bronze", "silver", "demand"}
    assert content_fingerprint(spark, env, "2024-01") == first
    silver = spark.read.parquet(str(env.path("silver") / "trips_clean"))
    assert silver.groupBy("record_id").count().where("count > 1").count() == 0  # no duplicates after the re-run


def test_a_new_month_is_processed_without_touching_the_old_one(spark, env):
    run_incremental(spark, env, ["2024-01"])
    jan_state, jan_out = state_rows(env, "2024-01"), content_fingerprint(spark, env, "2024-01")
    res = run_incremental(spark, env, ["2024-01", "2024-02"])
    assert list(res["processed"]) == ["2024-02"] and res["skipped_up_to_date"] == ["2024-01"]
    assert res["watermark_before"] == "2024-01" and res["watermark_after"] == "2024-02"
    assert finished_months(env) == ["2024-01", "2024-02"]
    assert state_rows(env, "2024-01") == jan_state  # January's state rows were not rewritten
    assert content_fingerprint(spark, env, "2024-01") == jan_out
    feats = spark.read.parquet(str(env.path("silver") / "zone_hour_features"))
    assert feats.agg(F.max("window_start")).first()[0].month == 2  # downstream saw both months


def test_changed_input_triggers_reprocessing_of_that_month(spark, env, monkeypatch):
    run_incremental(spark, env, ["2024-01"])
    monkeypatch.setattr(incremental, "fingerprint", lambda cfg, month: "restated-file")
    assert pending_stages(env, "2024-01", "restated-file") == ["bronze", "silver", "demand"]
    res = run_incremental(spark, env, ["2024-01"])
    assert set(res["processed"]["2024-01"]) == {"bronze", "silver", "demand"}
    assert state_rows(env, "2024-01")["silver"]["input_fingerprint"] == "restated-file"


def test_a_crash_resumes_at_the_failed_stage(spark, env, monkeypatch):
    import mobilitypulse.batch.demand as demand

    real = demand.build_zone_hour_demand
    monkeypatch.setattr(demand, "build_zone_hour_demand", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")))
    with pytest.raises(RuntimeError):
        run_incremental(spark, env, ["2024-01"])
    st = state_rows(env, "2024-01")
    assert (st["bronze"]["status"], st["silver"]["status"], st["demand"]["status"]) == ("done", "done", "failed")
    assert watermark(env) is None  # the month is not finished, so it does not count

    monkeypatch.setattr(demand, "build_zone_hour_demand", real)
    res = run_incremental(spark, env, ["2024-01"])
    assert set(res["processed"]["2024-01"]) == {"demand"}  # bronze and silver were not redone
    assert watermark(env) == "2024-01"


def test_missing_raw_data_fails_early_with_a_clear_message(spark, env):
    with pytest.raises(FileNotFoundError, match="2024-05"):
        run_incremental(spark, env, ["2024-01", "2024-05"])
    assert read_state(env).empty  # nothing was started
