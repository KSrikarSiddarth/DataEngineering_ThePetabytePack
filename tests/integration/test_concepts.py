"""Spark internals evidence pack (benchmark/concepts.py) on synthetic data."""
import pandas as pd
import pytest

from mobilitypulse import cli
from mobilitypulse.batch.analytics import run_analytics
from mobilitypulse.batch.demand import build_zone_hour_demand
from mobilitypulse.benchmark import concepts

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def env(spark, base_cfg, tmp_path_factory):
    root = tmp_path_factory.mktemp("concepts")
    cfg = base_cfg.with_overrides({
        "paths": {"data_dir": str(root / "data"), "results": str(root / "results")},
        "benchmark": {"repetitions": 1},
    })
    cli.run_many(["sample", "bronze", "silver"], cfg, ["2024-01"])
    build_zone_hour_demand(spark, cfg, ["2024-01"])
    run_analytics(spark, cfg, ["2024-01"])
    return cfg


def test_c1_transformations_run_no_jobs_but_actions_do(spark, env):
    out = concepts.c1_lazy(spark, env)
    by_kind = {}
    for r in out["rows"]:
        by_kind.setdefault(r["kind"], []).append(r["spark_jobs"])
    assert by_kind["transformation"] == [0] * 5
    assert by_kind["plan"] == [0]
    assert all(j > 0 for j in by_kind["action"])
    plan = (env.path("results") / "concepts" / "c1_lazy_plan.txt").read_text()
    assert "Exchange" in plan and "PushedFilters" in plan


def test_c2_stage_count_follows_shuffles(spark, env):
    out = concepts.c2_narrow_wide(spark, env)
    rows = {r["query"]: r for r in out["rows"]}
    assert rows["narrow_only"]["exchanges_in_plan"] == 0 and rows["narrow_only"]["type"] == "narrow"
    assert rows["wide_groupby"]["exchanges_in_plan"] == 1
    assert rows["wide_groupby_sort"]["exchanges_in_plan"] == 2
    assert spark.conf.get("spark.sql.adaptive.enabled") == "true"  # restored after the experiment
    assert (env.path("results") / "concepts" / "c2_wide_groupby_plan.txt").exists()


def test_c3_skew_is_injected_and_config_restored(spark, env):
    out = concepts.c3_skew(spark, env, copies=2)
    assert [r["variant"] for r in out["rows"]] == ["natural_data_aqe_off", "skewed_aqe_off", "skewed_aqe_skew_join",
                                                   "skewed_manual_salting"]
    assert spark.conf.get("spark.sql.autoBroadcastJoinThreshold") != "-1"
    assert spark.conf.get("spark.sql.adaptive.skewJoin.enabled") == "true"
    csv = pd.read_csv(env.path("results") / "concepts" / "c3_skew.csv")
    assert len(csv) == 4


def test_c4_silver_and_gold_give_the_same_answer(spark, env):
    out = concepts.c4_silver_vs_gold(spark, env, reps=1)
    assert out["rows"] and all(r["answers_match"] for r in out["rows"])
    csv = pd.read_csv(env.path("results") / "concepts" / "c4_silver_vs_gold.csv")
    assert set(csv["layer"]) == {"silver_scan", "gold_lookup"}


def test_cli_rejects_unknown_experiment(monkeypatch):
    monkeypatch.setattr("sys.argv", ["concepts", "c99"])
    with pytest.raises(SystemExit):
        concepts.main()
