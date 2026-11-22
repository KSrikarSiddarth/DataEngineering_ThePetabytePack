"""GBT validation tuning plus the M8 noise-floor and A4b recall-ceiling diagnostics, on synthetic data."""
import pytest
from pyspark.sql import functions as F

from mobilitypulse import cli
from mobilitypulse.common.utils import load_measurements
from mobilitypulse.ml.anomaly import run_anomaly
from mobilitypulse.ml.train import load_features, train_and_evaluate, tune_gbt, tuning_enabled

pytestmark = pytest.mark.integration

GRID = [{"max_depth": 3, "max_iter": 4, "step_size": 0.2}, {"max_depth": 5, "max_iter": 8, "step_size": 0.2}]


@pytest.fixture(scope="module")
def cfg(spark, base_cfg, tmp_path_factory):
    root = tmp_path_factory.mktemp("diag")
    c = base_cfg.with_overrides({
        "paths": {"data_dir": str(root / "data"), "results": str(root / "results")},
        "ml": {"tuning": {"enabled": True, "gbt_grid": GRID}},
    })
    cli.run_many(["sample", "bronze", "silver", "demand", "features"], c, ["2024-01"])
    return c


def test_tuning_is_off_by_default(base_cfg):
    assert tuning_enabled(base_cfg) is False


def test_tuning_picks_the_lowest_validation_mae_and_never_reads_test(spark, cfg):
    feats = load_features(spark, cfg)
    train, val = feats.where("split = 'train'"), feats.where("split = 'validation'")
    model, info = tune_gbt(cfg, train, val)
    maes = [t["validation_mae"] for t in info["trials"]]
    assert len(maes) == len(GRID) and info["best_validation_mae"] == min(maes)
    assert info["best_params"] == {k: GRID[maes.index(min(maes))][k] for k in GRID[0]}
    assert info["tuning_seconds"] >= info["best_fit_seconds"] > 0
    assert model.stages[-1].getMaxDepth() == info["best_params"]["max_depth"]


def test_training_with_tuning_records_diagnostics(spark, cfg):
    ml = train_and_evaluate(spark, cfg)
    anomaly = run_anomaly(spark, cfg)
    m = load_measurements(cfg)

    assert m["ml.gbt_tuning_min"]["value"] > 0 and m["ml.gbt_tuning_min"]["context"]["gbt_trials"] == len(GRID)
    assert m["ml.gbt_tuning_min"]["context"]["gbt_params"] in [{k: g[k] for k in GRID[0]} for g in GRID]
    assert ml["ml.gbt_train_min"] * 60 <= m["ml.gbt_tuning_min"]["value"] * 60 + 1e-6  # final fit <= all trials

    floor = m["ml.worst_borough_floor_wape_pct"]
    assert 0 < floor["value"] < 100 and floor["context"]["worst_borough"]
    # observed WAPE of the worst borough cannot be (much) below the noise floor of that same borough
    assert m["ml.worst_borough_wape_pct"]["value"] > 0.8 * floor["value"]

    rec, ceiling = m["anomaly.rule_recall"]["value"], m["anomaly.rule_recall_ceiling"]["value"]
    assert 0 <= rec <= ceiling <= 1 + 1e-9                     # recall can never exceed alerts / positives
    assert 0 <= m["anomaly.rule_positive_rate_pct"]["value"] <= 100
    assert anomaly["rule:residual_flag"]["tp"] + anomaly["rule:residual_flag"]["fn"] == m["anomaly.rule_recall_ceiling"]["context"]["rule_positives"]


def test_tuning_disabled_leaves_a_single_fit_and_no_tuning_measurement(spark, cfg, tmp_path):
    plain = cfg.with_overrides({"ml": {"tuning": {"enabled": False}},
                                "paths": {"results": str(tmp_path / "r2"), "models": str(tmp_path / "m2"),
                                          "gold": str(tmp_path / "g2")}})
    ml = train_and_evaluate(spark, plain)
    m = load_measurements(plain)
    assert "ml.gbt_tuning_min" not in m and ml["best_model"]
