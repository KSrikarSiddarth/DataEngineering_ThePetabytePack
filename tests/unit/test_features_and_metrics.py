import pytest
from pyspark.sql import functions as F

from mobilitypulse.batch.features import add_time_features
from mobilitypulse.ml.metrics import binary_metrics, regression_metrics


def test_lags_rolling_and_label(spark):
    rows = [(1, f"2024-01-01 {h:02d}:00:00", float(h)) for h in range(10)]
    df = spark.createDataFrame(rows, "zone_id int, window_start string, trip_count double") \
        .withColumn("window_start", F.to_timestamp("window_start")).withColumn("is_raining", F.lit(False))
    out = {r["window_start"].hour: r for r in add_time_features(df, [1, 3], [3], ["2024-01-01"]).collect()}
    assert out[5]["lag_1"] == 4 and out[5]["lag_3"] == 2
    assert out[5]["roll_mean_3"] == pytest.approx(4.0)
    assert out[5]["label_next_hour"] == 6
    assert out[9]["label_next_hour"] is None
    assert out[0]["lag_1"] is None
    assert out[5]["is_holiday"] == 1 and out[5]["day_of_week"] == 1   # 2024-01-01 is a Monday
    assert out[23 - 14]["target_hour_of_day"] == 10


def test_regression_metrics(spark):
    df = spark.createDataFrame([(10.0, 12.0), (20.0, 18.0), (0.0, 1.0), (30.0, 30.0)], "actual double, predicted double")
    m = regression_metrics(df, mape_min=10)
    assert m["mae"] == pytest.approx(1.25)
    assert m["rmse"] == pytest.approx((9 / 4) ** 0.5)
    assert m["wape_pct"] == pytest.approx(100 * 5 / 60)
    assert m["mape_pct"] == pytest.approx(100 * (0.2 + 0.1 + 0.0) / 3)
    assert 0.9 < m["r2"] < 1.0


def test_binary_metrics(spark):
    df = spark.createDataFrame([(True, True), (True, False), (False, True), (False, False)], "t boolean, f boolean")
    m = binary_metrics(df, "t", "f")
    assert (m["tp"], m["fp"], m["fn"]) == (1, 1, 1)
    assert m["precision"] == 0.5 and m["recall"] == 0.5 and m["alert_rate_pct"] == 50.0


def test_profile_feature_uses_train_only_and_leave_one_out(spark):
    from mobilitypulse.ml.train import add_profile_features

    rows = [(1, 1, 8, "train", 10.0), (1, 1, 8, "train", 20.0), (1, 1, 8, "train", 30.0),
            (1, 1, 8, "test", 1000.0)]
    df = spark.createDataFrame(rows, "zone_id int, target_day_of_week int, target_hour_of_day int, split string, label double")
    out = {(r["split"], r["label"]): r["zone_how_mean"] for r in add_profile_features(df).collect()}
    assert out[("train", 10.0)] == pytest.approx(25.0)      # (20 + 30) / 2, own label excluded
    assert out[("test", 1000.0)] == pytest.approx(20.0)      # mean of train labels only


def test_poisson_mad_matches_known_values_and_brute_force():
    import math

    from mobilitypulse.ml.metrics import poisson_mad

    assert poisson_mad(0) == 0.0 and poisson_mad(None) == 0.0
    assert poisson_mad(1.0) == pytest.approx(2 / math.e)          # textbook value for Poisson(1)
    for lam in (0.3, 1.08, 4.5, 20.0, 150.0):                      # against the definition, summed directly
        pmf, brute = math.exp(-lam), 0.0
        for k in range(0, int(lam * 4 + 60)):
            brute += abs(k - lam) * pmf
            pmf *= lam / (k + 1)
        assert poisson_mad(lam) == pytest.approx(brute, rel=1e-6)


def test_poisson_floor_by_group_separates_noisy_and_busy_groups(spark):
    from mobilitypulse.ml.metrics import poisson_floor_by_group

    # a low-volume group (rate ~1) and a busy one (rate ~100), both forecast perfectly on average
    rows = [("quiet", 0.0, 1.0), ("quiet", 2.0, 1.0), ("quiet", 1.0, 1.0), ("busy", 95.0, 100.0), ("busy", 105.0, 100.0)]
    df = spark.createDataFrame(rows, "borough string, actual double, predicted double")
    out = {r["borough"]: r for r in poisson_floor_by_group(df, "borough").collect()}
    assert out["quiet"]["floor_wape_pct"] == pytest.approx(100 * 3 * (2 / 2.718281828459045) / 3.0, rel=1e-6)
    assert out["busy"]["floor_wape_pct"] < 10 < out["quiet"]["floor_wape_pct"]  # same method, very different floor
