"""M6 tests: feature lists (no leakage), lag features, trip sampling, a repeatable model run, anomaly detection."""
import random
from datetime import datetime, timedelta

import pytest
from pyspark.sql import functions as F

from test_spark_transform import spark  # noqa: F401
from ml_features import FEATURES, LABELS, POST_TRIP, trip_features, zone_hour
from ml_job import MODELS, UNSUPERVISED, adjusted_rand_index, detect_anomalies, lift, run_model
from transform import DQ_FLAGS

CFG = {"seed": 7, "test_percent": 25, "cv_folds": 2, "gbt_iterations": 15, "gbt_max_depth": 4, "max_bins": 32,
       "forest_trees": 5}
T0 = datetime(2026, 1, 1)


def test_no_post_trip_column_is_a_feature():
    """O4: dropoff time, duration, total amount, tip, driver pay and on-scene time are never features, and no model
    reads its own label."""
    for name, features in FEATURES.items():
        assert not set(features) & set(POST_TRIP), name
        assert LABELS[name] not in features, name


def test_every_model_has_features_and_a_label():
    assert set(MODELS) == set(FEATURES) == set(LABELS)
    assert set(UNSUPERVISED) == {"zone_clusters", "demand_patterns", "anomaly", "pca"}


def test_adjusted_rand_index():
    """Identical partitions (even with renamed clusters) score 1; an unrelated split scores about 0 or less."""
    assert adjusted_rand_index([0, 0, 1, 1, 2, 2], [5, 5, 3, 3, 9, 9]) == pytest.approx(1.0)
    assert adjusted_rand_index([0, 0, 1, 1], [0, 1, 0, 1]) < 0.01


def test_lift_is_relative_to_the_baseline_error():
    assert lift("mae", 4.0, 3.0) == pytest.approx(0.25)
    assert lift("mae", 4.0, 5.0) < 0                                  # worse than the baseline is reported negative
    assert lift("auc", 0.5, 0.75) == pytest.approx(0.5)               # error = 1 - AUC


def test_zone_hour_lags_and_grid(spark):  # noqa: F811
    """10 days of one zone: missing hours count as 0, lags look back 1, 24 and 168 hours, and the first week
    (no 168-hour history) is dropped."""
    hours = [T0 + timedelta(hours=h) for h in range(240) if h % 7]   # every 7th hour has no trips
    od = spark.createDataFrame([(1, t, 10 + t.hour, 2026, 1) for t in hours],
                               "pu_location_id int, hour_start timestamp, trips long, year int, month int")
    table, demand = zone_hour(od, ["2026-01-09"])
    got = {r["hour_start"]: r.asDict() for r in table.collect()}
    assert {r["ym"] for r in table.collect()} == {"2026-01"}
    assert min(got) == T0 + timedelta(hours=168)
    assert len(got) == 240 - 168
    row = got[T0 + timedelta(hours=200)]
    assert row["lag_24h"] == (10 + (200 - 24) % 24 if (200 - 24) % 7 else 0)
    assert row["lag_168h"] == (10 + (200 - 168) % 24 if (200 - 168) % 7 else 0)
    assert row["is_holiday"] == 1 and got[T0 + timedelta(hours=170)]["is_holiday"] == 0   # 2026-01-09 is hours 192-215
    assert demand.collect()[0]["zone_demand"] > 0


def silver_rows(n, seed=1):
    """Synthetic yellow trips on a line: fare = 3 + 2.5 * miles + noise. Every 10th trip has a data-quality flag."""
    rnd = random.Random(seed)
    out = []
    for i in range(n):
        miles = round(rnd.uniform(0.5, 10), 2)
        ts = T0 + timedelta(minutes=7 * i)
        fare = 3 + 2.5 * miles + rnd.gauss(0, 0.5)
        flags = [i % 10 == 0 and f == "dq_negative_fare" for f in DQ_FLAGS]
        out.append((f"t{i}", "yellow", ts, 10 + i % 7, 1 + i % 5, miles, 1, 1, 1, round(fare, 2), round(fare * (0.1 + 0.2 * (i % 2)), 2),
                    int(miles * 150), None, None, None, None, 2026, 1, *flags))
    return out


SILVER_COLS = ("trip_id string, service_type string, pickup_ts timestamp, pu_location_id int, do_location_id int, "
               "distance_miles double, ratecode_id int, passenger_count int, payment_type int, base_fare double, "
               "tip_amount double, duration_sec long, request_ts timestamp, hvfhs_license_num string, "
               "shared_request_flag string, shared_match_flag string, year int, month int, " + ", ".join(f"{f} boolean" for f in DQ_FLAGS))


@pytest.fixture(scope="module")
def trips(spark):  # noqa: F811
    """Trips feature table of 400 synthetic trips (everything kept)."""
    silver = spark.createDataFrame(silver_rows(400), SILVER_COLS)
    demand = spark.createDataFrame([(z, 5.0) for z in range(10, 17)], "pu_location_id int, zone_demand double")
    return trip_features(silver, demand, [], sample_rows=10_000, seed=CFG["seed"]).cache()


def test_trip_features_keep_only_clean_trips_and_build_labels(trips):
    assert trips.count() == 360                                       # the 40 flagged trips are left out
    row = trips.where("trip_id = 't1'").first()
    assert row["service_code"] == 1 and row["pu_zone_demand"] == 5.0
    assert row["tip20"] == 1                                          # 30% tip on a card trip
    assert trips.where("trip_id = 't2'").first()["tip20"] == 0        # 10% tip
    assert row["wait_sec"] is None and row["shared_match"] is None    # HVFHV labels do not apply to yellow
    assert abs(row["duration_min"] - row["distance_miles"] * 150 / 60) < 0.02   # duration is whole seconds


def test_trip_sample_is_capped_and_repeatable(spark):  # noqa: F811
    silver = spark.createDataFrame(silver_rows(400), SILVER_COLS)
    demand = spark.createDataFrame([(10, 5.0)], "pu_location_id int, zone_demand double")
    ids = [sorted(r["trip_id"] for r in trip_features(silver, demand, [], 100, 3).collect()) for _ in range(2)]
    assert ids[0] == ids[1]
    assert 60 <= len(ids[0]) <= 140                                   # about 100 of the 360 clean trips


def test_fare_model_runs_scores_and_repeats(spark, trips, tmp_path):  # noqa: F811
    """The model writes its holdout predictions and metrics; the same seed gives the same metrics (O2)."""
    results = []
    for i in range(2):
        rows = []
        run_model("fare", trips, CFG, tmp_path / str(i), rows)
        results.append({(m, s): v for _, m, s, v in rows if m != "train_seconds"})
    assert results[0] == results[1]
    got = results[0]
    assert got[("mae", "test")] < 1.5 and got[("r2", "test")] > 0.8
    assert ("mae", "baseline") in got and ("lift", "test") in got and ("cv_std", "cv") in got
    assert ("mae", "month:2026-01") in got
    assert spark.read.parquet(str(tmp_path / "0" / "ml_fare_predictions")).count() > 0


def test_anomaly_detection_finds_injected_bad_trips(trips, tmp_path):
    """Fare x10 and distance x10 trips are flagged (high recall); real trips are flagged at about 1%."""
    cfg = {**CFG, "anomaly_quantile": 0.99, "anomaly_inject_percent": 10}
    fare = run_model("fare", trips, cfg, tmp_path, [])
    rows = []
    detect_anomalies(fare, cfg, tmp_path, rows)
    got = {(m, s): v for _, m, s, v in rows}
    assert got[("recall", "test")] >= 0.8
    assert got[("flagged_rate", "test")] <= 0.1
    assert got[("recall", "baseline")] < got[("recall", "test")]      # the speed rule misses the fare errors
