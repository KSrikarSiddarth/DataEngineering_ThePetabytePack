"""M4 tests: the Gold daily marts, computed on a small hand-checkable Silver (batch DataFrame)."""
import pytest
from pyspark.sql import functions as F

from test_spark_transform import NORMS, SPECS, VCFG, build_raw, msg, spark, zones  # noqa: F401
from gold import MARTS
from transform import process

TIME_COLS = ["hour_start", "pickup_date", "pickup_hour"]


@pytest.fixture(scope="module")
def silver(spark, zones):  # noqa: F811
    """Eight trips on 2026-01-10 (hour 10): 4 yellow (one out of period, one negative fare), green, fhv, 2 fhvhv."""
    items = [
        ("yellow", msg("yellow", Airport_fee=1.75, cbd_congestion_fee=0.75)),              # A: card, 21.00, two fees
        ("yellow", msg("yellow", tpep_pickup_datetime="2026-01-10T10:30:00", tpep_dropoff_datetime="2026-01-10T10:50:00",
                       trip_distance=2.0, fare_amount=10.0, tip_amount=0.0, total_amount=12.0, payment_type=2)),  # B: cash
        ("yellow", msg("yellow", tpep_pickup_datetime="2025-10-01T10:00:00",
                       tpep_dropoff_datetime="2025-10-01T10:20:00")),                      # C: out of period
        ("yellow", msg("yellow", fare_amount=-5.0)),                                       # D: negative fare
        ("green", msg("green")),
        ("fhv", msg("fhv")),
        ("hvfhv", msg("hvfhv", request_datetime="2026-01-10T09:55:00", on_scene_datetime="2026-01-10T09:58:00",
                      shared_request_flag="Y", shared_match_flag="N")),                    # E: wait 300 s, response 180 s
        ("hvfhv", msg("hvfhv", base_passenger_fare=10.0, driver_pay=6.0, request_datetime="2026-01-10T09:50:00",
                      on_scene_datetime="2026-01-10T09:52:00", shared_request_flag="Y", shared_match_flag="Y")),  # F
    ]
    return process(build_raw(spark, items), SPECS, NORMS, zones, VCFG)[0].cache()


def rows(df, **where):
    """Collect a mart as dicts, keeping only rows whose columns equal `where`."""
    return [r.asDict() for r in df.collect() if all(r[k] == v for k, v in where.items())]


def total(df, col, **where):
    """Sum one measure over the (filtered) rows of a mart."""
    return sum(r[col] for r in rows(df, **where))


def test_every_catalog_mart_exists():
    """The eight daily marts of specs/3_KPI.html, no more and no fewer."""
    assert set(MARTS) == {"trips_daily", "demand_hourly", "zone_od_daily", "revenue_daily", "trip_efficiency_daily",
                          "hvfhv_economics_daily", "hvfhv_service_daily", "data_quality_daily"}


def test_marts_share_the_time_columns(silver):
    for name, build in MARTS.items():
        assert build(silver).columns[:3] == TIME_COLS, name


def test_trips_daily_counts_in_period_trips(silver):
    df = MARTS["trips_daily"](silver)
    assert total(df, "trips") == 7                                  # 8 Silver rows minus the out-of-period one
    assert total(df, "trips", service_type="yellow") == 3           # A, B, D (C is outside the month)
    assert total(df, "trips", service_type="fhvhv") == 2


def test_demand_hourly_has_hour_and_day_of_week(silver):
    r = rows(MARTS["demand_hourly"](silver), service_type="green")[0]
    assert r["pickup_hour"] == 10 and r["day_of_week"] == 7         # 2026-01-10 is a Saturday


def test_zone_od_daily_sums_distance_per_pair(silver):
    r = rows(MARTS["zone_od_daily"](silver), service_type="yellow", pu_location_id=161, do_location_id=237)[0]
    assert r["trips"] == 3 and r["distance_trips"] == 3 and r["sum_distance_miles"] == pytest.approx(8.4)


def test_revenue_daily_is_yellow_green_only_and_skips_negative_fares(silver):
    df = MARTS["revenue_daily"](silver)
    assert {r["service_type"] for r in rows(df)} == {"yellow", "green"}
    card = rows(df, service_type="yellow", payment_label="Credit card")[0]
    assert (card["trips"], card["sum_total_amount"], card["sum_tip_amount"], card["sum_base_fare"]) == (1, 21.0, 3.0, 15.0)
    assert (card["sum_airport_fee"], card["sum_cbd_congestion_fee"], card["sum_congestion_surcharge"]) == (1.75, 0.75, 0.0)
    cash = rows(df, service_type="yellow", payment_label="Cash")[0]
    assert (cash["trips"], cash["sum_total_amount"], cash["sum_distance_miles"]) == (1, 12.0, 2.0)
    assert total(df, "trips", service_type="yellow") == 2          # D (negative fare) and C (out of period) excluded


def test_trip_efficiency_daily_sums_duration_and_distance(silver):
    r = rows(MARTS["trip_efficiency_daily"](silver), service_type="yellow")[0]
    assert r["trips"] == 3 and r["sum_duration_sec"] == 3600        # A, B, D: 20 minutes each
    assert r["sum_distance_miles"] == pytest.approx(8.4) and r["sum_duration_sec_dist"] == 3600


def test_hvfhv_economics_daily(silver):
    r = rows(MARTS["hvfhv_economics_daily"](silver), hvfhs_license_num="HV0003")[0]
    assert r["trips"] == 2 and r["sum_base_fare"] == pytest.approx(32.5) and r["sum_driver_pay"] == pytest.approx(23.0)
    assert r["sum_fees"] == 0 and r["sum_passenger_cost"] == pytest.approx(32.5)


def test_hvfhv_service_daily_wait_response_and_shared_match(silver):
    r = rows(MARTS["hvfhv_service_daily"](silver), hvfhs_license_num="HV0003")[0]
    assert (r["wait_trips"], r["sum_wait_sec"]) == (2, 300 + 600)
    assert (r["response_trips"], r["sum_response_sec"]) == (2, 180 + 120)
    assert (r["shared_requests"], r["shared_matches"]) == (2, 1)


def test_data_quality_daily_counts_every_row_and_each_flag(silver):
    df = MARTS["data_quality_daily"](silver)
    assert total(df, "rows") == 8                                   # nothing is filtered out, even out-of-period
    assert total(df, "n_pickup_out_of_range") == 1 and total(df, "n_negative_fare") == 1
    assert total(df, "n_null_pu_location") == 1                     # the FHV row


def test_hourly_rows_roll_up_to_days(silver):
    """Summing the hourly mart by pickup_date reproduces the day's total (additive measures)."""
    df = MARTS["trips_daily"](silver).groupBy("pickup_date").agg(F.sum("trips").alias("trips"))
    assert [(str(r["pickup_date"]), r["trips"]) for r in df.collect()] == [("2026-01-10", 7)]
