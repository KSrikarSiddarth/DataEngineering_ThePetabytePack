import pytest
from pyspark.sql import functions as F

from mobilitypulse.batch.bronze import decode_noaa, normalize_tlc
from mobilitypulse.batch.quality import apply_quality

BASE = dict(vendor_id=2, pickup_ts="2024-01-15 08:04:12", dropoff_ts="2024-01-15 08:19:40", pickup_zone_id=161,
            dropoff_zone_id=237, passenger_count=1.0, trip_distance=2.4, total_amount=18.5, source_month="2024-01",
            source_file="f.parquet", record_id="r0")

CASES = [
    ({}, None),
    ({"pickup_ts": None}, "NULL_TIMESTAMP"),
    ({"pickup_ts": "2009-01-01 00:10:00", "dropoff_ts": "2009-01-01 00:20:00"}, "OUTSIDE_SOURCE_MONTH"),
    ({"dropoff_ts": "2024-01-15 08:00:00"}, "NEGATIVE_DURATION"),
    ({"dropoff_ts": "2024-01-15 08:04:30"}, "DURATION_OUT_OF_RANGE"),
    ({"dropoff_ts": "2024-01-15 15:30:00"}, "DURATION_OUT_OF_RANGE"),
    ({"pickup_zone_id": 264}, "INVALID_ZONE"),
    ({"dropoff_zone_id": None}, "INVALID_ZONE"),
    ({"trip_distance": 0.0}, "DISTANCE_OUT_OF_RANGE"),
    ({"trip_distance": 250.0}, "DISTANCE_OUT_OF_RANGE"),
    ({"total_amount": -18.5}, "FARE_OUT_OF_RANGE"),
    ({"total_amount": 5000.0}, "FARE_OUT_OF_RANGE"),
    ({"passenger_count": 12.0}, "PASSENGERS_OUT_OF_RANGE"),
    ({"passenger_count": None}, None),
    ({"trip_distance": 60.0}, "SPEED_IMPLAUSIBLE"),
]


def _df(spark, rows):
    df = spark.createDataFrame(rows, schema=(
        "vendor_id int, pickup_ts string, dropoff_ts string, pickup_zone_id int, dropoff_zone_id int, "
        "passenger_count double, trip_distance double, total_amount double, source_month string, "
        "source_file string, record_id string"))
    return df.withColumn("pickup_ts", F.to_timestamp("pickup_ts")).withColumn("dropoff_ts", F.to_timestamp("dropoff_ts"))


@pytest.mark.parametrize("change,expected", CASES)
def test_quality_rule(spark, base_cfg, change, expected):
    row = {**BASE, **change}
    out = apply_quality(_df(spark, [row]), base_cfg.section("quality")).first()
    assert out["reject_reason"] == expected


def test_duplicates_keep_one_valid_copy(spark, base_cfg):
    rows = [{**BASE, "record_id": "same"}, {**BASE, "record_id": "same"}, {**BASE, "record_id": "other"}]
    out = apply_quality(_df(spark, rows), base_cfg.section("quality"))
    reasons = sorted([r["reject_reason"] or "OK" for r in out.collect()])
    assert reasons == ["DUPLICATE", "OK", "OK"]


def test_normalize_tlc_handles_source_names_and_record_id(spark):
    src = spark.createDataFrame(
        [(2, "2024-01-15 08:04:12", "2024-01-15 08:19:40", 1, 2.4, 1, "N", 161, 237, 1, 12.0, 18.5)],
        "VendorID int, tpep_pickup_datetime string, tpep_dropoff_datetime string, passenger_count long, "
        "trip_distance double, RatecodeID long, store_and_fwd_flag string, PULocationID int, DOLocationID int, "
        "payment_type long, fare_amount double, total_amount double")
    out = normalize_tlc(src, "x.parquet", "2024-01").first()
    assert out["pickup_zone_id"] == 161 and out["ratecode_id"] == 1
    assert str(out["pickup_ts"]) == "2024-01-15 08:04:12"
    assert len(out["record_id"]) == 64


def test_decode_noaa_fields_and_timezone(spark):
    rows = [("72505394728", "2024-01-09T18:51:00", "FM-15", "+0056,1", "-0011,1", "270,1,N,0046,1",
             "016093,1,9,9", "01,0064,9,1"),
            ("72505394728", "2024-07-01T12:51:00", "FM-15", "+9999,9", "+9999,9", "999,9,C,0000,1",
             "999999,9,9,9", "01,9999,9,9")]
    df = spark.createDataFrame(rows, "STATION string, DATE string, REPORT_TYPE string, TMP string, DEW string, "
                                     "WND string, VIS string, AA1 string")
    first, second = decode_noaa(df, "America/New_York").orderBy("obs_ts_utc").collect()
    assert first["temp_c"] == pytest.approx(5.6) and first["dew_point_c"] == pytest.approx(-1.1)
    assert first["wind_mps"] == pytest.approx(4.6) and first["visibility_m"] == 16093
    assert first["precip_mm"] == pytest.approx(6.4) and first["precip_period_h"] == 1
    assert str(first["obs_ts_local"]) == "2024-01-09 13:51:00"      # EST = UTC-5
    assert str(second["obs_ts_local"]) == "2024-07-01 08:51:00"     # EDT = UTC-4
    assert second["temp_c"] is None and second["visibility_m"] is None and second["precip_mm"] is None
