"""gold.data_quality_daily: exact numbers on hand-built rows, then end-to-end through silver, export and the API."""
from datetime import datetime

import pandas as pd
import pytest
from fastapi.testclient import TestClient
from pyspark.sql import functions as F

from mobilitypulse import cli
from mobilitypulse.batch.dq_daily import dq_daily_frame, reason_codes
from mobilitypulse.batch.silver import NULL_CHECK_COLUMNS
from mobilitypulse.serving.api import create_app
from mobilitypulse.serving.export import export_serving_db

pytestmark = pytest.mark.integration


def make_rows(spark, specs):
    """specs: (source_month, pickup_ts or None, reject_reason or None, passenger_count_is_null)."""
    rows = []
    for month, ts, reason, null_pax in specs:
        row = {c: 1.0 for c in NULL_CHECK_COLUMNS}
        row.update(source_month=month, pickup_ts=ts, reject_reason=reason,
                   ingested_at=datetime(2024, 2, 1, 12, 0, 0))
        if null_pax:
            row["passenger_count"] = None
        if ts is None:
            row["pickup_ts"] = None
        rows.append(row)
    pdf = pd.DataFrame(rows)
    pdf["pickup_ts"] = pd.to_datetime(pdf["pickup_ts"])
    pdf["passenger_count"] = pdf["passenger_count"].astype("float64")
    return spark.createDataFrame(pdf)


def test_exact_counts_rates_and_alerts(spark, base_cfg):
    d1, d2 = datetime(2024, 1, 10, 8, 0), datetime(2024, 1, 11, 9, 30)
    specs = (
        [("2024-01", d1, None, False)] * 8 + [("2024-01", d1, "FARE_OUT_OF_RANGE", False)] * 2         # 10 rows, 20 %
        + [("2024-01", d2, None, i < 5) for i in range(99)] + [("2024-01", d2, "DUPLICATE", False)]    # 100 rows, 1 %
        + [("2024-01", datetime(2023, 12, 31, 23, 0), "OUTSIDE_SOURCE_MONTH", False)] * 4              # unattributed
        + [("2024-01", None, "NULL_TIMESTAMP", False)] * 2
    )
    df = dq_daily_frame(make_rows(spark, specs), base_cfg.section("quality"))
    out = {(r["day_kind"], r["date"].isoformat() if r["date"] else None): r.asDict() for r in df.collect()}

    day1, day2 = out[("day", "2024-01-10")], out[("day", "2024-01-11")]
    assert (day1["input_rows"], day1["valid_rows"], day1["rejected_rows"]) == (10, 8, 2)
    assert day1["reject_rate_pct"] == 20.0 and day1["alert_reject_rate"] is True
    assert day1["rej_fare_out_of_range"] == 2 and day1["rej_duplicate"] == 0
    assert (day2["input_rows"], day2["rejected_rows"]) == (100, 1)
    assert day2["reject_rate_pct"] == 1.0 and day2["alert_reject_rate"] is False
    assert day2["rej_duplicate"] == 1
    assert day2["null_pct_passenger_count"] == 5.0 and day1["null_pct_passenger_count"] == 0.0
    assert day2["max_pickup_ts"] == d2
    assert day2["ingest_lag_h"] == pytest.approx((datetime(2024, 2, 1, 12) - d2).total_seconds() / 3600, abs=0.01)

    # rows with no usable day are counted once per month and never alert, even at 100 % rejects
    loose = out[("unattributed", None)]
    assert loose["input_rows"] == 6 and loose["reject_rate_pct"] == 100.0 and loose["alert_reject_rate"] is False
    assert loose["rej_outside_source_month"] == 4 and loose["rej_null_timestamp"] == 2

    # reconciliation per row, and nothing lost overall
    for r in out.values():
        assert r["input_rows"] == r["valid_rows"] + r["rejected_rows"]
        assert r["rejected_rows"] == sum(r[f"rej_{c.lower()}"] for c in reason_codes(base_cfg.section("quality")))
    assert sum(r["input_rows"] for r in out.values()) == len(specs)


def test_threshold_is_configurable(spark, base_cfg):
    specs = [("2024-01", datetime(2024, 1, 10, 8), None, False)] * 9 + [("2024-01", datetime(2024, 1, 10, 8), "DUPLICATE", False)]
    low = dict(base_cfg.section("quality"), alert_reject_rate_pct=5.0)
    high = dict(base_cfg.section("quality"), alert_reject_rate_pct=50.0)
    assert make_rows(spark, specs).transform(lambda d: dq_daily_frame(d, low)).first()["alert_reject_rate"] is True
    assert make_rows(spark, specs).transform(lambda d: dq_daily_frame(d, high)).first()["alert_reject_rate"] is False


@pytest.fixture(scope="module")
def built(spark, base_cfg, tmp_path_factory):
    root = tmp_path_factory.mktemp("dq")
    cfg = base_cfg.with_overrides({"paths": {"data_dir": str(root / "data"), "results": str(root / "results")}})
    cli.run_many(["sample", "bronze", "silver"], cfg, ["2024-01"])
    counts = export_serving_db(cfg)
    return cfg, counts


def test_silver_stage_writes_daily_table_that_reconciles_with_silver_and_quarantine(spark, built):
    cfg, _ = built
    dq = spark.read.parquet(str(cfg.path("gold") / "data_quality_daily"))
    total = dq.agg(F.sum("input_rows"), F.sum("valid_rows"), F.sum("rejected_rows")).first()
    silver = spark.read.parquet(str(cfg.path("silver") / "trips_clean")).count()
    quarantine = spark.read.parquet(str(cfg.path("quarantine") / "trips_rejected")).count()
    bronze = spark.read.parquet(str(cfg.path("bronze") / "trips")).count()
    assert total[0] == bronze and total[1] == silver and total[2] == quarantine
    days = dq.where("day_kind = 'day'").agg(F.min("date"), F.max("date")).first()
    assert days[0].isoformat() >= "2024-01-01" and days[1].isoformat() <= "2024-01-31"


def test_rerunning_silver_does_not_duplicate_daily_rows(spark, built):
    cfg, _ = built
    path = str(cfg.path("gold") / "data_quality_daily")
    before = spark.read.parquet(path).count()
    cli.run_many(["silver"], cfg, ["2024-01"])
    assert spark.read.parquet(path).count() == before


def test_export_and_api_expose_daily_quality(built):
    cfg, counts = built
    assert counts["data_quality_daily"] > 0
    client = TestClient(create_app(cfg.path("serving_db")))
    rows = client.get("/data-quality").json()
    assert rows and all(r["day_kind"] == "day" for r in rows)
    assert {"reject_rate_pct", "alert_reject_rate", "null_pct_passenger_count", "ingest_lag_h"} <= set(rows[0])
    with_loose = client.get("/data-quality", params={"include_unattributed": True}).json()
    assert len(with_loose) > len(rows)
    flagged = client.get("/data-quality", params={"alerts_only": True}).json()
    assert all(r["alert_reject_rate"] == 1 and r["reject_rate_pct"] > r["alert_threshold_pct"] for r in flagged)
    narrow = client.get("/data-quality", params={"start": "2024-01-05", "end": "2024-01-07"}).json()
    assert {r["date"] for r in narrow} <= {"2024-01-05", "2024-01-06"}
