import sys
from datetime import datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "producer"))

from serialize import Record  # noqa: E402


@pytest.fixture(scope="session")
def stream_cfg():
    return yaml.safe_load((ROOT / "stream-config.yaml").read_text(encoding="utf-8"))


@pytest.fixture(scope="session")
def specs():
    raw = yaml.safe_load((ROOT / "producer" / "datasets.yaml").read_text(encoding="utf-8"))["datasets"]
    return {name: {**spec, "name": name} for name, spec in raw.items()}


@pytest.fixture
def green_spec(specs):
    return specs["green"]


T0 = datetime(2026, 1, 1, 0, 0, 0)


def green_row(i, ts=None):
    ts = ts or T0 + timedelta(minutes=i)
    return {
        "VendorID": 2, "lpep_pickup_datetime": ts, "lpep_dropoff_datetime": ts + timedelta(minutes=10),
        "store_and_fwd_flag": "N", "RatecodeID": 1, "PULocationID": 10 + i % 5, "DOLocationID": 50,
        "passenger_count": 1, "trip_distance": 2.5, "fare_amount": 12.0, "extra": 0.5, "mta_tax": 0.5,
        "tip_amount": 2.0, "tolls_amount": 0.0, "ehail_fee": None, "improvement_surcharge": 1.0,
        "total_amount": 16.0, "payment_type": 1, "trip_type": 1, "congestion_surcharge": 0.0,
        "cbd_congestion_fee": 0.0,
    }


def make_records(n):
    rows = [green_row(i) for i in range(n)]
    return [Record(row=r, ts=r["lpep_pickup_datetime"], key=str(r["PULocationID"])) for r in rows]


@pytest.fixture
def records():
    return make_records


def write_green_parquet(path, n, shuffle=True):
    """Synthetic green-schema parquet with shuffled pickup times (like the real files)."""
    rows = [green_row(i) for i in range(n)]
    if shuffle:
        rows = rows[::2][::-1] + rows[1::2]
    cols = {k: [r[k] for r in rows] for k in rows[0]}
    schema_types = {"ehail_fee": pa.float64()}
    arrays = {k: pa.array(v, type=schema_types.get(k)) for k, v in cols.items()}
    pq.write_table(pa.table(arrays), path, row_group_size=max(1, n // 4))
