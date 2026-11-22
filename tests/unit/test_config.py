from datetime import date

import pytest

from mobilitypulse.common.config import apply_env_overrides, load_config
from mobilitypulse.common.utils import month_range, parse_months, us_holidays


def test_env_overrides_create_nested_and_coerce_types():
    raw = {"spark": {"shuffle_partitions": 32}}
    out = apply_env_overrides(raw, {"MP__SPARK__SHUFFLE_PARTITIONS": "64", "MP__NEW__FLAG": "true", "OTHER": "x"})
    assert out["spark"]["shuffle_partitions"] == 64
    assert out["new"]["flag"] is True
    assert raw["spark"]["shuffle_partitions"] == 32


def test_paths_resolve_against_data_dir(tmp_path):
    cfg = load_config(overrides={"paths": {"data_dir": str(tmp_path)}})
    assert cfg.path("bronze") == tmp_path / "bronze"
    assert cfg.path("stream_out") == tmp_path / "gold" / "stream_zone_metrics"


def test_data_dir_env(monkeypatch, tmp_path):
    monkeypatch.setenv("MOBILITYPULSE_DATA_DIR", str(tmp_path))
    assert load_config().data_dir == tmp_path


@pytest.mark.parametrize("spec,expected", [
    ("2024-01..2024-03", ["2024-01", "2024-02", "2024-03"]),
    ("2023-12..2024-01", ["2023-12", "2024-01"]),
    ("2024-02,2024-01", ["2024-01", "2024-02"]),
    ("small", ["2024-01"]),
])
def test_parse_months(spec, expected):
    assert parse_months(spec, load_config()) == expected


def test_parse_months_rejects_garbage():
    with pytest.raises(ValueError):
        parse_months("2024-13", load_config())


def test_month_range_crosses_year():
    start, end = month_range(["2024-11", "2024-12"])
    assert (start.month, end.year, end.month) == (11, 2025, 1)


def test_us_holidays_2024():
    h = set(us_holidays([2024]))
    assert date(2024, 1, 15) in h      # MLK Day
    assert date(2024, 5, 27) in h      # Memorial Day
    assert date(2024, 11, 28) in h     # Thanksgiving
    assert len(h) == 11
