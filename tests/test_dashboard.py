"""Dashboard tests: every dashboard page renders without an error, with no data and with a small batch store."""
from pathlib import Path

import duckdb
import pytest
from streamlit.testing.v1 import AppTest

APP = Path(__file__).resolve().parent.parent / "dashboard" / "app.py"
PAGES = ["Trends", "Revenue", "Efficiency", "Zones", "HVFHV", "Quality and health", "Live vs baseline", "ML models"]


def render(out, page, monkeypatch):
    """Run the app on the output folder `out` with `page` selected; return the AppTest."""
    monkeypatch.setenv("DASHBOARD_OUTPUT", str(out))
    at = AppTest.from_file(str(APP), default_timeout=60).run()
    return at.sidebar.radio[0].set_value(page).run()


@pytest.mark.parametrize("page", PAGES)
def test_pages_render_on_an_empty_store(page, tmp_path, monkeypatch):
    """No Parquet at all: each page shows a hint, not a traceback."""
    at = render(tmp_path, page, monkeypatch)
    assert not at.exception
    assert at.info


@pytest.fixture
def store(tmp_path):
    """A batch store with two months of trips_monthly (yellow and green) and one batch_run_metrics file."""
    con = duckdb.connect()
    for month in (11, 12):
        folder = tmp_path / "batch" / "gold" / "trips_monthly" / "year=2025" / f"month={month}"
        folder.mkdir(parents=True)
        con.execute(f"COPY (SELECT DATE '2025-{month}-01' AS month_start, s AS service_type, {month * 1000} AS trips "
                    f"FROM (VALUES ('yellow'), ('green')) t(s)) TO '{(folder / 'p.parquet').as_posix()}'")
    runs = tmp_path / "batch" / "gold" / "batch_run_metrics"
    runs.mkdir(parents=True)
    con.execute("COPY (SELECT 'yellow_2025-12.parquet' AS source_file, 'yellow' AS dataset, '2025-12' AS source_month, "
                "'success' AS status, 100 AS bronze_rows, 100 AS silver_rows, 0 AS duplicates_removed, 0 AS invalid_rows, "
                "0 AS late_correction_rows, 0 AS reconciliation_diff, 5 AS freshness_lag_days, 0 AS columns_added, "
                "0 AS columns_removed, 2.0 AS runtime_sec, 50.0 AS rows_per_sec, 2 AS months_expected) "
                f"TO '{(runs / 'p.parquet').as_posix()}'")
    return tmp_path


def test_trends_and_health_pages_show_data(store, monkeypatch):
    """The trips chart and growth table appear, and the load-time table lists the file."""
    trends = render(store, "Trends", monkeypatch)
    assert not trends.exception
    assert len(trends.dataframe) >= 1
    health = render(store, "Quality and health", monkeypatch)
    assert not health.exception
    assert "yellow_2025-12.parquet" in health.dataframe[0].value["source_file"].tolist()
