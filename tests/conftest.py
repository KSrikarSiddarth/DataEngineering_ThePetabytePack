import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from mobilitypulse.common.config import load_config  # noqa: E402

TEST_OVERRIDES = {
    "spark": {"master": "local[2]", "shuffle_partitions": 4, "driver_memory": "2g", "ui_enabled": False,
              "log_level": "ERROR"},
    "features": {"min_zone_trips": 50},
    "ml": {"test_days": 5, "validation_days": 3, "models": ["linear_regression", "gbt"],
           "gbt": {"max_iter": 8, "max_depth": 4, "step_size": 0.2}},
    "anomaly": {"kmeans_k": 3, "injection_min_expected": 5, "injection_fraction": 0.05},
    "sample": {"months": ["2024-01"], "active_zones": 8, "mean_trips_per_zone_hour": 6.0},
}


@pytest.fixture(scope="session")
def base_cfg(tmp_path_factory):
    data = tmp_path_factory.mktemp("mp_data")
    results = tmp_path_factory.mktemp("mp_results")
    os.environ.pop("MOBILITYPULSE_DATA_DIR", None)
    return load_config(overrides={**TEST_OVERRIDES, "paths": {"data_dir": str(data), "results": str(results)}})


@pytest.fixture(scope="session")
def spark(base_cfg):
    from mobilitypulse.common.spark import get_spark

    s = get_spark(base_cfg, "tests")
    yield s
    s.stop()
