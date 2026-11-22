#!/usr/bin/env python
"""Record a manually measured value (e.g. D4 fresh-clone time): python scripts/record_measurement.py repro.sample_e2e_min 18.5"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from mobilitypulse.common.config import load_config  # noqa: E402
from mobilitypulse.common.utils import record_measurements  # noqa: E402

if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit(__doc__)
    record_measurements(load_config(), {sys.argv[1]: float(sys.argv[2])}, {"source": "manual"})
    print(f"recorded {sys.argv[1]} = {sys.argv[2]}")
