#!/usr/bin/env python
"""Thin wrapper so the pipeline can be run without installing the package: python scripts/run_pipeline.py --stage all"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from mobilitypulse.cli import main  # noqa: E402

if __name__ == "__main__":
    main()
