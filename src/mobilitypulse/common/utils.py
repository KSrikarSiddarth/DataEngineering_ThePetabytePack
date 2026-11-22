"""Small shared helpers: logging, timing, parquet IO, month specs, US holidays, measurements store."""
from __future__ import annotations

import calendar
import json
import logging
import os
import tempfile
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

from .config import Config

_LOG_CONFIGURED = False


def get_logger(name: str) -> logging.Logger:
    global _LOG_CONFIGURED
    if not _LOG_CONFIGURED:
        logging.basicConfig(
            level=os.environ.get("MP_LOG_LEVEL", "INFO"),
            format="%(asctime)s %(levelname)-7s %(name)s - %(message)s",
            datefmt="%H:%M:%SZ",
        )
        logging.Formatter.converter = time.gmtime
        logging.getLogger("py4j").setLevel(logging.WARNING)
        _LOG_CONFIGURED = True
    return logging.getLogger(name)


class Timer:
    def __enter__(self) -> "Timer":
        self.start = time.perf_counter()
        self.seconds = 0.0
        return self

    def __exit__(self, *exc: Any) -> None:
        self.seconds = time.perf_counter() - self.start


# ---------------------------------------------------------------- parquet IO
def write_parquet(df, path: Path, partition_by: list[str] | None = None, mode: str = "overwrite",
                  replace_all: bool = False) -> None:
    """Overwrite only the partitions present in df (idempotent per month) unless replace_all."""
    writer = df.write.mode(mode)
    if replace_all:
        writer = writer.option("partitionOverwriteMode", "static")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.parquet(str(path))


def read_parquet(spark, path: Path, stage_hint: str = ""):
    if not Path(path).exists():
        hint = f" (run stage '{stage_hint}' first)" if stage_hint else ""
        raise FileNotFoundError(f"Missing dataset: {path}{hint}")
    return spark.read.parquet(str(path))


# ---------------------------------------------------------------- months
def _month_iter(start: str, end: str) -> list[str]:
    y, m = map(int, start.split("-"))
    ey, em = map(int, end.split("-"))
    out = []
    while (y, m) <= (ey, em):
        out.append(f"{y:04d}-{m:02d}")
        m += 1
        if m == 13:
            y, m = y + 1, 1
    return out


def parse_months(spec: str | Iterable[str] | None, cfg: Config) -> list[str]:
    """Accepts None (default set), a dataset name (small/medium/large), 'YYYY-MM..YYYY-MM', or a comma list."""
    datasets = cfg.section("datasets")
    if spec is None:
        return list(datasets["default"])
    if not isinstance(spec, str):
        months = list(spec)
    elif spec in datasets:
        months = list(datasets[spec])
    elif ".." in spec:
        start, end = spec.split("..")
        months = _month_iter(start.strip(), end.strip())
    else:
        months = [m.strip() for m in spec.split(",") if m.strip()]
    for m in months:
        datetime.strptime(m, "%Y-%m")
    return sorted(set(months))


def month_range(months: list[str]) -> tuple[datetime, datetime]:
    """[start, end) of the covered months as naive wall-clock datetimes."""
    first, last = min(months), max(months)
    y, m = map(int, first.split("-"))
    start = datetime(y, m, 1)
    y, m = map(int, last.split("-"))
    end = datetime(y + (m == 12), 1 if m == 12 else m + 1, 1)
    return start, end


# ---------------------------------------------------------------- holidays
def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    first = date(year, month, 1)
    return first + timedelta(days=(weekday - first.weekday()) % 7 + 7 * (n - 1))


def _last_weekday(year: int, month: int, weekday: int) -> date:
    last = date(year, month, calendar.monthrange(year, month)[1])
    return last - timedelta(days=(last.weekday() - weekday) % 7)


def us_holidays(years: Iterable[int]) -> list[date]:
    out = []
    for y in years:
        out += [
            date(y, 1, 1),
            _nth_weekday(y, 1, 0, 3),   # MLK Day
            _nth_weekday(y, 2, 0, 3),   # Presidents Day
            _last_weekday(y, 5, 0),     # Memorial Day
            date(y, 6, 19),
            date(y, 7, 4),
            _nth_weekday(y, 9, 0, 1),   # Labor Day
            _nth_weekday(y, 10, 0, 2),  # Columbus Day
            date(y, 11, 11),
            _nth_weekday(y, 11, 3, 4),  # Thanksgiving
            date(y, 12, 25),
        ]
    return sorted(out)


# ---------------------------------------------------------------- measurements
def _measurements_path(cfg: Config) -> Path:
    return cfg.path("results") / "measurements.json"


def load_measurements(cfg: Config) -> dict[str, Any]:
    p = _measurements_path(cfg)
    return json.loads(p.read_text()) if p.exists() else {}


def record_measurements(cfg: Config, values: dict[str, Any], context: dict[str, Any] | None = None) -> None:
    """Merge measurement values into results/measurements.json (atomic replace)."""
    p = _measurements_path(cfg)
    p.parent.mkdir(parents=True, exist_ok=True)
    data = load_measurements(cfg)
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    for key, value in values.items():
        if value is None:
            continue
        data[key] = {"value": float(value) if isinstance(value, (int, float)) else value,
                     "recorded_at": now, "context": context or {}}
    fd, tmp = tempfile.mkstemp(dir=p.parent, suffix=".json")
    with os.fdopen(fd, "w") as f:
        json.dump(data, f, indent=2, sort_keys=True)
    os.replace(tmp, p)


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, default=str))
