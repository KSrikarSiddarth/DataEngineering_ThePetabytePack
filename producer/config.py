"""Load settings for one producer run from stream-config.yaml, datasets.yaml and env vars.

Environment variables override the YAML (empty strings count as unset):
DATASET, CONFIG_PATH, DATASETS_PATH, KAFKA_BOOTSTRAP, DATA_DIR, MONTHS (comma list),
REPLAY_SPEEDUP, MAX_RECORDS, SAMPLE_FRACTION, QUALITY_PROFILE, STATS_DIR.
"""
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import yaml

HERE = Path(__file__).resolve().parent
RATE_KEYS = (
    "null_rate",
    "invalid_rate",
    "duplicate_rate",
    "late_rate",
    "out_of_order_rate",
    "malformed_json_rate",
    "schema_drift_rate",
)


def _env(env, name):
    value = env.get(name)
    return value if value not in (None, "") else None


def _load_yaml(path):
    with open(path, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def default_config_path():
    in_container = Path("/config/stream-config.yaml")
    return in_container if in_container.exists() else HERE.parent / "stream-config.yaml"


@dataclass
class Settings:
    dataset: str
    spec: dict
    topic: str
    bootstrap: str
    data_dir: Path
    months: list
    speedup: float
    max_eps: float
    start_ts: "datetime | None"
    max_records: int
    sample_fraction: float
    seed: int
    read_batch_rows: int
    profile: str
    rates: dict
    quality: dict
    delivery: dict
    progress_every: int
    connect_retry_s: int
    stats_dir: "Path | None"


def resolve_rates(quality, profile):
    profiles = quality["profiles"]
    if profile not in profiles:
        raise ValueError(f"unknown quality profile {profile!r}; choose from {sorted(profiles)}")
    rates = {key: float(profiles[profile].get(key, 0.0)) for key in RATE_KEYS}
    for key, value in rates.items():
        if not 0.0 <= value <= 1.0:
            raise ValueError(f"{key}={value} must be between 0 and 1")
    return rates


def load_settings(dataset=None, env=None):
    env = os.environ if env is None else env
    dataset = dataset or _env(env, "DATASET")
    if not dataset:
        raise ValueError("no dataset given (use --dataset or DATASET)")

    cfg = _load_yaml(_env(env, "CONFIG_PATH") or default_config_path())
    specs = _load_yaml(_env(env, "DATASETS_PATH") or HERE / "datasets.yaml")["datasets"]
    if dataset not in specs:
        raise ValueError(f"unknown dataset {dataset!r}; choose from {sorted(specs)}")
    spec = {**specs[dataset], "name": dataset}

    streaming = cfg["streaming"]
    pacing = streaming["pacing"]
    volume = streaming["volume"]
    quality = cfg["data_quality"]

    max_records = _env(env, "MAX_RECORDS")
    if max_records is None:
        max_records = volume.get("max_records", {}).get(dataset, 0)
    months = _env(env, "MONTHS")
    months = [m.strip() for m in months.split(",")] if months else list(streaming["months"])
    start = pacing.get("start_offset")
    stats_dir = _env(env, "STATS_DIR")
    profile = _env(env, "QUALITY_PROFILE") or quality["active_profile"]

    return Settings(
        dataset=dataset,
        spec=spec,
        topic=spec["topic"],
        bootstrap=_env(env, "KAFKA_BOOTSTRAP") or cfg["kafka"]["bootstrap_servers_internal"],
        data_dir=Path(_env(env, "DATA_DIR") or streaming["input_dir"]),
        months=months,
        speedup=float(_env(env, "REPLAY_SPEEDUP") or pacing["replay_speedup"]),
        max_eps=float(pacing.get("max_events_per_second", 0)),
        start_ts=datetime.fromisoformat(str(start)) if start and pacing.get("shared_clock") else None,
        max_records=int(max_records),
        sample_fraction=float(_env(env, "SAMPLE_FRACTION") or volume["sample_fraction"]),
        seed=int(streaming["seed"]),
        read_batch_rows=int(volume["read_batch_rows"]),
        profile=profile,
        rates=resolve_rates(quality, profile),
        quality=quality,
        delivery=streaming["delivery"],
        progress_every=int(streaming["logging"]["progress_every_records"]),
        connect_retry_s=int(streaming["delivery"]["connect_retry_seconds"]),
        stats_dir=Path(stats_dir) if stats_dir else None,
    )
