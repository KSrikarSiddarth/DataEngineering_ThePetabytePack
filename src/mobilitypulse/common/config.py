"""Configuration loading: YAML file + MP__SECTION__KEY env overrides + path templating."""
from __future__ import annotations

import copy
import os
from pathlib import Path
from typing import Any

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "configs" / "config.yaml"
TARGETS_PATH = PROJECT_ROOT / "configs" / "targets.yaml"
ENV_PREFIX = "MP__"
DATA_DIR_ENV = "MOBILITYPULSE_DATA_DIR"
CONFIG_ENV = "MOBILITYPULSE_CONFIG"


def _coerce(value: str) -> Any:
    try:
        return yaml.safe_load(value)
    except yaml.YAMLError:
        return value


def deep_merge(base: dict, override: dict) -> dict:
    out = copy.deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = deep_merge(out[key], value)
        else:
            out[key] = copy.deepcopy(value)
    return out


def apply_env_overrides(raw: dict, environ: dict[str, str] | None = None) -> dict:
    environ = os.environ if environ is None else environ
    out = copy.deepcopy(raw)
    for key, value in environ.items():
        if not key.startswith(ENV_PREFIX):
            continue
        parts = [p.lower() for p in key[len(ENV_PREFIX):].split("__") if p]
        if not parts:
            continue
        node = out
        for part in parts[:-1]:
            if not isinstance(node.get(part), dict):
                node[part] = {}
            node = node[part]
        node[parts[-1]] = _coerce(value)
    return out


class Config:
    """Thin wrapper around the resolved config dictionary."""

    def __init__(self, raw: dict[str, Any]):
        self.raw = raw
        self.data_dir = self._abs(str(raw["paths"]["data_dir"]))

    @staticmethod
    def _abs(value: str) -> Path:
        p = Path(os.path.expanduser(value))
        return p if p.is_absolute() else PROJECT_ROOT / p

    def __getitem__(self, key: str) -> Any:
        return self.raw[key]

    def section(self, name: str) -> dict[str, Any]:
        return self.raw.get(name, {}) or {}

    def path(self, name: str) -> Path:
        template = str(self.raw["paths"][name])
        return self._abs(template.format(data_dir=self.data_dir))

    def with_overrides(self, overrides: dict) -> "Config":
        return Config(deep_merge(self.raw, overrides))


def load_config(path: str | Path | None = None, overrides: dict | None = None) -> Config:
    """Precedence: explicit overrides > MP__ env vars > MOBILITYPULSE_DATA_DIR > YAML."""
    cfg_path = Path(path or os.environ.get(CONFIG_ENV) or DEFAULT_CONFIG_PATH)
    raw = yaml.safe_load(cfg_path.read_text())
    if os.environ.get(DATA_DIR_ENV):
        raw["paths"]["data_dir"] = os.environ[DATA_DIR_ENV]
    raw = apply_env_overrides(raw)
    if overrides:
        raw = deep_merge(raw, overrides)
    return Config(raw)


def load_targets(path: str | Path | None = None) -> list[dict[str, Any]]:
    return yaml.safe_load(Path(path or TARGETS_PATH).read_text())["targets"]
