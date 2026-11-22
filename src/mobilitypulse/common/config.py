"""Configuration loading: YAML file + MP__SECTION__KEY env overrides + path templating."""
from __future__ import annotations

import copy
import os
import re
from pathlib import Path
from typing import Any

import yaml

from .storage import AnyPath, configure_s3, is_uri, make_path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "configs" / "config.yaml"
TARGETS_PATH = PROJECT_ROOT / "configs" / "targets.yaml"
ENV_PREFIX = "MP__"
DATA_DIR_ENV = "MOBILITYPULSE_DATA_DIR"
CONFIG_ENV = "MOBILITYPULSE_CONFIG"
PROFILE_ENV = "MP_PROFILE"          # "setupf" = Spark standalone cluster + MinIO (see RunMobilityPulse_SetupF.md)
ENV_FILE_ENV = "MP_ENV_FILE"        # KEY=VALUE settings file for the profile (default: <project>/.env.setupF)
PROFILE_DIR = PROJECT_ROOT / "configs"


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


def load_env_file(path: str | Path) -> dict[str, str]:
    """Read KEY=VALUE lines (comments and blank lines skipped) and add them to os.environ without overriding
    variables that are already set. Returns what the file contained."""
    values: dict[str, str] = {}
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        m = re.match(r"^\s*([A-Za-z_][A-Za-z0-9_]*)=(.*)$", line)
        if m and not line.lstrip().startswith("#"):
            values[m.group(1)] = m.group(2).strip().strip('"').strip("'")
    for key, value in values.items():
        os.environ.setdefault(key, value)
    return values


def apply_cluster_env(raw: dict, environ: dict[str, str] | None = None) -> dict:
    """Map the SetupF settings file / environment (MASTER_IP, S3_*, memory, cores, ...) onto the config."""
    environ = os.environ if environ is None else environ
    out = copy.deepcopy(raw)
    spark, storage, kafka = (out.setdefault(k, {}) for k in ("spark", "storage", "kafka"))

    def get(name: str) -> str | None:
        value = (environ.get(name) or "").strip()
        return value or None

    if get("MASTER_IP"):
        spark["master"] = f"spark://{get('MASTER_IP')}:{get('MASTER_PORT') or 7077}"
        spark["driver_host"] = get("DRIVER_HOST") or get("MASTER_IP")
    for env_name, key in (("DRIVER_MEMORY", "driver_memory"), ("EXECUTOR_MEMORY", "executor_memory"),
                          ("EXECUTOR_CORES", "executor_cores"), ("TOTAL_CORES", "cores_max"),
                          ("EXECUTOR_PYTHON", "executor_python")):
        if get(env_name):
            spark[key] = _coerce(get(env_name)) if key.endswith(("cores", "cores_max")) else get(env_name)
    for env_name, key in (("S3_ENDPOINT", "s3_endpoint"), ("S3_ACCESS_KEY", "s3_access_key"),
                          ("S3_SECRET_KEY", "s3_secret_key"), ("S3_REGION", "s3_region")):
        if get(env_name):
            storage[key] = get(env_name)
    if get("KAFKA_BOOTSTRAP"):
        kafka["bootstrap_servers"] = get("KAFKA_BOOTSTRAP")
    return out


class Config:
    """Thin wrapper around the resolved config dictionary.

    ``paths.data_dir`` is the data lake root: a local folder, or an object-store URI such as
    ``s3a://lake/mobilitypulse``. ``paths.local_dir`` (default: the same as data_dir) holds what must stay on the
    driver machine: the downloaded raw files, the pipeline state table, the serving DB and the benchmark output.
    """

    def __init__(self, raw: dict[str, Any]):
        self.raw = raw
        st = raw.get("storage") or {}
        configure_s3(st.get("s3_endpoint"), st.get("s3_access_key"), st.get("s3_secret_key"), st.get("s3_region"))
        self.data_dir = self._abs(str(raw["paths"]["data_dir"]))
        self.local_dir = self._abs(str(raw["paths"].get("local_dir") or raw["paths"]["data_dir"]))

    @staticmethod
    def _abs(value: str) -> AnyPath:
        if is_uri(value):
            return make_path(value)
        p = Path(os.path.expanduser(value))
        return p if p.is_absolute() else PROJECT_ROOT / p

    @property
    def is_cluster(self) -> bool:
        return str(self.section("spark").get("master", "")).startswith("spark://")

    def __getitem__(self, key: str) -> Any:
        return self.raw[key]

    def section(self, name: str) -> dict[str, Any]:
        return self.raw.get(name, {}) or {}

    def path(self, name: str) -> AnyPath:
        template = str(self.raw["paths"][name])
        return self._abs(template.format(data_dir=self.data_dir, local_dir=self.local_dir))

    def with_overrides(self, overrides: dict) -> "Config":
        return Config(deep_merge(self.raw, overrides))


def load_config(path: str | Path | None = None, overrides: dict | None = None) -> Config:
    """Precedence: explicit overrides > MP__ env vars > MOBILITYPULSE_DATA_DIR > profile settings file > profile
    YAML > base YAML. A profile (MP_PROFILE=setupf) layers configs/profile_<name>.yaml and its settings file."""
    cfg_path = Path(path or os.environ.get(CONFIG_ENV) or DEFAULT_CONFIG_PATH)
    raw = yaml.safe_load(cfg_path.read_text(encoding="utf-8"))
    profile = os.environ.get(PROFILE_ENV)
    if profile:
        profile_file = PROFILE_DIR / f"profile_{profile}.yaml"
        if not profile_file.exists():
            raise FileNotFoundError(f"unknown profile {profile!r}: {profile_file} does not exist")
        raw = deep_merge(raw, yaml.safe_load(profile_file.read_text(encoding="utf-8")) or {})
        env_file = Path(os.environ.get(ENV_FILE_ENV) or PROJECT_ROOT / f".env.{profile}")
        if env_file.exists():
            load_env_file(env_file)
        raw = apply_cluster_env(raw)
    if os.environ.get(DATA_DIR_ENV):
        raw["paths"]["data_dir"] = os.environ[DATA_DIR_ENV]
    raw = apply_env_overrides(raw)
    if overrides:
        raw = deep_merge(raw, overrides)
    return Config(raw)


def load_targets(path: str | Path | None = None) -> list[dict[str, Any]]:
    return yaml.safe_load(Path(path or TARGETS_PATH).read_text(encoding="utf-8"))["targets"]
