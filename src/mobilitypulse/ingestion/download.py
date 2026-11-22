"""Step 1 - idempotent download of public sources into the RAW layer with a lineage manifest."""
from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

from ..common.config import Config
from ..common.utils import get_logger

log = get_logger(__name__)

try:
    import truststore

    # verify against the OS trust store so corporate TLS-inspecting proxies (e.g. Zscaler) work
    truststore.inject_into_ssl()
except ImportError:  # pragma: no cover
    pass

MANIFEST_COLUMNS = ["source", "url", "local_path", "period", "bytes", "sha256", "downloaded_at", "status"]


def sha256_of(path: Path, chunk: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(chunk), b""):
            h.update(block)
    return h.hexdigest()


class Manifest:
    def __init__(self, raw_dir: Path):
        self.path = raw_dir / "_manifest.parquet"
        if self.path.exists():
            self.df = pd.read_parquet(self.path)
        else:
            self.df = pd.DataFrame(columns=MANIFEST_COLUMNS)

    def get(self, local_path: Path) -> dict | None:
        rows = self.df[self.df["local_path"] == str(local_path)]
        return None if rows.empty else rows.iloc[-1].to_dict()

    def upsert(self, row: dict) -> None:
        self.df = self.df[self.df["local_path"] != row["local_path"]]
        self.df = pd.concat([self.df, pd.DataFrame([row], columns=MANIFEST_COLUMNS)], ignore_index=True)

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.df.astype({"bytes": "int64"}).to_parquet(self.path, index=False)


def validate_content(path: Path, suffix: str) -> None:
    """Reject error/redirect pages served with HTTP 200 (seen with moved NOAA URLs and proxies)."""
    with open(path, "rb") as f:
        head = f.read(512)
    if suffix == ".parquet" and not head.startswith(b"PAR1"):
        raise IOError(f"{path.name}: not a Parquet file")
    if suffix == ".csv" and head.lstrip().lower().startswith((b"<!doctype", b"<html")):
        raise IOError(f"{path.name}: received an HTML page instead of CSV")


def download(url: str, dest: Path, timeout: int = 120) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + ".part")
    with requests.get(url, stream=True, timeout=timeout) as resp:
        resp.raise_for_status()
        with open(tmp, "wb") as f:
            for block in resp.iter_content(chunk_size=1 << 20):
                f.write(block)
    if tmp.stat().st_size == 0:
        tmp.unlink()
        raise IOError(f"Empty download from {url}")
    try:
        validate_content(tmp, dest.suffix)
    except IOError:
        tmp.unlink()
        raise
    tmp.replace(dest)


def fetch(manifest: Manifest, source: str, url: str, dest: Path, period: str, force: bool = False) -> dict:
    known = manifest.get(dest)
    if known and (known["status"] == "generated" or known["url"] != url):
        known = None  # synthetic sample or changed source URL: fetch again
    if not force and dest.exists() and known and int(known["bytes"]) == dest.stat().st_size \
            and known["sha256"] == sha256_of(dest):
        log.info("skip  %-12s %s (unchanged)", source, dest.name)
        row = {**known, "status": "skipped"}
    else:
        log.info("fetch %-12s %s", source, url)
        download(url, dest)
        row = {
            "source": source, "url": url, "local_path": str(dest), "period": period,
            "bytes": dest.stat().st_size, "sha256": sha256_of(dest),
            "downloaded_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "status": "downloaded",
        }
    manifest.upsert(row)
    return row


def run_ingest(cfg: Config, months: list[str], force: bool = False) -> list[dict]:
    src = cfg.section("sources")
    raw = cfg.path("raw")
    manifest = Manifest(raw)
    rows = [fetch(manifest, "taxi_zones", src["zones_url"], raw / "reference" / "taxi_zone_lookup.csv", "static", force)]
    for month in months:
        name = f"{src['tlc_dataset']}_{month}.parquet"
        rows.append(fetch(manifest, "tlc_yellow", f"{src['tlc_base_url']}/{name}", raw / "tlc" / name, month, force))
    station = src["noaa_station"]
    for year in sorted({m[:4] for m in months}):
        rows.append(fetch(manifest, "noaa_isd", f"{src['noaa_base_url']}/{year}/{station}.csv",
                          raw / "noaa" / f"{station}_{year}.csv", year, force))
    manifest.save()
    return rows
