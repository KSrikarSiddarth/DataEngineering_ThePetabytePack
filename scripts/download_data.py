"""Download TLC monthly trip files into data/ (decisions.md D15).

Usage:
    python scripts/download_data.py                       # months + datasets from stream-config.yaml
    python scripts/download_data.py --datasets green fhv  # subset
    python scripts/download_data.py --months 2026-01 2026-02
    python scripts/download_data.py --batch               # the batch month range (batch: in stream-config.yaml)

Skips files that already exist and are valid Parquet. Downloads to a .part file
and renames on success, so an interrupted download never leaves a bad file.
Also records the date TLC published each file in data/tlc_published.json (the batch job's freshness KPI).
"""
import argparse
import json
import sys
import urllib.request
from email.utils import parsedate_to_datetime
from pathlib import Path

import pyarrow.parquet as pq
import yaml

ROOT = Path(__file__).resolve().parent.parent
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
FILE_PREFIX = {
    "yellow": "yellow_tripdata",
    "green": "green_tripdata",
    "fhv": "fhv_tripdata",
    "hvfhv": "fhvhv_tripdata",
}


def is_valid_parquet(path: Path) -> bool:
    try:
        return pq.ParquetFile(path).metadata.num_rows > 0
    except Exception:
        return False


def download(url: str, dest: Path) -> None:
    part = dest.with_suffix(dest.suffix + ".part")
    with urllib.request.urlopen(url, timeout=60) as resp, open(part, "wb") as out:
        total = int(resp.headers.get("Content-Length", 0))
        done = 0
        while chunk := resp.read(1 << 20):
            out.write(chunk)
            done += len(chunk)
            if total:
                print(f"\r  {done / 1e6:7.1f} / {total / 1e6:.1f} MB", end="", flush=True)
    print()
    part.replace(dest)


def published_date(url: str):
    """The date TLC published a file: the Last-Modified header of a HEAD request (None if unavailable)."""
    try:
        with urllib.request.urlopen(urllib.request.Request(url, method="HEAD"), timeout=60) as resp:
            value = resp.headers.get("Last-Modified")
        return parsedate_to_datetime(value).date().isoformat() if value else None
    except Exception:
        return None


def month_range(start: str, end: str) -> list:
    """Every 'YYYY-MM' from `start` to `end`, inclusive."""
    y, m = map(int, start.split("-"))
    end_y, end_m = map(int, end.split("-"))
    months = []
    while (y, m) <= (end_y, end_m):
        months.append(f"{y:04d}-{m:02d}")
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return months


def main() -> int:
    cfg = yaml.safe_load((ROOT / "stream-config.yaml").read_text(encoding="utf-8"))
    ap = argparse.ArgumentParser()
    ap.add_argument("--datasets", nargs="+", choices=FILE_PREFIX, default=list(FILE_PREFIX))
    ap.add_argument("--months", nargs="+")
    ap.add_argument("--batch", action="store_true", help="the batch month range from stream-config.yaml")
    ap.add_argument("--data-dir", default=str(ROOT / "data"))
    args = ap.parse_args()
    months = args.months or (month_range(cfg["batch"]["start_month"], cfg["batch"]["end_month"]) if args.batch
                             else cfg["streaming"]["months"])

    data_dir = Path(args.data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)
    published_file = data_dir / "tlc_published.json"
    published = json.loads(published_file.read_text()) if published_file.exists() else {}
    failures = 0
    for month in months:
        for ds in args.datasets:
            name = f"{FILE_PREFIX[ds]}_{month}.parquet"
            dest = data_dir / name
            if dest.exists() and is_valid_parquet(dest):
                print(f"skip     {name} (already present)")
                if name not in published and (date := published_date(f"{BASE_URL}/{name}")):
                    published[name] = date
                continue
            print(f"download {name}")
            try:
                download(f"{BASE_URL}/{name}", dest)
            except Exception as exc:
                print(f"  FAILED: {exc}")
                failures += 1
                continue
            if not is_valid_parquet(dest):
                print(f"  FAILED: {name} is not readable Parquet")
                dest.unlink(missing_ok=True)
                failures += 1
            else:
                rows = pq.ParquetFile(dest).metadata.num_rows
                print(f"  ok, {rows:,} rows")
                if date := published_date(f"{BASE_URL}/{name}"):
                    published[name] = date
    published_file.write_text(json.dumps(published, indent=2, sort_keys=True))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
