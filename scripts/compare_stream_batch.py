"""Compare the stream's Gold daily marts with a batch run over the same trips (M5 acceptance 3).

    python scripts/compare_stream_batch.py --stream output/stream/gold --batch output/check/gold

Run the batch job on the streamed month into a throwaway folder first (never into the batch store):
    python spark/batch_job.py --months 2026-01 --output-dir output/check [--max-records hvfhv=500000]
For every hour the stream wrote (only closed windows exist there), the batch row with the same keys must
have the same measures. Use the same quality profile (clean) and the same record cap in both paths: faults
injected into the stream and trips dropped as too late by the watermark make the two differ on purpose.
Exit code 1 on any difference.
"""
import argparse
import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "spark"))
from gold import KEYS, MARTS, TIME_COLS  # noqa: E402


def compare(con, name, stream_dir, batch_dir):
    """(stream rows, rows with a batch match, rows whose measures differ) for one mart."""
    s = f"read_parquet('{Path(stream_dir).resolve().as_posix()}/{name}/*.parquet')"
    b = f"read_parquet('{Path(batch_dir).resolve().as_posix()}/{name}/*/*/*.parquet')"
    keys = TIME_COLS + KEYS[name] + (["day_of_week"] if name == "demand_hourly" else [])
    columns = [d[0] for d in con.execute(f"SELECT * FROM {s} LIMIT 0").description]
    measures = [c for c in columns if c not in keys]
    on = " AND ".join(f"s.{k} IS NOT DISTINCT FROM b.{k}" for k in keys)
    differs = " OR ".join(f"abs(s.{m} - b.{m}) > 1e-6 * greatest(1, abs(s.{m}))" for m in measures)
    return con.execute(f"""
        SELECT count(*), count(b.hour_start), count(*) FILTER (WHERE b.hour_start IS NOT NULL AND ({differs}))
        FROM {s} s LEFT JOIN {b} b ON {on}""").fetchone()


def main():
    """Print one line per mart; return 1 if any stream row is missing from the batch or differs."""
    ap = argparse.ArgumentParser()
    ap.add_argument("--stream", default="output/stream/gold")
    ap.add_argument("--batch", default="output/check/gold")
    args = ap.parse_args()

    con = duckdb.connect()
    ok = True
    print(f"{'mart':26} {'stream rows':>12} {'matched':>9} {'differ':>7}")
    for name in MARTS:
        try:
            rows, matched, differ = compare(con, name, args.stream, args.batch)
        except duckdb.IOException:   # a mart the stream wrote nothing to (for example no complete hour yet)
            print(f"{name:26} (no files on one side)")
            continue
        ok &= rows == matched and differ == 0
        print(f"{name:26} {rows:12,} {matched:9,} {differ:7,}")
    print("\nOK: every stream row equals the batch row" if ok else "\nDIFFERENT: see the counts above")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
