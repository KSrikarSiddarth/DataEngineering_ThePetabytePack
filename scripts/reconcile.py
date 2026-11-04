"""Check that every message the producers sent ended up in exactly one Spark output.

    python scripts/reconcile.py [--output-dir output]

Compares, per service: producer-stats/<dataset>.json `messages_sent` against the row counts of
<output-dir>/trips_clean + <output-dir>/invalid, and prints the reject reasons. Exit code 1 on mismatch.

Reads the `service=<name>/*.parquet` partition files directly (ignoring Spark's _spark_metadata and
.crc files), so it does not depend on pyarrow's dataset discovery.
"""
import argparse
import json
import sys
from collections import Counter
from pathlib import Path

import pyarrow.parquet as pq


def partition_files(path):
    """Yield (service, parquet_file) for every committed data file under `path`."""
    for f in sorted(Path(path).glob("service=*/*.parquet")):
        yield f.parent.name.split("=", 1)[1], f


def count_by_service(path):
    counts = Counter()
    for service, f in partition_files(path):
        counts[service] += pq.ParquetFile(f).metadata.num_rows
    return counts


def reject_reasons(path):
    reasons = Counter()
    for _, f in partition_files(path):
        for value in pq.read_table(f, columns=["reject_reason"]).column(0).to_pylist():
            for part in (value or "").split(","):
                reasons[part] += 1
    return reasons


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-dir", default="output")
    out = Path(ap.parse_args().output_dir)

    stats = {p.stem: json.loads(p.read_text()) for p in sorted((out / "producer-stats").glob("*.json"))}
    clean, invalid = count_by_service(out / "trips_clean"), count_by_service(out / "invalid")
    if not stats or not (clean or invalid):
        sys.exit(f"nothing to reconcile under {out} (need producer-stats/ and Spark output)")

    ok = True
    print(f"{'service':8} {'sent':>9} {'clean':>9} {'invalid':>9} {'diff':>6}")
    for name, s in stats.items():
        sent = s["messages_sent"]
        diff = clean[name] + invalid[name] - sent
        ok &= diff == 0
        print(f"{name:8} {sent:9,} {clean[name]:9,} {invalid[name]:9,} {diff:6}")

    print("\nreject reasons:")
    for reason, n in reject_reasons(out / "invalid").most_common():
        print(f"  {reason:28} {n:,}")
    print("\nOK: every message accounted for" if ok else "\nMISMATCH: messages lost or duplicated by Spark")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
