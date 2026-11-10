"""Check that every message the producers sent ended up in exactly one Spark output.

    python scripts/reconcile.py [--output-dir output]

Per service: producer-stats/<dataset>.json `messages_sent` must equal Silver rows + invalid rows + duplicates
removed. Duplicates removed cannot be counted inside the streaming job, so they are derived
(sent - silver - invalid) and compared with the duplicates the producer injected. Every injected copy must be
either removed or rejected, so removed >= injected - invalid. Removed may exceed the injected count: trips that
are identical in every field (common in FHV, which has few fields) are collapsed too (PLAN 6.7 gap 4).
Also prints the reject reasons and the number of Silver rows carrying each dq_* flag. Exit code 1 on mismatch.

Reads the `service_type=<name>/*.parquet` partition files directly (ignoring Spark's _spark_metadata and
.crc files), so it does not depend on pyarrow's dataset discovery.
"""
import argparse
import json
import sys
from collections import Counter
from pathlib import Path

import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "spark"))
from transform import service_type  # noqa: E402


def partition_files(path):
    """Yield (service_type, parquet_file) for every committed data file under `path`."""
    for f in sorted(Path(path).glob("service_type=*/*.parquet")):
        yield f.parent.name.split("=", 1)[1], f


def count_by_service(path):
    """Row count per service_type, from the Parquet footers."""
    counts = Counter()
    for service, f in partition_files(path):
        counts[service] += pq.ParquetFile(f).metadata.num_rows
    return counts


def reject_reasons(path):
    """How often each reject reason occurs in the invalid output."""
    reasons = Counter()
    for _, f in partition_files(path):
        for value in pq.read_table(f, columns=["reject_reason"]).column(0).to_pylist():
            reasons[value] += 1
    return reasons


def flag_counts(path):
    """Number of Silver rows with each dq_* flag set."""
    flags = Counter()
    for _, f in partition_files(path):
        names = [n for n in pq.ParquetFile(f).schema_arrow.names if n.startswith("dq_")]
        table = pq.read_table(f, columns=names)
        for n in names:
            flags[n] += sum(table.column(n).to_pylist())
    return flags


def main():
    """Print the per-service reconciliation table; return 0 when it balances."""
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-dir", default="output")
    out = Path(ap.parse_args().output_dir)

    stats = {p.stem: json.loads(p.read_text()) for p in sorted((out / "producer-stats").glob("*.json"))}
    silver_dir, invalid_dir = out / "stream" / "silver" / "trips", out / "stream" / "silver_invalid"
    silver, invalid = count_by_service(silver_dir), count_by_service(invalid_dir)
    if not stats or not (silver or invalid):
        sys.exit(f"nothing to reconcile under {out} (need producer-stats/ and stream/silver output)")

    ok = True
    print(f"{'service':8} {'sent':>9} {'silver':>9} {'invalid':>9} {'removed':>9} {'injected dup':>13}")
    for name, s in stats.items():
        svc, sent = service_type(name), s["messages_sent"]
        removed = sent - silver[svc] - invalid[svc]
        injected = s.get("faults", {}).get("duplicate", 0)
        ok &= removed >= max(0, injected - invalid[svc])
        print(f"{svc:8} {sent:9,} {silver[svc]:9,} {invalid[svc]:9,} {removed:9,} {injected:13,}")

    print("\nreject reasons:")
    for reason, n in reject_reasons(invalid_dir).most_common():
        print(f"  {reason:28} {n:,}")
    print("\nSilver rows flagged (kept, not rejected):")
    for flag, n in flag_counts(silver_dir).most_common():
        print(f"  {flag:28} {n:,}")
    print("\nOK: every message accounted for" if ok else "\nMISMATCH: messages lost or invented by Spark")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
