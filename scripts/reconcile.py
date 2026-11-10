"""Check that every message the producers sent ended up in exactly one Spark output.

    python scripts/reconcile.py [--output-dir output]                     # the stream (M3/M4)
    python scripts/reconcile.py --batch [--output-dir output] [--data-dir data]   # the batch store (M5)

Stream, per service: producer-stats/<dataset>.json `messages_sent` must equal Silver rows + invalid rows + duplicates
removed. Duplicates removed cannot be counted inside the streaming job, so they are derived
(sent - silver - invalid) and compared with the duplicates the producer injected. Every injected copy must be
either removed or rejected, so removed >= injected - invalid. Removed may exceed the injected count: trips that
are identical in every field (common in FHV, which has few fields) are collapsed too (PLAN 6.7 gap 4).
Also prints the reject reasons and the number of Silver rows carrying each dq_* flag. Exit code 1 on mismatch.

Batch, per source file (the latest load in silver/batch_run_log): rows in the file = Silver rows + invalid
rows + duplicates removed, where the Silver and invalid rows are counted in the written Parquet and the file's
rows from its own footer. This is the catalog's Bronze-to-Silver row reconciliation, which should be 0.

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


def rows_under(path):
    """Rows in every Parquet file under `path` (0 when it does not exist), from the footers."""
    return sum(pq.ParquetFile(f).metadata.num_rows for f in Path(path).rglob("*.parquet"))


def reconcile_batch(out, data_dir):
    """Print the per-file Bronze-to-Silver reconciliation of the batch store; return 0 when every file balances."""
    batch = out / "batch"
    log_files = list((batch / "silver" / "batch_run_log").glob("*.parquet"))
    if not log_files:
        sys.exit(f"no batch_run_log under {batch / 'silver'}: run the batch job first")
    latest = {}   # source_file -> its most recent load
    for f in log_files:
        for row in pq.read_table(f).to_pylist():
            if row["source_file"] not in latest or row["start_ts"] > latest[row["source_file"]]["start_ts"]:
                latest[row["source_file"]] = row

    ok = True
    print(f"{'file':34} {'source':>11} {'silver':>11} {'invalid':>9} {'dups':>7} {'diff':>6}")
    for name, r in sorted(latest.items(), key=lambda kv: (kv[1]["source_month"], kv[0])):
        if r["status"] != "success":
            ok = False
            print(f"{name:34} FAILED: {r['error']}")
            continue
        part = f"service_type={r['service_type']}/year={int(r['source_month'][:4])}/month={int(r['source_month'][5:])}"
        source = pq.ParquetFile(Path(data_dir) / name).metadata.num_rows
        silver, invalid = rows_under(batch / "silver" / "trips" / part), rows_under(batch / "silver_invalid" / part)
        diff = silver + invalid + r["duplicates_removed"] - source
        ok &= diff == 0
        print(f"{name:34} {source:11,} {silver:11,} {invalid:9,} {r['duplicates_removed']:7,} {diff:6}")
    print("\nOK: every source row is accounted for" if ok else "\nMISMATCH: rows lost or invented, or a load failed")
    return 0 if ok else 1


def main():
    """Print the reconciliation table (stream, or batch with --batch); return 0 when it balances."""
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-dir", default="output")
    ap.add_argument("--batch", action="store_true", help="reconcile the batch store against the source files")
    ap.add_argument("--data-dir", default="data", help="folder with the source files (for --batch)")
    args = ap.parse_args()
    out = Path(args.output_dir)
    if args.batch:
        return reconcile_batch(out, args.data_dir)

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
