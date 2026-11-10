"""Compare the stream's measurable results with the proposed targets in evaluation.md.

    python scripts/evaluate_stream.py [--output-dir output]

Measures from the Silver Parquet only: S1 (latency = silver_write_ts - kafka_timestamp, per row) and
S3 (sustained throughput = Silver rows / seconds between the first and last write). The other targets
(S2 micro-batch duration, S4 Gold freshness, S5 restart, S7 memory) need Spark's progress log, the M8
restart test or `docker stats`, so they are listed as not measured here. Exit code 1 if a target is missed.
"""
import argparse
import sys
from pathlib import Path

import pyarrow.compute as pc
import pyarrow.parquet as pq

# evaluation.md section 2 (proposed values)
S1_P50, S1_P95, S1_MAX = 20.0, 45.0, 90.0
S3_MIN_PER_SEC = 7000


def read_silver_times(silver_dir):
    """Return (latency seconds list, write timestamps list) for every Silver row that has a Kafka timestamp."""
    files = sorted(Path(silver_dir).glob("service_type=*/*.parquet"))
    if not files:
        sys.exit(f"no Silver Parquet under {silver_dir}: run the Spark job first")
    latency, writes = [], []
    for f in files:
        t = pq.read_table(f, columns=["kafka_timestamp", "silver_write_ts"])
        t = t.filter(pc.is_valid(t["kafka_timestamp"]))
        diff = pc.subtract(t["silver_write_ts"], t["kafka_timestamp"])
        latency += [d.total_seconds() for d in diff.to_pylist()]
        writes += t["silver_write_ts"].to_pylist()
    return latency, writes


def percentile(values, q):
    """The q-quantile (0..1) of a non-empty list, by nearest rank."""
    s = sorted(values)
    return s[min(len(s) - 1, int(q * len(s)))]


def main():
    """Print each measured target with pass/fail; return 1 when any measured target fails."""
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-dir", default="output")
    out = Path(ap.parse_args().output_dir) / "stream" / "silver" / "trips"

    latency, writes = read_silver_times(out)
    if not latency:
        sys.exit("Silver has no kafka_timestamp values (was it produced from the files source?)")
    p50, p95, worst = percentile(latency, 0.5), percentile(latency, 0.95), max(latency)
    span = max((max(writes) - min(writes)).total_seconds(), 1.0)
    rate = len(writes) / span

    results = [
        (f"S1 latency p50 <= {S1_P50:g}s", f"{p50:.1f}s", p50 <= S1_P50),
        (f"S1 latency p95 <= {S1_P95:g}s", f"{p95:.1f}s", p95 <= S1_P95),
        (f"S1 latency max <= {S1_MAX:g}s", f"{worst:.1f}s", worst <= S1_MAX),
        (f"S3 throughput >= {S3_MIN_PER_SEC}/s", f"{rate:,.0f}/s over {span:.0f}s ({len(writes):,} rows)",
         rate >= S3_MIN_PER_SEC),
    ]
    for name, value, ok in results:
        print(f"{'PASS' if ok else 'FAIL'}  {name:28} measured {value}")
    print("not measured here: S2 (micro-batch duration), S4 (Gold freshness), S5 (restart), S7 (memory)")
    return 0 if all(ok for *_, ok in results) else 1


if __name__ == "__main__":
    sys.exit(main())
