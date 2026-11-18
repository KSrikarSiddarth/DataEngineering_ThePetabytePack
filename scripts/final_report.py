"""Write FinalReport.txt: the measured results of the pipeline against the targets in evaluation.md, read with DuckDB.

    python scripts/final_report.py [--output-dir output] [--report FinalReport.txt]

Three sections, each skipped with a note when its data does not exist yet:
  1. Batch processing: pipeline health (scripts/queries/batch_health.sql) and every file load's time and throughput.
  2. Streaming targets S1 (latency) and S3 (throughput), measured from the stream Silver Parquet.
  3. Model targets M1-M10 and O1/O3 from the latest Spark ML run (same checks as scripts/evaluate_models.py).
Targets that need Spark's progress log, `docker stats` or a restart test (S2, S4-S7, O2, O4) are listed as not measured.
"""
import argparse
from datetime import datetime
from pathlib import Path

import duckdb

from evaluate_models import evaluate

ROOT = Path(__file__).resolve().parent.parent
S1_P50, S1_P95, S1_MAX, S3_MIN_PER_SEC = 20.0, 45.0, 90.0, 7000   # evaluation.md section 2
WIDTH = 78


def parquet(folder, pattern="*.parquet"):
    """DuckDB read_parquet() expression for `pattern` under `folder`, or None when no such file exists."""
    return f"read_parquet('{(Path(folder) / pattern).resolve().as_posix()}')" if any(Path(folder).glob(pattern)) else None


def status(ok):
    """PASS / FAIL, or n/a when the target could not be measured (ok is None)."""
    return "n/a " if ok is None else "PASS" if ok else "FAIL"


def table(rows, headers):
    """Plain-text table: left-aligned columns sized to their widest cell."""
    rows = [[str(c) for c in r] for r in rows]
    widths = [max(len(h), *(len(r[i]) for r in rows)) for i, h in enumerate(headers)]
    line = lambda cells: "  ".join(c.ljust(w) for c, w in zip(cells, widths)).rstrip()
    return [line(headers), line(["-" * w for w in widths]), *(line(r) for r in rows)]


def batch_section(con, batch_dir):
    """Lines for the batch section: the health summary row, then one row per file load, slowest first."""
    source = parquet(Path(batch_dir) / "gold" / "batch_run_metrics")
    if not source:
        return [f"not available: no {batch_dir}/gold/batch_run_metrics (run the batch job first)"]
    con.execute(f"CREATE OR REPLACE VIEW batch_run_metrics AS SELECT * FROM {source}")
    health = con.sql((ROOT / "scripts" / "queries" / "batch_health.sql").read_text(encoding="utf-8")).fetchone()
    names = ["file loads", "success rate", "avg freshness lag (days)", "reconciliation abs diff (target 0)",
             "schema drift columns", "avg runtime per file (s)", "throughput (source rows/s)", "backfill completeness"]
    fmt = [str, "{:.1%}".format, "{:.1f}".format, "{:,.0f}".format, str, "{:.0f}".format, "{:,.0f}".format, "{:.1%}".format]
    lines = [f"{n:38} {'n/a' if v is None else f(v)}" for n, f, v in zip(names, fmt, health)]
    loads = con.sql("SELECT source_file, status, bronze_rows, round(runtime_sec), round(rows_per_sec) "
                    "FROM batch_run_metrics ORDER BY runtime_sec DESC").fetchall()
    return lines + ["", "file loads, slowest first:"] + table(
        [(f, s, f"{b or 0:,}", f"{t or 0:.0f}", f"{r or 0:,.0f}") for f, s, b, t, r in loads],
        ["file", "status", "bronze rows", "seconds", "rows/s"])


def stream_section(con, output_dir):
    """Lines for the stream section: S1 latency percentiles and S3 throughput, then the targets not measured here."""
    source = parquet(Path(output_dir) / "stream" / "silver" / "trips", "service_type=*/*.parquet")
    if not source:
        return [f"not available: no stream Silver under {output_dir}/stream/silver/trips (run the stream job first)"]
    n, p50, p95, worst, span = con.sql(f"""
        SELECT count(*), quantile_disc(lat, 0.5), quantile_disc(lat, 0.95), max(lat), epoch(max(w) - min(w))
        FROM (SELECT epoch(silver_write_ts - kafka_timestamp) AS lat, silver_write_ts AS w
              FROM {source} WHERE kafka_timestamp IS NOT NULL)""").fetchone()
    if not n:
        return ["not available: stream Silver has no kafka_timestamp values"]
    rate = n / max(span, 1.0)
    rows = [(f"S1 latency p50 <= {S1_P50:g}s", f"{p50:.1f}s", p50 <= S1_P50),
            (f"S1 latency p95 <= {S1_P95:g}s", f"{p95:.1f}s", p95 <= S1_P95),
            (f"S1 latency max <= {S1_MAX:g}s", f"{worst:.1f}s", worst <= S1_MAX),
            (f"S3 throughput >= {S3_MIN_PER_SEC}/s", f"{rate:,.0f}/s over {span:.0f}s ({n:,} rows)", rate >= S3_MIN_PER_SEC)]
    return [f"{status(ok)}  {name:30} measured {value}" for name, value, ok in rows] + [
        "not measured here: S2 micro-batch duration, S4 Gold freshness, S5 restart, S6 fault handling "
        "(reconcile.py, tests), S7 memory"]


def model_section(con, batch_dir):
    """Lines for the model section: every target with its measured value, then each model's lift over its baseline.
    Uses the most recent run in gold/ml_model_metrics."""
    source = parquet(Path(batch_dir) / "gold" / "ml_model_metrics")
    if not source:
        return [f"not available: no {batch_dir}/gold/ml_model_metrics (run ml_job.py first)"]
    rows = con.sql(f"SELECT model, metric, scope, value, run_id FROM {source} "
                   f"WHERE run_id = (SELECT run_id FROM {source} ORDER BY run_ts DESC LIMIT 1)").fetchall()
    metrics = {(m, k, s): v for m, k, s, v, _ in rows}
    lines = [f"ML run {rows[0][4]}"]
    lines += [f"{status(ok)}  {name:48} measured {value}" for name, value, ok in evaluate(metrics)]
    lines += ["", "lift over baseline (positive = the model is better):"]
    lines += [f"  {m:16} {v:+.1%}" for (m, k, s), v in sorted(metrics.items()) if k == "lift" and s == "test"]
    return lines + ["checked by tests/test_ml.py: O2 (same seed, same metrics), O4 (no leakage)"]


def build_report(output_dir):
    """The whole report as text, plus a PASS/FAIL/n-a tally of its target lines."""
    con = duckdb.connect()
    batch_dir = Path(output_dir) / "batch"
    sections = [("1. Batch processing", batch_section(con, batch_dir)),
                ("2. Streaming targets (evaluation.md section 2)", stream_section(con, output_dir)),
                ("3. Model targets (evaluation.md section 3)", model_section(con, batch_dir))]
    lines = ["FINAL REPORT - NYC taxi stream + batch pipeline", f"generated {datetime.now():%Y-%m-%d %H:%M}",
             f"output folder {Path(output_dir).resolve()}"]
    for title, body in sections:
        lines += ["", "=" * WIDTH, title, "=" * WIDTH, *body]
    marks = [ln[:4] for _, body in sections for ln in body]
    tally = {k: marks.count(k) for k in ("PASS", "FAIL", "n/a ")}
    lines += ["", "=" * WIDTH, f"targets: {tally['PASS']} PASS, {tally['FAIL']} FAIL, {tally['n/a ']} not measured"]
    return "\n".join(lines) + "\n"


def main():
    """Write the report file and print it."""
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-dir", default="output")
    ap.add_argument("--report", default="FinalReport.txt")
    args = ap.parse_args()
    text = build_report(args.output_dir)
    Path(args.report).write_text(text, encoding="utf-8")
    print(text)


if __name__ == "__main__":
    main()
