"""M5 batch job: TLC monthly Parquet files -> Silver -> Gold (daily, monthly and pipeline-health marts).

    python batch_job.py                                   # the month range in stream-config.yaml, all datasets
    python batch_job.py --months 2026-01 --datasets yellow green
    python batch_job.py --months 2026-01 --max-records 20000 --output-dir output/check   # throwaway check run

One file at a time: read with the typed-Parquet entry point, conform to Silver with the same code as the stream,
drop duplicate trip_ids, and replace the file's partition `service_type=/year=/month=` (rerunning a month
replaces only its own rows). `year` and `month` are those of the source file, because a file also holds a few
rows with pickup times outside its month (flagged by dq_pickup_out_of_range). Each file load adds one row to
silver/batch_run_log, and the health marts are computed from that log.

Output under <output_dir> (default /output/batch): silver/trips, silver/dim_*, silver/batch_run_log,
silver_invalid, gold/<daily, monthly and health marts>. Needs no Kafka.
"""
import argparse
import hashlib
import json
import shutil
import sys
import uuid
from datetime import date, datetime, timezone
from pathlib import Path

import pyspark.sql.functions as F
from pyspark.sql import Window
from pyspark.sql import types as T

from dims import write_dims
from gold import MARTS, MONTHLY, rollup_monthly
from stream_job import build_spark, load_config, load_zones
from transform import conform, read_typed, service_type, split_rows

LOG_SCHEMA = T.StructType([T.StructField(n, t) for n, t in [
    ("run_id", T.StringType()), ("source_file", T.StringType()), ("dataset", T.StringType()),
    ("service_type", T.StringType()), ("source_month", T.StringType()), ("tlc_published_date", T.DateType()),
    ("bronze_rows", T.LongType()), ("invalid_rows", T.LongType()), ("silver_rows", T.LongType()),
    ("duplicates_removed", T.LongType()), ("late_correction_rows", T.LongType()),
    ("schema_hash", T.StringType()), ("columns", T.StringType()),
    ("start_ts", T.TimestampType()), ("end_ts", T.TimestampType()), ("gold_available_ts", T.TimestampType()),
    ("status", T.StringType()), ("error", T.StringType()),
]])


def month_range(start, end):
    """Yield 'YYYY-MM' for every month from `start` to `end`, inclusive."""
    y, m = map(int, start.split("-"))
    end_y, end_m = map(int, end.split("-"))
    while (y, m) <= (end_y, end_m):
        yield f"{y:04d}-{m:02d}"
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)


def partition_path(base, **parts):
    """Folder of one Hive-style partition, e.g. base/service_type=yellow/year=2026/month=1."""
    return Path(base, *[f"{k}={v}" for k, v in parts.items()])


def write_partitions(df, base, **parts):
    """Replace the partition `parts` of the Parquet table `base` with `df` (idempotent per partition)."""
    shutil.rmtree(partition_path(base, **parts), ignore_errors=True)
    for name, value in parts.items():
        df = df.withColumn(name, F.lit(value))
    df.write.mode("append").partitionBy(*parts).parquet(str(base))


def count_rows(spark, path):
    """Rows in a written partition (0 when nothing was written), read back from the Parquet files."""
    return spark.read.parquet(str(path)).count() if Path(path).exists() else 0


def late_corrections(spark, old_dir, new_df):
    """Rows of the previous load of this partition whose trip_id is missing from the new load: TLC corrected
    or removed them. 0 on a first load."""
    if not Path(old_dir).exists():
        return 0
    old = spark.read.parquet(str(old_dir)).select("trip_id")
    return old.join(new_df.select("trip_id"), "trip_id", "left_anti").count()


def load_file(spark, ctx, name, month):
    """Load one source file into Silver and return its batch_run_log row (status 'failed' on any error)."""
    spec = ctx["specs"][name]
    fname = f"{spec['file_prefix']}_{month}.parquet"
    svc, year, mon = service_type(name), int(month[:4]), int(month[5:])
    row = {"run_id": ctx["run_id"], "source_file": fname, "dataset": name, "service_type": svc,
           "source_month": month, "tlc_published_date": ctx["published"].get(fname),
           "start_ts": datetime.now(timezone.utc), "status": "failed"}
    try:
        path = Path(ctx["input_dir"]) / fname
        if not path.exists():
            raise FileNotFoundError(f"{path} not found")
        raw = spark.read.parquet(str(path))
        cap = ctx["max_records"].get(name)
        if cap:   # check runs: the first `cap` trips by pickup time, ties in file order, like the producer's MAX_RECORDS
            raw = (raw.withColumn("_row", F.monotonically_increasing_id()).orderBy(spec["pickup_col"], "_row")
                   .limit(cap).drop("_row").cache())
        rows = conform(read_typed(raw, name, spec, ctx["norms"][name]), ctx["zones"],
                       {**ctx["vcfg"], "expected_month": month})
        silver, invalid = split_rows(rows)
        deduped = silver.dropDuplicates(["trip_id"])

        usable = silver.count()   # rows that enter deduplication (needs only the pickup column)
        s_dir = partition_path(ctx["trips"], service_type=svc, year=year, month=mon)
        row["late_correction_rows"] = late_corrections(spark, s_dir, deduped)
        write_partitions(deduped, ctx["trips"], service_type=svc, year=year, month=mon)
        write_partitions(invalid, ctx["invalid"], service_type=svc, year=year, month=mon)

        row["bronze_rows"] = raw.count()
        row["invalid_rows"] = count_rows(spark, partition_path(ctx["invalid"], service_type=svc, year=year, month=mon))
        row["silver_rows"] = count_rows(spark, s_dir)
        row["duplicates_removed"] = usable - row["silver_rows"]
        row["columns"] = ",".join(sorted(c.lower() for c in raw.columns))
        row["schema_hash"] = hashlib.sha256(raw.schema.simpleString().encode()).hexdigest()[:16]
        row["status"] = "success"
    except Exception as exc:   # one bad file must not stop the backfill; it is logged and counted
        row["error"] = str(exc)[:500]
    row["end_ts"] = datetime.now(timezone.utc)
    return row


def build_gold(spark, ctx, month, log_rows):
    """Build the daily marts for one loaded month from its Silver partitions, then roll them up to the monthly
    marts. data_quality_monthly also carries the month's load counts (duplicates, late corrections)."""
    year, mon = int(month[:4]), int(month[5:])
    month_only = (F.col("year") == year) & (F.col("month") == mon)
    silver = spark.read.parquet(str(ctx["trips"])).where(month_only)
    for name, build in MARTS.items():
        write_partitions(build(silver), ctx["gold"] / name, year=year, month=mon)

    loads = spark.createDataFrame(
        [(r["service_type"], r["bronze_rows"], r["invalid_rows"], r["duplicates_removed"], r["late_correction_rows"])
         for r in log_rows if r["status"] == "success"],
        "service_type string, bronze_rows long, invalid_rows long, duplicates_removed long, late_correction_rows long")
    for name, (daily, keys) in MONTHLY.items():
        daily_dir = partition_path(ctx["gold"] / daily, year=year, month=mon)
        if not daily_dir.exists():   # the daily mart has no rows this month (e.g. no yellow or green file loaded)
            shutil.rmtree(partition_path(ctx["gold"] / name, year=year, month=mon), ignore_errors=True)
            continue
        out = rollup_monthly(spark.read.parquet(str(daily_dir)), keys, year, mon)
        if name == "data_quality_monthly":
            out = out.join(loads, "service_type", "left")
        write_partitions(out, ctx["gold"] / name, year=year, month=mon)


def build_health(spark, ctx, months_expected):
    """gold.batch_run_metrics from silver.batch_run_log: success, freshness, reconciliation (should be 0),
    schema drift against the dataset's previous file, runtime and throughput, and the backfill target."""
    log = spark.read.parquet(str(ctx["silver"] / "batch_run_log"))
    prev = F.split(F.lag("columns").over(Window.partitionBy("dataset").orderBy("source_month", "start_ts")), ",")
    cur = F.split("columns", ",")
    runtime = F.col("end_ts").cast("double") - F.col("start_ts").cast("double")
    metrics = log.select(
        "run_id", "source_file", "dataset", "source_month", "status", "bronze_rows", "invalid_rows",
        "silver_rows", "duplicates_removed", "late_correction_rows",
        (F.col("silver_rows") + F.col("duplicates_removed") + F.col("invalid_rows") - F.col("bronze_rows"))
        .alias("reconciliation_diff"),
        F.datediff(F.to_date("gold_available_ts"), "tlc_published_date").alias("freshness_lag_days"),
        F.when(prev.isNotNull(), F.size(F.array_except(cur, prev))).otherwise(0).alias("columns_added"),
        F.when(prev.isNotNull(), F.size(F.array_except(prev, cur))).otherwise(0).alias("columns_removed"),
        runtime.alias("runtime_sec"), (F.col("bronze_rows") / runtime).alias("rows_per_sec"),
        F.lit(months_expected).alias("months_expected"))
    metrics.write.mode("overwrite").parquet(str(ctx["gold"] / "batch_run_metrics"))


def published_dates(input_dir):
    """File name -> TLC publication date, from the file the download script writes (empty if there is none)."""
    path = Path(input_dir) / "tlc_published.json"
    if not path.exists():
        return {}
    return {k: date.fromisoformat(v) for k, v in json.loads(path.read_text(encoding="utf-8")).items()}


def main(argv=None):
    """Run the backfill month by month; exit code 1 if any file failed."""
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--months", nargs="+", help="YYYY-MM list (default: the configured range)")
    ap.add_argument("--datasets", nargs="+", help="subset of the configured datasets")
    ap.add_argument("--max-records", nargs="+", default=[], metavar="DATASET=N",
                    help="keep only the first N trips (by pickup time) per dataset, for check runs")
    ap.add_argument("--input-dir", help="folder with the monthly files (default: batch.input_dir)")
    ap.add_argument("--output-dir", help="output folder (default: batch.output_dir)")
    args = ap.parse_args(argv)

    conf = load_config()
    cfg = conf["batch"]
    input_dir = args.input_dir or cfg["input_dir"]
    out = Path(args.output_dir or cfg["output_dir"])
    expected = list(month_range(cfg["start_month"], cfg["end_month"]))
    spark = build_spark({"shuffle_partitions": cfg["shuffle_partitions"]})
    spark.sparkContext.setLogLevel("WARN")
    ctx = {"specs": conf["specs"], "norms": conf["norms"], "vcfg": conf["proc"]["validation"],
           "zones": load_zones(spark, conf["zone_lookup"]), "input_dir": input_dir,
           "silver": out / "silver", "trips": out / "silver" / "trips", "invalid": out / "silver_invalid", "gold": out / "gold",
           "max_records": {k: int(v) for k, v in (a.split("=") for a in args.max_records)},
           "published": published_dates(input_dir), "run_id": uuid.uuid4().hex[:12]}
    datasets = [d for d in (args.datasets or cfg["datasets"]) if d in conf["specs"]]

    write_dims(spark, ctx["zones"], str(ctx["silver"]))
    failed = 0
    for month in args.months or expected:
        rows = [load_file(spark, ctx, name, month) for name in datasets]
        if any(r["status"] == "success" for r in rows):
            try:
                build_gold(spark, ctx, month, rows)
                for r in rows:
                    if r["status"] == "success":
                        r["gold_available_ts"] = datetime.now(timezone.utc)
            except Exception as exc:   # Silver is written but Gold is not: the loads count as failed
                for r in rows:
                    if r["status"] == "success":
                        r.update(status="failed", error=f"gold: {exc}"[:500])
        spark.createDataFrame([tuple(r.get(f.name) for f in LOG_SCHEMA) for r in rows], LOG_SCHEMA) \
            .write.mode("append").parquet(str(ctx["silver"] / "batch_run_log"))
        for r in rows:
            failed += r["status"] != "success"
            print(f"{r['source_file']:34} {r['status']:8} bronze={r.get('bronze_rows')} silver={r.get('silver_rows')} "
                  f"dups={r.get('duplicates_removed')} invalid={r.get('invalid_rows')} {r.get('error') or ''}")
    build_health(spark, ctx, len(expected))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
