"""M4 streaming job: Kafka (four raw topics) -> Silver (+ invalid) -> Gold daily marts.

    python stream_job.py                                  # Kafka source, Parquet sinks (container default)
    python stream_job.py --source files --input-dir DIR   # dev: read producer --dry-run NDJSON files instead
    python stream_job.py --stage silver|gold              # run only one stage (default: all)

Stage 1 (silver): Kafka -> parse/normalize/enrich/conform -> deduplicate on trip_id -> Parquet.
    <output_dir>/silver/trips (partitioned by service_type), <output_dir>/silver_invalid, <output_dir>/silver/dim_*
Stage 2 (gold): the Silver Parquet is read back as a stream and aggregated into the eight daily marts
    (spark/gold.py) with event-time windows and a watermark: <output_dir>/gold/<mart>.
Every query has its own checkpoint under <checkpoint_dir>. Configuration: config.yaml (`processing`
section) and producer/datasets.yaml; env vars override a few values (see load_config).
"""
import argparse
import os
import sys
from pathlib import Path

import pyspark.sql.functions as F
import yaml
from pyspark.sql import SparkSession
from pyspark.sql import types as T

from dims import write_dims
from gold import MARTS
from storage import join, mkdirs
from transform import SILVER_SCHEMA, deduplicate, process

HERE = Path(__file__).resolve().parent


def load_config(env=os.environ):
    cfg_path = Path(env.get("CONFIG_PATH") or HERE.parent / "config.yaml")
    ds_path = Path(env.get("DATASETS_PATH") or HERE.parent / "producer" / "datasets.yaml")
    cfg = yaml.safe_load(cfg_path.read_text(encoding="utf-8"))
    ds = yaml.safe_load(ds_path.read_text(encoding="utf-8"))
    proc = cfg["processing"]
    return {
        "bootstrap": env.get("KAFKA_BOOTSTRAP") or cfg["kafka"]["bootstrap_servers"],
        "specs": ds["datasets"],
        "norms": ds["normalize"],
        "proc": proc,
        "batch": cfg.get("batch", {}),
        "ml": cfg.get("ml", {}),
        "trigger_mode": env.get("TRIGGER_MODE") or proc.get("trigger_mode", "processing_time"),
        "zone_lookup": env.get("ZONE_LOOKUP") or proc["zone_lookup"],
        "output_dir": env.get("OUTPUT_DIR") or proc["output_dir"],
        "checkpoint_dir": env.get("CHECKPOINT_DIR") or proc["checkpoint_dir"],
    }


def build_spark(settings):
    """SparkSession for a job. `settings` is the job's config.yaml section (shuffle_partitions, spark_master,
    driver_memory); the SPARK_MASTER and SPARK_DRIVER_MEMORY environment variables override it for one-off runs.
    For ADLS Gen2, AZURE_STORAGE_ACCOUNT and AZURE_STORAGE_KEY give the account key (executors receive it with the
    Spark conf) and SPARK_PACKAGES lists Maven packages, e.g. org.apache.hadoop:hadoop-azure:3.3.4.
    For MinIO or another S3-compatible store (s3a:// paths), S3_ENDPOINT, S3_ACCESS_KEY and S3_SECRET_KEY give the
    server and its keys; Spark's default redaction already hides the keys in the UI and event log."""
    builder = (SparkSession.builder.appName("nyc-taxi-m4")
               .master(os.environ.get("SPARK_MASTER") or settings.get("spark_master", "local[2]"))
               .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY") or settings.get("driver_memory", "2g"))
               .config("spark.sql.session.timeZone", "UTC")   # naive ISO timestamps are treated as UTC
               .config("spark.sql.shuffle.partitions", str(settings["shuffle_partitions"]))
               .config("spark.sql.streaming.metricsEnabled", "false"))
    account, key = os.environ.get("AZURE_STORAGE_ACCOUNT"), os.environ.get("AZURE_STORAGE_KEY")
    if account and key:
        builder = (builder.config(f"spark.hadoop.fs.azure.account.key.{account}.dfs.core.windows.net", key)
                   # Spark hides secrets in its UI and event log; its default pattern misses the storage account key
                   .config("spark.redaction.regex", "(?i)secret|password|token|access[.]key|account[.]key"))
    endpoint = os.environ.get("S3_ENDPOINT")
    if endpoint:
        builder = (builder.config("spark.hadoop.fs.s3a.endpoint", endpoint)
                   .config("spark.hadoop.fs.s3a.access.key", os.environ["S3_ACCESS_KEY"])
                   .config("spark.hadoop.fs.s3a.secret.key", os.environ["S3_SECRET_KEY"])
                   .config("spark.hadoop.fs.s3a.path.style.access", "true")   # MinIO: bucket in the path, not the host
                   .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(endpoint.startswith("https")).lower()))
    if os.environ.get("SPARK_PACKAGES"):
        builder = builder.config("spark.jars.packages", os.environ["SPARK_PACKAGES"])
    return builder.getOrCreate()


def load_zones(spark, path):
    return spark.read.option("header", True).csv(str(path))


def kafka_source(spark, conf):
    topics = [s["topic"] for s in conf["specs"].values()]
    reader = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", conf["bootstrap"])
              .option("subscribe", ",".join(topics))
              .option("startingOffsets", conf["proc"]["starting_offsets"])
              .option("failOnDataLoss", "false"))
    max_offsets = int(conf["proc"].get("max_offsets_per_trigger") or 0)
    if max_offsets:
        reader = reader.option("maxOffsetsPerTrigger", max_offsets)
    return reader.load().select("topic", "partition", "offset", F.col("timestamp").alias("kafka_timestamp"),
                                F.col("value").cast("string").alias("value"))


def ndjson_source(spark, input_dir, conf):
    """Dev source: NDJSON files written by `producer.py --dry-run --out <dataset>.ndjson`."""
    schema = T.StructType([T.StructField("key", T.StringType()),
                           T.StructField("headers", T.MapType(T.StringType(), T.StringType())),
                           T.StructField("value", T.StringType())])
    stem = F.regexp_extract(F.input_file_name(), r"([^/\\]+)\.ndjson$", 1)
    topic = F.coalesce(*[F.when(stem == name, F.lit(spec["topic"])) for name, spec in conf["specs"].items()])
    return (spark.readStream.schema(schema).json(str(input_dir))
            .select(topic.alias("topic"), F.lit(0).alias("partition"),
                    # stand-in for a Kafka offset (monotonically_increasing_id is not allowed in streaming)
                    F.xxhash64(F.input_file_name(), "value").alias("offset"),
                    F.current_timestamp().alias("kafka_timestamp"), "value"))


def trigger_kwargs(conf):
    if conf["trigger_mode"] == "available_now":
        return {"availableNow": True}
    return {"processingTime": f"{int(conf['proc']['trigger_interval_seconds'])} seconds"}


def start_sink(df, fmt, name, out, ckpt, conf, partition_by=None):
    writer = df.writeStream.outputMode("append").queryName(name).trigger(**trigger_kwargs(conf))
    if fmt == "console":
        return writer.format("console").option("truncate", "false").option("numRows", 20).start()
    writer = writer.format("parquet").option("path", str(out)).option("checkpointLocation", str(ckpt))
    if partition_by:
        writer = writer.partitionBy(partition_by)
    return writer.start()


def silver_paths(conf):
    out = conf["output_dir"]
    return join(out, "silver", "trips"), join(out, "silver_invalid")


def start_silver(spark, conf, args, zones):
    raw = kafka_source(spark, conf) if args.source == "kafka" else ndjson_source(spark, args.input_dir, conf)
    silver, invalid = process(raw, conf["specs"], conf["norms"], zones, conf["proc"]["validation"])
    silver = deduplicate(silver, conf["proc"]["dedup_watermark_minutes"])
    trips_path, invalid_path = silver_paths(conf)
    ckpt = conf["checkpoint_dir"]
    return [start_sink(silver, args.sink, "silver_trips", trips_path, join(ckpt, "silver_trips"), conf, "service_type"),
            start_sink(invalid, args.sink, "silver_invalid", invalid_path, join(ckpt, "silver_invalid"), conf,
                       "service_type")]


def start_gold(spark, conf, args):
    """Read the Silver Parquet back as a stream and start one windowed aggregation per mart."""
    trips_path, _ = silver_paths(conf)
    mkdirs(spark, trips_path)   # the file source needs the folder to exist
    silver = (spark.readStream.schema(SILVER_SCHEMA).parquet(trips_path)
              .withWatermark("pickup_ts", f"{int(conf['proc']['watermark_minutes'])} minutes"))
    ckpt = conf["checkpoint_dir"]
    gold_dir = join(conf["output_dir"], "gold")
    return [start_sink(build(silver), args.sink, f"gold_{name}", join(gold_dir, name), join(ckpt, f"gold_{name}"), conf)
            for name, build in MARTS.items()]


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--source", choices=["kafka", "files"], default="kafka")
    ap.add_argument("--input-dir", help="NDJSON directory for --source files")
    ap.add_argument("--sink", choices=["parquet", "console"], default="parquet")
    ap.add_argument("--trigger-mode", choices=["processing_time", "available_now"])
    ap.add_argument("--stage", choices=["all", "silver", "gold"], default="all")
    args = ap.parse_args(argv)

    conf = load_config()
    if args.trigger_mode:
        conf["trigger_mode"] = args.trigger_mode
    if args.source == "files":
        if not args.input_dir:
            sys.exit("--source files needs --input-dir")
        if not args.trigger_mode:
            conf["trigger_mode"] = "available_now"   # file input is finite: process it, then stop

    spark = build_spark(conf["proc"])
    spark.sparkContext.setLogLevel("WARN")
    zones = load_zones(spark, conf["zone_lookup"])
    finite = conf["trigger_mode"] == "available_now"

    queries = []
    if args.stage in ("all", "silver"):
        if args.sink == "parquet":
            write_dims(spark, zones, join(conf["output_dir"], "silver"))
        silver_queries = start_silver(spark, conf, args, zones)
        if finite and args.stage == "all":
            # The Gold stream reads what Silver has committed, so a finite run finishes Silver first.
            for q in silver_queries:
                q.awaitTermination()
        else:
            queries += silver_queries
    if args.stage in ("all", "gold"):
        queries += start_gold(spark, conf, args)

    if finite:
        for q in queries:
            q.awaitTermination()
    else:
        spark.streams.awaitAnyTermination()
    return 0


if __name__ == "__main__":
    sys.exit(main())
