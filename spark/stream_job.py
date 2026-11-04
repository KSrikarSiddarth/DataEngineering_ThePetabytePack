"""M3 streaming job: Kafka (four raw topics) -> parse/normalize/enrich/validate -> Parquet.

    python stream_job.py                                  # Kafka source, Parquet sinks (container default)
    python stream_job.py --source files --input-dir DIR   # dev: read producer --dry-run NDJSON files instead

Writes <output_dir>/trips_clean and <output_dir>/invalid (partitioned by service), each with its own
checkpoint under <checkpoint_dir>. Configuration: stream-config.yaml (`processing` section) and
producer/datasets.yaml; env vars override a few values (see load_config).
"""
import argparse
import os
import sys
from pathlib import Path

import pyspark.sql.functions as F
import yaml
from pyspark.sql import SparkSession
from pyspark.sql import types as T

from transform import process

HERE = Path(__file__).resolve().parent


def _first_existing(*paths):
    for p in paths:
        if p and Path(p).exists():
            return Path(p)
    raise FileNotFoundError(f"none of these exist: {[str(p) for p in paths if p]}")


def load_config(env=os.environ):
    cfg_path = _first_existing(env.get("CONFIG_PATH"), "/config/stream-config.yaml", HERE.parent / "stream-config.yaml")
    ds_path = _first_existing(env.get("DATASETS_PATH"), "/config/datasets.yaml", HERE.parent / "producer" / "datasets.yaml")
    cfg = yaml.safe_load(cfg_path.read_text(encoding="utf-8"))
    ds = yaml.safe_load(ds_path.read_text(encoding="utf-8"))
    proc = cfg["processing"]
    return {
        "bootstrap": env.get("KAFKA_BOOTSTRAP") or cfg["kafka"]["bootstrap_servers_internal"],
        "specs": ds["datasets"],
        "norms": ds["normalize"],
        "proc": proc,
        "trigger_mode": env.get("TRIGGER_MODE") or proc.get("trigger_mode", "processing_time"),
        "zone_lookup": env.get("ZONE_LOOKUP") or proc["zone_lookup"],
        "output_dir": env.get("OUTPUT_DIR") or proc["output_dir"],
        "checkpoint_dir": env.get("CHECKPOINT_DIR") or proc["checkpoint_dir"],
    }


def build_spark(proc):
    return (SparkSession.builder.appName("nyc-taxi-m3")
            .master(os.environ.get("SPARK_MASTER", "local[2]"))
            .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "2g"))
            .config("spark.sql.session.timeZone", "UTC")   # naive ISO timestamps are treated as UTC
            .config("spark.sql.shuffle.partitions", str(proc["shuffle_partitions"]))
            .config("spark.sql.streaming.metricsEnabled", "false")
            .getOrCreate())


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
    return reader.load().select("topic", "partition", "offset", F.col("value").cast("string").alias("value"))


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
                    F.xxhash64(F.input_file_name(), "value").alias("offset"), "value"))


def trigger_kwargs(conf):
    if conf["trigger_mode"] == "available_now":
        return {"availableNow": True}
    return {"processingTime": f"{int(conf['proc']['trigger_interval_seconds'])} seconds"}


def start_sink(df, fmt, name, conf, args):
    writer = df.writeStream.outputMode("append").queryName(name).trigger(**trigger_kwargs(conf))
    if fmt == "console":
        return writer.format("console").option("truncate", "false").option("numRows", 20).start()
    out = Path(conf["output_dir"]) / name
    ckpt = Path(conf["checkpoint_dir"]) / name
    return (writer.format("parquet").partitionBy("service")
            .option("path", str(out)).option("checkpointLocation", str(ckpt)).start())


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--source", choices=["kafka", "files"], default="kafka")
    ap.add_argument("--input-dir", help="NDJSON directory for --source files")
    ap.add_argument("--sink", choices=["parquet", "console"], default="parquet")
    ap.add_argument("--trigger-mode", choices=["processing_time", "available_now"])
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
    raw = kafka_source(spark, conf) if args.source == "kafka" else ndjson_source(spark, args.input_dir, conf)
    clean, invalid = process(raw, conf["specs"], conf["norms"], zones, conf["proc"]["validation"])

    queries = [start_sink(clean, args.sink, "trips_clean", conf, args),
               start_sink(invalid, args.sink, "invalid", conf, args)]
    if conf["trigger_mode"] == "available_now":
        for q in queries:
            q.awaitTermination()
    else:
        spark.streams.awaitAnyTermination()
    return 0


if __name__ == "__main__":
    sys.exit(main())
