"""SparkSession factory. All timestamps in this project are NYC wall-clock values stored with session time zone UTC."""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path

from pyspark.sql import SparkSession

from .config import Config

_JAVA_CANDIDATES = (
    "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home",
    "/usr/local/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home",
    "/usr/lib/jvm/java-17-openjdk-amd64",
    "/usr/lib/jvm/java-17-openjdk-arm64",
)


def ensure_java_home() -> None:
    if os.environ.get("JAVA_HOME"):
        return
    for candidate in _JAVA_CANDIDATES:
        if Path(candidate).exists():
            os.environ["JAVA_HOME"] = candidate
            return


def get_spark(
    cfg: Config,
    app_suffix: str | None = None,
    conf: dict[str, str] | None = None,
    with_kafka: bool = False,
    master: str | None = None,
) -> SparkSession:
    ensure_java_home()
    # Python-side datetime <-> Spark conversions use the process TZ; pin it to match the session tz
    os.environ["TZ"] = "UTC"
    time.tzset()
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    s = cfg.section("spark")
    name = s.get("app_name", "NYC-MobilityPulse") + (f"-{app_suffix}" if app_suffix else "")
    builder = (
        SparkSession.builder.appName(name)
        .master(master or s.get("master", "local[*]"))
        .config("spark.driver.memory", s.get("driver_memory", "4g"))
        .config("spark.sql.shuffle.partitions", str(s.get("shuffle_partitions", 32)))
        .config("spark.sql.adaptive.enabled", str(s.get("adaptive_enabled", True)).lower())
        .config("spark.sql.autoBroadcastJoinThreshold", str(int(s.get("broadcast_threshold_mb", 10)) * 1024 * 1024))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.executor.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        # corporate VPNs often break hostname resolution for the local driver
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.enabled", str(s.get("ui_enabled", True)).lower())
        .config("spark.ui.port", str(s.get("ui_port", 4040)))
        .config("spark.ui.showConsoleProgress", "false")
    )
    if s.get("event_log"):
        events = cfg.path("spark_events")
        events.mkdir(parents=True, exist_ok=True)
        builder = builder.config("spark.eventLog.enabled", "true").config("spark.eventLog.dir", events.as_uri())
    if with_kafka:
        builder = builder.config("spark.jars.packages", cfg.section("streaming")["kafka_package"])
    for key, value in (conf or {}).items():
        builder = builder.config(key, str(value))

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(s.get("log_level", "WARN"))
    return spark
