"""Smoke test for the SetupF cluster: MinIO reachable, executors registered, shuffle + Python UDF + s3a round trip.

    python -m mobilitypulse.cluster_check          (or: .\mp.ps1 check -Cluster / make cluster-check CLUSTER=1)

Run it before the first pipeline run and after any change to the VMs. It writes only under <data_dir>/_check.
"""
from __future__ import annotations

import socket
import sys
import uuid

from .common.config import load_config
import pyarrow.fs as pafs

from .common.storage import RemotePath, rmtree, s3_filesystem
from .common.utils import get_logger

log = get_logger("mobilitypulse.check")


def _ok(msg: str) -> None:
    print(f"  OK    {msg}")


def main() -> int:
    cfg = load_config()
    spark_cfg = cfg.section("spark")
    print(f"master   : {spark_cfg.get('master')}")
    print(f"data dir : {cfg.data_dir}   (local dir: {cfg.local_dir})")
    print(f"driver   : {socket.gethostname()} (executors must reach {spark_cfg.get('driver_host') or '127.0.0.1'})")
    failures = 0

    # 1. object store, from the driver process (pyarrow)
    probe = cfg.data_dir / "_check" / f"{uuid.uuid4().hex[:8]}.txt"
    try:
        if isinstance(cfg.data_dir, RemotePath):
            bucket = cfg.data_dir.key.split("/", 1)[0]
            if s3_filesystem().get_file_info(bucket).type == pafs.FileType.NotFound:
                s3_filesystem().create_dir(bucket)       # SetupF 7.4 creates it with mc; this covers a fresh MinIO
                print(f"  note  bucket '{bucket}' did not exist and was created")
        probe.write_text("ok")
        assert probe.read_text() == "ok"
        rmtree(probe)
        _ok(f"read/write on {cfg.data_dir}")
    except Exception as exc:  # noqa: BLE001 - report everything
        failures += 1
        print(f"  FAIL  object store: {exc}")
        if isinstance(cfg.data_dir, RemotePath):
            print("        check S3_ENDPOINT / S3_ACCESS_KEY / S3_SECRET_KEY and that MinIO is running on VM2")
        return 1

    # 2. Spark
    from pyspark.sql import functions as F

    from .common.spark import get_spark
    from .ml.metrics import poisson_mad

    spark = get_spark(cfg, "check")
    sc = spark.sparkContext
    print(f"app      : {sc.applicationId}  UI {sc.uiWebUrl}")
    try:
        hosts = sorted(set(sc.parallelize(range(8), 4).mapPartitions(lambda it: [socket.gethostname()]).collect()))
        _ok(f"tasks ran on: {', '.join(hosts)}")

        # executors are requested lazily: count them after the first job has run
        executors = sc._jsc.sc().getExecutorMemoryStatus().size() - 1  # minus the driver
        if cfg.is_cluster and executors < 1:
            failures += 1
            print("  FAIL  no executor registered: check the worker on VM2 and the driver ports 40000-40002 on this host")
        else:
            _ok(f"{executors} executor(s) registered" if cfg.is_cluster else "local mode (no cluster executors)")

        counts = spark.range(100000).select((F.col("id") % 7).alias("k")).groupBy("k").count().collect()
        assert sum(r["count"] for r in counts) == 100000
        _ok("shuffle (groupBy) works")

        udf = F.udf(lambda v: poisson_mad(float(v)), "double")  # the package must be importable on the executors
        val = spark.range(1).select(udf(F.lit(4.0)).alias("v")).first()["v"]
        assert abs(val - poisson_mad(4.0)) < 1e-9
        _ok("Python UDF using the mobilitypulse package works on the executors")

        out = cfg.data_dir / "_check" / f"parquet-{uuid.uuid4().hex[:8]}"
        spark.range(1000).write.mode("overwrite").parquet(str(out))
        assert spark.read.parquet(str(out)).count() == 1000
        rmtree(out)
        _ok(f"parquet write + read on {out.parent}")
    except Exception as exc:  # noqa: BLE001
        failures += 1
        print(f"  FAIL  {type(exc).__name__}: {str(exc)[:600]}")
    finally:
        spark.stop()
    print("cluster check:", "PASSED" if not failures else f"{failures} problem(s)")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
