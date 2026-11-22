"""SparkSession factory. All timestamps in this project are NYC wall-clock values stored with session time zone UTC."""
from __future__ import annotations

import glob
import hashlib
import os
import sys
import tempfile
import time
import zipfile
from pathlib import Path

from pyspark.sql import SparkSession

from .config import Config
from .storage import RemotePath

_JAVA_CANDIDATES = (
    "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home",
    "/usr/local/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home",
    "/usr/lib/jvm/java-17-openjdk-amd64",
    "/usr/lib/jvm/java-17-openjdk-arm64",
)


IS_WINDOWS = os.name == "nt"

# Windows JDK 17 install locations (Temurin / Microsoft / Oracle / Zulu / Corretto); newest match wins
_WIN_JAVA_GLOBS = (
    r"C:\Program Files\Eclipse Adoptium\jdk-17*",
    r"C:\Program Files\Microsoft\jdk-17*",
    r"C:\Program Files\Java\jdk-17*",
    r"C:\Program Files\Java\jdk17*",
    r"C:\Program Files\Zulu\zulu-17*",
    r"C:\Program Files\Amazon Corretto\jdk17*",
)


def ensure_java_home() -> None:
    if not os.environ.get("JAVA_HOME"):
        if IS_WINDOWS:
            for pattern in _WIN_JAVA_GLOBS:
                hits = sorted(glob.glob(pattern), reverse=True)
                if hits:
                    os.environ["JAVA_HOME"] = hits[0]
                    break
        else:
            for candidate in _JAVA_CANDIDATES:
                if Path(candidate).exists():
                    os.environ["JAVA_HOME"] = candidate
                    break
    if IS_WINDOWS:
        ensure_hadoop_home()


def ensure_hadoop_home() -> None:
    """Spark on Windows needs winutils.exe + hadoop.dll (HADOOP_HOME/bin) to write files and checkpoints.

    setup_windows.ps1 installs them into <project>/tools/hadoop; an existing HADOOP_HOME is respected.
    """
    from .config import PROJECT_ROOT

    home = os.environ.get("HADOOP_HOME")
    if not home:
        for candidate in (PROJECT_ROOT / "tools" / "hadoop", Path.home() / "hadoop", Path(r"C:\hadoop")):
            if (candidate / "bin" / "winutils.exe").exists():
                home = str(candidate)
                os.environ["HADOOP_HOME"] = home
                break
    if not home:
        return
    hadoop_bin = str(Path(home) / "bin")
    if hadoop_bin.lower() not in os.environ.get("PATH", "").lower():
        os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ.get("PATH", "")


def pin_process_timezone_utc() -> None:
    """Python-side datetime <-> Spark conversions use the process TZ; pin it to match the session tz (UTC)."""
    if IS_WINDOWS:
        # MS CRT syntax ("UTC" + offset 0). Child processes (Spark Python workers) inherit it from startup.
        os.environ["TZ"] = "UTC0"
        try:  # re-read TZ in this already-running process (time.tzset does not exist on Windows)
            import ctypes

            ctypes.CDLL("ucrtbase")._tzset()
        except (OSError, AttributeError):
            pass  # the launcher (mp.ps1) also sets TZ before Python starts
    else:
        os.environ["TZ"] = "UTC"
        time.tzset()


def _package_zip() -> str:
    """Zip of the mobilitypulse package for ``addPyFile``: executors unpickle Python UDFs that reference it."""
    pkg = Path(__file__).resolve().parents[1]
    files = sorted(p for p in pkg.rglob("*.py"))
    digest = hashlib.sha256()
    for f in files:
        digest.update(f.relative_to(pkg).as_posix().encode())
        digest.update(f.read_bytes())
    target = Path(tempfile.gettempdir()) / f"mobilitypulse-{digest.hexdigest()[:12]}.zip"
    if not target.exists():
        tmp = target.with_suffix(f".{os.getpid()}.tmp")
        with zipfile.ZipFile(tmp, "w", zipfile.ZIP_DEFLATED) as z:
            for f in files:
                z.write(f, Path("mobilitypulse") / f.relative_to(pkg))
        os.replace(tmp, target)
    return str(target)


def _s3a_conf(cfg: Config) -> dict[str, str]:
    """Hadoop S3A settings for MinIO. spark.hadoop.* options travel to the executors with the job."""
    from .storage import s3_settings

    st = s3_settings()
    if not st["endpoint"]:
        raise RuntimeError("an s3a:// data dir needs S3_ENDPOINT, S3_ACCESS_KEY and S3_SECRET_KEY (see setupF.env.example)")
    endpoint = st["endpoint"] if "://" in st["endpoint"] else "http://" + st["endpoint"]
    return {
        "spark.hadoop.fs.s3a.endpoint": endpoint,
        "spark.hadoop.fs.s3a.access.key": st["access_key"] or "",
        "spark.hadoop.fs.s3a.secret.key": st["secret_key"] or "",
        "spark.hadoop.fs.s3a.path.style.access": "true",            # MinIO: bucket in the path, not in the host name
        "spark.hadoop.fs.s3a.connection.ssl.enabled": str(endpoint.startswith("https")).lower(),
        # plain key pair: skips the default chain, whose cloud-metadata lookup is slow on Azure VMs
        "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        "spark.hadoop.fs.s3a.connection.maximum": "64",
        "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version": "2",   # fewer copy+delete "renames" on S3
        "spark.hadoop.fs.s3a.fast.upload": "true",
    }


def get_spark(
    cfg: Config,
    app_suffix: str | None = None,
    conf: dict[str, str] | None = None,
    with_kafka: bool = False,
    master: str | None = None,
) -> SparkSession:
    ensure_java_home()
    pin_process_timezone_utc()
    s = cfg.section("spark")
    master = master or s.get("master", "local[*]")
    cluster = str(master).startswith("spark://")
    if cluster:
        if not s.get("driver_host"):  # check before touching the environment: a failed call must leave no trace
            raise RuntimeError("cluster mode needs spark.driver_host (this machine's private IP; DRIVER_HOST / MASTER_IP "
                               "in setupF.env): executors connect back to the driver on that address")
        # executors run on other (Linux) machines: they must not be sent the driver's own python path
        os.environ["PYSPARK_PYTHON"] = str(s.get("executor_python") or "python3")
        os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    else:
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    name = s.get("app_name", "NYC-MobilityPulse") + (f"-{app_suffix}" if app_suffix else "")
    host = (s.get("driver_host") or "127.0.0.1") if cluster else "127.0.0.1"
    bind = s.get("driver_bind_address") or ("0.0.0.0" if cluster else "127.0.0.1")
    builder = (
        SparkSession.builder.appName(name)
        .master(master)
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
        .config("spark.driver.host", host)
        .config("spark.driver.bindAddress", bind)
        .config("spark.ui.enabled", str(s.get("ui_enabled", True)).lower())
        .config("spark.ui.port", str(s.get("ui_port", 4040)))
        .config("spark.ui.showConsoleProgress", "false")
    )
    if s.get("event_log"):
        events = cfg.path("spark_events")
        events.mkdir(parents=True, exist_ok=True)
        builder = builder.config("spark.eventLog.enabled", "true").config("spark.eventLog.dir", events.as_uri())
    packages: list[str] = []
    if cluster:
        builder = (
            builder.config("spark.driver.port", str(s.get("driver_port", 40000)))
            .config("spark.blockManager.port", str(int(s.get("driver_port", 40000)) + 1))
            .config("spark.driver.blockManager.port", str(int(s.get("driver_port", 40000)) + 2))
            .config("spark.executor.memory", str(s.get("executor_memory", "3g")))
            .config("spark.executor.cores", str(s.get("executor_cores", 3)))
            .config("spark.cores.max", str(s.get("cores_max", 3)))
            .config("spark.pyspark.python", str(s.get("executor_python") or "python3"))
        )
    if any(isinstance(cfg.path(n), RemotePath) for n in ("bronze", "silver", "gold", "raw_lake")):
        for key, value in _s3a_conf(cfg).items():
            builder = builder.config(key, value)
        packages += list(s.get("s3a_packages") or [])
    if with_kafka:
        packages.append(cfg.section("streaming")["kafka_package"])
    if packages:
        builder = builder.config("spark.jars.packages", ",".join(packages))
    for key, value in (conf or {}).items():
        builder = builder.config(key, str(value))

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(s.get("log_level", "WARN"))
    if cluster and s.get("ship_package", True):
        spark.sparkContext.addPyFile(_package_zip())
    return spark
