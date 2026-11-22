"""Steps 2-3 - RAW -> BRONZE: canonical, typed schemas with lineage. No rows are dropped here."""
from __future__ import annotations

from functools import reduce

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from ..common.config import Config
from ..common.utils import get_logger, write_parquet

log = get_logger(__name__)

# canonical name -> (accepted source names, Spark type)
TLC_COLUMNS: dict[str, tuple[tuple[str, ...], str]] = {
    "vendor_id": (("VendorID",), "int"),
    "pickup_ts": (("tpep_pickup_datetime",), "timestamp"),
    "dropoff_ts": (("tpep_dropoff_datetime",), "timestamp"),
    "passenger_count": (("passenger_count",), "double"),
    "trip_distance": (("trip_distance",), "double"),
    "ratecode_id": (("RatecodeID", "RateCodeID"), "int"),
    "pickup_zone_id": (("PULocationID",), "int"),
    "dropoff_zone_id": (("DOLocationID",), "int"),
    "payment_type": (("payment_type",), "int"),
    "fare_amount": (("fare_amount",), "double"),
    "total_amount": (("total_amount",), "double"),
}
RECORD_KEY_COLUMNS = ["vendor_id", "pickup_ts", "dropoff_ts", "pickup_zone_id", "dropoff_zone_id",
                      "trip_distance", "total_amount"]
BAD_QUALITY_FLAGS = ["2", "3", "6", "7"]


def _pick(df: DataFrame, names: tuple[str, ...]) -> Column:
    lookup = {c.lower(): c for c in df.columns}
    for name in names:
        if name.lower() in lookup:
            return F.col(f"`{lookup[name.lower()]}`")
    return F.lit(None)


def record_id(cols: list[str]) -> Column:
    return F.sha2(F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in cols]), 256)


def normalize_tlc(df: DataFrame, source_file: str, month: str) -> DataFrame:
    # TIMESTAMP_NTZ (TLC wall clock) cast under session tz UTC keeps the wall-clock value unchanged
    out = df.select(*[_pick(df, names).cast(typ).alias(name) for name, (names, typ) in TLC_COLUMNS.items()])
    return (
        out.withColumn("record_id", record_id(RECORD_KEY_COLUMNS))
        .withColumn("source_file", F.lit(source_file))
        .withColumn("source_month", F.lit(month))
        .withColumn("ingested_at", F.current_timestamp())
    )


def build_bronze_trips(spark: SparkSession, cfg: Config, months: list[str]) -> int:
    raw = cfg.path("raw_lake") / "tlc"
    dataset = cfg.section("sources")["tlc_dataset"]
    frames = []
    for month in months:
        path = raw / f"{dataset}_{month}.parquet"
        if not path.exists():
            raise FileNotFoundError(f"{path} missing - run stage 'ingest' (or 'sample') first")
        frames.append(normalize_tlc(spark.read.parquet(str(path)), path.name, month))
    df = reduce(DataFrame.unionByName, frames)
    write_parquet(df, cfg.path("bronze") / "trips", partition_by=["source_month"])
    n = spark.read.parquet(str(cfg.path("bronze") / "trips")).where(F.col("source_month").isin(months)).count()
    log.info("bronze/trips: %d rows for %s", n, months)
    return n


def build_bronze_zones(spark: SparkSession, cfg: Config) -> int:
    q = cfg.section("quality")
    path = cfg.path("raw_lake") / "reference" / "taxi_zone_lookup.csv"
    df = spark.read.option("header", True).csv(str(path))
    zones = df.select(
        F.col("LocationID").cast("int").alias("zone_id"),
        F.col("Borough").alias("borough"),
        F.col("Zone").alias("zone_name"),
        F.col("service_zone"),
    ).withColumn("is_known_zone", F.col("zone_id").between(q["min_location_id"], q["max_location_id"]))
    write_parquet(zones.coalesce(1), cfg.path("bronze") / "taxi_zones")
    return zones.count()


# ------------------------------------------------------------------ NOAA ISD decoding
def _field(col: str, idx: int) -> Column:
    return F.split(F.col(col), ",").getItem(idx)


def decode_scaled(col: str, value_idx: int, quality_idx: int, missing: int, scale: float) -> Column:
    """Decode an ISD packed field (e.g. TMP '+0056,1' -> 5.6). Missing codes and bad-quality flags -> null."""
    value = F.regexp_replace(_field(col, value_idx), r"^\+", "").cast("int")
    ok = value.isNotNull() & (F.abs(value) != missing) & ~_field(col, quality_idx).isin(BAD_QUALITY_FLAGS)
    return F.when(ok, value / F.lit(scale))


def decode_noaa(df: DataFrame, local_tz: str) -> DataFrame:
    for c in ["TMP", "DEW", "WND", "VIS", "AA1", "REPORT_TYPE"]:
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None).cast("string"))
    obs_utc = F.to_timestamp("DATE", "yyyy-MM-dd'T'HH:mm:ss")
    period = F.regexp_replace(_field("AA1", 0), r"^\+", "").cast("int")
    return df.select(
        F.col("STATION").alias("station"),
        obs_utc.alias("obs_ts_utc"),
        F.from_utc_timestamp(obs_utc, local_tz).alias("obs_ts_local"),
        F.trim("REPORT_TYPE").alias("report_type"),
        decode_scaled("TMP", 0, 1, 9999, 10.0).alias("temp_c"),
        decode_scaled("DEW", 0, 1, 9999, 10.0).alias("dew_point_c"),
        decode_scaled("WND", 3, 4, 9999, 10.0).alias("wind_mps"),
        decode_scaled("VIS", 0, 1, 999999, 1.0).alias("visibility_m"),
        decode_scaled("AA1", 1, 3, 9999, 10.0).alias("precip_mm"),
        F.when(period != 99, period).alias("precip_period_h"),
    ).withColumn("year", F.year("obs_ts_local"))


def build_bronze_weather(spark: SparkSession, cfg: Config, months: list[str]) -> int:
    station = cfg.section("sources")["noaa_station"]
    years = sorted({m[:4] for m in months})
    paths = [str(cfg.path("raw_lake") / "noaa" / f"{station}_{y}.csv") for y in years]
    df = spark.read.option("header", True).option("inferSchema", False).csv(paths)
    obs = decode_noaa(df, cfg.section("project")["timezone"])
    write_parquet(obs, cfg.path("bronze") / "weather_obs", partition_by=["year"])
    return obs.count()


def run_bronze(spark: SparkSession, cfg: Config, months: list[str]) -> dict:
    # cluster mode: executors read the raw files from the lake, so push anything downloaded locally (unchanged
    # files are skipped by size; a no-op when the lake is a local folder)
    from ..ingestion.download import sync_raw
    sync_raw(cfg)
    return {
        "zones": build_bronze_zones(spark, cfg),
        "weather_obs": build_bronze_weather(spark, cfg, months),
        "trips": build_bronze_trips(spark, cfg, months),
    }
