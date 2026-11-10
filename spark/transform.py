"""M3/M4 transformations: parse -> normalize -> enrich -> conform to Silver -> split.

Pure DataFrame functions with no I/O, so the same code runs on streaming DataFrames
(Kafka source) and on small batch DataFrames in tests. M5's batch path reuses `conform`
(and `deduplicate`) after a typed-Parquet reader, skipping `parse_dataset`.

Input  : DataFrame(topic, partition, offset, [kafka_timestamp], value:string), one Kafka message per row.
Output : (silver, invalid) DataFrames, see SILVER_FIELDS / INVALID_FIELDS (specs/silver-schema.md).

Validation policy (D27): a row is only *rejected* (-> silver_invalid) when it cannot be used at all:
malformed JSON or a null pickup time. Every other problem is kept in Silver and marked by a boolean
`dq_*` column, so the data-quality KPIs can count them and Gold can filter on them.
"""
from datetime import datetime, timedelta
from functools import reduce

import pyspark.sql.functions as F
from pyspark.sql import types as T

from dims import PAYMENT_TYPES

CORRUPT = "_corrupt_record"
SPARK_TYPES = {"int": T.LongType(), "float": T.DoubleType(), "string": T.StringType(),
               "timestamp": T.StringType()}  # timestamps arrive as ISO strings; parsed explicitly below
FIELD_TYPES = {"string": T.StringType(), "int": T.IntegerType(), "long": T.LongType(),
               "double": T.DoubleType(), "timestamp": T.TimestampType(), "date": T.DateType(),
               "boolean": T.BooleanType()}

# normalize key (producer/datasets.yaml) -> (Silver column, Spark type)
NORM_COLUMNS = [
    ("pickup", "pickup_ts", "timestamp"), ("dropoff", "dropoff_ts", "timestamp"),
    ("request", "request_ts", "timestamp"), ("on_scene", "on_scene_ts", "timestamp"),
    ("pu_location", "pu_location_id", "int"), ("do_location", "do_location_id", "int"),
    ("trip_distance", "distance_miles", "double"), ("passenger_count", "passenger_count", "int"),
    ("base_fare", "base_fare", "double"), ("tip_amount", "tip_amount", "double"),
    ("total_amount", "total_amount", "double"), ("payment_type", "payment_type", "int"),
    ("ratecode", "ratecode_id", "int"), ("driver_pay", "driver_pay", "double"),
    ("hvfhs_license", "hvfhs_license_num", "string"), ("tolls", "tolls", "double"),
    ("bcf", "bcf", "double"), ("sales_tax", "sales_tax", "double"),
    ("congestion_surcharge", "congestion_surcharge", "double"), ("airport_fee", "airport_fee", "double"),
    ("cbd_congestion_fee", "cbd_congestion_fee", "double"),
    ("shared_request_flag", "shared_request_flag", "string"),
    ("shared_match_flag", "shared_match_flag", "string"),
]

DQ_FLAGS = [
    "dq_dropoff_before_pickup", "dq_zero_distance", "dq_negative_fare",
    "dq_unmatched_pu_zone", "dq_unmatched_do_zone",
    "dq_null_dropoff_ts", "dq_null_pu_location", "dq_null_do_location",
    "dq_pickup_out_of_range", "dq_trip_too_long",
]

SILVER_FIELDS = [
    ("trip_id", "string"), ("service_type", "string"), ("hvfhs_license_num", "string"),
    ("request_ts", "timestamp"), ("on_scene_ts", "timestamp"),
    ("pickup_ts", "timestamp"), ("dropoff_ts", "timestamp"),
    ("pickup_date", "date"), ("pickup_hour", "int"),
    ("pu_location_id", "int"), ("do_location_id", "int"),
    ("pu_borough", "string"), ("pu_zone", "string"), ("do_borough", "string"), ("do_zone", "string"),
    ("distance_miles", "double"), ("duration_sec", "long"), ("passenger_count", "int"),
    ("ratecode_id", "int"), ("payment_type", "int"), ("payment_label", "string"),
    ("base_fare", "double"), ("tip_amount", "double"), ("total_amount", "double"), ("driver_pay", "double"),
    ("tolls", "double"), ("bcf", "double"), ("sales_tax", "double"), ("congestion_surcharge", "double"),
    ("airport_fee", "double"), ("cbd_congestion_fee", "double"),
    ("shared_request_flag", "string"), ("shared_match_flag", "string"),
    *[(f, "boolean") for f in DQ_FLAGS],
    ("extras", "string"),
    ("kafka_partition", "int"), ("kafka_offset", "long"), ("kafka_timestamp", "timestamp"),
    ("silver_write_ts", "timestamp"),
]
INVALID_FIELDS = [
    ("service_type", "string"), ("kafka_partition", "int"), ("kafka_offset", "long"),
    ("kafka_timestamp", "timestamp"), ("reject_reason", "string"), ("raw_value", "string"),
]


def schema_of(fields):
    return T.StructType([T.StructField(n, FIELD_TYPES[t], True) for n, t in fields])


SILVER_SCHEMA = schema_of(SILVER_FIELDS)


def service_type(dataset):
    """Datasets/topics use `hvfhv`; Silver and Gold use the spec's `fhvhv` (PLAN 6.7 gap 6)."""
    return "fhvhv" if dataset == "hvfhv" else dataset


def raw_schema(spec):
    fields = [T.StructField(c, SPARK_TYPES[t], True) for c, t in spec["columns"].items()]
    fields.append(T.StructField(CORRUPT, T.StringType(), True))
    return T.StructType(fields)


def _pick(source_col, dtype):
    if source_col is None:
        return F.lit(None).cast(dtype)
    col = F.col(f"j.{source_col}")
    return F.to_timestamp(col) if dtype == "timestamp" else col.cast(dtype)


def parse_dataset(raw, name, spec, norm):
    """Return (good, bad) for one dataset's topic. `good` has the unified typed columns plus extras, raw_value."""
    parsed = (raw.where(F.col("topic") == spec["topic"])
              .withColumn("j", F.from_json("value", raw_schema(spec),
                                           {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": CORRUPT})))
    is_bad = F.col("j").isNull() | F.col(f"j.{CORRUPT}").isNotNull()
    kafka_ts = (F.col("kafka_timestamp").cast("timestamp") if "kafka_timestamp" in raw.columns
                else F.lit(None).cast("timestamp"))
    lineage = [F.lit(service_type(name)).alias("service_type"),
               F.col("partition").cast("int").alias("kafka_partition"),
               F.col("offset").cast("long").alias("kafka_offset"),
               kafka_ts.alias("kafka_timestamp")]

    mapped = {v for v in norm.values() if v}
    extras = F.to_json(F.struct(*[F.col(f"j.{c}").alias(c) for c in spec["columns"] if c not in mapped]))

    good = parsed.where(~is_bad).select(
        *lineage,
        *[_pick(norm[key], dtype).alias(col) for key, col, dtype in NORM_COLUMNS],
        extras.alias("extras"),
        F.col("value").alias("raw_value"),
    )
    bad = parsed.where(is_bad).select(
        *lineage,
        F.lit("malformed_json").alias("reject_reason"),
        F.col("value").alias("raw_value"),
    )
    return good, bad


def enrich(df, zones):
    """Left-join the zone lookup (broadcast) for pickup and dropoff."""
    pu = zones.select(F.col("LocationID").cast("int").alias("pu_id"),
                      F.col("Borough").alias("pu_borough"), F.col("Zone").alias("pu_zone"))
    do = zones.select(F.col("LocationID").cast("int").alias("do_id"),
                      F.col("Borough").alias("do_borough"), F.col("Zone").alias("do_zone"))
    return (df.join(F.broadcast(pu), F.col("pu_location_id") == F.col("pu_id"), "left").drop("pu_id")
              .join(F.broadcast(do), F.col("do_location_id") == F.col("do_id"), "left").drop("do_id"))


def _flag(cond):
    """A boolean flag that is never null (a null condition means the problem is not shown)."""
    return F.coalesce(cond, F.lit(False))


def month_bounds(vcfg):
    """[lo, hi) accepted pickup range: the expected month plus the tolerance on both sides."""
    month = datetime.strptime(vcfg["expected_month"], "%Y-%m")
    tol = timedelta(days=int(vcfg["date_tolerance_days"]))
    nxt = datetime(month.year + month.month // 12, month.month % 12 + 1, 1)
    return month - tol, nxt + tol


def _trip_id(df):
    """Deterministic key: TLC files have no trip id, so hash every identifying column.
    Two genuinely identical trips collapse into one (accepted, see PLAN 6.7 gap 4)."""
    cols = [c for c, _ in SILVER_FIELDS
            if c in df.columns and c not in ("trip_id", "service_type", "pickup_date", "pickup_hour", "duration_sec",
                                             "payment_label", "pu_borough", "pu_zone", "do_borough", "do_zone",
                                             "kafka_partition", "kafka_offset", "kafka_timestamp",
                                             "silver_write_ts", "extras", *DQ_FLAGS)]
    parts = [F.col("service_type")] + [F.coalesce(F.col(c).cast("string"), F.lit("~")) for c in cols] \
        + [F.coalesce(F.col("extras"), F.lit("~"))]
    return F.substring(F.sha2(F.concat_ws("|", *parts), 256), 1, 32)


def conform(df, zones, vcfg):
    """Unified typed rows -> Silver rows (SILVER_FIELDS) plus an `unusable` mask column `reject_reason`."""
    lo, hi = month_bounds(vcfg)
    lo_ts = F.lit(lo.strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp")
    hi_ts = F.lit(hi.strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp")
    max_secs = int(vcfg["max_trip_hours"]) * 3600

    pt = F.col("payment_type")
    pay_map = F.create_map(*[x for k, v in PAYMENT_TYPES.items() for x in (F.lit(k), F.lit(v))])
    pickup, dropoff = F.col("pickup_ts"), F.col("dropoff_ts")
    has_distance = F.col("service_type") != "fhv"

    out = (enrich(df, zones)
           .withColumn("trip_id", _trip_id(df))
           .withColumn("pickup_date", F.to_date("pickup_ts"))
           .withColumn("pickup_hour", F.hour("pickup_ts"))
           .withColumn("duration_sec", (F.unix_timestamp(dropoff) - F.unix_timestamp(pickup)).cast("long"))
           .withColumn("payment_label", F.when(pt.isNull(), F.lit(None).cast("string"))
                                         .otherwise(F.coalesce(pay_map[pt], F.lit("Unknown"))))
           .withColumn("dq_dropoff_before_pickup", _flag(dropoff < pickup))
           .withColumn("dq_zero_distance", _flag(has_distance & (F.col("distance_miles").isNull()
                                                                 | (F.col("distance_miles") <= 0))))
           .withColumn("dq_negative_fare", _flag((F.col("base_fare") < 0) | (F.col("tip_amount") < 0)
                                                 | (F.col("total_amount") < 0)))
           .withColumn("dq_unmatched_pu_zone", _flag(F.col("pu_location_id").isNotNull() & F.col("pu_zone").isNull()))
           .withColumn("dq_unmatched_do_zone", _flag(F.col("do_location_id").isNotNull() & F.col("do_zone").isNull()))
           .withColumn("dq_null_dropoff_ts", dropoff.isNull())
           .withColumn("dq_null_pu_location", F.col("pu_location_id").isNull())
           .withColumn("dq_null_do_location", F.col("do_location_id").isNull())
           .withColumn("dq_pickup_out_of_range", _flag((pickup < lo_ts) | (pickup >= hi_ts)))
           .withColumn("dq_trip_too_long", _flag(F.col("duration_sec") > max_secs))
           .withColumn("silver_write_ts", F.current_timestamp()))
    return out


def deduplicate(silver, watermark_minutes):
    """Drop repeated `trip_id`s that arrive within `watermark_minutes` of ingest time (streaming only).

    The watermark is on `kafka_timestamp`, not on the pickup time: a late trip (old pickup time) must not be
    treated as expired by the deduplication, because late rows are kept in Silver and handled by Gold."""
    return (silver.withWatermark("kafka_timestamp", f"{int(watermark_minutes)} minutes")
                  .dropDuplicatesWithinWatermark(["trip_id"]))


def _typed_nulls(df, fields):
    """Select `fields` from df, filling absent columns with typed nulls."""
    present = set(df.columns)
    return df.select(*[F.col(n) if n in present else F.lit(None).cast(FIELD_TYPES[t]).alias(n)
                       for n, t in fields])


def process(raw, specs, norms, zones, vcfg):
    """raw Kafka-shaped DataFrame -> (silver, invalid). Silver is not yet deduplicated (see `deduplicate`)."""
    parts = [parse_dataset(raw, name, spec, norms[name]) for name, spec in specs.items()]
    good = reduce(lambda a, b: a.unionByName(b), [g for g, _ in parts])
    bad = reduce(lambda a, b: a.unionByName(b), [b for _, b in parts])

    rows = conform(good, zones, vcfg)
    usable = F.col("pickup_ts").isNotNull()
    silver = _typed_nulls(rows.where(usable), SILVER_FIELDS)
    no_pickup = (rows.where(~usable).withColumn("reject_reason", F.lit("null_pickup_time")))
    invalid = (_typed_nulls(no_pickup, INVALID_FIELDS)
               .unionByName(_typed_nulls(bad, INVALID_FIELDS)))
    return silver, invalid
