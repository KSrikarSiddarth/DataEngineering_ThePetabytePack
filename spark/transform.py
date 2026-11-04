"""M3 transformations: parse -> normalize -> enrich -> validate -> split.

Pure DataFrame functions with no I/O, so the same code runs on streaming DataFrames
(Kafka source) and on small batch DataFrames in tests.

Input  : DataFrame(topic, partition, offset, value:string) - one Kafka message per row.
Output : (clean, invalid) DataFrames, see CLEAN_FIELDS / INVALID_FIELDS.
"""
from datetime import datetime, timedelta
from functools import reduce

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql import types as T

CORRUPT = "_corrupt_record"
SPARK_TYPES = {"int": T.LongType(), "float": T.DoubleType(), "string": T.StringType(),
               "timestamp": T.StringType()}  # timestamps arrive as ISO strings; parsed explicitly below

CLEAN_FIELDS = [
    ("service", "string"), ("kafka_partition", "int"), ("kafka_offset", "long"),
    ("pickup_ts", "timestamp"), ("dropoff_ts", "timestamp"),
    ("PULocationID", "int"), ("DOLocationID", "int"),
    ("PUBorough", "string"), ("PUZone", "string"), ("DOBorough", "string"), ("DOZone", "string"),
    ("trip_distance", "double"), ("passenger_count", "int"), ("fare_amount", "double"),
    ("tip_amount", "double"), ("total_amount", "double"), ("payment_type", "int"),
    ("extras", "string"), ("dq_flags", "string"),
]
INVALID_FIELDS = CLEAN_FIELDS + [("reject_reason", "string"), ("raw_value", "string")]


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
    """Return (good, bad) for one dataset's topic. `good` is in the unified layout plus raw_value."""
    parsed = (raw.where(F.col("topic") == spec["topic"])
              .withColumn("j", F.from_json("value", raw_schema(spec),
                                           {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": CORRUPT})))
    is_bad = F.col("j").isNull() | F.col(f"j.{CORRUPT}").isNotNull()

    mapped = {v for v in norm.values() if v}
    extras = F.to_json(F.struct(*[F.col(f"j.{c}").alias(c) for c in spec["columns"] if c not in mapped]))

    good = parsed.where(~is_bad).select(
        F.lit(name).alias("service"),
        F.col("partition").cast("int").alias("kafka_partition"),
        F.col("offset").cast("long").alias("kafka_offset"),
        _pick(norm["pickup"], "timestamp").alias("pickup_ts"),
        _pick(norm["dropoff"], "timestamp").alias("dropoff_ts"),
        _pick(norm["pu_location"], "int").alias("PULocationID"),
        _pick(norm["do_location"], "int").alias("DOLocationID"),
        _pick(norm["trip_distance"], "double").alias("trip_distance"),
        _pick(norm["passenger_count"], "int").alias("passenger_count"),
        _pick(norm["fare_amount"], "double").alias("fare_amount"),
        _pick(norm["tip_amount"], "double").alias("tip_amount"),
        _pick(norm["total_amount"], "double").alias("total_amount"),
        _pick(norm["payment_type"], "int").alias("payment_type"),
        extras.alias("extras"),
        F.col("value").alias("raw_value"),
    )
    bad = parsed.where(is_bad).select(
        F.lit(name).alias("service"),
        F.col("partition").cast("int").alias("kafka_partition"),
        F.col("offset").cast("long").alias("kafka_offset"),
        F.lit("malformed_json").alias("reject_reason"),
        F.col("value").alias("raw_value"),
    )
    return good, bad


def enrich(df, zones):
    """Left-join the zone lookup (broadcast) for pickup and dropoff."""
    pu = zones.select(F.col("LocationID").cast("int").alias("pu_id"),
                      F.col("Borough").alias("PUBorough"), F.col("Zone").alias("PUZone"))
    do = zones.select(F.col("LocationID").cast("int").alias("do_id"),
                      F.col("Borough").alias("DOBorough"), F.col("Zone").alias("DOZone"))
    return (df.join(F.broadcast(pu), F.col("PULocationID") == F.col("pu_id"), "left").drop("pu_id")
              .join(F.broadcast(do), F.col("DOLocationID") == F.col("do_id"), "left").drop("do_id"))


def _why(cond, reason):
    return F.when(cond, F.lit(reason))


def validate(df, vcfg):
    """Add reject_reason (comma-joined, '' when valid) and dq_flags (non-rejecting notes)."""
    month = datetime.strptime(vcfg["expected_month"], "%Y-%m")
    tol = timedelta(days=int(vcfg["date_tolerance_days"]))
    nxt = datetime(month.year + month.month // 12, month.month % 12 + 1, 1)
    lo = F.lit((month - tol).strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp")
    hi = F.lit((nxt + tol).strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp")

    pickup, dropoff = F.col("pickup_ts"), F.col("dropoff_ts")
    pu, do = F.col("PULocationID"), F.col("DOLocationID")
    required = F.col("service").isin(*vcfg["location_required_services"])
    max_secs = int(vcfg["max_trip_hours"]) * 3600

    reasons = F.filter(F.array(
        _why(pickup.isNull(), "null_pickup_time"),
        _why(pickup.isNotNull() & ((pickup < lo) | (pickup >= hi)), "pickup_out_of_range"),
        _why(dropoff < pickup, "dropoff_before_pickup"),
        _why((F.unix_timestamp(dropoff) - F.unix_timestamp(pickup)) > max_secs, "trip_too_long"),
        _why(F.col("trip_distance") < 0, "negative_distance"),
        _why((F.col("fare_amount") < 0) | (F.col("tip_amount") < 0) | (F.col("total_amount") < 0),
             "negative_amount"),
        _why(pu.isNotNull() & F.col("PUZone").isNull(), "unknown_pickup_location"),
        _why(do.isNotNull() & F.col("DOZone").isNull(), "unknown_dropoff_location"),
        _why(required & pu.isNull(), "null_pickup_location"),
        _why(required & do.isNull(), "null_dropoff_location"),
    ), lambda x: x.isNotNull())

    flags = F.filter(F.array(
        _why(~required & pu.isNull(), "missing_pu_location"),
        _why(~required & do.isNull(), "missing_do_location"),
    ), lambda x: x.isNotNull())

    return (df.withColumn("reasons", reasons)
              .withColumn("reject_reason", F.concat_ws(",", "reasons"))
              .withColumn("dq_flags", F.when(F.size(flags) > 0, F.concat_ws(",", flags))))


def _typed_nulls(df, fields):
    """Select `fields` from df, filling absent columns with typed nulls."""
    present = set(df.columns)
    return df.select(*[F.col(n) if n in present else F.lit(None).cast(t).alias(n) for n, t in fields])


def process(raw, specs, norms, zones, vcfg):
    """raw Kafka-shaped DataFrame -> (clean, invalid)."""
    parts = [parse_dataset(raw, name, spec, norms[name]) for name, spec in specs.items()]
    good = reduce(lambda a, b: a.unionByName(b), [g for g, _ in parts])
    bad = reduce(lambda a, b: a.unionByName(b), [b for _, b in parts])

    checked = validate(enrich(good, zones), vcfg)
    clean = _typed_nulls(checked.where(F.size("reasons") == 0), CLEAN_FIELDS)
    rejected = _typed_nulls(checked.where(F.size("reasons") > 0), INVALID_FIELDS)
    invalid = rejected.unionByName(_typed_nulls(bad, INVALID_FIELDS))
    return clean, invalid
