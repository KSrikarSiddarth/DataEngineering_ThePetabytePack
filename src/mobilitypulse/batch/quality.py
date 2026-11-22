"""Step 4 data-quality rules. Pure column expressions so the same rules run in batch and in streaming."""
from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

DUPLICATE = "DUPLICATE"


def add_derived(df: DataFrame) -> DataFrame:
    duration = (F.col("dropoff_ts").cast("long") - F.col("pickup_ts").cast("long")) / 60.0
    return (
        df.withColumn("duration_min", duration)
        .withColumn("speed_mph", F.when(F.col("duration_min") > 0,
                                        F.col("trip_distance") / (F.col("duration_min") / 60.0)))
    )


def rules(q: dict) -> list[tuple[str, Column]]:
    """Ordered (reason_code, violated?) pairs. Order defines the primary reject_reason."""
    zone_ok = lambda c: F.col(c).between(q["min_location_id"], q["max_location_id"])  # noqa: E731
    return [
        ("NULL_TIMESTAMP", F.col("pickup_ts").isNull() | F.col("dropoff_ts").isNull()),
        ("OUTSIDE_SOURCE_MONTH", F.date_format("pickup_ts", "yyyy-MM") != F.col("source_month")),
        ("NEGATIVE_DURATION", F.col("dropoff_ts") < F.col("pickup_ts")),
        ("DURATION_OUT_OF_RANGE", (F.col("duration_min") >= 0) & ((F.col("duration_min") < q["min_trip_minutes"])
                                                                  | (F.col("duration_min") > q["max_trip_minutes"]))),
        ("INVALID_ZONE", ~F.coalesce(zone_ok("pickup_zone_id") & zone_ok("dropoff_zone_id"), F.lit(False))),
        ("DISTANCE_OUT_OF_RANGE", F.col("trip_distance").isNull() | (F.col("trip_distance") <= 0)
         | (F.col("trip_distance") > q["max_trip_miles"])),
        ("FARE_OUT_OF_RANGE", F.col("total_amount").isNull() | (F.col("total_amount") < 0)
         | (F.col("total_amount") > q["max_fare"])),
        ("PASSENGERS_OUT_OF_RANGE", F.col("passenger_count") > q["max_passengers"]),
        ("SPEED_IMPLAUSIBLE", F.col("speed_mph") > q["max_speed_mph"]),
    ]


def add_reject_reasons(df: DataFrame, q: dict) -> DataFrame:
    flags = [F.when(F.coalesce(cond, F.lit(False)), F.lit(code)) for code, cond in rules(q)]
    return df.withColumn("reject_reasons", F.filter(F.array(*flags), lambda x: x.isNotNull()))


def flag_duplicates(df: DataFrame) -> DataFrame:
    """Keep the first copy of each record_id (preferring a valid copy); later copies get DUPLICATE."""
    w = Window.partitionBy("record_id").orderBy(F.size("reject_reasons"), F.col("source_file"))
    rn = F.row_number().over(w)
    return df.withColumn(
        "reject_reasons",
        F.when((rn > 1) & (F.size("reject_reasons") == 0), F.array(F.lit(DUPLICATE))).otherwise(F.col("reject_reasons")),
    )


def apply_quality(df: DataFrame, q: dict) -> DataFrame:
    out = flag_duplicates(add_reject_reasons(add_derived(df), q))
    return out.withColumn("reject_reason", F.element_at("reject_reasons", 1))
