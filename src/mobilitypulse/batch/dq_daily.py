"""GOLD data_quality_daily: per-day row counts, null rates, reject counts by reason, freshness and alert flags.

Built from the quality-flagged bronze rows (the same frame silver and quarantine are split from), so
`input_rows = valid_rows + rejected_rows` holds per day by construction.

Rows whose timestamp is null or falls outside their source month cannot be placed on a real day. They are kept in one
`day_kind = 'unattributed'` row per month (date is null) and never alert - otherwise a few stray out-of-range
timestamps would show up as fake 100 %-reject days.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ..common.config import Config
from ..common.utils import get_logger, write_parquet
from .quality import DUPLICATE, rules
from .silver import NULL_CHECK_COLUMNS

log = get_logger(__name__)

DEFAULT_ALERT_REJECT_RATE_PCT = 8.0  # PRD target D2: alert when a day rejects more than 8 % of its rows


def reason_codes(q: dict) -> list[str]:
    return [code for code, _ in rules(q)] + [DUPLICATE]


def dq_daily_frame(flagged: DataFrame, q: dict) -> DataFrame:
    """One row per (source_month, day); `date` is null for rows that cannot be attributed to a day."""
    threshold = float(q.get("alert_reject_rate_pct", DEFAULT_ALERT_REJECT_RATE_PCT))
    in_month = F.date_format("pickup_ts", "yyyy-MM") == F.col("source_month")
    day = F.when(F.col("pickup_ts").isNotNull() & in_month, F.to_date("pickup_ts"))
    rejected = F.col("reject_reason").isNotNull()
    n = F.count(F.lit(1))

    aggs = [
        n.alias("input_rows"),
        F.sum((~rejected).cast("long")).alias("valid_rows"),
        F.sum(rejected.cast("long")).alias("rejected_rows"),
        *[F.sum((F.col("reject_reason") == r).cast("long")).alias(f"rej_{r.lower()}") for r in reason_codes(q)],
        *[F.round(100.0 * F.sum(F.col(c).isNull().cast("long")) / n, 3).alias(f"null_pct_{c}")
          for c in NULL_CHECK_COLUMNS],
        F.max("pickup_ts").alias("max_pickup_ts"),
        F.max("ingested_at").alias("ingested_at"),
    ]
    out = flagged.groupBy("source_month", day.alias("date")).agg(*aggs)
    return (
        out.withColumn("day_kind", F.when(F.col("date").isNull(), "unattributed").otherwise("day"))
        .withColumn("reject_rate_pct", F.round(100.0 * F.col("rejected_rows") / F.col("input_rows"), 3))
        .withColumn("alert_reject_rate", (F.col("day_kind") == "day") & (F.col("reject_rate_pct") > threshold))
        .withColumn("alert_threshold_pct", F.lit(threshold))
        .withColumn("ingest_lag_h", F.round((F.col("ingested_at").cast("long") - F.col("max_pickup_ts").cast("long"))
                                            / 3600.0, 2))
        .withColumn("computed_at", F.current_timestamp())
    )


def write_dq_daily(flagged: DataFrame, cfg: Config, months: list[str]) -> int:
    """Write gold/data_quality_daily, replacing only the months in `months` (idempotent per month)."""
    df = dq_daily_frame(flagged.where(F.col("source_month").isin(months)), cfg.section("quality"))
    path = cfg.path("gold") / "data_quality_daily"
    write_parquet(df.repartition("source_month"), path, partition_by=["source_month"])
    written = flagged.sparkSession.read.parquet(str(path)).where(F.col("source_month").isin(months))
    rows = written.count()
    alerts = written.where("alert_reject_rate").count()
    log.info("gold/data_quality_daily: %d rows for %s, %d alert day(s) (reject rate > threshold)", rows, months, alerts)
    return rows
