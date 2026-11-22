"""Command-line entry point for the batch + ML pipeline.

Examples:
  python -m mobilitypulse.cli --stage all --months small
  python -m mobilitypulse.cli --stage silver --months 2024-01..2024-03
  python -m mobilitypulse.cli --stage demo            # synthetic data, full pipeline, no internet needed
"""
from __future__ import annotations

import argparse
import json

from .common.config import load_config
from .common.utils import Timer, get_logger, parse_months, record_measurements

log = get_logger("mobilitypulse")

BATCH_STAGES = ["bronze", "silver", "demand", "features", "analytics", "ml", "anomaly", "export"]
STAGES = ["sample", "ingest", *BATCH_STAGES, "scorecard", "all", "demo"]


def run_stage(stage: str, cfg, months: list[str], force: bool = False, spark=None):
    if stage == "sample":
        from .ingestion.sample import generate_sample
        return generate_sample(cfg, months)
    if stage == "ingest":
        from .ingestion.download import run_ingest
        return run_ingest(cfg, months, force)
    if stage == "export":
        from .serving.export import export_serving_db
        return export_serving_db(cfg)
    if stage == "scorecard":
        from .benchmark.scorecard import build_scorecard
        return [f"{r['id']}:{r['status']}" for r in build_scorecard(cfg)]

    from .common.spark import get_spark
    spark = spark or get_spark(cfg, stage)
    if stage == "bronze":
        from .batch.bronze import run_bronze
        return run_bronze(spark, cfg, months)
    if stage == "silver":
        from .batch.silver import run_silver
        return run_silver(spark, cfg, months)
    if stage == "demand":
        from .batch.demand import build_zone_hour_demand
        return build_zone_hour_demand(spark, cfg, months)
    if stage == "features":
        from .batch.features import build_features
        with Timer() as t:
            out = build_features(spark, cfg, months)
        record_measurements(cfg, {"features.build_min": t.seconds / 60}, {"months": months})
        return out
    if stage == "analytics":
        from .batch.analytics import run_analytics
        return run_analytics(spark, cfg, months)
    if stage == "ml":
        from .ml.train import train_and_evaluate
        return train_and_evaluate(spark, cfg)
    if stage == "anomaly":
        from .ml.anomaly import run_anomaly
        return run_anomaly(spark, cfg)
    raise ValueError(stage)


def run_many(stages: list[str], cfg, months: list[str], force: bool = False) -> dict:
    spark = None
    timings = {}
    for stage in stages:
        if stage not in ("sample", "ingest", "export", "scorecard") and spark is None:
            from .common.spark import get_spark
            spark = get_spark(cfg, "pipeline")
        with Timer() as t:
            out = run_stage(stage, cfg, months, force, spark)
        timings[stage] = round(t.seconds, 1)
        log.info("stage %-9s done in %6.1fs -> %s", stage, t.seconds, _short(out))
    return timings


def _short(out) -> str:
    text = json.dumps(out, default=str)
    return text if len(text) < 300 else text[:297] + "..."


def main() -> None:
    ap = argparse.ArgumentParser(description="NYC MobilityPulse pipeline")
    ap.add_argument("--stage", choices=STAGES, default="all")
    ap.add_argument("--months", help="small|medium|large, YYYY-MM..YYYY-MM or comma list (default: datasets.default)")
    ap.add_argument("--force", action="store_true", help="re-download even if unchanged")
    a = ap.parse_args()
    cfg = load_config()
    log.info("data dir: %s", cfg.data_dir)

    if a.stage == "demo":
        months = parse_months(a.months, cfg) if a.months else list(cfg.section("sample")["months"])
        with Timer() as t:
            timings = run_many(["sample", *BATCH_STAGES], cfg, months)
        record_measurements(cfg, {"repro.sample_e2e_min": t.seconds / 60}, {"stages": timings})
    elif a.stage == "all":
        months = parse_months(a.months, cfg)
        timings = run_many(["ingest", *BATCH_STAGES], cfg, months, a.force)
    else:
        months = parse_months(a.months, cfg)
        timings = run_many([a.stage], cfg, months, a.force)
    log.info("finished: %s", timings)


if __name__ == "__main__":
    main()
