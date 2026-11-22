"""Step 10 - anomaly detection on zone-hours: forecast-residual detector, MAD rule labels, KMeans distance.

Evaluated two ways: (1) on synthetically injected spikes/drops (known ground truth) and
(2) against explainable MAD rule labels on the real test period.
"""
from __future__ import annotations

import math

import pandas as pd
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ..common.config import Config
from ..common.storage import write_parquet_pandas
from ..common.utils import get_logger, read_parquet, record_measurements, write_parquet
from .metrics import binary_metrics
from .train import load_best_model_name

log = get_logger(__name__)


def hour_of_week(col: str) -> F.Column:
    return F.weekday(col) * 24 + F.hour(col)


def mad_baseline(history: DataFrame) -> DataFrame:
    h = history.withColumn("how", hour_of_week("window_start"))
    med = h.groupBy("zone_id", "how").agg(F.percentile_approx("trip_count", 0.5).alias("hist_median"))
    mad = h.join(med, ["zone_id", "how"]).groupBy("zone_id", "how").agg(
        F.percentile_approx(F.abs(F.col("trip_count") - F.col("hist_median")), 0.5).alias("hist_mad"))
    return med.join(mad, ["zone_id", "how"])


def scaled_residual() -> F.Column:
    # count noise grows ~sqrt(level) (Poisson), so a raw residual of 20 is normal in Midtown but extreme elsewhere
    return (F.col("actual") - F.col("predicted")) / F.sqrt(F.greatest(F.col("predicted"), F.lit(0.0)) + 1.0)


def score(df: DataFrame, stats: DataFrame, baseline: DataFrame, acfg: dict, threshold: float | None = None) -> DataFrame:
    """Adds residual_z, mad_z and the residual/rule flags. df needs zone_id, target_hour, actual, predicted."""
    threshold = acfg["residual_z_threshold"] if threshold is None else threshold
    return (
        df.withColumn("residual", F.col("actual") - F.col("predicted"))
        .withColumn("scaled_residual", scaled_residual())
        .join(stats, "zone_id", "left")
        .withColumn("residual_z", (F.col("scaled_residual") - F.coalesce("res_mean", F.lit(0.0)))
                    / F.coalesce("res_sd", F.lit(float(acfg["residual_min_sigma"]))))
        .withColumn("how", hour_of_week("target_hour"))
        .join(baseline, ["zone_id", "how"], "left")
        .withColumn("mad_z", (F.col("actual") - F.col("hist_median"))
                    / (1.4826 * F.greatest(F.coalesce("hist_mad", F.lit(1.0)), F.lit(1.0))))
        .withColumn("residual_flag", F.coalesce(F.abs("residual_z") >= threshold, F.lit(False)))
        .withColumn("rule_eligible", F.coalesce(F.col("hist_median") >= acfg["rule_min_expected"], F.lit(False)))
        .withColumn("mad_flag", F.col("rule_eligible") & F.coalesce(F.abs("mad_z") >= acfg["rule_z_threshold"], F.lit(False)))
    )


def inject(df: DataFrame, acfg: dict) -> DataFrame:
    seed = acfg["injection_seed"]
    chosen = (F.col("predicted") >= acfg["injection_min_expected"]) & (F.rand(seed) < acfg["injection_fraction"])
    kind = F.when(chosen, F.when(F.rand(seed + 1) < 0.5, "spike").otherwise("drop"))
    return (
        df.withColumn("injection", kind)
        .withColumn("is_injected", F.col("injection").isNotNull())
        .withColumn("actual", F.when(F.col("injection") == "spike",
                                     F.round(F.greatest("actual", "predicted") * acfg["spike_factor"]))
                    .when(F.col("injection") == "drop", F.floor(F.least("actual", "predicted") * acfg["drop_factor"]))
                    .otherwise(F.col("actual")).cast("double"))
    )


class KMeansDetector:
    INPUTS = ["log_actual", "log_predicted", "residual_z"]

    def __init__(self, k: int, quantile: float, seed: int):
        self.k, self.quantile, self.seed = k, quantile, seed

    @staticmethod
    def _prep(df: DataFrame) -> DataFrame:
        return (df.withColumn("log_actual", F.log1p(F.greatest("actual", F.lit(0.0))))
                .withColumn("log_predicted", F.log1p(F.greatest("predicted", F.lit(0.0)))))

    def _distance(self, df: DataFrame) -> DataFrame:
        centers = self.centers

        @F.udf("double")
        def dist(vec, cluster):
            c = centers[cluster]
            return math.sqrt(sum((float(a) - b) ** 2 for a, b in zip(vec, c)))

        vec = VectorAssembler(inputCols=self.INPUTS, outputCol="km_features", handleInvalid="skip")
        return self.model.transform(vec.transform(self._prep(df))).withColumn(
            "kmeans_distance", dist("km_features", "km_cluster")).drop("km_features", "km_cluster")

    def fit(self, df: DataFrame) -> "KMeansDetector":
        vec = VectorAssembler(inputCols=self.INPUTS, outputCol="km_features", handleInvalid="skip")
        self.model = KMeans(k=self.k, seed=self.seed, featuresCol="km_features",
                            predictionCol="km_cluster").fit(vec.transform(self._prep(df)))
        self.centers = [c.tolist() for c in self.model.clusterCenters()]
        self.threshold = self._distance(df).approxQuantile("kmeans_distance", [self.quantile], 0.001)[0]
        return self

    def apply(self, df: DataFrame) -> DataFrame:
        return self._distance(df).withColumn("kmeans_flag", F.col("kmeans_distance") > self.threshold) \
            .drop("log_actual", "log_predicted")


def run_anomaly(spark: SparkSession, cfg: Config) -> dict:
    acfg = cfg.section("anomaly")
    best = load_best_model_name(cfg)
    fc = read_parquet(spark, cfg.path("gold") / "forecasts", "ml").where(F.col("model") == best)
    val, test = fc.where("split = 'validation'"), fc.where("split = 'test'")

    stats = val.withColumn("scaled_residual", scaled_residual()).groupBy("zone_id").agg(
        F.avg("scaled_residual").alias("res_mean"),
        F.greatest(F.coalesce(F.stddev_pop("scaled_residual"), F.lit(0.0)),
                   F.lit(float(acfg["residual_min_sigma"]))).alias("res_sd"))
    test_start = test.agg(F.min("target_hour")).first()[0]
    demand = read_parquet(spark, cfg.path("silver") / "zone_hour_demand", "demand")
    baseline = mad_baseline(demand.where(F.col("window_start") < F.lit(test_start)))

    base_cols = ["zone_id", "borough", "target_hour", "actual", "predicted"]
    val_scored = score(val.select(*base_cols), stats, baseline, acfg)
    threshold = float(acfg["residual_z_threshold"])
    if acfg.get("residual_alert_rate"):
        q = val_scored.select(F.abs("residual_z").alias("az")).approxQuantile("az", [1 - acfg["residual_alert_rate"]], 0.0005)[0]
        threshold = max(threshold, q)
    log.info("residual detector threshold |z| >= %.2f", threshold)
    km = KMeansDetector(acfg["kmeans_k"], acfg["kmeans_quantile"], cfg.section("ml")["seed"]).fit(val_scored)

    real = km.apply(score(test.select(*base_cols), stats, baseline, acfg, threshold)).cache()
    injected = km.apply(score(inject(test.select(*base_cols), acfg), stats, baseline, acfg, threshold)).cache()

    results = {}
    # injection only targets rows with predicted >= injection_min_expected, so evaluate on that population
    eligible = injected.where(F.col("predicted") >= acfg["injection_min_expected"])
    for detector in ["residual_flag", "mad_flag", "kmeans_flag"]:
        results[f"injected:{detector}"] = binary_metrics(eligible, "is_injected", detector)
    results["rule:residual_flag"] = binary_metrics(real.where("rule_eligible"), "mad_flag", "residual_flag")
    results["real:residual_flag"] = binary_metrics(real.withColumn("_t", F.lit(False)), "_t", "residual_flag")

    severity_z = F.greatest(F.abs("residual_z"), F.coalesce(F.abs("mad_z"), F.lit(0.0)))
    context = demand.select(F.col("zone_id"), F.col("window_start").alias("target_hour"), "zone_name",
                            "weather_condition", "temp_c", "precip_mm")
    anomalies = (
        real.where(F.col("residual_flag") | F.col("mad_flag") | F.coalesce("kmeans_flag", F.lit(False)))
        .join(context, ["zone_id", "target_hour"], "left")
        .withColumn("expected", F.col("predicted"))
        .withColumn("direction", F.when(F.col("residual") >= 0, "spike").otherwise("drop"))
        .withColumn("severity", F.when(severity_z > 6, "high").when(severity_z > 4, "medium").otherwise("low"))
        .withColumn("detectors_fired", F.filter(F.array(
            F.when(F.col("residual_flag"), F.lit("residual")), F.when(F.col("mad_flag"), F.lit("mad")),
            F.when(F.coalesce("kmeans_flag", F.lit(False)), F.lit("kmeans"))), lambda x: x.isNotNull()))
        .withColumn("hour_of_day", F.hour("target_hour"))
        .select("zone_id", "zone_name", "borough", F.col("target_hour").alias("window_start"), "hour_of_day",
                "actual", "expected", "residual", "residual_z", "mad_z", "kmeans_distance", "direction", "severity",
                "detectors_fired", "weather_condition", "temp_c", "precip_mm")
    )
    write_parquet(anomalies.coalesce(1), cfg.path("gold") / "anomalies", replace_all=True)

    rows = [{"evaluation": k.split(":")[0], "detector": k.split(":")[1], **v} for k, v in results.items()]
    out = cfg.path("gold") / "anomaly_metrics"
    write_parquet_pandas(pd.DataFrame(rows), out / "anomaly_metrics.parquet")

    inj, rule, live = results["injected:residual_flag"], results["rule:residual_flag"], results["real:residual_flag"]
    # Diagnostic for A4b: with `alerts` residual alerts against `positives` rule labels, recall can never exceed
    # alerts / positives, even if every alert were a true match. It shows how far the alert-rate cap (A5) limits A4b.
    rule_positives = rule["tp"] + rule["fn"]
    record_measurements(cfg, {
        "anomaly.injected_recall": inj["recall"], "anomaly.injected_precision": inj["precision"],
        "anomaly.injected_f1": inj["f1"], "anomaly.rule_precision": rule["precision"],
        "anomaly.rule_recall": rule["recall"], "anomaly.alert_rate_pct": live["alert_rate_pct"],
        "anomaly.rule_recall_ceiling": min(1.0, (rule["tp"] + rule["fp"]) / rule_positives) if rule_positives else None,
        "anomaly.rule_positive_rate_pct": 100.0 * rule_positives / max(rule["n"], 1),
    }, {"best_model": best, "injected_rows": inj["tp"] + inj["fn"], "rule_positives": rule_positives})
    n_anom = spark.read.parquet(str(cfg.path("gold") / "anomalies")).count()
    real.unpersist()
    injected.unpersist()
    log.info("anomalies=%d injected P/R/F1=%.2f/%.2f/%.2f rule P/R=%.2f/%.2f alert rate=%.2f%%", n_anom,
             inj["precision"], inj["recall"], inj["f1"], rule["precision"], rule["recall"], live["alert_rate_pct"])
    return {"anomalies": n_anom, **{k: v for k, v in results.items()}}
