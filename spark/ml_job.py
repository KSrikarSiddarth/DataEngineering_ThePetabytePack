"""M6 Spark ML job: trains and scores the KPI-catalog models on the batch store.

    python ml_job.py                                        # all models on the configured batch store
    python ml_job.py --batch-dir output/ml_dev --models fare anomaly

Reads <batch_dir>/silver/trips and <batch_dir>/gold/zone_od_daily, demand_hourly and revenue_daily, builds the
`silver/ml_features` tables (spark/ml_features.py), and writes <batch_dir>/gold/ml_* (the holdout predictions of every
model, the clusters and the flagged anomalies) plus gold/ml_model_metrics. The metrics table is appended to on every
run (run_id, run_ts) so training time and error can be followed over runs.

Models: demand forecast (GBTRegressor), fare, duration and wait time (GBTRegressor), tip of 20% or more
(RandomForestClassifier), shared-ride match (GBTClassifier), zone and demand-pattern clustering (KMeans), anomaly
detection (fare-model residuals) and PCA. Supervised models are scored on a holdout chosen by a hash of the row key
(`ml.test_percent`), so the same seed gives the same split and the same metrics. Needs no Kafka.

ml_model_metrics rows are (model, metric, scope, value); scope is `test` (holdout), `baseline` (the simple rule),
`cv` (cross-validation on the training part), `month:YYYY-MM` (the primary metric per month of the holdout),
`drift` (metric = feature name: shift of the feature mean from the first to the last month, in first-month
standard deviations) or `train` (seconds).
"""
import argparse
import sys
import time
import uuid
from collections import Counter
from datetime import datetime, timezone
from itertools import accumulate
from pathlib import Path
from statistics import mean, stdev

import pyspark.sql.functions as F
from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier, RandomForestClassifier
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import BinaryClassificationEvaluator, ClusteringEvaluator
from pyspark.ml.feature import PCA, StandardScaler, VectorAssembler
from pyspark.ml.regression import GBTRegressor, LinearRegression

from ml_features import FEATURES, LABELS, ROWS, TRIP, trip_features, zone_hour
from stream_job import build_spark, load_config

METRIC_SCHEMA = "model string, metric string, scope string, value double"
HOURS = [f"h{h}" for h in range(24)]
MAX_SPEED_MPH = 80                        # the fixed anomaly rule the models are compared with
MIN_ROWS = 10                             # fewer rows than this: the model is skipped


# --- estimators and baselines --------------------------------------------------------------------------------------

def gbt_regressor(cfg):
    return GBTRegressor(maxIter=cfg["gbt_iterations"], maxDepth=cfg["gbt_max_depth"], maxBins=cfg["max_bins"],
                        seed=cfg["seed"])


def gbt_classifier(cfg):
    return GBTClassifier(maxIter=cfg["gbt_iterations"], maxDepth=cfg["gbt_max_depth"], maxBins=cfg["max_bins"],
                         seed=cfg["seed"])


def forest_classifier(cfg):
    return RandomForestClassifier(numTrees=cfg["forest_trees"], maxDepth=cfg["gbt_max_depth"], maxBins=cfg["max_bins"],
                                  seed=cfg["seed"])


def same_hour_last_week(train, test, label):
    """Baseline for the demand forecast: the trips of the same zone and hour one week earlier."""
    return test.withColumn("baseline", F.col("lag_168h").cast("double"))


def group_mean(by):
    """Baseline: the mean label of the training rows with the same `by` columns (overall mean for an unseen group)."""
    def baseline(train, test, label):
        means = train.groupBy(*by).agg(F.avg(label).alias("baseline"))
        overall = train.agg(F.avg(label)).first()[0]
        return test.join(means, by, "left").withColumn("baseline", F.coalesce("baseline", F.lit(overall)))
    return baseline


def linear_on_distance(train, test, label):
    """Baseline for the fare: a linear fit on the trip distance only."""
    fit = Pipeline(stages=[VectorAssembler(inputCols=["distance_miles"], outputCol="distance_vec"),
                           LinearRegression(featuresCol="distance_vec", labelCol=label, predictionCol="baseline")]).fit(train)
    return fit.transform(test)


def overall_rate(train, test, label):
    """Baseline for the classifiers: every row gets the training positive rate (AUC 0.5)."""
    return test.withColumn("baseline", F.lit(train.agg(F.avg(label)).first()[0]))


# table: which feature table feeds the model; primary: the metric the catalog names (lower error is better, except auc)
MODELS = {
    "demand_forecast": dict(table="zone_hour", kind="reg", est=gbt_regressor, baseline=same_hour_last_week,
                            primary="wape", keys=["pu_location_id", "hour_start"], out="ml_demand_forecast"),
    "fare": dict(table="trips", kind="reg", est=gbt_regressor, baseline=linear_on_distance, primary="mae",
                 keys=["trip_id"], out="ml_fare_predictions"),
    "duration": dict(table="trips", kind="reg", est=gbt_regressor,
                     baseline=group_mean(["pu_location_id", "do_location_id", "hour"]), primary="mae",
                     keys=["trip_id"], out="ml_duration_predictions"),
    "tip": dict(table="trips", kind="cls", est=forest_classifier, baseline=overall_rate, primary="auc",
                keys=["trip_id"], out="ml_tip_predictions"),
    "wait": dict(table="trips", kind="reg", est=gbt_regressor, baseline=group_mean(["pu_location_id", "hour"]),
                 primary="mae", keys=["trip_id"], out="ml_wait_predictions"),
    "shared_match": dict(table="trips", kind="cls", est=gbt_classifier, baseline=overall_rate, primary="auc",
                         keys=["trip_id"], out="ml_shared_match_predictions"),
}
UNSUPERVISED = ["zone_clusters", "demand_patterns", "anomaly", "pca"]


# --- scoring ---------------------------------------------------------------------------------------------------------

def regression_scores(df, label, pred):
    """MAE, RMSE, WAPE (sum |error| / sum |label|), R2 and the share of rows within 10% of the label."""
    e = F.col(pred) - F.col(label)
    return df.agg(F.avg(F.abs(e)).alias("mae"), F.sqrt(F.avg(e * e)).alias("rmse"),
                  (F.sum(F.abs(e)) / F.sum(F.abs(F.col(label)))).alias("wape"),
                  (1 - F.avg(e * e) / F.var_pop(label)).alias("r2"),
                  F.avg((F.abs(e) <= 0.1 * F.abs(F.col(label))).cast("double")).alias("within_10pct")).first().asDict()


def classification_scores(df, label, score):
    """AUC-ROC of `score`, plus accuracy, precision, recall and F1 at the 0.5 threshold."""
    auc = BinaryClassificationEvaluator(labelCol=label, rawPredictionCol=score, metricName="areaUnderROC").evaluate(df)
    hit, pos = F.col(score) >= 0.5, F.col(label) == 1
    r = df.agg(F.sum((hit & pos).cast("int")).alias("tp"), F.sum(hit.cast("int")).alias("hits"),
               F.sum(pos.cast("int")).alias("pos"), F.avg((hit == pos).cast("double")).alias("accuracy")).first()
    precision = r["tp"] / r["hits"] if r["hits"] else 0.0
    recall = r["tp"] / r["pos"] if r["pos"] else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    return {"auc": auc, "accuracy": r["accuracy"], "precision": precision, "recall": recall, "f1": f1}


def scores(kind, df, label, pred):
    """All metrics of one prediction column, by model kind (reg or cls)."""
    return (regression_scores if kind == "reg" else classification_scores)(df, label, pred)


def error_of(metric, value):
    """The error form of a metric, so a lift can be computed: 1 - AUC for auc, the value itself otherwise."""
    return 1 - value if metric == "auc" else value


def lift(metric, baseline, model):
    """(baseline error - model error) / baseline error (positive = the model beats the baseline)."""
    b, m = error_of(metric, baseline), error_of(metric, model)
    return (b - m) / b if b else 0.0


# --- supervised models -----------------------------------------------------------------------------------------------

def fit(spec, train, features, label, cfg):
    """Fit a feature-assembler + estimator pipeline on `train`."""
    stages = [VectorAssembler(inputCols=features, outputCol="features"), spec["est"](cfg).setLabelCol(label)]
    return Pipeline(stages=stages).fit(train)


def predict(model, df, kind):
    """Model predictions; classifiers also get `score` = probability of the positive class."""
    out = model.transform(df)
    if kind == "reg":
        return out
    positive = F.udf(lambda v: float(v[1]), "double")   # pyspark.ml.functions.vector_to_array would need pandas
    return out.withColumn("score", positive("probability"))


def hash_bucket(seed, keys, buckets):
    """Deterministic 0..buckets-1 bucket of a row from its key columns (the same data gives the same split)."""
    return F.pmod(F.hash(F.lit(seed), *keys), F.lit(buckets))


def cross_validate(spec, train, features, label, cfg):
    """(mean, standard deviation) of the primary metric over `ml.cv_folds` folds of the training part."""
    folds = hash_bucket(cfg["seed"] + 1, spec["keys"], cfg["cv_folds"])
    pred_col = "prediction" if spec["kind"] == "reg" else "score"
    values = []
    for k in range(cfg["cv_folds"]):
        model = fit(spec, train.where(folds != k), features, label, cfg)
        held = predict(model, train.where(folds == k), spec["kind"])
        values.append(scores(spec["kind"], held, label, pred_col)[spec["primary"]])
    return mean(values), stdev(values)


def feature_drift(df, features):
    """(feature, shift) pairs: |mean in the last month - mean in the first month| / std in the first month.
    Empty when the data covers one month."""
    stats = {r["ym"]: r for r in df.groupBy("ym").agg(
        *[F.avg(c).alias(f"{c}_mean") for c in features], *[F.stddev(c).alias(f"{c}_std") for c in features]).collect()}
    first, last = stats[min(stats)], stats[max(stats)]
    if len(stats) < 2:
        return []
    return [(c, abs(last[f"{c}_mean"] - first[f"{c}_mean"]) / first[f"{c}_std"] if first[f"{c}_std"] else 0.0)
            for c in features]


def run_model(name, table, cfg, out_dir, rows):
    """Train, score and report one supervised model on its feature `table`. Returns (model, train, test) or None
    when there are too few rows. Writes gold/<out> (holdout predictions) and appends the metric rows."""
    spec, features, label = MODELS[name], FEATURES[name], LABELS[name]
    data = table.where(ROWS[name]) if name in ROWS else table
    in_test = hash_bucket(cfg["seed"], spec["keys"], 100) < cfg["test_percent"]
    train, test = data.where(~in_test).cache(), data.where(in_test).cache()
    if train.count() < MIN_ROWS or test.count() < MIN_ROWS:
        print(f"{name:16} skipped (not enough rows)")
        return None

    kind, primary = spec["kind"], spec["primary"]
    started = time.time()
    model = fit(spec, train, features, label, cfg)
    rows.append((name, "train_seconds", "train", time.time() - started))

    pred_col = "prediction" if kind == "reg" else "score"
    pred = spec["baseline"](train, predict(model, test, kind), label).cache()
    test_scores, base_scores = scores(kind, pred, label, pred_col), scores(kind, pred, label, "baseline")
    rows += [(name, m, "test", v) for m, v in test_scores.items()]
    rows += [(name, m, "baseline", v) for m, v in base_scores.items()]
    rows.append((name, "lift", "test", lift(primary, base_scores[primary], test_scores[primary])))
    for ym in sorted(r["ym"] for r in pred.select("ym").distinct().collect()):
        rows.append((name, primary, f"month:{ym}", scores(kind, pred.where(F.col("ym") == ym), label, pred_col)[primary]))
    if cfg["cv_folds"] >= 2:
        cv_mean, cv_std = cross_validate(spec, train, features, label, cfg)
        rows += [(name, "cv_mean", "cv", cv_mean), (name, "cv_std", "cv", cv_std)]
    rows += [(name, c, "drift", v) for c, v in feature_drift(data, features)]

    out = pred.select(*spec["keys"], "ym", label, F.col(pred_col).alias("prediction"), "baseline")
    out.withColumn("error", F.col("prediction") - F.col(label)).write.mode("overwrite").parquet(str(out_dir / spec["out"]))
    print(f"{name:16} {primary}={test_scores[primary]:.4f} baseline={base_scores[primary]:.4f} "
          f"train={train.count()} test={test.count()}")
    return model, train, test


# --- clustering, anomalies, PCA --------------------------------------------------------------------------------------

def adjusted_rand_index(a, b):
    """Adjusted Rand index of two labelings of the same items: 1 = identical partitions, about 0 = random."""
    def pairs(n):
        return n * (n - 1) / 2
    n = len(a)
    index = sum(pairs(v) for v in Counter(zip(a, b)).values())
    sa, sb = sum(pairs(v) for v in Counter(a).values()), sum(pairs(v) for v in Counter(b).values())
    expected, maximum = sa * sb / pairs(n), (sa + sb) / 2
    return 1.0 if maximum == expected else (index - expected) / (maximum - expected)


def cluster(df, columns, key, k, seed, name, rows):
    """KMeans on standardized `columns`. Adds silhouette and stability (mean ARI against two other seeds) to `rows`
    and returns the DataFrame with a `cluster` column; None when there are not enough rows."""
    if df.count() <= k:
        print(f"{name:16} skipped (not enough rows)")
        return None
    vec = VectorAssembler(inputCols=columns, outputCol="raw").transform(df)
    scaled = StandardScaler(inputCol="raw", outputCol="features", withMean=True, withStd=True).fit(vec).transform(vec)
    fits = [KMeans(k=k, seed=seed + i).fit(scaled).transform(scaled).orderBy(key).cache() for i in range(3)]
    silhouette = ClusteringEvaluator().evaluate(fits[0])
    labels = [[r["prediction"] for r in f.select("prediction").collect()] for f in fits]
    stability = mean(adjusted_rand_index(labels[0], other) for other in labels[1:])
    rows += [(name, "silhouette", "test", silhouette), (name, "stability_ari", "test", stability)]
    print(f"{name:16} silhouette={silhouette:.3f} stability={stability:.3f}")
    return fits[0].withColumnRenamed("prediction", "cluster").select(key, *columns, "cluster")


def zone_clusters(gold, cfg, out_dir, rows):
    """Zones with a similar demand profile: the share of trips in each pickup hour, the average distance and the
    average fare (yellow and green). Zones with fewer than `ml.min_zone_trips` trips are left out."""
    od = gold("zone_od_daily").where(F.col("pu_location_id").isNotNull())
    by_hour = od.groupBy("pu_location_id").agg(
        F.sum("trips").alias("total"), F.sum("sum_distance_miles").alias("dist"), F.sum("distance_trips").alias("dist_n"),
        *[F.sum(F.when(F.col("pickup_hour") == h, F.col("trips"))).alias(f"h{h}") for h in range(24)])
    fares = (gold("revenue_daily").groupBy("pu_location_id")
             .agg((F.sum("sum_total_amount") / F.sum("trips")).alias("avg_fare")))
    profile = (by_hour.where(F.col("total") >= cfg["min_zone_trips"]).join(fares, "pu_location_id", "left")
               .select("pu_location_id", F.coalesce(F.col("dist") / F.col("dist_n"), F.lit(0.0)).alias("avg_distance"),
                       F.coalesce("avg_fare", F.lit(0.0)).alias("avg_fare"),
                       *[(F.coalesce(F.col(h), F.lit(0)) / F.col("total")).alias(h) for h in HOURS]))
    out = cluster(profile, HOURS + ["avg_distance", "avg_fare"], "pu_location_id", cfg["zone_clusters"], cfg["seed"],
                  "zone_clusters", rows)
    if out is not None:
        out.select("pu_location_id", "cluster", "avg_distance", "avg_fare").write.mode("overwrite") \
            .parquet(str(out_dir / "ml_zone_clusters"))


def demand_patterns(gold, cfg, out_dir, rows):
    """Typical daily demand shapes: the city-wide share of trips in each hour, one row per complete day."""
    daily = (gold("demand_hourly").groupBy("pickup_date", "day_of_week", "pickup_hour").agg(F.sum("trips").alias("trips"))
             .groupBy("pickup_date", "day_of_week")
             .agg(F.count("*").alias("hours"), F.sum("trips").alias("total"),
                  *[F.sum(F.when(F.col("pickup_hour") == h, F.col("trips"))).alias(f"h{h}") for h in range(24)]))
    shapes = (daily.where(F.col("hours") == 24)
              .select("pickup_date", "day_of_week", *[(F.col(h) / F.col("total")).alias(h) for h in HOURS]))
    out = cluster(shapes, HOURS, "pickup_date", cfg["demand_pattern_clusters"], cfg["seed"], "demand_patterns", rows)
    if out is not None:
        out.join(shapes.select("pickup_date", "day_of_week"), "pickup_date") \
            .select("pickup_date", "day_of_week", "cluster").write.mode("overwrite") \
            .parquet(str(out_dir / "ml_demand_pattern_clusters"))


def detect_anomalies(fare, cfg, out_dir, rows):
    """Anomaly detection with the fare model: a trip is flagged when |fare - predicted fare| exceeds the
    `ml.anomaly_quantile` quantile of the training residuals. There is no ground truth, so recall and precision are
    measured on injected bad trips: `ml.anomaly_inject_percent` of the holdout gets a fare x10 or a distance x10.
    The baseline is a fixed rule (speed above 80 mph). Flagged real trips go to gold/ml_trip_anomalies."""
    model, train, test = fare
    residual = F.abs(F.col("prediction") - F.col("base_fare"))
    limit = model.transform(train).select(residual.alias("r")).approxQuantile("r", [cfg["anomaly_quantile"]], 0.001)[0]

    bad = hash_bucket(cfg["seed"] + 2, ["trip_id"], 100) < cfg["anomaly_inject_percent"]
    fare_x10 = hash_bucket(cfg["seed"] + 3, ["trip_id"], 2) == 0
    injected = (test.withColumn("bad", bad)
                .withColumn("base_fare", F.when(bad & fare_x10, F.col("base_fare") * 10).otherwise(F.col("base_fare")))
                .withColumn("distance_miles", F.when(bad & ~fare_x10, F.col("distance_miles") * 10)
                            .otherwise(F.col("distance_miles"))))
    scored = (model.transform(injected)
              .withColumn("flag", residual > limit)
              .withColumn("speed_mph", F.col("distance_miles") / (F.col("duration_min") / 60))
              .withColumn("rule", F.col("speed_mph") > MAX_SPEED_MPH).cache())
    r = scored.agg(F.sum(F.col("bad").cast("int")).alias("bad"),
                   F.sum((F.col("flag") & F.col("bad")).cast("int")).alias("tp"),
                   F.sum(F.col("flag").cast("int")).alias("flagged"),
                   F.sum((F.col("rule") & F.col("bad")).cast("int")).alias("rule_tp"),
                   F.sum(F.col("rule").cast("int")).alias("rule_flagged"),
                   F.sum((F.col("flag") & ~F.col("bad")).cast("int")).alias("clean_flagged"),
                   F.sum((~F.col("bad")).cast("int")).alias("clean")).first()

    def ratio(a, b):
        return r[a] / r[b] if r[b] else 0.0
    rows += [("anomaly", "recall", "test", ratio("tp", "bad")), ("anomaly", "precision", "test", ratio("tp", "flagged")),
             ("anomaly", "flagged_rate", "test", ratio("clean_flagged", "clean")),
             ("anomaly", "recall", "baseline", ratio("rule_tp", "bad")),
             ("anomaly", "precision", "baseline", ratio("rule_tp", "rule_flagged"))]
    scored.where(F.col("flag") & ~F.col("bad")).select(
        "trip_id", "service_type", "pickup_ts", "distance_miles", "base_fare", F.col("prediction").alias("predicted_fare"),
        (F.col("base_fare") - F.col("prediction")).alias("residual"), "speed_mph").write.mode("overwrite") \
        .parquet(str(out_dir / "ml_trip_anomalies"))
    print(f"anomaly          recall={ratio('tp', 'bad'):.3f} precision={ratio('tp', 'flagged'):.3f} "
          f"flagged_rate={ratio('clean_flagged', 'clean'):.4f}")


def run_pca(trips, rows):
    """PCA on the standardized fare and trip features: components needed for 90% of the variance, and the
    variance explained by half as many components as features."""
    cols = TRIP + ["base_fare", "duration_min"]
    data = trips.where(ROWS["fare"]).select(*cols).dropna()
    if data.count() < MIN_ROWS:
        print("pca              skipped (not enough rows)")
        return
    vec = VectorAssembler(inputCols=cols, outputCol="raw").transform(data)
    scaled = StandardScaler(inputCol="raw", outputCol="scaled", withMean=True, withStd=True).fit(vec).transform(vec)
    cumulative = list(accumulate(PCA(k=len(cols), inputCol="scaled", outputCol="pca").fit(scaled).explainedVariance.toArray()))
    k90 = next(i + 1 for i, v in enumerate(cumulative) if v >= 0.9 - 1e-12)
    half = cumulative[len(cols) // 2 - 1]
    rows += [("pca", "n_features", "test", float(len(cols))), ("pca", "components_for_90pct", "test", float(k90)),
             ("pca", "variance_at_half", "test", half)]
    print(f"pca              {k90} of {len(cols)} components for 90% variance; half the features explain {half:.3f}")


# --- main ------------------------------------------------------------------------------------------------------------

def main(argv=None):
    """Build the feature tables, run the selected models and append their metrics."""
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--batch-dir", help="batch store with silver/ and gold/ (default: ml.batch_dir)")
    ap.add_argument("--models", nargs="+", choices=list(MODELS) + UNSUPERVISED, help="subset of models (default: all)")
    args = ap.parse_args(argv)

    cfg = load_config()["ml"]
    base = Path(args.batch_dir or cfg["batch_dir"])
    wanted = set(args.models or list(MODELS) + UNSUPERVISED)
    if "anomaly" in wanted:
        wanted.add("fare")
    spark = build_spark({"shuffle_partitions": cfg["shuffle_partitions"]})
    spark.sparkContext.setLogLevel("WARN")
    gold, features_dir, out_dir = (lambda mart: spark.read.parquet(str(base / "gold" / mart))), base / "silver" / "ml_features", base / "gold"
    if not (base / "silver" / "trips").exists():
        print(f"no batch Silver in {base}; run batch_job.py first")
        return 1

    zh, zone_demand = zone_hour(gold("zone_od_daily"), cfg["holidays"])
    zh.write.mode("overwrite").parquet(str(features_dir / "zone_hour"))
    trips = trip_features(spark.read.parquet(str(base / "silver" / "trips")), zone_demand, cfg["holidays"],
                          cfg["sample_rows"], cfg["seed"])
    trips.write.mode("overwrite").parquet(str(features_dir / "trips"))
    tables = {"zone_hour": spark.read.parquet(str(features_dir / "zone_hour")),
              "trips": spark.read.parquet(str(features_dir / "trips")).cache()}

    rows, fare = [], None
    for name, spec in MODELS.items():
        if name in wanted:
            result = run_model(name, tables[spec["table"]], cfg, out_dir, rows)
            fare = result if name == "fare" else fare
    if "zone_clusters" in wanted:
        zone_clusters(gold, cfg, out_dir, rows)
    if "demand_patterns" in wanted:
        demand_patterns(gold, cfg, out_dir, rows)
    if "anomaly" in wanted and fare:
        detect_anomalies(fare, cfg, out_dir, rows)
    if "pca" in wanted:
        run_pca(tables["trips"], rows)

    run = (uuid.uuid4().hex[:12], datetime.now(timezone.utc))
    spark.createDataFrame([(m, k, s, float(v)) for m, k, s, v in rows], METRIC_SCHEMA).select(F.lit(run[0]).alias("run_id"), F.lit(run[1]).alias("run_ts"), "*") \
        .write.mode("append").parquet(str(out_dir / "ml_model_metrics"))
    print(f"run {run[0]}: {len(rows)} metric rows -> {out_dir / 'ml_model_metrics'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
