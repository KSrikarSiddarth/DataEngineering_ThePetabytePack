"""Step 9 - next-hour zone demand forecasting with Spark ML (+ naive baselines) and evaluation."""
from __future__ import annotations

import json

import pandas as pd
from pyspark import StorageLevel
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import Imputer, OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.regression import GBTRegressor, LinearRegression, RandomForestRegressor
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ..common.config import Config
from ..common.storage import write_parquet_pandas
from ..common.utils import Timer, get_logger, read_parquet, record_measurements, write_parquet
from .metrics import grouped_regression_metrics, poisson_floor_by_group

log = get_logger(__name__)

WEATHER_COLS = ["temp_c", "precip_mm", "wind_mps", "visibility_m"]
CATEGORICAL = ["borough_idx", "target_hour_of_day", "target_day_of_week"]
KEY_COLS = ["zone_id", "borough", "window_start", "target_hour", "target_hour_of_day", "target_day_of_week",
            "is_holiday", "split"]
BASELINES = ["persistence", "seasonal_naive"]
MAJOR_BOROUGHS = ["Manhattan", "Brooklyn", "Queens", "Bronx"]


def numeric_features(cfg: Config) -> list[str]:
    f = cfg.section("features")
    return (["trip_count", "dropoff_count"] + [f"lag_{n}" for n in f["lags"]]
            + [f"roll_mean_{k}" for k in f["rolling_windows"]]
            + ["target_hour_last_week", "target_hour_yesterday", "target_hour_avg_2w", "diff_day",
               "zone_how_mean", "zone_mean", "target_hour_of_day", "target_day_of_week", "is_weekend",
               "is_holiday", "is_raining_int"])


def add_profile_features(df: DataFrame) -> DataFrame:
    """Zone x target-hour-of-week mean label from the TRAIN split only (leave-one-out on train rows)."""
    train = df.where("split = 'train'")
    keys = ["zone_id", "target_day_of_week", "target_hour_of_day"]
    how = train.groupBy(*keys).agg(F.sum("label").alias("_how_sum"), F.count(F.lit(1)).alias("_how_n"))
    zone = train.groupBy("zone_id").agg(F.avg("label").alias("zone_mean"))
    is_train = F.col("split") == "train"
    loo_sum = F.when(is_train, F.col("_how_sum") - F.col("label")).otherwise(F.col("_how_sum"))
    loo_n = F.when(is_train, F.col("_how_n") - 1).otherwise(F.col("_how_n"))
    return (df.join(F.broadcast(how), keys, "left").join(F.broadcast(zone), "zone_id", "left")
            .withColumn("zone_how_mean", F.when(loo_n > 0, loo_sum / loo_n).otherwise(F.col("zone_mean")))
            .drop("_how_sum", "_how_n"))


def build_regressor(name: str, mcfg: dict, label: str):
    common = {"featuresCol": "features", "labelCol": label, "predictionCol": "prediction"}
    if name == "linear_regression":
        p = mcfg["linear_regression"]
        return LinearRegression(regParam=p["reg_param"], elasticNetParam=p["elastic_net"], maxIter=100, **common)
    if name == "random_forest":
        p = mcfg["random_forest"]
        return RandomForestRegressor(numTrees=p["num_trees"], maxDepth=p["max_depth"], seed=mcfg["seed"],
                                     subsamplingRate=0.8, **common)
    if name == "gbt":
        p = mcfg["gbt"]
        return GBTRegressor(maxIter=p["max_iter"], maxDepth=p["max_depth"], stepSize=p["step_size"],
                            subsamplingRate=p.get("subsampling_rate", 1.0), seed=mcfg["seed"], **common)
    raise ValueError(f"Unknown model {name}")


def uses_log_target(cfg: Config, name: str) -> bool:
    return name in (cfg.section("ml").get("log_target_models") or [])


def build_pipeline(cfg: Config, name: str) -> Pipeline:
    mcfg = cfg.section("ml")
    imputed = [f"{c}_imp" for c in WEATHER_COLS]
    ohe_out = [f"{c}_ohe" for c in CATEGORICAL]
    return Pipeline(stages=[
        StringIndexer(inputCol="borough", outputCol="borough_idx", handleInvalid="keep"),
        OneHotEncoder(inputCols=CATEGORICAL, outputCols=ohe_out, handleInvalid="keep"),
        Imputer(inputCols=WEATHER_COLS, outputCols=imputed, strategy="median"),
        VectorAssembler(inputCols=numeric_features(cfg) + imputed + ohe_out, outputCol="features",
                        handleInvalid="keep"),
        build_regressor(name, mcfg, "label_log" if uses_log_target(cfg, name) else "label"),
    ])


def load_features(spark: SparkSession, cfg: Config) -> DataFrame:
    df = read_parquet(spark, cfg.path("silver") / "zone_hour_features", "features")
    for c in WEATHER_COLS:
        df = df.withColumn(c, F.col(c).cast("double"))
    df = df.withColumn("label", F.col(cfg.section("ml")["label_col"]).cast("double"))
    return add_profile_features(df).withColumn("label_log", F.log1p("label"))


def baseline_predictions(df: DataFrame) -> DataFrame:
    keys = [F.col(c) for c in KEY_COLS]
    persistence = df.select(*keys, F.lit("persistence").alias("model"), F.col("label").alias("actual"),
                            F.col("trip_count").cast("double").alias("predicted"))
    seasonal = df.select(*keys, F.lit("seasonal_naive").alias("model"), F.col("label").alias("actual"),
                         F.col("target_hour_last_week").cast("double").alias("predicted"))
    return persistence.unionByName(seasonal)


def ensemble_predictions(model_preds: list[DataFrame]) -> tuple[DataFrame, dict]:
    """Blend the two models with the lowest validation MAE; the weight is chosen on validation only."""
    preds = model_preds[0]
    for p in model_preds[1:]:
        preds = preds.unionByName(p)
    val = preds.where("split = 'validation'")
    ranked = val.groupBy("model").agg(F.avg(F.abs(F.col("predicted") - F.col("actual"))).alias("mae")) \
        .orderBy("mae").limit(2).collect()
    a, b = ranked[0]["model"], ranked[1]["model"]
    keys = ["zone_id", "window_start"]
    pa = preds.where(F.col("model") == a).withColumnRenamed("predicted", "pa").drop("model")
    pb = preds.where(F.col("model") == b).select(*keys, F.col("predicted").alias("pb"))
    joined = pa.join(pb, keys)
    weights = [i / 10 for i in range(11)]
    row = joined.where("split = 'validation'").agg(*[
        F.avg(F.abs(F.lit(w) * F.col("pa") + F.lit(1 - w) * F.col("pb") - F.col("actual"))).alias(f"w{i}")
        for i, w in enumerate(weights)]).first()
    best_i = min(range(len(weights)), key=lambda i: row[f"w{i}"])
    w = weights[best_i]
    log.info("ensemble: %.1f x %s + %.1f x %s (validation MAE %.3f)", w, a, 1 - w, b, row[f"w{best_i}"])
    ens = joined.withColumn("predicted", F.lit(w) * F.col("pa") + F.lit(1 - w) * F.col("pb")) \
        .withColumn("model", F.lit("ensemble")).drop("pa", "pb")
    return ens.select(*KEY_COLS, "model", "actual", "predicted"), {"members": [a, b], "weight": w}


def tuning_enabled(cfg: Config) -> bool:
    return bool((cfg.section("ml").get("tuning") or {}).get("enabled"))


def validation_mae(cfg: Config, model: PipelineModel, name: str, validation: DataFrame) -> float:
    pred = F.expm1("prediction") if uses_log_target(cfg, name) else F.col("prediction")
    return float(model.transform(validation).select(
        F.avg(F.abs(F.greatest(pred, F.lit(0.0)) - F.col("label")))).first()[0])


def tune_gbt(cfg: Config, train: DataFrame, validation: DataFrame) -> tuple[PipelineModel, dict]:
    """Validation-only grid search: fit each configured candidate on train, keep the lowest validation MAE.

    The test split is never touched. Returns the winning fitted model plus a record of every trial.
    """
    grid = (cfg.section("ml").get("tuning") or {}).get("gbt_grid") or []
    if not grid:
        raise ValueError("ml.tuning.enabled is true but ml.tuning.gbt_grid is empty")
    trials, best = [], None
    for params in grid:
        trial_cfg = cfg.with_overrides({"ml": {"gbt": params}})
        with Timer() as t:
            model = build_pipeline(trial_cfg, "gbt").fit(train)
        mae = validation_mae(trial_cfg, model, "gbt", validation)
        trials.append({**params, "validation_mae": mae, "fit_seconds": t.seconds})
        log.info("gbt tuning %s: validation MAE %.4f (fit %.1fs)", params, mae, t.seconds)
        if best is None or mae < best[0]:
            best = (mae, model, len(trials) - 1)
    winner = trials[best[2]]
    return best[1], {"best_params": {k: winner[k] for k in grid[0]}, "best_validation_mae": best[0],
                     "best_fit_seconds": winner["fit_seconds"], "trials": trials,
                     "tuning_seconds": sum(t["fit_seconds"] for t in trials)}


def segment_column(peak_hours: list[int]) -> F.Column:
    peak = (F.col("target_day_of_week") <= 5) & (F.col("is_holiday") == 0) & F.col("target_hour_of_day").isin(peak_hours)
    return F.explode(F.array(
        F.lit("overall"),
        F.concat(F.lit("borough="), F.coalesce(F.col("borough"), F.lit("Unknown"))),
        F.when(peak, "peak_weekday").otherwise("off_peak"),
        F.when(F.col("target_day_of_week") >= 6, "weekend").otherwise("weekday"),
    ))


def train_and_evaluate(spark: SparkSession, cfg: Config) -> dict:
    mcfg = cfg.section("ml")
    feats = load_features(spark, cfg).persist(StorageLevel.MEMORY_AND_DISK)
    train = feats.where("split = 'train'")
    evals = feats.where("split IN ('validation', 'test')")
    test = feats.where("split = 'test'")
    log.info("training rows=%d evaluation rows=%d", train.count(), evals.count())

    predictions = [baseline_predictions(evals)]
    timings: dict[str, dict] = {}
    for name in mcfg["models"]:
        tuning = None
        if name == "gbt" and tuning_enabled(cfg):
            model, tuning = tune_gbt(cfg, train, feats.where("split = 'validation'"))
            fit_seconds = tuning["best_fit_seconds"]  # O1 is about training the chosen model; tuning cost is separate
        else:
            with Timer() as t_fit:
                model = build_pipeline(cfg, name).fit(train)
            fit_seconds = t_fit.seconds
        with Timer() as t_inf:
            model.transform(test).write.format("noop").mode("overwrite").save()
        n_test = test.count()
        timings[name] = {"train_seconds": fit_seconds, "infer_rows_per_sec": n_test / max(t_inf.seconds, 1e-9)}
        if tuning:
            timings[name]["tuning"] = tuning
        model.write().overwrite().save(str(cfg.path("models") / name))
        log.info("%s: fit %.1fs, inference %.0f rows/s", name, fit_seconds, timings[name]["infer_rows_per_sec"])
        pred = F.expm1("prediction") if uses_log_target(cfg, name) else F.col("prediction")
        predictions.append(model.transform(evals).select(
            *[F.col(c) for c in KEY_COLS], F.lit(name).alias("model"), F.col("label").alias("actual"),
            F.greatest(pred, F.lit(0.0)).alias("predicted")))

    if mcfg.get("ensemble") and len(predictions) >= 3:
        ens, info = ensemble_predictions(predictions[1:])
        predictions.append(ens)
        timings["ensemble"] = {
            "train_seconds": sum(timings[m]["train_seconds"] for m in info["members"]),
            "infer_rows_per_sec": 1 / sum(1 / timings[m]["infer_rows_per_sec"] for m in info["members"]),
            **info}

    forecasts = predictions[0]
    for p in predictions[1:]:
        forecasts = forecasts.unionByName(p)
    forecasts = forecasts.withColumn("residual", F.col("actual") - F.col("predicted"))
    fc_path = cfg.path("gold") / "forecasts"
    write_parquet(forecasts, fc_path, partition_by=["model"], replace_all=True)
    forecasts = spark.read.parquet(str(fc_path))

    metrics = grouped_regression_metrics(
        forecasts.withColumn("segment", segment_column(mcfg["peak_hours"])), ["model", "split", "segment"],
        mape_min=mcfg["mape_min_actual"]).toPandas()
    metrics["train_seconds"] = metrics["model"].map(lambda m: timings.get(m, {}).get("train_seconds"))
    metrics["infer_rows_per_sec"] = metrics["model"].map(lambda m: timings.get(m, {}).get("infer_rows_per_sec"))
    out = cfg.path("gold") / "model_metrics"
    write_parquet_pandas(metrics, out / "model_metrics.parquet")
    out_summary = summarise(cfg, metrics, timings, forecasts)
    feats.unpersist()
    return out_summary


def summarise(cfg: Config, metrics: pd.DataFrame, timings: dict, forecasts: DataFrame | None = None) -> dict:
    overall = metrics[metrics["segment"] == "overall"].set_index(["model", "split"])
    ml_models = [m for m in [*cfg.section("ml")["models"], "ensemble"] if (m, "validation") in overall.index]
    best = min(ml_models, key=lambda m: overall.loc[(m, "validation"), "mae"])
    (cfg.path("models")).mkdir(parents=True, exist_ok=True)
    (cfg.path("models") / "best_model.json").write_text(json.dumps({"model": best}), encoding="utf-8")

    def test_metric(model: str, metric: str):
        return float(overall.loc[(model, "test"), metric]) if (model, "test") in overall.index else None

    def improvement(baseline: str):
        b, m = test_metric(baseline, "mae"), test_metric(best, "mae")
        return None if not b or m is None else 100 * (b - m) / b

    test_seg = metrics[(metrics["model"] == best) & (metrics["split"] == "test")].set_index("segment")
    boroughs = [f"borough={b}" for b in MAJOR_BOROUGHS if f"borough={b}" in test_seg.index]
    values = {f"ml.best_{k}": test_metric(best, k) for k in ["mae", "rmse", "wape_pct", "mape_pct", "r2"]}
    values.update({
        "ml.mae_improvement_vs_seasonal_pct": improvement("seasonal_naive"),
        "ml.mae_improvement_vs_persistence_pct": improvement("persistence"),
        "ml.worst_borough_wape_pct": max(test_seg.loc[boroughs, "wape_pct"]) if boroughs else None,
        "ml.peak_wape_pct": float(test_seg.loc["peak_weekday", "wape_pct"]) if "peak_weekday" in test_seg.index else None,
        "ml.inference_rows_per_s": timings[best]["infer_rows_per_sec"],
    })
    if "gbt" in timings:
        values["ml.gbt_train_min"] = timings["gbt"]["train_seconds"] / 60
    if "gbt" in ml_models and "linear_regression" in ml_models:
        lr, gbt = test_metric("linear_regression", "mae"), test_metric("gbt", "mae")
        values["ml.gbt_vs_lr_mae_improvement_pct"] = 100 * (lr - gbt) / lr if lr else None

    context = {"best_model": best}
    # Diagnostic for M8: how much of the worst borough's WAPE is just Poisson count noise (see poisson_mad)?
    if forecasts is not None and boroughs:
        floor = poisson_floor_by_group(forecasts.where((F.col("model") == best) & (F.col("split") == "test")),
                                       "borough").toPandas().set_index("borough")
        worst = max(boroughs, key=lambda b: test_seg.loc[b, "wape_pct"]).removeprefix("borough=")
        if worst in floor.index:
            values["ml.worst_borough_floor_wape_pct"] = float(floor.loc[worst, "floor_wape_pct"])
            context.update(worst_borough=worst, worst_borough_mean_trips=float(floor.loc[worst, "mean_actual"]))
    if "gbt" in timings and "tuning" in timings["gbt"]:
        tuning = timings["gbt"]["tuning"]
        values["ml.gbt_tuning_min"] = tuning["tuning_seconds"] / 60
        context.update(gbt_params=tuning["best_params"], gbt_trials=len(tuning["trials"]))
    record_measurements(cfg, values, context)
    log.info("best model=%s test metrics=%s", best,
             {k: round(v, 3) for k, v in values.items() if isinstance(v, float)})
    return {"best_model": best, **values}


def load_best_model_name(cfg: Config) -> str:
    return json.loads((cfg.path("models") / "best_model.json").read_text(encoding="utf-8"))["model"]


def load_model(cfg: Config, name: str) -> PipelineModel:
    return PipelineModel.load(str(cfg.path("models") / name))
