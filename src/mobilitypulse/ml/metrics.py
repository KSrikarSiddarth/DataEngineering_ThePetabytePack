"""Distributed regression and classification metrics computed with Spark aggregations."""
from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

METRIC_NAMES = ["n_rows", "mae", "rmse", "wape_pct", "mape_pct", "r2"]


def regression_exprs(label: str = "actual", pred: str = "predicted", mape_min: float = 10) -> list[Column]:
    y, e = F.col(label), F.col(pred) - F.col(label)
    return [
        F.count(F.lit(1)).alias("n_rows"),
        F.avg(F.abs(e)).alias("mae"),
        F.sqrt(F.avg(e * e)).alias("rmse"),
        # WAPE is robust to the many zero-demand hours, unlike plain MAPE
        (100 * F.sum(F.abs(e)) / F.sum(y)).alias("wape_pct"),
        (100 * F.avg(F.when(y >= mape_min, F.abs(e) / y))).alias("mape_pct"),
        (1 - F.avg(e * e) / F.var_pop(y)).alias("r2"),
    ]


def regression_metrics(df: DataFrame, label: str = "actual", pred: str = "predicted", mape_min: float = 10) -> dict:
    row = df.agg(*regression_exprs(label, pred, mape_min)).first()
    return {k: (None if row[k] is None else float(row[k])) for k in METRIC_NAMES}


def grouped_regression_metrics(df: DataFrame, group_cols: list[str], label: str = "actual",
                               pred: str = "predicted", mape_min: float = 10) -> DataFrame:
    return df.groupBy(*group_cols).agg(*regression_exprs(label, pred, mape_min))


def binary_metrics(df: DataFrame, truth: str, flag: str) -> dict:
    row = df.agg(
        F.sum((F.col(truth) & F.col(flag)).cast("long")).alias("tp"),
        F.sum((~F.col(truth) & F.col(flag)).cast("long")).alias("fp"),
        F.sum((F.col(truth) & ~F.col(flag)).cast("long")).alias("fn"),
        F.count(F.lit(1)).alias("n"),
        F.sum(F.col(flag).cast("long")).alias("alerts"),
    ).first()
    tp, fp, fn = (row["tp"] or 0), (row["fp"] or 0), (row["fn"] or 0)
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    return {"tp": tp, "fp": fp, "fn": fn, "n": row["n"], "alerts": row["alerts"] or 0,
            "precision": precision, "recall": recall, "f1": f1,
            "alert_rate_pct": 100.0 * (row["alerts"] or 0) / max(row["n"], 1)}
