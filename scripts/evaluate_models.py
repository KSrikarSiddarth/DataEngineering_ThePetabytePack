"""Compare the latest Spark ML run (gold/ml_model_metrics) with the proposed targets in evaluation.md.

    python scripts/evaluate_models.py [--batch-dir output/batch]

Prints every model target (M1 to M10) and model-operations target (O1 cross-validation spread, O3 error drift)
as PASS or FAIL with the measured value, then each model's lift over its baseline (reported whether positive or
not). O2 (same seed, same metrics) and O4 (no leakage) are checked by tests/test_ml.py. A target whose metric
was not produced (model skipped) is shown as n/a. Exit code 1 if a measured target is missed.
"""
import argparse
import sys
from pathlib import Path

import pyarrow.parquet as pq

# evaluation.md section 3 (proposed values): (id, model, metric, direction, target); direction ">=" or "<="
TARGETS = [
    ("M1", "demand_forecast", "wape", "<=", 0.25), ("M1", "demand_forecast", "lift", ">=", 0.10),
    ("M2", "fare", "mae", "<=", 2.50), ("M2", "fare", "r2", ">=", 0.90),
    ("M3", "duration", "mae", "<=", 4.0), ("M3", "duration", "within_10pct", ">=", 0.50),
    ("M4", "tip", "auc", ">=", 0.70),
    ("M5", "wait", "mae", "<=", 150.0), ("M5", "wait", "lift", ">=", 0.10),
    ("M6", "shared_match", "auc", ">=", 0.70),
    ("M7", "zone_clusters", "silhouette", ">=", 0.25), ("M7", "zone_clusters", "stability_ari", ">=", 0.8),
    ("M8", "demand_patterns", "silhouette", ">=", 0.25),
    ("M9", "anomaly", "recall", ">=", 0.80), ("M9", "anomaly", "precision", ">=", 0.80),
    ("M9", "anomaly", "flagged_rate", ">=", 0.005), ("M9", "anomaly", "flagged_rate", "<=", 0.02),
    ("M10", "pca", "variance_at_half", ">=", 0.90),
]
O1_MAX_SPREAD = 0.05      # cross-validation std as a share of the metric
O3_MAX_DRIFT = 0.15       # error of the last month vs the first month


def latest_metrics(batch_dir):
    """{(model, metric, scope): value} of the most recent run in gold/ml_model_metrics."""
    path = Path(batch_dir) / "gold" / "ml_model_metrics"
    if not path.exists():
        sys.exit(f"no {path}: run ml_job.py first")
    rows = pq.read_table(path).to_pylist()
    run = max(rows, key=lambda r: r["run_ts"])["run_id"]
    return {(r["model"], r["metric"], r["scope"]): r["value"] for r in rows if r["run_id"] == run}, run


def month_drift(metrics, model):
    """(first-month error, last-month error, relative change) of the model's primary metric; None for one month."""
    months = sorted((s[6:], m, v) for (mod, m, s), v in metrics.items() if mod == model and s.startswith("month:"))
    if len(months) < 2:
        return None
    err = (lambda v: 1 - v) if months[0][1] == "auc" else (lambda v: v)
    first, last = err(months[0][2]), err(months[-1][2])
    return first, last, (last - first) / first if first else 0.0


def evaluate(metrics):
    """[(target name, measured value as text, True/False/None)] for every target of `metrics` (None = not measured):
    the M targets, then O1 and O3 per model."""
    results = []
    for tid, model, metric, op, target in TARGETS:
        v = metrics.get((model, metric, "test"))
        ok = None if v is None else (v >= target if op == ">=" else v <= target)
        results.append((f"{tid} {model} {metric} {op} {target:g}", "n/a" if v is None else f"{v:.4f}", ok))
    models = sorted({m for m, _, _ in metrics})
    for model in models:
        mean_, std = metrics.get((model, "cv_mean", "cv")), metrics.get((model, "cv_std", "cv"))
        if mean_:
            results.append((f"O1 {model} cv spread <= {O1_MAX_SPREAD:.0%}", f"{std / abs(mean_):.1%}",
                            std / abs(mean_) <= O1_MAX_SPREAD))
        drift = month_drift(metrics, model)
        if drift:
            results.append((f"O3 {model} error drift <= {O3_MAX_DRIFT:.0%}", f"{drift[2]:+.1%} ({drift[0]:.4f} -> {drift[1]:.4f})",
                            drift[2] <= O3_MAX_DRIFT))
    return results


def main():
    """Print the evaluation table; return 1 when any measured target fails."""
    ap = argparse.ArgumentParser()
    ap.add_argument("--batch-dir", default="output/batch")
    metrics, run = latest_metrics(ap.parse_args().batch_dir)
    print(f"run {run}")

    models = sorted({m for m, _, _ in metrics})
    results = evaluate(metrics)
    for name, value, ok in results:
        print(f"{'n/a ' if ok is None else 'PASS' if ok else 'FAIL'}  {name:46} measured {value}")

    print("\nlift over baseline (positive = the model is better):")
    for model in models:
        if (model, "lift", "test") in metrics:
            print(f"  {model:16} {metrics[(model, 'lift', 'test')]:+.1%}")
    print("checked by tests/test_ml.py: O2 (same seed, same metrics), O4 (no leakage)")
    return 0 if all(ok is not False for *_, ok in results) else 1


if __name__ == "__main__":
    sys.exit(main())
