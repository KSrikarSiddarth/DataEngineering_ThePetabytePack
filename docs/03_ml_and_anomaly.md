# 03 · ML and anomaly detection

## 1. Forecasting task

Predict `label_next_hour` = pickups in zone *z* during hour *t+1*, using only information available at the end
of hour *t*.

| Group | Features |
|---|---|
| Current | `trip_count`, `dropoff_count` |
| History | `lag_1`, `lag_2`, `lag_3`, `lag_23`, `lag_24`, `lag_168`, `roll_mean_3`, `roll_mean_24`, `roll_mean_168`, `diff_day` (= *t* − *t−24h*) |
| Seasonal (target hour) | `target_hour_last_week` (*t+1−168h*), `target_hour_yesterday` (*t+1−24h*), `target_hour_avg_2w` (mean of the last two weeks at that hour) |
| Zone profile | `zone_how_mean` = mean label of the zone at that hour-of-week and `zone_mean`, both computed from the **train split only** (leave-one-out on train rows) |
| Calendar (of target hour) | `target_hour_of_day`, `target_day_of_week` (numeric + one-hot), `is_weekend`, `is_holiday` |
| Weather (observed at *t*) | `temp_c`, `precip_mm`, `wind_mps`, `visibility_m` (median-imputed), `is_raining_int` |
| Location | `borough` (StringIndexer + one-hot) |

Leakage controls: rolling windows end at *t*; weather is observed, not future; the split is by time; the
Imputer, indexers and the zone profile are fitted on the training split only (profile uses leave-one-out so a
train row never sees its own label — covered by a unit test).

**Eligibility:** zones with fewer than `features.min_zone_trips` (500) trips in the range are excluded — percentage
errors on near-empty zones are meaningless. The list is saved to `silver/eligible_zones`.

**Split:** last `ml.test_days` (14) = test; previous `ml.validation_days` (7) = validation; rest = train. The
first 168 hours per zone are dropped (no `lag_168`).

## 2. Models

| Model | Role | Config |
|---|---|---|
| `persistence` | naive baseline: ŷ = `trip_count(t)` | — |
| `seasonal_naive` | naive baseline: ŷ = `trip_count(t+1−168h)` | — |
| `linear_regression` | interpretable baseline | `reg_param` 0.01 |
| `random_forest` | non-linear, trained on log1p(label) | 60 trees, depth 10 |
| `gbt` | non-linear, trained on log1p(label) | 150 iterations, depth 8, step 0.1, subsampling 0.8 |
| `ensemble` | w · best + (1 − w) · second-best, w ∈ {0, 0.1, …, 1} chosen on validation | `ml.ensemble: true` |

Log targets (`ml.log_target_models`) stabilise errors that grow with volume; predictions are `expm1`-transformed
back. Best model = lowest **validation** MAE among the Spark ML models and the ensemble; test metrics of that
model are the reported numbers (no test-set selection). Predictions are clipped at 0.

### Improvement log (real data 2024-01..03, test = last 14 days of March)

| Version | Selected model | Test MAE | RMSE | WAPE | vs seasonal-naive | vs persistence |
|---|---|---|---|---|---|---|
| v1: base features, GBT 60×6 | linear regression | 5.46 | 13.01 | 18.9 % | 8.1 % | 29.5 % |
| v2: + seasonal/profile features, log-target trees, GBT 150×8, ensemble | ensemble 0.7 LR + 0.3 GBT | **4.76** | **12.11** | **16.5 %** | **19.9 %** | **38.6 %** |
| v2b: GBT on raw target (rejected: worse validation MAE 4.586 vs 4.547) | ensemble 0.8 LR + 0.2 RF | 4.79 | 12.04 | 16.6 % | 19.5 % | 38.2 % |

Remaining gap: the four major-borough WAPE target (M8) is dominated by the Bronx, where yellow-taxi demand is
≈ 0.6 trips per zone-hour, so relative errors stay > 100 % even with MAE < 1 trip.

**Metrics** (`ml/metrics.py`, computed as Spark aggregations): MAE, RMSE, WAPE = Σ|e| / Σy (robust to zero
hours), MAPE on rows with y ≥ 10, R². Segments: overall, each borough, weekday peak (07–09, 16–19) vs off-peak,
weekday vs weekend. Output: `gold/forecasts` (partitioned by model), `gold/model_metrics/model_metrics.parquet`,
saved PipelineModels in `models/<name>`, `models/best_model.json`.

## 3. Anomaly detection

Scored on the test period with the best model's forecasts.

| Detector | Definition | Threshold |
|---|---|---|
| Residual (main) | scaled residual r = (actual − forecast) / √(forecast + 1) (count noise grows with volume); z = (r − mean_val) / max(sd_val, 1) per zone, statistics from the validation period | \|z\| ≥ max(3.0, 99.5th percentile of validation \|z\|) — calibrated to a 0.5 % alert budget (`anomaly.residual_alert_rate`); 3.25 on real data |
| MAD rule labels | robust z = (y − median) / (1.4826 · MAD) vs same zone × hour-of-week history before the test period | \|z\| ≥ 3.5 and median ≥ 5 |
| KMeans | distance to nearest of k = 8 centres in (log1p actual, log1p predicted, residual z) | > 99.5th percentile of validation distances |

Severity from max(\|residual z\|, \|MAD z\|): low 3–4, medium 4–6, high > 6. Direction: spike / drop.

**Evaluation**
1. *Injected ground truth:* 2 % of test zone-hours with forecast ≥ 20 receive a spike (actual = 3 × max(actual,
   forecast)) or a drop (0.2 × min(actual, forecast)), seed 7. Precision/recall/F1 are computed over that eligible
   population (targets A1–A3).
2. *Rule agreement:* residual detector vs MAD labels on real data (A4).
3. *Operator load:* share of all test zone-hours flagged by the residual detector without injection (A5).

Outputs: `gold/anomalies` (real data only, with zone, weather, severity, detectors fired) and
`gold/anomaly_metrics/anomaly_metrics.parquet`.

| Version (real data) | Injected P / R / F1 | Rule P / R | Alert rate |
|---|---|---|---|
| v1: raw residual z, fixed threshold 3.0 | 0.48 / 0.85 / 0.62 | 0.28 / 0.07 | 1.75 % |
| v2: volume-scaled residual, calibrated threshold, improved forecasts | **0.66 / 0.90 / 0.76** | 0.68 / 0.10 | **0.78 %** |
