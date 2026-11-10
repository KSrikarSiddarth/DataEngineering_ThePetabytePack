# evaluation.md — Final Evaluation Targets

Numerical targets for streaming latency and model accuracy. **Status: proposed.** The streaming targets are not measured (no Kafka or Docker run yet). The model targets were measured once, on 2 real months (section 6); they are still proposals. The numbers come from the plan's settings and from general experience with NYC taxi data. They are a first draft to confirm, then set for good after the first real run, so that they are neither trivial nor impossible.

Related: `PLAN.md` (milestones M4, M6, M8), `specs/3_KPI.html` (model KPIs), `decisions.md`.

## 1. Reference setup

All streaming targets apply to this setup. Change any item and the targets must be re-checked.

| Item | Value |
|---|---|
| Machine | Docker with 4 CPUs and 8 GB memory (the current Azure VM cannot run Docker, so this needs another machine) |
| Spark | Local mode, trigger `processing_time` 10 s, shuffle partitions about 8 |
| Load | `REPLAY_SPEEDUP=3600`, `QUALITY_PROFILE=realistic`, HVFHV capped at 500,000 records |
| Volume | About 5M messages in about 12 minutes, roughly 7,000 messages per second |
| Stream month | 2026-01 (all four datasets) |

If the machine cannot keep up, lower `REPLAY_SPEEDUP`. The latency targets stay the same; the throughput target scales with the load.

## 2. Streaming targets

| # | Metric | Target | How it is measured |
|---|---|---|---|
| S1 | End-to-end latency to Silver (Kafka append to Silver write) | p50 ≤ 20 s, p95 ≤ 45 s, max ≤ 90 s | Silver carries `kafka_timestamp` and `silver_write_ts`; the evaluation script computes the difference per row |
| S2 | Keeping up with the load | Micro-batch duration below 10 s in at least 95% of batches; Kafka lag back to zero within 60 s after the producers finish | Spark query progress (processed rate vs input rate); `kafka-get-offsets` against the committed offsets |
| S3 | Sustained throughput | At least 7,000 messages per second averaged over the run | Spark query progress, or total messages divided by run time |
| S4 | Gold freshness | A daily mart row is written no more than 30 s after the watermark passes its window | Window end plus watermark compared with the file write time |
| S5 | Restart from checkpoint | Resumes within 60 s; loss = 0 and duplicates = 0 | The M8 restart test, comparing counts with a clean run |
| S6 | Fault handling | 100% of injected invalid and malformed rows flagged or rejected with the correct reason; at least 99% of duplicates that arrive within the watermark removed | The fault-accounting checks (`reconcile.py` and the M3 tests, extended in M4) |
| S7 | Memory | 6 GB or less across all containers | `docker stats` sampled during the run |

**Notes**
- The latency measures the pipeline on replayed data, not real-world taxi latency.
- The watermark is event time (10 minutes). At `REPLAY_SPEEDUP=3600` it is only about 0.2 s of real time, so S4 is dominated by the trigger interval and batch duration.
- Latency cannot be measured today: Silver has no ingest or write timestamps. M4 adds `kafka_timestamp` (from the Kafka source's `timestamp` column) and `silver_write_ts` to `silver.trips`.

## 3. Model accuracy targets

For every model the main target is beating a simple baseline, because the right absolute number depends on the data. The absolute figures are rough expectations, not measurements. Where they conflict, the baseline comparison wins.

| # | Model (M6) | Primary metric | Target | Baseline it must beat |
|---|---|---|---|---|
| M1 | Demand forecast (trips per zone-hour) | WAPE | At most 25% overall, and lift of at least 10% | Same hour last week |
| M2 | Fare (Yellow, Green) | MAE and R² | MAE at most $2.50 and R² at least 0.90 | Linear fit on distance only |
| M3 | Trip duration / ETA | MAE and share within ±10% | MAE at most 4 min, and at least 50% of trips within ±10% | Mean duration per zone pair and hour |
| M4 | Tip of 20% or more (card payments only) | AUC-ROC | At least 0.70 | Majority class (AUC 0.5) |
| M5 | Wait time (HVFHV) | MAE | At most 150 s, and lift of at least 10% | Mean wait per zone and hour |
| M6 | Shared-ride match (HVFHV) | AUC-ROC | At least 0.70 | Overall match rate |
| M7 | Zone clusters | Silhouette; stability | At least 0.25; adjusted Rand index of at least 0.8 across seeds | A single cluster |
| M8 | Demand-pattern clusters | Silhouette | At least 0.25 | A single cluster |
| M9 | Anomaly detection | Recall and precision on injected bad trips; flagged rate | Both at least 80%; flagged rate 0.5% to 2% | Fixed rule (speed above 80 mph) |
| M10 | PCA | Variance explained | At least 90% with at most half the features | None |

**Model-operations targets** (catalog group "Model operations"):

| # | Metric | Target |
|---|---|---|
| O1 | Cross-validation spread (standard deviation of the metric across folds) | At most 5% of the metric |
| O2 | Reproducibility | The same seed gives the same metrics |
| O3 | Error drift from the first to the last scoring month | At most 15% worse |
| O4 | No leakage | No post-trip column (dropoff time, total amount, tip) in a model's features where it would leak the label |

**Notes**
- The fare, duration, tip and wait-time numbers come from general experience with this kind of data. The tip (M4) and shared-match (M6) targets are the least certain and may need lowering.
- Anomaly detection (M9) is scored on synthetic bad trips injected into a test set, because the batch data has no ground truth for what is anomalous.
- FHV has no fares or distance, so it is excluded from M2 and M3. Models M5 and M6 use HVFHV only.
- Trip-level models train on a configurable sample (for example 5M rows), so the figures describe the sample's held-out part.

## 4. How the evaluation is run

1. **Streaming:** run the reference load, then `scripts/evaluate_stream.py` reads the Spark progress log and Silver, and prints each metric against its target with pass or fail.
2. **Models:** the M6 job writes every model's metrics to `gold.ml_model_metrics`, and `scripts/evaluate_models.py` prints them against the targets.
3. **Report:** a single table in the final README of target, measured value and status. A missed target is reported as missed, with the measured value, not removed.

Both scripts are written in the milestone that produces the data (M4 for streaming, M6 for models).

## 5. Open points

| # | Question |
|---|---|
| E1 | Confirm or adjust the numbers above (they are proposals). |
| E2 | Is 4 CPUs and 8 GB the right reference machine? A smaller machine means lower speedup or looser latency. |
| E3 | After the first Docker run, replace S1 to S4 with values based on measurement. After the first model run, replace the absolute figures in M2 to M6. |
| E4 | Whether the streaming targets should also include a lighter reference load for development machines. |

## 6. First model run (2026-11-10)

2025-12 and 2026-01, 300,000-trip sample per service, first 400,000 HVFHV trips of 2026-01, random 20% holdout (D32). `scripts/evaluate_models.py` output:

| # | Measured | Target | Status |
|---|---|---|---|
| M1 demand | WAPE 0.239, lift +41% | WAPE <= 0.25, lift >= 10% | met |
| M2 fare | MAE $3.99, R² 0.80 | MAE <= $2.50, R² >= 0.90 | missed |
| M3 duration | MAE 4.2 min, 26% within ±10% | MAE <= 4, >= 50% | missed |
| M4 tip | AUC 0.646 | >= 0.70 | missed |
| M5 wait | MAE 178 s, lift +2% | <= 150 s, lift >= 10% | missed |
| M6 shared match | AUC 0.682 | >= 0.70 | missed |
| M7 zones | silhouette 0.47, stability ARI 0.67 | >= 0.25, >= 0.8 | silhouette met, stability missed |
| M8 patterns | silhouette 0.60 | >= 0.25 | met |
| M9 anomalies | recall 0.86, precision 0.81, flagged 1.05% | >= 80%, 0.5% to 2% | met |
| M10 PCA | 68.7% with half the features (11 of 14 components for 90%) | >= 90% with half | missed |
| O1 | 0.2% to 1.8% | <= 5% | met |
| O3 | demand +8%, others lower | <= 15% worse | met |

Every supervised model beats its baseline. The wait-time model barely does (+2%): pickup zone, hour and company carry little signal. The tip and shared-match targets were flagged above as the least certain. Per E3, the absolute figures should be revisited after a run on the full range; a temporal split would make the demand and drift figures stricter.

## Change log

| Date | Change |
|---|---|
| 2026-11-05 | Created with proposed targets. |
| 2026-11-10 | Added section 6: first model run on 2 real months. |
