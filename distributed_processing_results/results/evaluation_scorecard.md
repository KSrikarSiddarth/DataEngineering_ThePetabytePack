# NYC MobilityPulse - Evaluation Scorecard

Generated 2026-11-17 18:54 UTC on SHSFT6MG22XFWM (Darwin arm64, Python 3.11.18).
Targets: `configs/targets.yaml` (fixed before results). Measurements: `results/measurements.json`.

**PASS 30 | FAIL 14 | NOT RUN 0 | INFO 1**

| ID | Area | Metric | Target | Measured | Status |
|---|---|---|---|---|---|
| S1 | Streaming | End-to-end latency p50 @ 2,000 ev/s | <= 5 s | 4.678 | **PASS** |
| S2 | Streaming | End-to-end latency p95 @ 2,000 ev/s | <= 10 s | 10.486 | **FAIL** |
| S3 | Streaming | End-to-end latency p99 @ 2,000 ev/s | <= 15 s | 21.703 | **FAIL** |
| S4 | Streaming | Micro-batches finishing within trigger interval | >= 95 % | 95.327 | **PASS** |
| S5 | Streaming | Max sustained processing rate (no lag growth) | >= 10000 ev/s | 5,000.0 | **FAIL** |
| S6 | Streaming | Kafka backlog p95 after warm-up @ 2,000 ev/s | <= 10000 events | 0.000 | **PASS** |
| S7 | Streaming | p95 latency reduction, 4 vs 1 Kafka partitions @ 10k ev/s | >= 30 % | -22.654 | **FAIL** |
| S8 | Streaming | Checkpoint recovery time (kill -> first committed batch) | <= 30 s | 9.759 | **PASS** |
| S9 | Streaming | Window counts match after restart | >= 100 % | 100.0 | **PASS** |
| S10 | Streaming | Injected malformed events routed to DLQ | >= 100 % | 100.0 | **PASS** |
| S10b | Streaming | Events unaccounted for (sent - processed - DLQ) | == 0 events | 0.000 | **PASS** |
| S11 | Streaming | Stream vs batch agreement on closed hourly windows | >= 100 % | 100.0 | **PASS** |
| M1a | Forecast | Best-model MAE improvement vs seasonal-naive | >= 20 % | 19.931 | **FAIL** |
| M1b | Forecast | Best-model MAE improvement vs persistence | >= 30 % | 38.565 | **PASS** |
| M2 | Forecast | MAE | <= 6.000 trips/zone-hour | 4.760 | **PASS** |
| M3 | Forecast | RMSE | <= 12.000 trips/zone-hour | 12.111 | **FAIL** |
| M4 | Forecast | WAPE | <= 20 % | 16.512 | **PASS** |
| M5 | Forecast | MAPE (actual >= 10) | <= 25 % | 19.704 | **PASS** |
| M6 | Forecast | R squared | >= 0.900 | 0.966 | **PASS** |
| M7 | Forecast | GBT MAE improvement vs Linear Regression | >= 10 % | -9.642 | **FAIL** |
| M8 | Forecast | Worst major-borough WAPE | <= 35 % | 111.9 | **FAIL** |
| M9 | Forecast | Weekday peak-hour WAPE | <= 22 % | 14.160 | **PASS** |
| A1 | Anomaly | Recall on injected anomalies (residual detector) | >= 0.800 | 0.901 | **PASS** |
| A2 | Anomaly | Precision on injected anomalies | >= 0.700 | 0.662 | **FAIL** |
| A3 | Anomaly | F1 on injected anomalies | >= 0.750 | 0.763 | **PASS** |
| A4a | Anomaly | Precision vs MAD rule labels (real data) | >= 0.600 | 0.685 | **PASS** |
| A4b | Anomaly | Recall vs MAD rule labels (real data) | >= 0.600 | 0.097 | **FAIL** |
| A5 | Anomaly | Alert rate on un-injected test data | <= 1.000 % | 0.779 | **PASS** |
| O1 | ML ops | GBT training time (medium) | <= 15 min | 2.231 | **PASS** |
| O2 | ML ops | Batch inference throughput | >= 100000 rows/s | 30,448.9 | **FAIL** |
| O3 | ML ops | Feature build time (medium) | <= 10 min | 0.057 | **PASS** |
| B1 | Batch | Small dataset bronze -> features wall-clock | <= 3 min | 0.277 | **PASS** |
| B2 | Batch | Bronze -> silver throughput (medium) | >= 250000 rows/s | 210,401.5 | **FAIL** |
| B3 | Batch | Worst data-scaling efficiency (S->M, M->L) | >= 0.750 | 0.849 | **PASS** |
| B4 | Batch | Broadcast vs sort-merge join speedup | >= 1.200 x | 1.771 | **PASS** |
| B5 | Batch | Cached vs uncached reuse speedup | >= 1.500 x | 0.197 | **FAIL** |
| B6 | Batch | Best shuffle-partition count (informational) | - | 64.000 | **INFO** |
| B7 | Batch | Parquet vs CSV scan speedup | >= 3 x | 2.866 | **FAIL** |
| D1 | Data quality | Max reconciliation diff (input - valid - rejected) | == 0 rows | 0.000 | **PASS** |
| D2 | Data quality | Max monthly rejection rate | <= 8 % | 4.771 | **PASS** |
| D3 | Data quality | Duplicate trips in silver | == 0 rows | 0.000 | **PASS** |
| D3b | Data quality | Zone-hour demand vs silver trip reconciliation diff | == 0 rows | 0.000 | **PASS** |
| D4 | Reproducibility | Fresh clone -> sample end-to-end | <= 30 min | 3.067 | **PASS** |
| D5a | Reproducibility | Test pass rate | >= 100 % | 100.0 | **PASS** |
| D5b | Reproducibility | Line coverage of src/ | >= 70 % | 81.898 | **PASS** |

Every FAIL must be explained in the report with the measured value, root cause (Spark UI / metrics evidence) and remediation attempted.
