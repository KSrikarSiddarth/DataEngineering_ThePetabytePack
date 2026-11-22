# NYC MobilityPulse - Evaluation Scorecard (two-year run, 2022-07..2024-06)

Generated 2026-11-18 08:22 UTC on SHSFT6MG22XFWM (Darwin arm64, Python 3.11.18).
Targets: `configs/targets.yaml` (fixed before results). Measurements: `results_two_years/measurements.json` (other machine, see run_log.txt).

**PASS 13 | FAIL 10 | NOT RUN 22 | INFO 0**

| ID | Area | Metric | Target | Measured | Status |
|---|---|---|---|---|---|
| S1 | Streaming | End-to-end latency p50 @ 2,000 ev/s | <= 5 s | - | **NOT RUN** |
| S2 | Streaming | End-to-end latency p95 @ 2,000 ev/s | <= 10 s | - | **NOT RUN** |
| S3 | Streaming | End-to-end latency p99 @ 2,000 ev/s | <= 15 s | - | **NOT RUN** |
| S4 | Streaming | Micro-batches finishing within trigger interval | >= 95 % | - | **NOT RUN** |
| S5 | Streaming | Max sustained processing rate (no lag growth) | >= 10000 ev/s | - | **NOT RUN** |
| S6 | Streaming | Kafka backlog p95 after warm-up @ 2,000 ev/s | <= 10000 events | - | **NOT RUN** |
| S7 | Streaming | p95 latency reduction, 4 vs 1 Kafka partitions @ 10k ev/s | >= 30 % | - | **NOT RUN** |
| S8 | Streaming | Checkpoint recovery time (kill -> first committed batch) | <= 30 s | - | **NOT RUN** |
| S9 | Streaming | Window counts match after restart | >= 100 % | - | **NOT RUN** |
| S10 | Streaming | Injected malformed events routed to DLQ | >= 100 % | - | **NOT RUN** |
| S10b | Streaming | Events unaccounted for (sent - processed - DLQ) | == 0 events | - | **NOT RUN** |
| S11 | Streaming | Stream vs batch agreement on closed hourly windows | >= 100 % | - | **NOT RUN** |
| M1a | Forecast | Best-model MAE improvement vs seasonal-naive | >= 20 % | 31.476 | **PASS** |
| M1b | Forecast | Best-model MAE improvement vs persistence | >= 30 % | 40.055 | **PASS** |
| M2 | Forecast | MAE | <= 6.000 trips/zone-hour | 6.573 | **FAIL** |
| M3 | Forecast | RMSE | <= 12.000 trips/zone-hour | 14.298 | **FAIL** |
| M4 | Forecast | WAPE | <= 20 % | 15.266 | **PASS** |
| M5 | Forecast | MAPE (actual >= 10) | <= 25 % | 19.504 | **PASS** |
| M6 | Forecast | R squared | >= 0.900 | 0.965 | **PASS** |
| M7 | Forecast | GBT MAE improvement vs Linear Regression | >= 10 % | 7.615 | **FAIL** |
| M8 | Forecast | Worst major-borough WAPE | <= 35 % | 81.852 | **FAIL** |
| M9 | Forecast | Weekday peak-hour WAPE | <= 22 % | 12.681 | **PASS** |
| A1 | Anomaly | Recall on injected anomalies (residual detector) | >= 0.800 | 0.894 | **PASS** |
| A2 | Anomaly | Precision on injected anomalies | >= 0.700 | 0.428 | **FAIL** |
| A3 | Anomaly | F1 on injected anomalies | >= 0.750 | 0.579 | **FAIL** |
| A4a | Anomaly | Precision vs MAD rule labels (real data) | >= 0.600 | 0.292 | **FAIL** |
| A4b | Anomaly | Recall vs MAD rule labels (real data) | >= 0.600 | 0.365 | **FAIL** |
| A5 | Anomaly | Alert rate on un-injected test data | <= 1.000 % | 1.718 | **FAIL** |
| O1 | ML ops | GBT training time (medium) | <= 15 min | 10.084 | **PASS** |
| O2 | ML ops | Batch inference throughput | >= 100000 rows/s | 15,598.3 | **FAIL** |
| O3 | ML ops | Feature build time (medium) | <= 10 min | 0.494 | **PASS** |
| B1 | Batch | Small dataset bronze -> features wall-clock | <= 3 min | - | **NOT RUN** |
| B2 | Batch | Bronze -> silver throughput (medium) | >= 250000 rows/s | - | **NOT RUN** |
| B3 | Batch | Worst data-scaling efficiency (S->M, M->L) | >= 0.750 | - | **NOT RUN** |
| B4 | Batch | Broadcast vs sort-merge join speedup | >= 1.200 x | - | **NOT RUN** |
| B5 | Batch | Cached vs uncached reuse speedup | >= 1.500 x | - | **NOT RUN** |
| B6 | Batch | Best shuffle-partition count (informational) | - | - | **NOT RUN** |
| B7 | Batch | Parquet vs CSV scan speedup | >= 3 x | - | **NOT RUN** |
| D1 | Data quality | Max reconciliation diff (input - valid - rejected) | == 0 rows | 0.000 | **PASS** |
| D2 | Data quality | Max monthly rejection rate | <= 8 % | 6.229 | **PASS** |
| D3 | Data quality | Duplicate trips in silver | == 0 rows | 0.000 | **PASS** |
| D3b | Data quality | Zone-hour demand vs silver trip reconciliation diff | == 0 rows | 0.000 | **PASS** |
| D4 | Reproducibility | Fresh clone -> sample end-to-end | <= 30 min | - | **NOT RUN** |
| D5a | Reproducibility | Test pass rate | >= 100 % | - | **NOT RUN** |
| D5b | Reproducibility | Line coverage of src/ | >= 70 % | - | **NOT RUN** |

Every FAIL must be explained in the report with the measured value, root cause (Spark UI / metrics evidence) and remediation attempted.
