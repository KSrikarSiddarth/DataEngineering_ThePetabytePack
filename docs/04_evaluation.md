# 04 · Evaluation

All targets are fixed in [configs/targets.yaml](../configs/targets.yaml) (copied from Plan §6). Each pipeline stage
and benchmark writes its numbers to `results/measurements.json`; `make scorecard` compares them with the targets
and writes `results/evaluation_scorecard.md` with **PASS / FAIL / NOT RUN** per target.

## 1. Where every number comes from

| Targets | Produced by | Command |
|---|---|---|
| D1, D2, D3 | `batch/silver.py` — re-reads written silver + quarantine and reconciles against bronze | `make silver` / `make pipeline` |
| D3b | `batch/demand.py` — Σ zone-hour `trip_count` vs silver trip count | `make demand` |
| O3 | `cli.py` features stage timing, or E1 medium | `make features` / `make bench-batch` |
| M1–M9, O1, O2 | `ml/train.py` | `make ml` |
| A1–A5 | `ml/anomaly.py` | `make anomaly` |
| B1–B3 (+O3) | E1 data scaling (small/medium/large) | `make bench-batch` |
| B6 | E2 shuffle partitions {8…256} with AQE coalescing off | `make bench-batch` |
| B4 | E3 broadcast vs sort-merge (`autoBroadcastJoinThreshold=-1`) | `make bench-batch` |
| B5 | E4 five aggregations over a reused DataFrame, cached vs not | `make bench-batch` |
| B7 | E5 same aggregation on Parquet vs CSV copy of one month | `make bench-batch` |
| S1–S4, S6, S10, S10b, S11 | stream main trial (2k ev/s, 4 partitions, 10 min) | `make bench-stream` |
| S5 | rate sweep 1k–20k ev/s | `python -m mobilitypulse.benchmark.stream_bench rates` |
| S7 | partition sweep 1/2/4/8 @ 10k ev/s | `... stream_bench partitions` |
| S8, S9 | kill -9 + restart from checkpoint | `... stream_bench recovery` |
| D4 | `make demo` total time, or manual fresh-clone timing via `scripts/record_measurement.py` | `make demo` |
| D5 | pytest junit + coverage json | `make test` |

## 2. Method rules

* Each batch experiment runs `benchmark.repetitions` (3) times; the **median** is used; all repetitions are kept
  in `results/benchmarks/<experiment>.csv` together with Spark stage metrics (tasks, shuffle read/write bytes,
  spill, input bytes) read from the Spark UI REST API per job group.
* Cache is cleared before every repetition.
* Scaling efficiency for growth factor $k = \text{rows}_B / \text{rows}_A$:
  $\eta = \dfrac{T_A \cdot k}{T_B}$ (1.0 = perfectly linear; > 1 means fixed overheads are amortised).
* Streaming numbers exclude a 60 s warm-up (JIT, Kafka connection, first state-store creation).
* Accuracy is always reported on the **test** split of the best-on-validation model.
* Machine details are printed in the scorecard header; record CPU/RAM in the report.

## 3. Dataset sizes (approved: laptop-friendly)

| Name | Months | Approx. trips |
|---|---|---|
| small | 2024-01 | ~3.0 M |
| medium | 2024-01..03 | ~9.5 M |
| large | 2024-01..06 | ~20 M |

## 4. Reporting a FAIL

For every FAIL the report states: measured value, root cause with evidence (Spark UI stage view, shuffle/spill
numbers, progress CSV), what was tried (e.g. partition count, broadcast hint, trigger interval, model params),
and the residual gap. Targets are never edited after results are seen.

## 5. Interpreting the scaling analysis

Deviation from linear scaling is explained using: fixed job/stage scheduling overhead (dominant on *small*),
shuffle volume (zone-hour aggregation, dedup window), skew (Midtown/JFK/LaGuardia zones), small files, executor
memory and spill, and the single-machine ceiling (all executors share 10 cores) — cf. Amdahl's law for the serial
fraction (driver-side planning, `collect` of DQ counts) versus Gustafson-style scaled workloads.

## 6. Root causes of FAILs that were investigated

Targets are unchanged. The scorecard gained INFO rows (M8f, M7t, A4c, A4d, B5w, B5n) that never pass or fail; they
carry the evidence below. The numbers quoted here come from the saved real-data runs in `results/` (3 months) and
`results_two_years/`; anything marked *not measured* needs the real data directory and was not re-run.

### B5 · cache speedup (0.197× → benchmark was wrong; fixed)

The original E4 cached the full-width silver table and then ran five aggregations that each need 3–4 columns.
Recomputing was a cheap, column-pruned Parquet scan, so the cache could only lose: the cached run read **9.15 GB**
against **273 MB** uncached (`results/benchmarks/e4_cache.csv`). That is a design flaw, not a Spark problem.

E4 now measures the reuse the silver build really has: the quality-flagged bronze frame (one window-based dedupe, a
shuffle) feeding three consumers. It also keeps the old scan-only case as a labelled contrast (wide table cached vs
only the needed columns cached). On the synthetic sample: pipeline reuse **1.76×** (target ≥ 1.5×), scan-only wide
**0.59×** with 4.5× more input read, scan-only narrow **0.97×**. Lesson for the report: caching pays when recomputation
is expensive (shuffle, join, window) and the cached data is narrow; it hurts when recomputation is a cheap columnar scan.
*Not measured on real data:* the scorecard still shows the old value until `make bench-batch` (E4) is re-run there.

### M8 · worst-borough WAPE (111.9 % in 3 months, 81.9 % in 2 years): a noise floor, not a model bug

The worst borough is the Bronx, with a mean of about 1.08 trips per zone-hour (MAE 0.882 / WAPE 0.8185, 2-year GBT,
337 rows). Counts that small are dominated by Poisson noise. If the forecast were the exact rate, the expected
absolute error would still be `2·e^-λ·λ^(k+1)/k!` (k = ⌊λ⌋); as WAPE that is a floor:

| Mean trips per zone-hour | 0.5 | 1.08 | 2 | 5 | 20 | 100 |
|---|---|---|---|---|---|---|
| Poisson floor on WAPE | 121 % | **73 %** | 54 % | 35 % | 18 % | 8 % |

At the Bronx's mean the single-rate floor is about 73 %, and GBT reaches 81.9 % (linear regression 118.8 %), so the
model is within ~9 points of the best any model could do. A 35 % target needs zones averaging at least ~5 trips per
zone-hour, which the Bronx does not have. The scorecard row **M8f** now reports the floor computed row by row from the
forecasts at each `make ml`, next to the measured value. Honest framing: M8 stays a FAIL as specified; the cause is
the target, not the forecaster. *Not measured:* the row-by-row floor on real data (the 73 % above is the single-rate
hand calculation).

### M7 · GBT vs linear regression (−9.6 % in 3 months, +7.6 % in 2 years; target ≥ +10 %)

A real modelling gap: Plan §4.5 promised a light grid search on validation only, and it was never implemented (fixed
hyper-parameters in `configs/config.yaml`). Added, **off by default**: `ml.tuning.enabled: true` fits each of the
`ml.tuning.gbt_grid` candidates on train, scores validation MAE, and keeps the best; the test split is never read.
`ml.gbt_train_min` (O1) still times only the chosen model's fit; the whole search is reported as **M7t**.
Enable with `MP__ML__TUNING__ENABLED=true make ml`. Expect the cost to be about `len(gbt_grid)` times the GBT fit
(O1 was 10 min on 2 years, so 4 candidates ≈ 40 min there). *Not measured:* whether tuning moves M7; that needs the
real data. The 3-month result also suggests the data volume matters (GBT lost with 3 months, won with 24).

### A4b · recall against MAD rule labels (0.097 in 3 months)

Recall cannot exceed `alerts / positives` however good the detector is. With the detector calibrated to alert on about
0.5 % of zone-hours (`anomaly.residual_alert_rate`) and the MAD rule labelling more than that, the ceiling is low:
in the 3-month run recall / precision = 0.0973 / 0.6845 = **0.142**, so even a perfect-precision detector at that alert
volume could not reach 0.60. Reaching recall 0.6 at precision 0.6 would need alerts on about as many zone-hours as the
rule labels (**A4d**), and the alert-rate cap A5 (≤ 1 %) forbids that when the rule labels more than ~1 %. So A4a/A4b
and A5 pull in opposite directions by construction. **A4c** now reports the ceiling at each `make anomaly`. Making the
residual detector agree with the MAD rule by loosening its threshold would only trade A4b for A5 and A4a.

The 2-year run is a different problem: alert rate 1.72 % (A5 FAIL, 3.4× the 0.5 % calibration) and precision 0.29
(A4a FAIL, A2 0.43). There the ceiling is not binding; the threshold calibrated on 7 validation days does not carry
over to the 14-day test period. *Open, not diagnosed further without the real data.*
