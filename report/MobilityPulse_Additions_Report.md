# NYC MobilityPulse: Additions and Changes Report

**Project:** NYC MobilityPulse: Scalable Taxi Demand Forecasting and Mobility Anomaly Detection
**Team:** The Petabyte Pack: Mohammad Ayan Khan, K Srikar Siddarth, Chandana N, Ritesh T Patil
**Course:** DA231O Data Engineering at Scale (IISc)
**Date:** 20 November 2026
**Companion to:** `report/MobilityPulse_Report.md` (the final report, referred to below as the *original report*)

---

## 1. Purpose and summary

The original report describes the project as it stood on 18 November 2026. This document records what was **added or
changed afterwards**, so that a reader with the original report and this codebase can see exactly what is new, why
it was added, how it was tested, and what is still unverified.

**In one paragraph.** Four groups of work were added on top of the existing pipeline: (1) a Spark internals evidence
pack that demonstrates *why* Spark behaves as it does (lazy evaluation, stage boundaries, data skew, precomputed
tables); (2) a daily data-quality table in the gold layer, exposed through the API and a new dashboard page, with
alert flags; (3) incremental, idempotent month-by-month processing driven by a state table; and (4) an investigation
of several FAILed targets that separated one real benchmark bug (B5) from three cases where the target itself, not
the code, is the cause (M8, A4b, partly M7), plus an opt-in hyper-parameter search for the gradient-boosted model.
Nothing that existed was removed, no evaluation target was edited, and the existing result files were not touched.

**Important limit of this change set.** The real NYC data is not on the machine where these additions were built.
Everything below was tested on the project's **synthetic sample data** (449,385 trips, 40 zones, 2 months). The code
is verified by tests; the *numbers* are not results about real taxi data, and the real-data re-runs listed in
Section 9 have not been done.

| | Original report | After these additions |
|---|---|---|
| Tests | 64 | **87** (23 new) |
| Line coverage of `src/` | 82 % | **84 %** |
| Evaluation scorecard rows | 45 | **51** (6 new INFO rows; no target edited) |
| API endpoints | 8 | **9** |
| Dashboard pages | 7 | **8** |
| Pipeline CLI stages | 9 | **10** (`incremental` added) |
| Gold tables written by the batch pipeline | 6 analytics + forecasts, anomalies, metrics | **+ `data_quality_daily`** |

---

## 2. Baseline: what existed before

### 2.1 Capabilities (all in the original report)

| Area | What existed |
|---|---|
| Ingestion | Idempotent downloader with SHA-256 manifest for TLC trips, taxi zones and NOAA weather |
| Layers | raw → bronze → silver → gold in Parquet; quarantine for rejected rows |
| Data quality | 9 rules plus duplicate detection; per-month DQ report; reconciliation `input = valid + rejected` |
| Batch analytics | Zone-hour demand grid, feature table, 6 Spark SQL gold tables |
| Streaming | Kafka replay producer, Structured Streaming with watermark, windows, dead-letter queue, checkpoints |
| ML | Baselines, linear regression, random forest, GBT, validation-selected ensemble |
| Anomaly detection | Residual z-score detector, MAD rule labels, KMeans distance |
| Serving | FastAPI over a SQLite copy of gold, Streamlit dashboard |
| Evaluation | Benchmarks E1–E5 (batch) and streaming experiments, 45 targets, auto-generated scorecard |

### 2.2 Measured state at the baseline

| Run | PASS | FAIL | INFO | Not repeated |
|---|---|---|---|---|
| Graded run (3 months for ML; 1/3/6 months for scaling) | 30 | 14 | 1 | 0 |
| Two-year run (78.5 M trips) | 13 | 10 | – | 22 |

The 14 FAILs in the graded run were S2, S3, S5, S7, M1a, M3, M7, M8, A2, A4b, O2, B2, B5, B7.

### 2.3 How the original testing was done

Taken from the run logs and the original report, for context:

* **Graded run:** one Apple-silicon Mac. Spark in `local[*]`; Kafka (single KRaft broker) in Docker through Colima
  at `localhost:9092` on the **same machine**. The replay producer, the Kafka broker and the Spark stream job all
  shared that laptop.
* **Two-year run:** a separate Windows host with Docker Desktop (20 vCPU, 15.3 GiB), Spark in `local[*]` then
  `local[8]`. Each machine downloaded the public data itself; only result files were copied into the repository.
* **No multi-node Spark cluster was used.** There is no standalone master or workers. This is why the streaming
  partition result (S7) is a single-machine result, and why concepts that need a second node remain open
  (Section 9).

### 2.4 Defects and open questions recorded in the original report that motivated the additions

* B5: caching made the benchmark 5× slower (0.20×).
* M8: worst-borough WAPE 111.9 % (3 months), 81.9 % (2 years) against a 35 % target.
* A4b: recall against the MAD rule labels 0.10 against 0.60.
* M7: GBT 9.6 % worse than linear regression on 3 months, 7.6 % better on 2 years; target 10 % better.
* The original report notes, as next steps, "select needed columns before `cache()`", "an absolute-error target would
  be appropriate" for near-empty boroughs, and that the anomaly alert budget was kept for operator load.

---

## 3. Overview of what was added or changed

| # | Change | Kind | Main files | Tests added |
|---|---|---|---|---|
| A | Spark internals evidence pack (4 experiments) | New | `benchmark/concepts.py` | 5 |
| B | `gold.data_quality_daily`, API endpoint, dashboard page, alert flags | New | `batch/dq_daily.py`, `serving/*` | 5 |
| C | Incremental, idempotent processing with state table and watermark | New | `batch/incremental.py` | 7 |
| D1 | B5: caching benchmark redesigned | Changed | `benchmark/batch_bench.py` | covered by existing E4 test |
| D2 | M8: Poisson-noise-floor diagnostic | New | `ml/metrics.py`, `ml/train.py` | 2 unit tests + the shared diagnostics test |
| D3 | A4b: recall-ceiling diagnostic | New | `ml/anomaly.py` | the shared diagnostics test |
| D4 | M7: opt-in validation grid search for GBT (off by default) | New | `ml/train.py`, `config.yaml` | 4 integration tests (diagnostics file, shared with D2 and D3) |

New tests in total: 5 + 5 + 7 + 4 (diagnostics file) + 2 (unit) = **23**.

---

## 4. Details of each addition

### 4.A Spark internals evidence pack

**What it is.** Four small experiments that produce evidence of how Spark executes work, run with `make concepts`.
They are demonstration queries over the silver layer, not pipeline steps. Output goes to `results/concepts/`
(CSV tables and saved physical plans); the Spark UI shows the same events live while they run.

| ID | Concept | What is measured | Result on the synthetic sample |
|---|---|---|---|
| C1 | Lazy evaluation and the DAG | Spark jobs launched per step, using the status tracker | 5 chained transformations and `explain()`: **0 jobs** each. `count()`: **4 jobs**. A second `count()` with nothing cached: 4 jobs again. |
| C2 | Narrow vs wide transformations | Stages, tasks and shuffle bytes per query (AQE off for classic stage shape) | narrow-only: **1 stage**, 0 MB shuffle. Zone-hour aggregation (the real one from `demand.py`): **2 stages**, 2.2 MB. Aggregation plus sort: **4 stages**. `repartition`: 2 stages, 69 MB. |
| C3 | Data skew | Per-task shuffle-read spread in the join stage under four variants | See below |
| C4 | Silver scan vs gold lookup | Time, stages and bytes read for the same question from each layer, with an answer-equality check | trips per borough and month: gold **3.6× faster**, reads 0.005 MB vs 6.81 MB. Top-10 zones for one day: a tie, because silver is partitioned by day. |

**C3 detail.** The sample is only mildly skewed (busiest zone 2.6× the median), so a hot zone was **injected on
purpose** (50 % of trips remapped to zone 161, making it 41× the median). This is synthetic and is stated as such.
Broadcasting is disabled to force a shuffle join, and the AQE skew thresholds are lowered to 512 KB so they trigger
on small data.

| Variant | Largest task / median task (shuffle read) | Slowest task | Tasks |
|---|---|---|---|
| Natural data, no handling | 6.5× | 352 ms | 32 |
| Skewed, no handling | **38.9×** | 536 ms | 32 |
| Skewed, AQE skew join | **5.3×** | 156 ms | 38 |
| Skewed, manual salting (8 salts) | 7.7× | 395 ms | 32 |

The read-size ratios are deterministic. The millisecond columns are single runs and vary by tens of milliseconds.

**Findings worth keeping.**

1. AQE's skew-join optimisation is skipped when splitting a partition would force an extra shuffle. Grouping by the
   *join key* right after the join reuses the join's partitioning, so the skew is left alone; grouping by a different
   column lets AQE split it. The code groups by `borough` for this reason and says so in a comment.
2. Gold is built on the dense zone × hour grid, so for a day with fewer than ten active zones it lists zero-trip
   zones that silver cannot see. The C4 comparison filters `trips > 0` on the gold side to stay like-for-like.

### 4.B `gold.data_quality_daily`

**What it is.** A gold table, written by the silver stage, with one row per source month and pickup day. It is
built from the same quality-flagged frame that silver and quarantine are split from, so
`input_rows = valid_rows + rejected_rows` holds on every row.

| Column group | Contents |
|---|---|
| Keys | `source_month`, `date`, `day_kind` (`day` or `unattributed`) |
| Volume | `input_rows`, `valid_rows`, `rejected_rows`, `reject_rate_pct` |
| Rejects by reason | one `rej_<reason>` column per quality rule, plus `rej_duplicate` |
| Null rates | `null_pct_<column>` for the nine checked columns |
| Freshness | `max_pickup_ts`, `ingested_at`, `ingest_lag_h`, `computed_at` |
| Alert | `alert_reject_rate` (reject rate above the threshold), `alert_threshold_pct` |

**Design decision.** Rows with a null timestamp, or a timestamp outside their source month, cannot be placed on a real
day. They are kept in one `unattributed` row per month and **never alert**. Without that rule, a few stray 2023
timestamps in a 2024 file would appear as fake 100 %-reject days.

**Alert threshold.** `quality.alert_reject_rate_pct: 8.0` in `configs/config.yaml`, the same 8 % as target D2.

**Serving.** `make export` copies the table into SQLite. `GET /data-quality` returns it (`start`, `end`,
`alerts_only`, `include_unattributed`, `limit`). The dashboard has a new **Data quality** page: days monitored, alert
days, overall reject rate, newest event, a reject-rate line with the threshold, rejects by reason per day, a
null-rate table, the alert list and per-day freshness.

**Checked on the sample** (a scratch copy, so the real sample folder was not changed): 62 rows (60 days plus 2
unattributed), 0 alert days. The sample's reject rate is a flat 3.19 %, so the alert path is demonstrated by a unit
test with known inputs instead (a 20 % day alerts, a 1 % day does not, a 100 %-rejected unattributed bucket does not).
The dashboard page was rendered headlessly with no exceptions.

### 4.C Incremental, idempotent processing

**What it is.** `make incremental MONTHS=...` processes only the months that are new, unfinished or changed, and
records progress in a state table.

* **State table:** `<data_dir>/state/pipeline_state.parquet`, one row per (month, stage) with
  `status` (`running`, `done`, `failed`), the SHA-256 fingerprint of the month's raw TLC file, rows produced, run id
  and timestamp. It is replaced atomically.
* **What gets processed:** a month needs work from the first month-level stage (`bronze`, `silver`, `demand`) that
  is missing, not `done`, or was run against a different fingerprint. Later stages of that month re-run too.
* **Watermark:** the latest month whose three month-level stages are all `done`. It is reported, not used as a hard
  cut-off, so a backfill and a restatement are still picked up.
* **Downstream:** after new work, `features` and `analytics` are rebuilt over every finished month. `ml`, `anomaly`
  and `export` are deliberately not part of the incremental run, because retraining is expensive.
* **Why re-running is safe:** the writers overwrite only the partitions of the months they are given, and
  `record_id` is a deterministic hash, so a re-run replaces rows instead of appending duplicates. This extends
  design decision 1 of the original report (idempotent writes) from "safe to re-run" to "only does the work needed".
* **Crash behaviour:** each stage commits on its own. A crash leaves earlier stages `done` and the failed one
  `failed`; the next run resumes at the failed stage.

**What the tests prove** (synthetic data):

| Scenario | Assertion |
|---|---|
| First run | Month processed, watermark set, state rows `done` |
| Second run, nothing new | Nothing runs; state table unchanged |
| Forced re-run | Order-independent content hash and row count of bronze, silver, quarantine, demand and `data_quality_daily` identical; no duplicate `record_id` |
| New month added | Only the new month processed; old month's state and content unchanged; features see both months |
| Changed input | The month is reprocessed and the new fingerprint stored |
| Crash in `demand` | Bronze and silver stay `done`, the month is not counted in the watermark, the rerun redoes only `demand` |
| Missing raw file | Clear `FileNotFoundError` before anything is written |

The content hash excludes columns that are wall-clock stamps by design (`ingested_at`, `rejected_at`,
`computed_at`, `ingest_lag_h`); everything else must match exactly.

### 4.D Investigation of FAILed targets

No target was changed. The investigation used the saved real-data results (`results/` and `results_two_years/`).
Six INFO rows were added to the scorecard to carry the evidence; INFO rows never pass or fail.

#### D1. B5 cache speedup (0.197×): a real benchmark bug, fixed

* **Cause.** The original E4 cached the full-width silver table and then ran five aggregations that each need three
  or four columns. Recomputing was a cheap, column-pruned Parquet scan, so a cache could only lose: the cached run
  read **9.15 GB** against **273 MB** uncached (`results/benchmarks/e4_cache.csv`).
* **Fix.** E4 now measures the reuse the silver build really has: the quality-flagged bronze frame (one window-based
  dedupe, which is a shuffle) feeding three consumers (silver output, quarantine output, DQ report). The old
  scan-only case is kept as a labelled contrast with wide and narrow caching.
* **Result on the synthetic sample.** Pipeline reuse **1.76×** (target 1.5×). Scan-only, wide table cached **0.59×**
  with 4.5× more input read. Scan-only, only the needed columns cached **0.97×**. The original report's own
  "next step" (select columns before caching) would therefore not have reached 1.5×; the cause was the choice of
  what to cache and what it was compared with.
* **Lesson.** Caching pays when recomputation is expensive (shuffle, join, window) and the cached data is narrow. It
  hurts when recomputation is a cheap columnar scan.
* **Status.** Not re-measured on real data. The scorecard still shows 0.197× until `make bench-batch` is re-run there.
  Measurement keys added: `batch.cache_speedup_scan_wide`, `batch.cache_speedup_scan_narrow`. The old rows in
  `e4_cache.csv` (variants `uncached`, `cached`) are kept as legacy evidence of the original design.

#### D2. M8 worst-borough WAPE (111.9 % and 81.9 %): a noise floor, not a model bug

The worst borough is the Bronx, with a mean of about 1.08 trips per zone-hour (MAE 0.882 divided by WAPE 0.8185,
2-year GBT, 337 rows). Counts that small are dominated by Poisson noise. If a forecast were the exact rate, the
expected absolute error would still be `2·e^-λ·λ^(k+1) / k!` with `k = ⌊λ⌋`. As a WAPE floor:

| Mean trips per zone-hour | 0.5 | 1.08 | 2 | 5 | 20 | 100 |
|---|---|---|---|---|---|---|
| Poisson floor on WAPE | 121 % | **73 %** | 54 % | 35 % | 18 % | 8 % |

At the Bronx mean, GBT (81.9 %) is within about nine points of the floor (linear regression: 118.8 %). A 35 % target
needs zones averaging at least about five trips per zone-hour, which the Bronx does not have. This agrees with the
original report's conclusion that an absolute-error target would suit near-empty boroughs, and puts a number on it.

* **Added:** `poisson_mad` and `poisson_floor_by_group` in `ml/metrics.py`; the ML stage records
  `ml.worst_borough_floor_wape_pct` (scorecard row **M8f**) next to the measured value.
* **M8 stays a FAIL as specified.** The cause is the target, not the forecaster.
* **Not measured:** the row-by-row floor on real data. The 73 % figure above is a single-rate hand calculation.

#### D3. A4b recall against the MAD labels (0.097): a ceiling set by the alert budget

Recall cannot exceed `alerts / positives`, however good the detector is. In the 3-month run the original report gives
168 residual alerts against 1,182 MAD-flagged hours, so recall can never exceed **0.14**. Even a detector with
perfect precision at that alert volume could not reach 0.60. Reaching recall 0.6 at precision 0.6 would need alerts on
about as many zone-hours as the rule labels, and the alert-rate cap (A5, at most 1 %) forbids that. A4a/A4b and A5
therefore pull in opposite directions by construction.

* **Added:** the anomaly stage records `anomaly.rule_recall_ceiling` (row **A4c**) and
  `anomaly.rule_positive_rate_pct` (row **A4d**).
* **Not changed:** the detector. Loosening its threshold would only trade A4b for A5 and A4a.
* **Open:** the 2-year run is a different problem. There the alert rate was 1.72 % (A5 FAIL, 3.4× the 0.5 %
  calibration) and precision 0.29. The ceiling is not binding there; the threshold calibrated on 7 validation days
  does not carry over to the 14-day test period. This was not diagnosed further without the real data.

#### D4. M7 GBT vs linear regression: an opt-in hyper-parameter search

The project plan (§4.5) calls for a light grid search on validation data only; the code had fixed hyper-parameters.
Added, **off by default**:

* `ml.tuning.enabled: true` fits each candidate in `ml.tuning.gbt_grid` on the training split, scores validation MAE,
  and keeps the best. The test split is never read.
* The default grid has four candidates (depth 6/8/10, 150–300 iterations, step 0.05/0.1).
* `ml.gbt_train_min` (target O1) still times only the chosen model's fit. The whole search is reported separately as
  `ml.gbt_tuning_min` (row **M7t**).
* Enable with `MP__ML__TUNING__ENABLED=true make ml`. Expect the cost to be about the grid size times the GBT fit
  (O1 was 10 min on two years, so four candidates is roughly 40 min there).
* **Not measured:** whether tuning moves M7. That needs the real data. The original report's own explanation (trees
  need more data to beat a linear model) is supported by the 3-month vs 2-year contrast and is unchanged.

---

## 5. Effect on the scorecard and the targets

* **No target was edited.** The 45 original targets keep their thresholds. The original report's statement that they
  were fixed before results were seen remains true for those 45.
* **Six INFO rows were added after results existed**, as diagnostics: M8f, M7t, A4c, A4d, B5w, B5n. They never pass
  or fail. They appear as NOT RUN until the matching stage is re-run.
* **The recorded scorecards are unchanged.** `results/evaluation_scorecard.md` and `results/measurements.json` were
  not modified by this work (their timestamps pre-date it). B5 will change only when E4 is re-run on real data.

| Target | Status after this work |
|---|---|
| B5 | Benchmark fixed; expected to change on a real-data re-run |
| M8 | Still FAIL by design; floor evidence added (M8f) |
| A4b | Still FAIL; ceiling evidence added (A4c, A4d) |
| M7 | Still FAIL; opt-in tuning available, effect unmeasured |
| All others | Unchanged |

---

## 6. Testing and verification

**Environment for this work:** a 16-core Linux machine, 30 GB RAM, Python 3.11, Java 17, PySpark 3.5.3 (the project's
`mp311` virtual environment). Not the Apple-silicon and Windows machines used for the original real-data runs.

| Check | Result |
|---|---|
| Full test suite | **87 passed** (about 3.4 min), up from 64 |
| Coverage of `src/` | **84 %** (was 82 %). New modules: `dq_daily` 100 %, `incremental` 99 %, `concepts` 84 %, `metrics` 100 % |
| Dashboard Data quality page | Rendered headlessly with Streamlit's test harness on a scratch copy of the sample: no exceptions, metrics correct |
| Re-run identity | Content hash of five outputs identical after a forced re-run |
| E4 redesign | Run on a scratch copy of the sample with 3 repetitions |

To avoid touching your data, the dashboard check, the E4 run and the silver/export runs used a **scratch copy** of the
sample with a separate results folder. The full test suite uses temporary directories. The project's `results/` and
`results_two_years/` were not written to.

**What this verification does not show:** any result on real NYC data.

---

## 7. Defects found and fixed while building the additions

These were caught by the new tests or by inspecting the first runs, and fixed before the work was finished.

1. **Experiment c2 failed without the Spark UI.** Per-stage data was only created when stages existed, so a test
   session with the UI disabled raised a `KeyError`. The metrics helpers now return empty results when the UI is off,
   the same way the existing benchmark harness does.
2. **Configuration leak in the skew experiment.** After the run, `spark.sql.adaptive.skewJoin.enabled` stayed `false`
   for any session that had never set it explicitly. Keys that were unset are now unset again afterwards.
3. **Silver vs gold answers differed on the test data.** Cause: the dense grid lists zero-trip zones (see 4.A). The
   comparison now filters them so both layers answer the same question.
4. **AQE skew join had no effect on the first run.** Cause: grouping by the join key right after the join (see 4.A).
5. **A pandas FutureWarning** from concatenating an empty state table in the incremental module; the state is now
   rebuilt from records.

---

## 8. Files changed

**New source:** `src/mobilitypulse/benchmark/concepts.py`, `src/mobilitypulse/batch/dq_daily.py`,
`src/mobilitypulse/batch/incremental.py`

**Modified source:**

| File | Change |
|---|---|
| `batch/silver.py` | Writes `data_quality_daily` after the quality flags are computed (only when reports are on) |
| `cli.py` | New `incremental` stage; `--force` help text |
| `serving/export.py` | Exports `data_quality_daily`, adds an index |
| `serving/queries.py` | `data_quality_daily()` query |
| `serving/api.py` | `GET /data-quality` |
| `serving/dashboard.py` | New Data quality page |
| `benchmark/batch_bench.py` | E4 rewritten; imports |
| `ml/metrics.py` | `poisson_mad`, `poisson_floor_by_group` |
| `ml/train.py` | Tuning functions, noise-floor diagnostic, optional `forecasts` argument to `summarise` |
| `ml/anomaly.py` | Recall-ceiling measurements |

**Configuration:** `configs/config.yaml` (`paths.state`, `quality.alert_reject_rate_pct`, `ml.tuning`),
`configs/targets.yaml` (six INFO rows), `Makefile` (`concepts`, `incremental`).

**New tests:** `tests/integration/test_concepts.py`, `test_dq_daily.py`, `test_incremental.py`,
`test_model_diagnostics.py`; two unit tests appended to `tests/unit/test_features_and_metrics.py`.

**Documentation:** new `docs/07_spark_internals.md` and `docs/08_incremental_and_dq.md`; new §6 in
`docs/04_evaluation.md`; README additions (commands, API row, dashboard pages, layout, `state/` directory).

**Generated output:** `results/concepts/` (11 files from the synthetic sample).

**Behavioural impact on existing runs:** the silver stage now also writes one small extra gold table; the ML stage
computes one extra aggregation over the best model's test rows and records extra measurements; the anomaly stage
records two extra measurements. With tuning off (the default), model training is unchanged.

---

## 9. How to run the additions

```bash
make concepts                              # all four experiments -> results/concepts/
make concepts EXPERIMENTS="c1 c3"          # a subset
make silver MONTHS=2024-01..2024-03        # now also writes gold/data_quality_daily
make export                                # copies it to SQLite; then: make api / make dashboard
make incremental MONTHS=2024-01..2024-06   # only new / unfinished / changed months
make incremental MONTHS=2024-03 FORCE=1    # re-run a month even if up to date
MP__ML__TUNING__ENABLED=true make ml       # optional GBT grid search (off by default)
make test && make scorecard                # 87 tests; scorecard now has the six INFO rows
```

**To validate everything on real data** (not yet done): `make ingest MONTHS=large`, `make pipeline MONTHS=medium`,
run `make incremental` twice (the second run should do nothing), `make ml anomaly export`, then
`make bench-batch`, `make concepts`, `make test`, `make scorecard`, and open the dashboard's Data quality page.

---

## 10. Limitations and open items

| Item | Status |
|---|---|
| Real-data validation of every addition | **Not done.** All numbers above are from the synthetic sample. |
| C3 skew experiment | Uses an injected hot zone. On real data the natural skew (Midtown, JFK, LaGuardia) replaces it. |
| B5, M8f, A4c/A4d, M7 tuning | Need a real-data re-run to appear in the scorecard |
| Timings in the evidence pack | Single runs on small data; stage counts, task-size ratios and shuffle sizes are the reliable evidence, millisecond columns are not |
| Incremental weather boundary | The 3-hour weather forward-fill restarts at each month boundary when months are processed one at a time. Re-runs of one month are identical to each other, but may differ slightly from a multi-month batch at that boundary. |
| Incremental concurrency | The state table assumes one incremental run at a time |
| `ml`, `anomaly`, `export` | Not part of `incremental`; run them explicitly to refresh serving |
| 2-year anomaly calibration | Alert rate 1.72 % and precision 0.29 are open; not diagnosed without real data |
| Concepts needing a second node | Shuffle over a real network, 1 vs 2 workers, and killing a worker are not implemented (stretch goal E12). The Docker Compose cluster is not built. |
| The original report | Not edited. See Section 11. |

---

## 11. Statements in the original report that this supersedes

| Original report | Now |
|---|---|
| "All 45 evaluation targets were fixed before results were seen" | Still true for those 45; the scorecard has 51 rows with 6 INFO diagnostics |
| 64 tests, 82 % coverage (§12) | 87 tests, 84 % |
| FastAPI with 8 endpoints; 7 dashboard pages | 9 endpoints; 8 pages |
| Gold and data-directory layout (§4) | Gold also holds `data_quality_daily`; the data directory has `state/` |
| E4 result 0.20× and the next step "select needed columns" (§8.1, §9 B5) | E4 is redesigned (4.D1); the old number comes from the old design |
| M8 and A4b root causes (§9) | Extended with the noise floor and the recall ceiling (4.D2, 4.D3) |
| Reproducibility commands (§12) | Add `make incremental` and `make concepts` |
| Key design decision 1, idempotent writes (§4) | Extended by the incremental state table (4.C) |
| "Tuning off" in the 2-year run (§8.3) | This codebase had no tuning code before D4. If the machine that ran it had its own switch, reconcile the two before merging. |
