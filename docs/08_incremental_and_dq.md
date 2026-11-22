# 08 · Incremental processing and daily data quality

## 1. Incremental, idempotent month processing

```bash
make incremental MONTHS=2024-01..2024-06     # only new / unfinished / changed months are processed
make incremental MONTHS=2024-03 FORCE=1      # re-run a month even though it is up to date
```

Code: [batch/incremental.py](../src/mobilitypulse/batch/incremental.py). Tests:
[tests/integration/test_incremental.py](../tests/integration/test_incremental.py). Raw data must already exist
(`make ingest`); a missing raw file fails up front, before any state is written.

**State table** `<data_dir>/state/pipeline_state.parquet`, one row per (month, stage):
`month, stage, status (running | done | failed), input_fingerprint, rows_out, run_id, processed_at`.
The fingerprint is the SHA-256 of the month's raw TLC file, so it changes if and only if the input content changes.

**What gets processed.** A month needs work from the first month-level stage (`bronze`, `silver`, `demand`) that is
missing, not `done`, or was run against a different fingerprint; later stages of that month then re-run too because
they depend on it. Everything else is skipped without touching state or data.

**Watermark** = the latest month whose three month-level stages are all `done`. It is reported, not used as a hard
cut-off, so a backfill (an older month that was never processed) and a restatement (changed input) are still picked up.

**After new work** the global stages `features` and `analytics` are rebuilt over every finished month and recorded
under month `ALL`. `ml`, `anomaly` and `export` are *not* part of the incremental run (retraining is expensive):
run `make ml anomaly export` when you want the serving layer refreshed.

**Why re-running is safe.** Bronze, silver, quarantine, demand and `data_quality_daily` overwrite only the partitions
of the months they are given (dynamic partition overwrite), and `record_id` is a deterministic hash, so a re-run
replaces rows instead of appending duplicates. Each stage commits on its own: a crash leaves earlier stages `done`
and the failed one `failed`, and the next run resumes at the failed stage.

**What the tests prove** (synthetic data):

| Test | Assertion |
|---|---|
| first run | month processed, watermark set, state rows `done` |
| second run, nothing new | nothing runs, state table unchanged (same rows, same timestamps) |
| forced re-run | order-independent content hash and row count of bronze, silver, quarantine, demand and `data_quality_daily` identical; zero duplicate `record_id` |
| new month added | only the new month is processed, the old month's state and content unchanged, features see both months |
| changed input | the month is reprocessed and the new fingerprint stored |
| crash in `demand` | bronze and silver stay `done`, month not counted in the watermark, rerun redoes only `demand` |
| missing raw file | clear `FileNotFoundError`, no state written |

The content hash excludes columns that are wall-clock stamps by design (`ingested_at`, `rejected_at`, `computed_at`,
`ingest_lag_h`); everything else must match exactly.

**Known limits.** (1) The hourly weather forward-fill (3 h) restarts at each month boundary when months are processed
one at a time, so the first hours of a month with no observation may stay empty, whereas a multi-month batch fills
them from the previous month. Re-runs of the same month are identical to each other; they may differ slightly from a
multi-month batch run at that boundary. (2) The state table is a single Parquet file replaced atomically; it assumes
one incremental run at a time.

## 2. `gold.data_quality_daily`

Built inside the `silver` stage from the quality-flagged bronze rows, so `input_rows = valid_rows + rejected_rows`
holds for every row. Code: [batch/dq_daily.py](../src/mobilitypulse/batch/dq_daily.py). Tests:
[tests/integration/test_dq_daily.py](../tests/integration/test_dq_daily.py).

One row per (source month, pickup day). Columns:

| Group | Columns |
|---|---|
| keys | `source_month`, `date`, `day_kind` (`day` or `unattributed`) |
| volume | `input_rows`, `valid_rows`, `rejected_rows`, `reject_rate_pct` |
| rejects by reason | `rej_<reason>` for every rule in `batch/quality.py` plus `rej_duplicate` (primary reason per row) |
| null rates | `null_pct_<column>` for the nine checked columns |
| freshness | `max_pickup_ts` (newest event that day), `ingested_at`, `ingest_lag_h`, `computed_at` |
| alert | `alert_reject_rate` (true when `reject_rate_pct` > `alert_threshold_pct`), `alert_threshold_pct` |

Rows with a null timestamp or a timestamp outside their source month cannot be placed on a real day. They are kept in
one `unattributed` row per month (`date` is null) and **never alert**; otherwise a few stray out-of-range timestamps
would appear as fake 100 %-reject days.

The alert threshold is `quality.alert_reject_rate_pct` in `configs/config.yaml` (8.0, the PRD target D2).

**Serving.** `make export` copies the table to SQLite. `GET /data-quality` returns it
(`start`, `end`, `alerts_only`, `include_unattributed`, `limit`). The dashboard has a **Data quality** page: days
monitored, alert days, overall reject rate, newest event, reject-rate line with the threshold, rejects by reason per
day, null-rate table, alert list and per-day freshness.

**Checked on the sample data** (copy of `~/mobilitypulse_sample`): 62 rows (60 days + 2 unattributed), 0 alert days;
the sample's reject rate is a flat 3.19 %, so the alert path is demonstrated by the unit test (a 20 % day alerts, a
1 % day does not, an all-rejected unattributed bucket does not) rather than by the sample.
