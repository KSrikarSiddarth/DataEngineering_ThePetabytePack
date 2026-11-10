# duckdb_queries.md — DuckDB queries for checking the pipeline

The DuckDB queries given in the conversation, in one place. Every query was run against the dev store and returns rows. Run them from the project root, in PowerShell, with the venv active (`pip install -r requirements-dev.txt` installs `duckdb`).

**Paths.** The batch store is `output/batch`. On the dev machine the data used so far is in `output/ml_dev`; replace `output/batch` with it in the commands below. Stream output is in `output/stream`.

**Quoting.** In PowerShell, the SQL is inside `\"...\"` and string literals inside the SQL use single quotes. If printing fails with `UnicodeEncodeError` (the result table uses box-drawing characters), run `$env:PYTHONUTF8="1"` first.

## 1. Open the combined database

`scripts/combined_views.py` writes `output/combined.duckdb`. It holds no data, only views over the Parquet files: the batch tables (`trips_monthly`, `silver_trips`, `batch_run_metrics`, ...) and `combined_<mart>` (batch before the cutover date, stream from it on).

```powershell
python scripts/combined_views.py                     # create or refresh the views
duckdb output/combined.duckdb                        # interactive shell (needs the DuckDB CLI)
```

Inside the shell: `.tables` lists the views, `SELECT * FROM combined_trips_daily LIMIT 10;` reads them, `.quit` exits. Without the CLI, use Python:

```powershell
python -c "import duckdb; print(duckdb.connect('output/combined.duckdb').sql('show tables'))"
```

Run one of the KPI queries from `scripts/queries/` against the same database (`live_vs_baseline.sql` has a `{cutover}` placeholder to replace first):

```powershell
python -c "import duckdb; print(duckdb.connect('output/combined.duckdb').sql(open('scripts/queries/batch_health.sql').read()))"
```

The views use absolute paths, so the database file can be opened from any folder.

## 2. Which file loads were slow (M5)

`gold/batch_run_metrics` has one row per file load. This lists the slowest first. HVFHV normally dominates; a file with a much lower `rows_per_sec` than the others is the one to investigate.

```powershell
python -c "import duckdb; print(duckdb.sql(\"select source_file, bronze_rows, round(runtime_sec) sec, round(rows_per_sec) rows_per_sec from read_parquet('output/batch/gold/batch_run_metrics/*.parquet') order by runtime_sec desc\"))"
```

`scripts/queries/batch_health.sql` gives the same figures as one summary row (success rate, average runtime, rows per second, backfill completeness). The runtime covers a whole file load, not its phases.

## 3. How accurate are the ML models (M6)

`gold/ml_model_metrics` is a long table of `(run_id, run_ts, model, metric, scope, value)`, appended on every run. The scopes are `test`, `baseline`, `cv`, `month:YYYY-MM`, `drift` and `train` (see `specs/gold-schema.md`).

Test and baseline metrics side by side (this reads all runs; add `where run_id = '<id>'` for one):

```powershell
python -c "import duckdb; print(duckdb.sql(\"select model, metric, scope, round(value,4) v from read_parquet('output/batch/gold/ml_model_metrics/*.parquet') where scope in ('test','baseline') order by model, metric\").show(max_rows=200))"
```

The runs in the table, newest first:

```powershell
python -c "import duckdb; print(duckdb.sql(\"select run_id, run_ts, count(*) metrics from read_parquet('output/batch/gold/ml_model_metrics/*.parquet') group by all order by run_ts desc\"))"
```

Fare error by month, model against baseline, from the holdout predictions (the other `ml_*_predictions` tables have the same columns; replace `base_fare` with the table's label: `duration_min`, `wait_sec`, `tip20`, `shared_match`):

```powershell
python -c "import duckdb; print(duckdb.sql(\"select ym, round(avg(abs(error)),3) model_mae, round(avg(abs(baseline - base_fare)),3) baseline_mae, count(*) n from read_parquet('output/batch/gold/ml_fare_predictions/*.parquet') group by ym order by ym\"))"
```

The PASS/FAIL view of the same numbers against `evaluation.md` is `python scripts/evaluate_models.py --batch-dir output/batch`.

## 4. Your own query on the stream output

`scripts/show_results.py` registers the stream's `silver`, `invalid` and the eight marts as DuckDB tables. Gold holds hourly sums, so compute ratios in the query:

```powershell
python scripts/show_results.py --sql "SELECT pickup_date, sum(sum_total_amount) / sum(trips) AS avg_fare FROM revenue_daily GROUP BY ALL ORDER BY 1"
```

Add `--output-dir <path>` for output somewhere other than `output/`.

## Reading Parquet directly

- Partitioned tables (`silver/trips`, every batch Gold mart) need `hive_partitioning=true` to expose `service_type`, `year` and `month` as columns, and a glob that skips Spark's `_spark_metadata` folder: `*/*/*/*.parquet` for Silver, `*/*/*.parquet` for a Gold mart.
- Unpartitioned tables (`batch_run_metrics`, `ml_*`) use `*.parquet`.
