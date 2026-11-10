-- Batch pipeline health in one row (gold.batch_run_metrics): success rate, freshness lag, reconciliation
-- (total absolute difference, should be 0), schema drift events, runtime and throughput, backfill completeness.
SELECT COUNT(*) AS runs,
       SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS success_rate,
       AVG(freshness_lag_days) AS avg_freshness_lag_days,
       SUM(ABS(reconciliation_diff)) AS reconciliation_abs_diff,
       SUM(columns_added + columns_removed) AS schema_drift_columns,
       AVG(runtime_sec) AS avg_runtime_sec,
       SUM(bronze_rows) * 1.0 / NULLIF(SUM(runtime_sec), 0) AS rows_per_sec,
       COUNT(DISTINCT CASE WHEN status = 'success' THEN source_month END) * 1.0 / MAX(months_expected) AS backfill_completeness
FROM batch_run_metrics
