-- Monthly data quality (gold.data_quality_monthly): row count against the prior month, duplicate rate,
-- late-correction rate, and null and unmatched-zone shares with their change from the prior month.
-- Prior-month columns are NULL for the first loaded month.
WITH q AS (
  SELECT service_type, year, month, year * 12 + month AS mi,
         SUM(rows) AS n, SUM(bronze_rows) AS bronze, SUM(duplicates_removed) AS dups, SUM(late_correction_rows) AS late,
         SUM(n_null_pu_location) AS null_pu, SUM(n_unmatched_pu_zone + n_unmatched_do_zone) AS unmatched
  FROM data_quality_monthly GROUP BY service_type, year, month
)
SELECT q.service_type, q.year, q.month, q.n AS silver_rows,
       q.n * 1.0 / p.n AS rows_vs_prior_month,
       q.dups * 1.0 / NULLIF(q.bronze, 0) AS duplicate_rate,
       q.late * 1.0 / q.n AS late_correction_rate,
       q.null_pu * 1.0 / q.n AS null_pickup_zone_rate,
       q.null_pu * 1.0 / q.n - p.null_pu * 1.0 / p.n AS null_pickup_zone_rate_change,
       q.unmatched * 1.0 / (2 * q.n) AS unmatched_zone_share,
       q.unmatched * 1.0 / (2 * q.n) - p.unmatched * 1.0 / (2 * p.n) AS unmatched_zone_share_change
FROM q
LEFT JOIN q p ON p.service_type = q.service_type AND p.mi = q.mi - 1
ORDER BY q.service_type, q.year, q.month
