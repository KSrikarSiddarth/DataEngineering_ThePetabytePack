-- Share of a year's trips that start in each hour, and the change from the prior year (gold.demand_monthly_hourly).
-- share_change_vs_prior_year is NULL when the prior year is not loaded.
WITH h AS (
  SELECT service_type, year, pickup_hour, SUM(trips) AS trips FROM demand_monthly_hourly
  GROUP BY service_type, year, pickup_hour
),
s AS (
  SELECT service_type, year, pickup_hour, trips * 1.0 / SUM(trips) OVER (PARTITION BY service_type, year) AS share FROM h
)
SELECT s.service_type, s.year, s.pickup_hour, s.share, s.share - p.share AS share_change_vs_prior_year
FROM s
LEFT JOIN s p ON p.service_type = s.service_type AND p.pickup_hour = s.pickup_hour AND p.year = s.year - 1
ORDER BY s.service_type, s.year, s.pickup_hour
