-- Average total amount and fee amounts per trip before and after two policy start months (gold.revenue_monthly):
-- the Airport fee (2022-01) and the CBD congestion fee (2025-01). A period with no loaded months is
-- reported as "not enough history" instead of being computed.
WITH m AS (
  SELECT year * 12 + month AS mi, SUM(trips) AS trips, SUM(sum_total_amount) AS total,
         SUM(sum_airport_fee) AS airport, SUM(sum_cbd_congestion_fee) AS cbd
  FROM revenue_monthly GROUP BY year, month
),
p AS (
  SELECT 'airport_fee' AS policy, 2022 * 12 + 1 AS eff UNION ALL SELECT 'cbd_congestion_fee', 2025 * 12 + 1
),
per AS (
  SELECT policy, eff, 'before' AS period FROM p UNION ALL SELECT policy, eff, 'after' FROM p
)
SELECT per.policy, per.period, COUNT(m.mi) AS months,
       CASE WHEN COUNT(m.mi) = 0 THEN 'not enough history' ELSE 'ok' END AS status,
       SUM(m.total) * 1.0 / SUM(m.trips) AS avg_total_amount,
       SUM(m.airport) * 1.0 / SUM(m.trips) AS avg_airport_fee,
       SUM(m.cbd) * 1.0 / SUM(m.trips) AS avg_cbd_fee
FROM per
LEFT JOIN m ON (per.period = 'before' AND m.mi < per.eff) OR (per.period = 'after' AND m.mi >= per.eff)
GROUP BY per.policy, per.period
ORDER BY per.policy, per.period DESC
