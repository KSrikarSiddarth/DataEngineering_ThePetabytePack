-- Month-over-month and year-over-year trip growth, rolling 12-month average and seasonality index (gold.trips_monthly).
-- A value is NULL when the months it needs are not loaded; history_status says so ("not enough history").
WITH m AS (
  SELECT service_type, year, month, year * 12 + month AS mi, SUM(trips) AS trips
  FROM trips_monthly GROUP BY service_type, year, month
),
w AS (
  SELECT service_type, year, month, mi, trips,
         AVG(trips) OVER (PARTITION BY service_type ORDER BY mi RANGE BETWEEN 11 PRECEDING AND CURRENT ROW) AS avg12,
         COUNT(*) OVER (PARTITION BY service_type ORDER BY mi RANGE BETWEEN 11 PRECEDING AND CURRENT ROW) AS n12,
         AVG(trips) OVER (PARTITION BY service_type, year) AS year_avg,
         COUNT(*) OVER (PARTITION BY service_type, year) AS months_in_year
  FROM m
)
SELECT w.service_type, w.year, w.month, w.trips,
       (w.trips - p.trips) * 1.0 / p.trips AS mom_growth,
       (w.trips - y.trips) * 1.0 / y.trips AS yoy_growth,
       CASE WHEN w.n12 = 12 THEN w.avg12 END AS rolling_12m_avg,
       CASE WHEN w.months_in_year = 12 THEN w.trips * 1.0 / w.year_avg END AS seasonality_index,
       CASE WHEN y.trips IS NULL OR w.n12 < 12 OR w.months_in_year < 12 THEN 'not enough history' ELSE 'ok' END AS history_status
FROM w
LEFT JOIN m p ON p.service_type = w.service_type AND p.mi = w.mi - 1
LEFT JOIN m y ON y.service_type = w.service_type AND y.mi = w.mi - 12
ORDER BY w.service_type, w.year, w.month
