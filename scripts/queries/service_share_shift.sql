-- Service-type share of trips per year (gold.trips_monthly): how the mix between yellow, green, fhv and fhvhv moves.
WITH y AS (
  SELECT year, service_type, SUM(trips) AS trips FROM trips_monthly GROUP BY year, service_type
)
SELECT year, service_type, trips, trips * 1.0 / SUM(trips) OVER (PARTITION BY year) AS share_of_year
FROM y
ORDER BY year, service_type
