-- Trips per pickup zone and year, and the change from the prior year (gold.zone_od_monthly).
-- change and change_pct are NULL when the prior year is not loaded.
WITH z AS (
  SELECT service_type, pu_location_id, year, SUM(trips) AS trips FROM zone_od_monthly
  GROUP BY service_type, pu_location_id, year
)
SELECT z.service_type, z.pu_location_id, z.year, z.trips,
       z.trips - p.trips AS change, (z.trips - p.trips) * 1.0 / p.trips AS change_pct
FROM z
LEFT JOIN z p ON p.service_type = z.service_type AND p.pu_location_id = z.pu_location_id AND p.year = z.year - 1
ORDER BY z.service_type, z.pu_location_id, z.year
