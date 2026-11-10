-- Average duration (seconds) and speed (mph) by year and pickup hour (gold.trip_efficiency_monthly).
SELECT service_type, year, pickup_hour,
       SUM(sum_duration_sec) * 1.0 / SUM(trips) AS avg_duration_sec,
       SUM(sum_distance_miles) * 1.0 / NULLIF(SUM(sum_duration_sec_dist) / 3600.0, 0) AS avg_speed_mph
FROM trip_efficiency_monthly
GROUP BY service_type, year, pickup_hour
ORDER BY service_type, year, pickup_hour
