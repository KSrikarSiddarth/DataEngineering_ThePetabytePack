-- Streamed hours against the same weekday and hour in the batch history (combined_demand_hourly, D28).
-- {cutover} is replaced by combined.cutover_date. vs_baseline = 1.0 means a usual level; baseline_days
-- says how many past days it is based on.
WITH base AS (
  SELECT service_type, day_of_week, pickup_hour, AVG(day_trips) AS baseline_trips, COUNT(*) AS baseline_days
  FROM (SELECT service_type, pickup_date, day_of_week, pickup_hour, SUM(trips) AS day_trips
        FROM combined_demand_hourly WHERE pickup_date < DATE '{cutover}'
        GROUP BY service_type, pickup_date, day_of_week, pickup_hour) d
  GROUP BY service_type, day_of_week, pickup_hour
),
live AS (
  SELECT service_type, pickup_date, day_of_week, pickup_hour, SUM(trips) AS live_trips
  FROM combined_demand_hourly WHERE pickup_date >= DATE '{cutover}'
  GROUP BY service_type, pickup_date, day_of_week, pickup_hour
)
SELECT live.service_type, live.pickup_date, live.pickup_hour, live.live_trips,
       base.baseline_trips, base.baseline_days, live.live_trips * 1.0 / base.baseline_trips AS vs_baseline
FROM live
LEFT JOIN base ON base.service_type = live.service_type AND base.day_of_week = live.day_of_week
              AND base.pickup_hour = live.pickup_hour
ORDER BY live.service_type, live.pickup_date, live.pickup_hour
