-- Average fare, fare per mile, card and cash share, card tip rate by month (gold.revenue_monthly; Yellow and Green).
SELECT service_type, year, month,
       SUM(sum_total_amount) * 1.0 / SUM(trips) AS avg_fare,
       SUM(sum_total_amount_dist) * 1.0 / NULLIF(SUM(sum_distance_miles), 0) AS fare_per_mile,
       SUM(CASE WHEN payment_label = 'Credit card' THEN trips ELSE 0 END) * 1.0 / SUM(trips) AS card_share,
       SUM(CASE WHEN payment_label = 'Cash' THEN trips ELSE 0 END) * 1.0 / SUM(trips) AS cash_share,
       SUM(CASE WHEN payment_label = 'Credit card' THEN sum_tip_amount ELSE 0 END) * 1.0
         / NULLIF(SUM(CASE WHEN payment_label = 'Credit card' THEN sum_base_fare ELSE 0 END), 0) AS card_tip_rate
FROM revenue_monthly
GROUP BY service_type, year, month
ORDER BY service_type, year, month
