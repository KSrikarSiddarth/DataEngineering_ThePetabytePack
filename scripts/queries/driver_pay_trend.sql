-- Driver pay as a share of the base fare, by month and HVFHV company (gold.hvfhv_economics_monthly).
SELECT hvfhs_license_num, year, month,
       SUM(sum_driver_pay) * 1.0 / NULLIF(SUM(sum_base_fare_with_pay), 0) AS driver_pay_share
FROM hvfhv_economics_monthly
GROUP BY hvfhs_license_num, year, month
ORDER BY hvfhs_license_num, year, month
