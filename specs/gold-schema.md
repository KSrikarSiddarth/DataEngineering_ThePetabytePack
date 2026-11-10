# gold-schema.md — the daily, monthly and health marts

The eight daily Gold marts of `specs/3_KPI.html`, built in M4 by `spark/gold.py` and checked by `tests/test_gold.py`. The stream writes them to `output/stream/gold/<mart>`; the batch job (M5) builds the same definitions over the batch Silver, plus the monthly and pipeline-health marts described below.

## Rules

- **Hourly grain, additive measures.** Every row is one pickup hour (`hour_start`, `pickup_date`, `pickup_hour`) plus the mart's keys. Measures are counts and sums only. A "daily" figure is the sum by `pickup_date`, and a ratio is computed from sums at query time. This keeps stream and batch marts equal after roll-up and lets them be unioned (D28, D29).
- **Built from Silver.** In the stream, Gold reads the Silver Parquet back as a stream, with a 10-minute event-time watermark (`processing.watermark_minutes`) and append mode: an hour is written once, after the watermark passes the end of it.
- **Filters by flag (D27).** Unless the mart says otherwise, rows with `dq_pickup_out_of_range` are left out. Only `data_quality_daily` counts every Silver row.

## Marts

| Mart | Keys | Measures | KPI formula |
|---|---|---|---|
| `trips_daily` | `service_type` | `trips` | trips per day; share = trips / total trips of the day |
| `demand_hourly` | `service_type` (+ `day_of_week`, 1 = Sunday) | `trips` | trips by hour of day and day of week |
| `zone_od_daily` | `service_type`, `pu_location_id`, `do_location_id` | `trips`, `distance_trips`, `sum_distance_miles` | average distance = `sum_distance_miles / distance_trips` |
| `revenue_daily` (yellow, green; no negative fares) | `service_type`, `pu_location_id`, `pu_borough`, `payment_type`, `payment_label` | `trips`, `sum_total_amount`, `sum_base_fare`, `sum_tip_amount`, `distance_trips`, `sum_distance_miles`, `sum_total_amount_dist`, `sum_congestion_surcharge`, `sum_airport_fee`, `sum_cbd_congestion_fee` | average fare = `sum_total_amount / trips`; fare per mile = `sum_total_amount_dist / sum_distance_miles`; card tip rate = `sum_tip_amount / sum_base_fare` where `payment_type = 1`; card share = trips by `payment_label` / trips |
| `trip_efficiency_daily` (valid timestamps only) | `service_type` | `trips`, `sum_duration_sec`, `speed_trips`, `sum_distance_miles`, `sum_duration_sec_dist` | average duration = `sum_duration_sec / trips`; speed = `sum_distance_miles / (sum_duration_sec_dist / 3600)` |
| `hvfhv_economics_daily` (fhvhv; no negative fares) | `hvfhs_license_num` | `trips`, `sum_base_fare`, `trips_with_pay`, `sum_driver_pay`, `sum_base_fare_with_pay`, `sum_fees`, `sum_passenger_cost` | driver pay share = `sum_driver_pay / sum_base_fare_with_pay`; fee share = `sum_fees / sum_passenger_cost` |
| `hvfhv_service_daily` (fhvhv) | `hvfhs_license_num` | `trips`, `wait_trips`, `sum_wait_sec`, `response_trips`, `sum_response_sec`, `shared_requests`, `shared_matches` | wait = `sum_wait_sec / wait_trips`; response = `sum_response_sec / response_trips`; match rate = `shared_matches / shared_requests` |
| `data_quality_daily` (all rows) | `service_type` | `rows`, and one `n_*` count per `dq_*` flag (`n_dropoff_before_pickup`, `n_zero_distance`, ...) | rate = `n_*` / `rows`; unmatched-zone share = (`n_unmatched_pu_zone` + `n_unmatched_do_zone`) / (2 × `rows`) |

**Definitions.** Valid timestamps = not `dq_dropoff_before_pickup`, not `dq_trip_too_long`, duration not null. Distance-based measures use trips with distance above zero. Wait = `pickup_ts - request_ts` and response = `on_scene_ts - request_ts`, both only when not negative. Passenger cost (HVFHV) = base fare + tolls + bcf + sales tax + congestion surcharge + airport fee + CBD fee (tips excluded); fees = passenger cost - base fare.

**Not in the stream.** The catalog's "row count vs prior month" needs file row counts, which only the batch path has (`data_quality_monthly`).

## Batch-only marts (M5)

Written to `output/batch/gold/<mart>/year=/month=`, where year and month are those of the source file (D31). Every batch Gold table is replaced partition by partition when a month is reloaded.

**Daily marts.** The same eight tables and columns as above, built per loaded month from the batch Silver.

**Monthly marts** = the daily mart summed over the load month, with fewer keys. The first column is `month_start` (the first day of the load month); every other column is a key or a sum.

| Mart | From | Keys | Measures |
|---|---|---|---|
| `trips_monthly` | `trips_daily` | `service_type` | `trips` |
| `revenue_monthly` | `revenue_daily` | `service_type`, `payment_label` | the `revenue_daily` measures, including the fee sums |
| `trip_efficiency_monthly` | `trip_efficiency_daily` | `service_type`, `pickup_hour` | the `trip_efficiency_daily` measures |
| `demand_monthly_hourly` | `demand_hourly` | `service_type`, `pickup_hour` | `trips` |
| `zone_od_monthly` | `zone_od_daily` | `service_type`, `pu_location_id`, `do_location_id` | the `zone_od_daily` measures |
| `hvfhv_economics_monthly` | `hvfhv_economics_daily` | `hvfhs_license_num` | the `hvfhv_economics_daily` measures |
| `data_quality_monthly` | `data_quality_daily` | `service_type` | `rows`, the `n_*` flag counts, and from the load log `bronze_rows`, `invalid_rows`, `duplicates_removed`, `late_correction_rows` |

Growth, rolling averages, seasonality, shares, card-versus-cash trends and the fee-impact comparison are computed by `scripts/queries/*.sql` over these tables. A value that needs months that are not loaded is NULL and flagged "not enough history".

**`batch_run_metrics`** (replaced at the end of every run) has one row per `silver/batch_run_log` row: `run_id`, `source_file`, `dataset`, `source_month`, `status`, the row counts, `reconciliation_diff` (`silver_rows + duplicates_removed + invalid_rows - bronze_rows`, should be 0), `freshness_lag_days` (Gold availability minus TLC publication date), `columns_added` and `columns_removed` against the dataset's previous file, `runtime_sec`, `rows_per_sec`, and `months_expected` (the months in the configured range). `batch_health.sql` turns it into the success rate, backfill completeness and the other KPIs.

## Spark ML tables (M6)

Written by `spark/ml_job.py` to `output/batch/gold/ml_*`, replaced on every run except the metrics table. Predictions are for the **holdout** rows only (`ml.test_percent`, chosen by a hash of the row key), so they are honest out-of-sample values. `ym` is the month (`YYYY-MM`).

| Table | Columns |
|---|---|
| `ml_demand_forecast` | `pu_location_id`, `hour_start`, `ym`, `trips`, `prediction`, `baseline` (same hour last week), `error` |
| `ml_fare_predictions` | `trip_id`, `ym`, `base_fare`, `prediction`, `baseline` (linear fit on distance), `error` |
| `ml_duration_predictions` | `trip_id`, `ym`, `duration_min`, `prediction`, `baseline` (mean per zone pair and hour), `error` |
| `ml_tip_predictions` | `trip_id`, `ym`, `tip20`, `prediction` (probability of a tip of 20% or more), `baseline` (the positive rate), `error` |
| `ml_wait_predictions` | `trip_id`, `ym`, `wait_sec`, `prediction`, `baseline` (mean per zone and hour), `error` |
| `ml_shared_match_predictions` | `trip_id`, `ym`, `shared_match`, `prediction` (probability of a match), `baseline`, `error` |
| `ml_zone_clusters` | `pu_location_id`, `cluster`, `avg_distance`, `avg_fare` (zones with at least `ml.min_zone_trips` trips) |
| `ml_demand_pattern_clusters` | `pickup_date`, `day_of_week`, `cluster` (complete days only) |
| `ml_trip_anomalies` | Real holdout trips flagged by the fare residual: `trip_id`, `service_type`, `pickup_ts`, `distance_miles`, `base_fare`, `predicted_fare`, `residual`, `speed_mph` |
| `ml_model_metrics` | `run_id`, `run_ts`, `model`, `metric`, `scope`, `value`. **Appended** on every run |

`ml_model_metrics` scopes: `test` (holdout metrics: `mae`, `rmse`, `wape`, `r2`, `within_10pct`, or `auc`, `accuracy`, `precision`, `recall`, `f1`, `lift`, and for clusters `silhouette` and `stability_ari`, for anomalies `recall`, `precision`, `flagged_rate`, for PCA `n_features`, `components_for_90pct`, `variance_at_half`), `baseline` (the same metrics of the simple rule), `cv` (`cv_mean`, `cv_std` of the primary metric over the cross-validation folds), `month:YYYY-MM` (the primary metric on that month's holdout rows), `drift` (`metric` = feature name; |mean in the last month - mean in the first month| / std in the first month; absent for one month) and `train` (`train_seconds`). `lift` = (baseline error - model error) / baseline error, with error = 1 - AUC for classifiers. `scripts/evaluate_models.py` prints the latest run against `evaluation.md`.

## Behaviour to know

- A window is written only after the watermark passes it, so the last hour(s) of a finite run are not written until later data arrives.
- Each mart's watermark follows the event time of the rows it reads. The producers share one simulated clock so the four services advance together; a service far ahead of the others would make the slower services' rows late for the marts that read all services (`trips_daily`, `demand_hourly`, `zone_od_daily`, `trip_efficiency_daily`, `data_quality_daily`).
- Rows that arrive after their window closed are dropped by the aggregation. The Silver table still has them.
