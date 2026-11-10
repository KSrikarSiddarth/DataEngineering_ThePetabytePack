# silver-schema.md — `silver.trips` and its companions

The contract for the Silver layer, built in M4 from `specs/2_nyc_taxi_medallion_architecture.html` and `specs/3_KPI.html`. The code is `spark/transform.py` (`SILVER_FIELDS`, `INVALID_FIELDS`) and `spark/dims.py`; `tests/test_spark_transform.py` checks the layout. The stream writes it to `output/stream/silver/`; the batch path (M5) writes the same schema to `output/batch/silver/`.

## silver.trips

One row per trip, all four services, partitioned by `service_type`. `service_type` is `yellow`, `green`, `fhv` or `fhvhv` (the datasets and topics keep the name `hvfhv`).

| Column | Type | Meaning / source |
|---|---|---|
| `trip_id` | string | First 32 hex characters of SHA-256 over `service_type` and every identifying column (times, zones, distance, amounts, flags, extras). TLC has no key; two trips identical in every field collapse into one. |
| `service_type` | string | See above |
| `hvfhs_license_num` | string | HVFHV only (`HV0002` Juno, `HV0003` Uber, `HV0004` Via, `HV0005` Lyft) |
| `request_ts`, `on_scene_ts` | timestamp | HVFHV only |
| `pickup_ts`, `dropoff_ts` | timestamp | `tpep_/lpep_/pickup_datetime`, `dropoff/dropOff`. `pickup_ts` is never null in Silver. |
| `pickup_date`, `pickup_hour` | date, int | From `pickup_ts` |
| `pu_location_id`, `do_location_id` | int | Zone IDs |
| `pu_borough`, `pu_zone`, `do_borough`, `do_zone` | string | From the zone lookup; null when the ID is null or not in the lookup |
| `distance_miles` | double | `trip_distance` / `trip_miles`; null for FHV |
| `duration_sec` | long | `dropoff_ts - pickup_ts`; can be negative (flagged) |
| `passenger_count`, `ratecode_id`, `payment_type` | int | Yellow/Green only (`passenger_count` etc. are null for FHV and HVFHV) |
| `payment_label` | string | From `payment_type` via `dim_payment_type`; `Unknown` for an unlisted code, null when there is no type |
| `base_fare`, `tip_amount`, `total_amount` | double | `fare_amount` / `base_passenger_fare`, `tip_amount` / `tips`, `total_amount` (null for HVFHV) |
| `driver_pay` | double | HVFHV only |
| `tolls`, `bcf`, `sales_tax`, `congestion_surcharge`, `airport_fee`, `cbd_congestion_fee` | double | Typed fee columns, null where the service does not have them |
| `shared_request_flag`, `shared_match_flag` | string | HVFHV only (`Y`/`N`) |
| `dq_*` | boolean | Never null. See below. |
| `extras` | string | JSON of the source columns not mapped above |
| `kafka_partition`, `kafka_offset`, `kafka_timestamp` | int, long, timestamp | Lineage. `kafka_timestamp` is the broker append time (null in the batch path). |
| `silver_write_ts` | timestamp | Start time of the micro-batch that wrote the row. With `kafka_timestamp` it gives the ingest-to-Silver latency (`evaluation.md` S1). |

### Data-quality flags (D27: flagged and kept, not rejected)

| Flag | True when |
|---|---|
| `dq_dropoff_before_pickup` | `dropoff_ts < pickup_ts` |
| `dq_zero_distance` | service has a distance (not FHV) and it is null, zero or negative |
| `dq_negative_fare` | `base_fare`, `tip_amount` or `total_amount` is negative |
| `dq_unmatched_pu_zone`, `dq_unmatched_do_zone` | the zone ID is present but not in the lookup |
| `dq_null_dropoff_ts`, `dq_null_pu_location`, `dq_null_do_location` | the field is null (about 85% of FHV rows have no pickup zone) |
| `dq_pickup_out_of_range` | pickup outside the expected month plus the tolerance (`processing.validation`) |
| `dq_trip_too_long` | duration over `max_trip_hours` |

`dq_pickup_out_of_range` and `dq_trip_too_long` are not in the medallion page; they replace the M3 rejects for those cases so Gold can filter on them.

## silver_invalid

Rows that cannot be used at all, partitioned by `service_type`: `service_type`, `kafka_partition`, `kafka_offset`, `kafka_timestamp`, `reject_reason`, `raw_value`. `reject_reason` is `malformed_json` or `null_pickup_time`.

## Deduplication

Streaming only: `dropDuplicatesWithinWatermark(["trip_id"])` with the watermark on `kafka_timestamp` (`processing.dedup_watermark_minutes`, default 2). A copy is removed only if it arrives within that time of the first one. The watermark is on ingest time, not pickup time, so late trips (old pickup time) are not mistaken for expired state and stay in Silver (D30).

## Dimensions

Written once per run to `output/stream/silver/dim_*`, from `spark/dims.py` and the zone lookup:

| Table | Columns |
|---|---|
| `dim_zone` | `location_id`, `borough`, `zone`, `service_zone` |
| `dim_payment_type` | `payment_type`, `payment_label` |
| `dim_ratecode` | `ratecode_id`, `ratecode_label` |
| `dim_hvfhs_company` | `hvfhs_license_num`, `company` |

## Batch path (M5)

The batch job writes the same `silver.trips` and `silver_invalid` to `output/batch/silver/trips` and `output/batch/silver_invalid`, partitioned by `service_type/year/month`, where year and month are those of the **source file** (D31). `kafka_*` columns are null; `silver_write_ts` is the load time. Duplicates are removed per file with `dropDuplicates(["trip_id"])`. The dimension tables are written to `output/batch/silver/dim_*`.

`silver.batch_run_log` (append only, `output/batch/silver/batch_run_log`) has one row per file load: `run_id`, `source_file`, `dataset`, `service_type`, `source_month`, `tlc_published_date`, `bronze_rows`, `invalid_rows`, `silver_rows`, `duplicates_removed`, `late_correction_rows`, `schema_hash`, `columns`, `start_ts`, `end_ts`, `gold_available_ts`, `status`, `error`. Source rows = `silver_rows + invalid_rows + duplicates_removed` for a successful load.

## ml_features (M6)

Written by `spark/ml_job.py` (code in `spark/ml_features.py`) to `output/batch/silver/ml_features/`, replaced on every run. Two tables:

**`zone_hour`**: one row per pickup zone and hour on a complete grid of the zones and days present (0 trips for an hour without trips), from the gold `zone_od_daily` mart. Rows without 1, 24 and 168 hours of history are dropped.

| Column | Meaning |
|---|---|
| `pu_location_id`, `hour_start`, `ym` | Zone, hour, month (`YYYY-MM`) |
| `trips` | Label of the demand forecast |
| `lag_1h`, `lag_24h`, `lag_168h` | Trips of the same zone 1, 24 and 168 hours earlier (`lag_168h` is the baseline, "same hour last week") |
| `hour`, `day_of_week`, `is_weekend`, `is_holiday` | Calendar; holidays come from `ml.holidays` in `stream-config.yaml` |

**`trips`**: a deterministic sample (a hash of `trip_id`, at most `ml.sample_rows` per service) of **clean** Silver trips, meaning no `dq_*` flag, for yellow, green and fhvhv (FHV has no distance or fare).

| Column | Meaning |
|---|---|
| `trip_id`, `service_type`, `pickup_ts`, `ym` | Key, service, pickup time, month |
| calendar columns | As above |
| `pu_location_id`, `do_location_id`, `distance_miles`, `ratecode_id`, `passenger_count` | Trip features (null ratecode and passengers become 0) |
| `service_code`, `company` | 1 yellow, 2 green, 3 fhvhv; HVFHV license number (`HV0003` gives 3), 0 otherwise |
| `pu_zone_demand`, `do_zone_demand` | Zone profile: mean trips per hour of the pickup and dropoff zone |
| `base_fare`, `duration_min` | Labels of the fare and duration models |
| `tip20` | Card trips with a fare: 1 if tip is at least 20% of the base fare, else 0; null otherwise |
| `wait_sec` | HVFHV: `pickup_ts - request_ts`, kept when 0 to 3600 s; null otherwise |
| `shared_match` | HVFHV shared requests: 1 if matched, else 0; null otherwise |

**No leakage (O4).** `dropoff_ts`, `duration_sec`, `total_amount`, `tip_amount`, `driver_pay` and `on_scene_ts` are never model features; `FEATURES` in `ml_features.py` lists what each model reads and `tests/test_ml.py` checks it. Labels are in the table but never in a model's own feature list. `base_fare` is a feature only of the tip model (the fare is known before the tip is given).
