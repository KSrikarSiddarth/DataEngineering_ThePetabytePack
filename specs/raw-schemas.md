# Raw event schemas (verified against the 2026-01 files)

Read directly from the parquet footers and data, not recalled from the data dictionaries. The machine-readable
version is `producer/datasets.yaml`; `tests/test_schema_and_e2e.py` fails if the two ever disagree.

One Kafka message = one parquet row as JSON. Timestamps are ISO-8601 strings (`2026-01-02T03:04:05`),
`NaN`/`inf` become `null`, and nulls are preserved. Topic per dataset, key per the table below.

## Dataset overview (2026-01 files)

| Dataset | File | Rows | Topic | Key | Event time | In Jan 2026 | Sorted by pickup? |
|---|---|---|---|---|---|---|---|
| Yellow | `yellow_tripdata_2026-01.parquet` | 3,724,889 | `taxi.yellow.raw` | `PULocationID` | `tpep_pickup_datetime` | 3,724,882 | No |
| Green | `green_tripdata_2026-01.parquet` | 40,272 | `taxi.green.raw` | `PULocationID` | `lpep_pickup_datetime` | 40,250 | No |
| FHV | `fhv_tripdata_2026-01.parquet` | 1,941,722 | `taxi.fhv.raw` | `dispatching_base_num` | `pickup_datetime` | 1,941,722 | No |
| HVFHV | `fhvhv_tripdata_2026-01.parquet` | 20,940,373 | `taxi.hvfhv.raw` | `PULocationID` | `pickup_datetime` | 20,940,373 | No |

Because no file is sorted, the producer sorts before replaying (index-based, so memory stays bounded; see `producer/replay.py`).

## Findings that shaped the design

- **Natural nulls are large.** Yellow: `passenger_count`, `RatecodeID`, `store_and_fwd_flag`, `congestion_surcharge`, `Airport_fee` are each null in 29.2% of rows. Green: 13.4% in several columns, and `ehail_fee` is 100% null. FHV: `PUlocationID` 84.8% null, `DOlocationID` 11.1%, `SR_Flag` 100%. HVFHV: only `originating_base_num` (27.1%). Injected nulls come on top of these.
- **FHV key.** Because `PUlocationID` is mostly null, FHV messages are keyed by `dispatching_base_num` (never null), so partitioning stays meaningful. Spark can still enrich FHV rows that have a location ID.
- **Natural out-of-range rows exist.** 7 Yellow rows and 22 Green rows have a pickup outside January 2026 (earliest Yellow: 2025-12-31 23:57, earliest Green: 2025-12-27). The validation step in M3 should catch them without any injection.
- **New 2026 column.** `cbd_congestion_fee` is present in Yellow, Green and HVFHV.
- **Names differ by dataset.** FHV uses `dropOff_datetime`, `PUlocationID` and `DOlocationID` (different casing from the others). HVFHV uses `trip_miles`, `trip_time`, `base_passenger_fare`, `driver_pay` and has no `total_amount`.

## Yellow — `taxi.yellow.raw`

`VendorID` int · `tpep_pickup_datetime` timestamp · `tpep_dropoff_datetime` timestamp · `passenger_count` int ·
`trip_distance` float · `RatecodeID` int · `store_and_fwd_flag` string · `PULocationID` int · `DOLocationID` int ·
`payment_type` int · `fare_amount` · `extra` · `mta_tax` · `tip_amount` · `tolls_amount` · `improvement_surcharge` ·
`total_amount` · `congestion_surcharge` · `Airport_fee` · `cbd_congestion_fee` (all float)

## Green — `taxi.green.raw`

`VendorID` int · `lpep_pickup_datetime` timestamp · `lpep_dropoff_datetime` timestamp · `store_and_fwd_flag` string ·
`RatecodeID` int · `PULocationID` int · `DOLocationID` int · `passenger_count` int · `trip_distance` float ·
`fare_amount` · `extra` · `mta_tax` · `tip_amount` · `tolls_amount` · `ehail_fee` · `improvement_surcharge` ·
`total_amount` (float) · `payment_type` int · `trip_type` int · `congestion_surcharge` · `cbd_congestion_fee` (float)

## FHV — `taxi.fhv.raw`

`dispatching_base_num` string · `pickup_datetime` timestamp · `dropOff_datetime` timestamp · `PUlocationID` int ·
`DOlocationID` int · `SR_Flag` int · `Affiliated_base_number` string

No fare, distance or payment fields.

## HVFHV — `taxi.hvfhv.raw`

`hvfhs_license_num` · `dispatching_base_num` · `originating_base_num` (string) · `request_datetime` ·
`on_scene_datetime` · `pickup_datetime` · `dropoff_datetime` (timestamp) · `PULocationID` · `DOLocationID` (int) ·
`trip_miles` float · `trip_time` int · `base_passenger_fare` · `tolls` · `bcf` · `sales_tax` ·
`congestion_surcharge` · `airport_fee` · `tips` · `driver_pay` (float) · `shared_request_flag` ·
`shared_match_flag` · `access_a_ride_flag` · `wav_request_flag` · `wav_match_flag` (string) · `cbd_congestion_fee` float

## Message headers

When the producer injects a fault, it adds a Kafka header `fault` (comma-separated tags, e.g.
`invalid:negative_amount`, `null_fields`, `late`, `out_of_order`, `duplicate`, `malformed_json`, `schema_drift`).
The message body is unchanged by the tag. Spark ignores the header; tests and reconciliation use it to separate
injected problems from natural ones.
