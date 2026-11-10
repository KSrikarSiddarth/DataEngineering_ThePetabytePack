# PLAN.md — NYC Taxi Stream + Batch Pipeline (Kafka + Spark + Docker Compose)

Spec-driven development: each milestone has a spec, acceptance criteria, and a verification command. Implement in order; do not start a milestone until the previous one's acceptance criteria pass.

## 1. Goal

A simple, locally runnable big data pipeline with **two processing paths over the same NYC TLC trip data** (Yellow, Green, FHV, and HVFHV). Everything runs with Docker Compose.

1. **Stream processing.** Trip records are replayed as streams through Kafka and processed with Spark Structured Streaming (parse, validate, enrich, deduplicate, windowed aggregation) into Silver trips and the daily Gold KPI marts. It answers "what is happening now?".
2. **Batch processing of historic data.** Many months of trip files are processed by a Spark batch job into Silver and into the daily, monthly and pipeline-health Gold marts, queried with SQL engines (DuckDB, Spark SQL). It answers "what happened over time?" and shows how queries behave on hundreds of millions of rows. Spark ML models (demand, fare, duration, tips, clustering, anomalies) are trained on this history.

The data is organized in **Bronze, Silver and Gold layers** (`specs/2_nyc_taxi_medallion_architecture.html`), and the KPIs it must answer are defined in `specs/3_KPI.html`. Both paths share the same Silver model and the same validation and enrichment code, so a trip is judged the same way in either path, and the two can be reconciled against each other. The stream's output is **never merged into the historic store**; the two are joined only at query time, through views with a cutover date (§6.8).

## 2. Scope

**In scope**
- Config-driven producer that replays all four TLC datasets (Yellow, Green, FHV, HVFHV) into one Kafka topic each, simulating real-time via event time.
- Normalization of the four schemas into one unified trip model, so downstream applications can query across services.
- Kafka (KRaft mode, single broker) with a raw trips topic.
- Spark Structured Streaming job: parse, validate, enrich with taxi zones, aggregate in windows.
- Sink: Parquet files (and console) on a mounted volume; optional second topic for aggregates.
- **Medallion layers** (Bronze, Silver, Gold) for both paths, and every KPI in `specs/3_KPI.html` with its Gold table (§6.6, §6.7).
- **Batch path:** download a range of months (default 24) of all four datasets; a Spark batch job that reuses the stream's normalize, validate and enrich logic and writes Silver partitioned by `service_type/year/month`, plus the monthly and pipeline-health Gold marts; a query layer (DuckDB and Spark SQL) with a benchmark script that times the KPI queries.
- **Spark ML** on the historic data: the models and model-operations KPIs in the catalog (M6).
- Single-command startup, health checks, README with run instructions.

**Out of scope (initially)**
- Cloud deployment, multi-broker clusters, schema registry, auth, dashboards (stretch goal only).

## 3. Existing Inputs (in `data/`; fetch the trip files with `python scripts/download_data.py`)

**Data source:** NYC TLC Trip Record Data — https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page (monthly Parquet files for Yellow, Green, FHV, and HVFHV trips, plus the data dictionaries, zone lookup CSV, and taxi zone shapefile). The stream path replays one month (2026-01). The batch path needs many months: by default 24 months, 2024-01 to 2025-12, which **ends before the streamed month** so the two stores never overlap (D28). 12 months is not enough for the year-over-year and policy-date KPIs in the catalog (D25). Roughly 13–14 GB of raw files for the batch range, plus the streamed month, of which HVFHV is most (about 500M rows). `scripts/download_data.py --months ...` already fetches any range.

| File | Use |
|---|---|
| `yellow_tripdata_2026-01.parquet` | Yellow source stream (present) |
| `green_tripdata_2026-01.parquet` | Green source stream (to download into `data/`) |
| `fhv_tripdata_2026-01.parquet` | FHV source stream (to download into `data/`) |
| `fhvhv_tripdata_2026-01.parquet` | HVFHV source stream (to download into `data/`) |
| `taxi_zone_lookup.csv` | Static enrichment (LocationID → Borough, Zone) |
| `taxi_zones/` shapefile | Not needed for MVP (stretch: geo maps) |
| `data_dictionary_trip_records_yellow.pdf` | Field reference for schema |
| `specs/1_nyc_taxi_field_reference.html` | Field reference for all datasets |
| `specs/2_nyc_taxi_medallion_architecture.html` | Bronze, Silver and Gold layers and tables (§6.7) |
| `specs/3_KPI.html` | KPI catalog: question, calculation and Gold table per KPI (§6.6) |

## 4. Architecture

```
4 parquet datasets ──> [producer ×4] ──> Kafka topics: taxi.yellow.raw, taxi.green.raw,
                                          taxi.fhv.raw, taxi.hvfhv.raw
                                      │
                                      ▼
                        [Spark Structured Streaming]
                         parse → validate → enrich(zone lookup)
                         → watermark + window aggregations
                              │                  │
                              ▼                  ▼
                   output/stream/silver/trips   output/stream/gold/*  (Parquet)
                   (+ silver_invalid for rejects)

            STREAM PATH (above): live, one month replayed, daily Gold marts

24 months of parquet ──> [Spark batch job] ──> output/batch/silver/trips (service_type/year/month)
(data/)                   reuse normalize →             │
                          validate → enrich →           ▼
                          deduplicate        output/batch/gold/*  (daily, monthly, health)
                                                        │
                                          ┌─────────────┴─────────────┐
                                          ▼                           ▼
                                 [DuckDB / Spark SQL]         [Spark ML, M6]
                                 KPI queries + benchmark      output/batch/gold/ml_*
            BATCH PATH (above): history, month-scale questions, ad hoc SQL, models
```

**Layers (details in §6.7):** Bronze = the Kafka topics and the files in `data/` (not copied again); Silver = cleaned, conformed trips plus dimensions; Gold = the KPI marts.

### Services (docker-compose.yml)

| Service | Image / Build | Notes |
|---|---|---|
| `kafka` | `apache/kafka` (KRaft, no ZooKeeper) | Ports 9092 (host), 29092 (internal) |
| `kafka-init` | same image | Creates topics, then exits |
| `spark-master` / `spark-worker` | `apache/spark` (or bitnami equivalent) | Or single `spark` container in local mode for MVP |
| `spark-job` | build from `./spark` | Submits streaming job with Kafka connector package |
| `producer-yellow`, `producer-green`, `producer-fhv`, `producer-hvfhv` | one image built from `./producer` (Python), different `DATASET` env | Each replays one dataset to its topic |
| `spark-batch` | build from `./spark` (same image as `spark-job`) | Compose profile `batch`; runs the historic backfill, then exits. Needs no Kafka |
| `spark-ml` | build from `./spark` (same image) | Compose profile `ml`; trains and scores the Spark ML models on the batch Silver/Gold data (M6), then exits |
| `kafka-ui` (optional) | `provectuslabs/kafka-ui` | Port 8080, for inspection |

Pin all image versions; keep Spark and its Kafka connector (`spark-sql-kafka-0-10`) versions matched, along with Scala version.

## 5. Repository Layout

```
.
├── PLAN.md
├── README.md
├── docker-compose.yml
├── .env                      # versions, topic names, replay speed
├── data/                     # input data (mounted read-only)
├── producer/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── datasets.yaml         # per-dataset topic, file pattern, event-time col, key col
│   └── producer.py
├── spark/
│   ├── Dockerfile
│   ├── transform.py          # shared by both paths: normalize, enrich, conform to Silver, dq flags
│   ├── dims.py               # code-to-label dimension tables (M4)
│   ├── stream_job.py         # Kafka -> Silver -> Gold (M4)
│   ├── gold.py               # M4/M5: Gold mart definitions shared by stream and batch
│   ├── batch_job.py          # M5: historic backfill, Gold monthly and health marts
│   └── ml_job.py             # M6: Spark ML models
├── scripts/                  # download_data.py, reconcile.py, show_results.py, evaluate_stream.py,
│                             #   benchmark_queries.py, combined_views.py, queries/*.sql (M5)
├── specs/                    # one spec per component (see §6)
├── tests/
│   ├── test_schema.py
│   └── smoke_test.ps1
└── output/                   # sink (gitignored)
```

## 6. Specs (contracts)

### 6.1 Raw event schemas — one topic per dataset

| Dataset | Topic | File pattern | Event-time column | Key | Notable differences |
|---|---|---|---|---|---|
| Yellow | `taxi.yellow.raw` | `yellow_tripdata_YYYY-MM.parquet` | `tpep_pickup_datetime` | `PULocationID` | Full fare fields |
| Green | `taxi.green.raw` | `green_tripdata_YYYY-MM.parquet` | `lpep_pickup_datetime` | `PULocationID` | `lpep_*` times, `trip_type`, `ehail_fee` |
| FHV | `taxi.fhv.raw` | `fhv_tripdata_YYYY-MM.parquet` | `pickup_datetime` | `dispatching_base_num` | No fare/distance fields; `PUlocationID` is ~85% null, so it cannot be the key |
| HVFHV | `taxi.hvfhv.raw` | `fhvhv_tripdata_YYYY-MM.parquet` | `pickup_datetime` | `PULocationID` | Driver pay, wait times, shared-ride flags; very large |

File patterns and column names above were **verified against the real 2026-01 files in M2**. The full schemas and the data findings are in `specs/raw-schemas.md`, and the machine-readable copy is `producer/datasets.yaml`.

**Yellow (`taxi.yellow.raw`)**
- Key: `PULocationID` as string (keeps partition affinity by pickup zone).
- Value: JSON, one trip per message, fields following the yellow data dictionary: `VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, Airport_fee` (verified against the file; it also has `cbd_congestion_fee`; see `specs/raw-schemas.md`).
- Timestamps: ISO-8601 strings. Nulls preserved as JSON `null`.
- Partitions: 3 per topic (HVFHV: 6). Retention: 1 day. JSON, ISO-8601 timestamps, and null handling follow the Yellow rules for every dataset.

### 6.2 Producer behavior
- Reads parquet in batches (pyarrow) — never loads the whole file into memory.
- Sorts by the dataset's pickup column (the TLC files are not sorted; the sort runs on indices to bound memory) and replays with configurable speedup `REPLAY_SPEEDUP` (e.g. 3600 = 1 simulated hour per real second); `0` = as fast as possible.
- One code path for all datasets: `DATASET` (yellow|green|fhv|hvfhv) selects topic, file pattern, event-time column, and key column from `producer/datasets.yaml`.
- Config via env vars: `DATASET`, `KAFKA_BOOTSTRAP`, `REPLAY_SPEEDUP`, `MAX_RECORDS` (0 = all), `DATA_DIR`. HVFHV defaults to a capped `MAX_RECORDS` (see Decisions) because of its volume.
- All four producers use the same simulated clock (`REPLAY_SPEEDUP` applied to the same start time) so the streams stay roughly time-aligned.
- Retries Kafka connection on startup; exits 0 on completion; logs throughput every N records.

### 6.3 Spark job behavior
1. Read the four raw topics (`startingOffsets=earliest`), tagging each row with `service` (yellow|green|fhv|hvfhv) from the topic name.
2. Parse JSON per dataset with an explicit `StructType` (no schema inference); cast timestamps. Then **normalize** every dataset to one typed trip model using the `normalize` section of `producer/datasets.yaml` (times, zones, distance, fares, driver pay, typed fees, shared-ride flags, rate code); what is not mapped is kept as JSON in `extras`. Fields a dataset lacks (e.g. fares for FHV) are null. *(Built in M4: the output is `silver.trips`, §6.7 and `specs/silver-schema.md`.)*
3. **Validation** (D27) — only rows that cannot be used are *rejected* to `silver_invalid`: malformed JSON and null pickup time. Everything else stays in Silver with a boolean `dq_*` flag (dropoff before pickup, zero or negative distance, negative fare, unmatched or null zone, pickup out of range, trip over 24 hours). Rules that need a field apply only to datasets that have it.
4. **Enrichment** — broadcast join with `taxi_zone_lookup.csv` for pickup and dropoff borough/zone.
5. **Deduplicate** on `trip_id` with `dropDuplicatesWithinWatermark`, with the watermark on Kafka ingest time (D30). The removed rows are derived by `scripts/reconcile.py` (sent - Silver - invalid).
6. **Gold daily marts** (M4; `spark/gold.py`): a second stage reads the Silver Parquet back as a stream and builds `trips_daily`, `demand_hourly`, `zone_od_daily`, `revenue_daily`, `trip_efficiency_daily`, `hvfhv_economics_daily`, `hvfhv_service_daily`, `data_quality_daily` with event-time windows and a 10-minute watermark. The grain is the hour and the measures are additive, so a day is a sum by `pickup_date` (D29, `specs/gold-schema.md`). Services without the needed fields are excluded (e.g. no FHV in revenue). *(Replaces the earlier `agg_zone_hourly`, `agg_service_hourly` and `agg_payment_5min` proposal.)*
7. **Sinks**: Silver trips, `silver_invalid` and the dimension tables as Parquet under `output/stream/silver*`, Gold marts under `output/stream/gold/<mart>` (append mode), checkpoints in `/output/checkpoints/<query>`; console sink in dev mode. A finite (`available_now`) run finishes Silver before starting Gold.
8. Must restart cleanly from checkpoint without duplicating output (tested with the file source).

### 6.4 Batch job behavior (historic data, M5)
1. Read the monthly Parquet files for each dataset from `data/` (all months in the configured range) with Spark's batch reader, using the explicit per-dataset schema. No Kafka and no JSON round-trip: `spark/transform.py` gets a typed-Parquet entry point that skips `from_json`, and the normalize, enrich and validate steps are the same functions the stream uses.
2. Same validation policy as the stream (D27), same Silver model (§6.7), same deduplication on `trip_id`. Unusable rows go to `output/batch/silver_invalid`, partitioned the same way.
3. Write Silver trips to `output/batch/silver/trips`, partitioned by `service_type/year/month` (year and month of the pickup time), as Parquet. The write is idempotent: rerunning a month overwrites only that month's partition (dynamic partition overwrite).
4. Write one row per file load to `silver.batch_run_log` (`source_file`, `tlc_published_date`, `bronze_rows`, `silver_rows`, `duplicates_removed`, `schema_hash`, `start_ts`, `end_ts`, `status`). The pipeline-health KPIs are computed from it. `tlc_published_date` comes from the download script, which records it for each file.
5. Build the Gold marts: daily (same definitions as the stream, over the full range), monthly, and pipeline health, into `output/batch/gold/`.
6. Fault injection does not apply to the batch path: it reads the original files. Real data problems (nulls, rows outside the month, negative values) are flagged by the `dq_*` rules.

**Query layer.** The Silver and Gold Parquet are queried in place (no load step):
- DuckDB (`scripts/show_results.py --store batch`, or `duckdb` directly) and Spark SQL (`spark-sql` via the batch image) over the same Parquet files.
- `scripts/queries/*.sql` holds the KPI queries (§6.6). `scripts/benchmark_queries.py` runs them on both engines, prints timings, and compares a query that can use partition pruning and column selection against the same query forced to scan everything.

### 6.5 Config (`.env`)
`KAFKA_VERSION, SPARK_VERSION, SCALA_VERSION, TOPIC_INVALID, REPLAY_SPEEDUP, MAX_RECORDS, MAX_RECORDS_HVFHV` (raw topic names come from `producer/datasets.yaml`). Batch settings (`batch:` section: months range, datasets, output paths, shuffle partitions) and the `combined:` section (`cutover_date`) live in `stream-config.yaml`.

### 6.6 KPIs

The KPI catalog is **`specs/3_KPI.html`** and is the source of truth: each KPI, the question it answers, its calculation and its Gold table. This section only says which path and milestone builds each group. (The catalog notes that it was written without a `KPI.md`; reconcile it if one exists.)

| KPI group | Scope | Gold tables | Built by | Milestone |
|---|---|---|---|---|
| Demand and volume | All services | `trips_daily`, `demand_hourly`, `zone_od_daily` | Stream (daily marts), rebuilt by batch | M4, M5 |
| Revenue and pricing | Yellow, Green | `revenue_daily` | Stream, batch | M4, M5 |
| Trip efficiency | All with valid timestamps | `trip_efficiency_daily` | Stream, batch | M4, M5 |
| HVFHV platform | HVFHV | `hvfhv_economics_daily`, `hvfhv_service_daily` | Stream, batch | M4, M5 |
| Data quality | All services | `data_quality_daily` | Stream, batch (the "row count vs prior month" KPI is batch only) | M4, M5 |
| Historical trends | Monthly grain, full history | `trips_monthly`, `revenue_monthly`, `trip_efficiency_monthly`, `demand_monthly_hourly`, `zone_od_monthly`, `hvfhv_economics_monthly` | Batch only | M5 |
| Batch pipeline health | Batch runs | `batch_run_metrics`, `data_quality_monthly` | Batch only | M5 |
| Machine learning (Spark ML) | Per model | `ml_demand_forecast`, `ml_fare_predictions`, `ml_duration_predictions`, `ml_tip_predictions`, `ml_wait_predictions`, `ml_shared_match_predictions`, `ml_zone_clusters`, `ml_demand_pattern_clusters`, `ml_trip_anomalies` | Batch only | M6 |
| Model operations | All models | `ml_model_metrics` | Batch only | M6 |

**Assumptions in this split** (the catalog does not say which path builds a mart; confirm in D26):
- The stream path builds the daily marts incrementally from streamed Silver, so the "what is happening now" side shows up in the same KPI definitions as the batch side. The catalog has no minute-scale KPIs, so none are added.
- The batch path owns everything that needs history or run metadata: the monthly marts, pipeline health, ML.

**History needed.** Several Historical-trends KPIs need more than 12 months: year-over-year growth needs 13 or more months (24 for a full series), seasonality needs at least two cycles, and the "fee impact around policy dates" KPI compares before and after the CBD congestion fee (introduced January 2025) and the Airport fee (2022). See D25.

**Field limits.** FHV has no fares, distance, or shared-ride fields, and about 85% of its pickup zones are null, so it appears only in volume, demand, zone and efficiency marts. Tips exist only for card payments.

**Project-specific KPIs (not in the catalog).**
- Totals reconcile: for the streamed month, the stream's and the batch's daily marts match, apart from trips dropped as too late by the watermark and rows altered by fault injection (checked in M5).
- Query performance: timing of each KPI query on DuckDB and Spark SQL, and partition pruning versus a full scan (M5).
- Live against the baseline: streamed days compared with the batch history for the same weekday and hour, through the combined views (§6.8, M5).

### 6.7 Medallion data model

The layers follow **`specs/2_nyc_taxi_medallion_architecture.html`**. `specs/1_nyc_taxi_field_reference.html` is the field reference.

| Layer | Stream path | Batch path |
|---|---|---|
| **Bronze** (raw, as landed, original column names) | Kafka raw topics (`taxi.*.raw`) | TLC Parquet files in `data/` |
| **Silver** (cleaned, conformed, deduplicated) | `output/stream/silver/trips` plus `silver_invalid` | `output/batch/silver/trips` (partitioned by `service_type/year/month`) plus `silver_invalid` |
| **Gold** (KPI marts) | `output/stream/gold/<table>` (daily marts) | `output/batch/gold/<table>` (daily, monthly, health, ML) |

Bronze is not copied a second time: the Kafka topics and the downloaded files are the landing layer, which avoids another 7 GB or more of disk (D26). **The stream's output is never appended to the historic store (D28).** Stream and batch write to separate folders, and the batch range ends before the streamed month, so the two stores cover different dates. They are combined only at query time (§6.8).

**Silver tables** (full column lists in the spec): `silver.trips` (one row per trip: `trip_id`, `service_type`, `request_ts`, `on_scene_ts`, `pickup_ts`, `dropoff_ts`, `pu_location_id`, `do_location_id`, `distance_miles`, `duration_sec`, `passenger_count`, `payment_type`, `base_fare`, `tip_amount`, `total_amount`, `driver_pay`, `shared_request_flag`, `shared_match_flag`, and the `dq_*` flags), `dim_zone`, `dim_payment_type`, `dim_ratecode`, `dim_hvfhs_company`, `batch_run_log` (batch), and `ml_features` (M6).

**Silver transformations** (spec): rename the timestamp columns to `pickup_ts`/`dropoff_ts`; cast types; map `trip_distance`/`trip_miles` to `distance_miles`; map `tip_amount`/`tips` to one tip column; add `service_type`; union the four sources; derive `duration_sec`; compute the dq flags; deduplicate.

**Gaps and conflicts found when comparing the specs with the current code.** Resolved in M4 as proposed below, except gap 7 (M5); the result is in `specs/silver-schema.md`. Gap 5 is D27 and still awaits confirmation. Gap 4 has a measured side effect: trips identical in every field collapse (FHV, which has few fields, lost 25 per 20,000 rows beyond the injected duplicates, see D30).

| # | Gap | Proposed resolution |
|---|---|---|
| 1 | The KPI catalog uses `hvfhs_license_num`, `payment_label`, `pickup_date` and `pickup_hour`, which are not in the `silver.trips` column list. | Add them to Silver. |
| 2 | "Fee share of passenger cost" (HVFHV) and "fee impact around policy dates" need the fee columns (tolls, bcf, sales tax, congestion surcharge, airport fee, CBD congestion fee). Only `extras` holds them today. | Carry them as typed Silver columns. |
| 3 | The medallion page has `dq_unmatched_zone`; the KPI page uses `dq_unmatched_pu_zone` and `dq_unmatched_do_zone`, `dq_negative_fare` and `dq_null_*`. | Use the KPI page's names. |
| 4 | TLC files have no trip key, but Silver needs `trip_id` and deduplication. | Derive a deterministic hash of the identifying fields. Two genuinely identical trips would collapse; accepted and documented. |
| 5 | **Validation policy.** M3 rejects trips with dropoff before pickup, negative distance or fare, and out-of-range pickup times (D22). The catalog's data-quality KPIs measure those same cases as flags on Silver (`dq_dropoff_before_pickup`, `dq_zero_distance`, `dq_negative_fare`). | Keep those rows in Silver with a `dq_*` flag, and have Gold filter on the flags. Only unparseable or null-pickup rows go to `silver_invalid`. See D27. |
| 6 | The specs write the service as `fhvhv`; the code, topics and `datasets.yaml` use `hvfhv`. | Silver and Gold use the spec's `fhvhv`; topics and dataset keys keep `hvfhv`. |
| 7 | The streamed month and the batch range need to cover the multi-year KPIs. | Longer default range (D25). |

### 6.8 Combining stream and batch at query time (D28)

The stream's results are not written into the historic store. The two are joined only when queried, through views over the Gold (and Silver) tables:

- **Cutover date** `combined.cutover_date` in `stream-config.yaml` (default `2026-01-01`, the first day of the streamed month). The batch range ends the month before.
- **View definition.** For each daily mart, `combined.<table>` = batch rows with date **before** the cutover, `UNION ALL` stream rows with date **on or after** it. Each date comes from exactly one side, so nothing is counted twice. The batch result is authoritative before the cutover and the stream result from the cutover on.
- **Where.** DuckDB views created by `scripts/combined_views.py` over `output/batch/gold/*` and `output/stream/gold/*` (no data copied), and the same logic as Spark SQL temp views.
- **Tables covered.** The daily marts that both paths build (`trips_daily`, `demand_hourly`, `zone_od_daily`, `revenue_daily`, `trip_efficiency_daily`, `hvfhv_economics_daily`, `hvfhv_service_daily`, `data_quality_daily`). Monthly, health and ML tables exist only on the batch side and are not combined.
- **Live against the baseline.** `scripts/queries/live_vs_baseline.sql` compares the streamed days with the batch history, for example "demand in this zone this hour compared with its usual level for the same weekday and hour".
- **Check.** The stream/batch comparison for the streamed month (M5) is separate: the batch job is run once on 2026-01 into a throwaway folder (`output/check/`), compared with the stream, then deleted. It is never part of the combined view.

### 6.9 Evaluation targets

Numerical targets for streaming latency (S1 to S7) and model accuracy (M1 to M10, O1 to O4) are in **`evaluation.md`**. They are proposed, not yet measured. M4 adds the `kafka_timestamp` and `silver_write_ts` columns to `silver.trips` so that latency can be measured, and writes `scripts/evaluate_stream.py`. M6 writes `scripts/evaluate_models.py`. M8 reports measured values against the targets in the README.

## 7. Milestones

### M1 — Infrastructure up
- Compose file with `kafka` + `kafka-init` + optional `kafka-ui`; healthcheck on broker.
- **Accept:** `docker compose up -d kafka kafka-init` → topics `taxi.yellow.raw`, `taxi.green.raw`, `taxi.fhv.raw`, `taxi.hvfhv.raw`, and `taxi.trips.invalid` exist (`kafka-topics.sh --list`).

### M2 — Producer
- Download the Green, FHV, and HVFHV files for 2026-01 into `data/`; inspect all four parquet schemas and write `specs/` schemas plus `producer/datasets.yaml`.
- Implement `producer.py` (dataset-driven) + Dockerfile.
- **Accept:** for each of the four datasets, with `MAX_RECORDS=1000 REPLAY_SPEEDUP=0`, a consumer reads exactly 1000 messages from that dataset's topic matching spec 6.1; keys are set; JSON validates against that dataset's schema test.

### M3 — Spark: parse + validate + enrich
- Implement steps 1–4 (multi-topic read, per-dataset parse, normalization, validation, enrichment) writing clean trips to Parquet.
- **Accept:** with 1000 records produced per dataset (4000 total), `output/trips_clean` row count + invalid count = 4000, and the per-`service` counts reconcile with each producer's `messages_sent` (`python scripts/reconcile.py`; injected duplicates count as separate messages and are not removed in M3); enriched rows have non-null `PUZone` for valid location IDs.

### M4 — Silver conformance and stream Gold (daily marts) — built 2026-11-09
**Status.** Code and tests are done and verified on file input (no Kafka, no Docker); the Kafka source, the Docker image and `scripts/evaluate_stream.py` against a real run are not verified. Acceptance below: 1 (schema test) done; 2 (reconcile) done on an 80,000-message run, with the removed-duplicates rule in D30; 3 done (on the same run, all 389 written service-hours equal the Silver count, and one zone-day matches DuckDB: 69 = 69); 4 done (the eight marts, columns in `specs/gold-schema.md`).

Two parts. Both follow `specs/2_nyc_taxi_medallion_architecture.html` and `specs/3_KPI.html` (§6.6, §6.7).

**A. Silver conformance (rework of the M3 output).**
- Change the unified model to `silver.trips` as in §6.7: renamed columns, `trip_id`, `duration_sec`, the extra typed columns, the `dq_*` flags, and (stream only) `kafka_timestamp` and `silver_write_ts` so latency can be measured (`evaluation.md` S1). Write `scripts/evaluate_stream.py`. Add the dimension tables (`dim_zone`, `dim_payment_type`, `dim_ratecode`, `dim_hvfhs_company`).
- Deduplicate on `trip_id` with `dropDuplicatesWithinWatermark`, and count the removed rows.
- Apply the validation policy chosen in D27 (reject vs flag). Move the outputs to `output/stream/silver/trips` and `output/stream/silver_invalid` (M3 wrote `output/trips_clean` and `output/invalid`).

**B. Stream Gold: the daily marts**, built by event-time windowed aggregations with a watermark and checkpoints: `trips_daily`, `demand_hourly`, `zone_od_daily`, `revenue_daily`, `trip_efficiency_daily`, `hvfhv_economics_daily`, `hvfhv_service_daily`, `data_quality_daily` (KPI groups Demand, Revenue, Efficiency, HVFHV and Data quality). The streaming grain is the hour; the daily date columns are roll-ups. Written to `output/stream/gold/<table>`.

- **Accept:**
  1. A schema test compares the Silver output with `specs/silver-schema.md` (written in this milestone from §6.7).
  2. `scripts/reconcile.py`: per service, Silver rows + invalid rows + duplicates removed = messages sent. Only duplicates that arrive within the watermark delay are removed, so the producer's duplicate gap stays below it.
  3. For fully closed windows, the sum of `trips_daily` counts equals the Silver trip count for those days. One zone-day is spot-checked against DuckDB on the Silver Parquet.
  4. Each Gold daily table contains the KPI columns for its scope in `specs/3_KPI.html` (for example, `revenue_daily` has no FHV rows).

### M5 — Batch processing of historic data (Silver, Gold monthly marts, queries)
- Download the configured range of months for all four datasets (default 2024-01 to 2025-12, 24 months, ending before the streamed month; see D25 for why 12 is not enough, and D28 for why the range stops there).
- Add the typed-Parquet entry point to `spark/transform.py` and implement `spark/batch_job.py` (§6.4). It writes `output/batch/silver/trips` partitioned by `service_type/year/month` and `output/batch/silver_invalid/`, the dimensions, and one row per file load to `silver.batch_run_log`. Add the `spark-batch` Compose service (profile `batch`).
- Build the Gold marts in batch: the daily marts again over the full history, the monthly marts (`trips_monthly`, `revenue_monthly`, `trip_efficiency_monthly`, `demand_monthly_hourly`, `zone_od_monthly`, `hvfhv_economics_monthly`), and the batch health marts (`batch_run_metrics`, `data_quality_monthly`). These are KPI groups Historical trends and Batch pipeline health.
- Write the KPI queries in `scripts/queries/` and `scripts/benchmark_queries.py`, which times them on DuckDB and Spark SQL (a project-specific addition, not in the KPI catalog).
- Add the query-time combination (§6.8): `scripts/combined_views.py` and `scripts/queries/live_vs_baseline.sql`.
- **Accept:**
  1. Per service and month, Silver rows + invalid rows + duplicates removed = rows in the source file (`scripts/reconcile.py --batch`; this is the catalog's "Bronze-to-Silver row reconciliation", which should be 0).
  2. Rerunning one month leaves the other months' partitions and the totals unchanged (idempotent), and adds a second row to `batch_run_log`.
  3. For the streamed month (2026-01), batch daily marts equal the stream's daily marts, run with the `clean` quality profile and the same HVFHV cap in both paths. The batch run for this check writes to `output/check/` and is deleted afterwards; it is not in the batch store.
  4. Combined views: every date appears from exactly one side (no date in both), the combined row count equals batch + stream, and `live_vs_baseline.sql` returns a result for the streamed days.
  5. Every KPI in the Historical trends and Batch pipeline health groups of `specs/3_KPI.html` returns a result over the full range. A KPI that needs more history than the loaded range (see D25) is reported as "not enough history", not computed.
  6. `benchmark_queries.py` prints a timing table, and the partition-pruned query reads fewer files than the full scan.

### M6 — Spark ML on the historic data
- Build `silver.ml_features` (lags, hour, day of week, holiday flag, zone profile) from Silver and Gold. Post-trip fields (dropoff time, total amount, tip) are excluded where they would leak the label. Add the `spark-ml` Compose service (profile `ml`).
- Train and score the models in the KPI catalog (Machine learning group): demand forecast, fare, duration/ETA, tip, wait time, shared-ride match, zone clustering, demand-pattern clustering, anomaly detection, and PCA. Write predictions to `output/batch/gold/ml_*` and model KPIs (including training time, error over time, feature drift, lift over baseline and cross-validation spread) to `gold.ml_model_metrics`.
- Training uses a configurable sample (for example, 5M rows) for the trip-level models, because training on every row is not realistic in a local-mode container.
- **Accept:**
  1. Each model trains, writes its predictions table, and adds a row to `ml_model_metrics` with the metric named in the catalog.
  2. A test checks that no excluded post-trip column is in any model's feature list.
  3. Each model's lift over the baseline (for example "same hour last week") is reported, positive or not.
  4. Training is repeatable: the same seed gives the same metrics.

### M7 — End-to-end with Compose
- Add `spark-job` and `producer` services with `depends_on` (healthy/completed conditions), and a documented way to run the batch backfill and ML alongside (`docker compose --profile batch up spark-batch`, `--profile ml up spark-ml`).
- **Accept:** from a clean checkout, `docker compose up --build` processes the full file without manual steps; `output/stream/` contains Silver, invalid, and Gold datasets; `docker compose down -v` leaves nothing behind. The batch and ML profiles run on their own and do not need Kafka.

### M8 — Hardening & docs
- Restart test: stop `spark-job` mid-run, restart, verify no duplicates/loss (compare counts).
- README: prerequisites (Docker Desktop on Windows, ≥6 GB RAM allotted), run, inspect (Kafka UI, reading Parquet with pandas/DuckDB), troubleshooting, config table.
- **Accept:** a new user can follow README and reproduce results.

### Stretch
- Late-data/out-of-order injection to demonstrate watermark behavior.
- Write aggregates to a Kafka topic and a simple dashboard (Streamlit/Grafana).
- Geo visualization with `taxi_zones` shapefile.
- Replay the full HVFHV month (no cap) on a bigger machine or cluster.
- Switch sink to Delta Lake/Iceberg.
- Add Trino or a Postgres/ClickHouse serving layer over the historical store, to compare with DuckDB and Spark SQL.

## 8. Testing Strategy

| Level | What | How |
|---|---|---|
| Unit | Row→JSON serialization, validation rules | `pytest` (producer functions, Spark transforms on small DataFrames in local mode) |
| Integration | Producer → Kafka → Spark on 1k records | `tests/smoke_test.ps1` against compose stack |
| Data quality | Counts reconcile (raw = clean + invalid) | Assertion script reading output Parquet |
| Silver | Schema matches `specs/silver-schema.md`; dq flags set as expected; duplicates removed; reconcile (sent = silver + invalid + duplicates removed) | `pytest` (extends the M3 fault-reconciliation tests), `scripts/reconcile.py` |
| Gold | Each mart's columns and calculations match `specs/3_KPI.html` on a small hand-computed fixture; out-of-scope services are excluded | `pytest` on a small Silver fixture, DuckDB cross-check |
| Batch | Per-service and per-month row reconciliation; idempotent rerun; `batch_run_log` rows; stream vs batch daily marts for the streamed month | `scripts/reconcile.py --batch`, pytest on a small multi-month fixture |
| Query | KPI queries return expected values on a small hand-checked fixture; DuckDB and Spark SQL agree | `pytest`, then `scripts/benchmark_queries.py` on the full store |
| ML | No leakage columns in features; metrics written for every model; same seed gives the same result | `pytest` on a small feature fixture |
| Recovery | Checkpoint restart | Manual script in M8 |

## 9. Risks & Decisions

| Risk | Mitigation |
|---|---|
| Spark ↔ Kafka connector version mismatch | Pin versions in `.env`; use `--packages org.apache.spark:spark-sql-kafka-0-10_${SCALA}:${SPARK}` |
| Connector download at runtime is slow/offline | Bake jars into the Spark image at build time |
| Docker memory limits on Windows (WSL2) | Document ≥6 GB; use `local[2]` Spark for MVP; limit shuffle partitions to 8 |
| Windows file mount/permission issues for output | Use named volume or `./output` with `user:` override; test early in M3 |
| Parquet schema drift (new columns in 2026 files) | Derive field list from the actual file in M2; fail loudly on unknown columns in tests |
| HVFHV volume (tens of millions of rows/month) overwhelms a laptop | Cap with `MAX_RECORDS_HVFHV`, give it 6 partitions, and measure throughput in M2 before choosing the cap |
| Four schemas drift or differ from recalled column names | Verify against the real files and PDFs in M2; one schema test per dataset |
| Streams arrive misaligned in event time, stalling the watermark | Shared simulated clock across producers; watermark advances on the slowest-moving input, so document and test it in M4 |
| Windowed output delayed by watermark in append mode | Expected; document, and flush with a final trailing event or use `update` mode for dev |
| 24 months of HVFHV (~500M rows, ~12 GB raw) is slow or runs out of memory in a local-mode Spark container | Process one month at a time (the job loops over months), keep shuffle partitions low, and allow a smaller range (for example 3 months) in `stream-config.yaml` for development |
| A short history range cannot answer the multi-year KPIs (year-over-year, seasonality, fee-policy before/after) | Default to 24 months; the KPI is reported as "not enough history" when the range is too short (D25). The 2022 Airport fee comparison would need data back to 2021 |
| Disk use of raw files plus processed output | Check free space (see `storage_requirements.md`: about 40–50 GB for 24 months) before the download; `data/` and `output/` are gitignored |
| A date is counted twice in the combined views, or falls in a gap between the batch range and the stream | Cutover date in one config value; each view takes one side per date; a test checks no date is on both sides and the batch range ends the month before the cutover |
| The specs conflict with the built M3 output (column names, reject vs flag, missing columns; §6.7 gaps) | M4 starts with Silver conformance and a schema test; decisions D26 and D27 |
| Spark ML on hundreds of millions of rows does not fit a local-mode container | Train trip-level models on a configurable sample; zone-hour demand models use the small aggregated table |
| ML features leak post-trip information | A test checks the feature lists against the excluded-columns list (M6) |
| `dropDuplicatesWithinWatermark` only removes duplicates that arrive within the watermark delay | Keep the producer's duplicate gap below the watermark; document that later duplicates remain |
| Batch and stream drift apart (two code paths) | Both call the same functions in `spark/transform.py`; the stream-vs-batch reconciliation in M5 catches drift |
| Monthly files contain rows from other months | Partition by the pickup month in the data, not the file name; rows far outside the range are rejected by validation (as in the stream) |
| Schema changes between months (columns added or renamed) | The batch job checks each file's columns against `datasets.yaml` and fails loudly, per file |

**Decisions to confirm before M1**
1. Spark deployment: single local-mode container (simpler, recommended for MVP) vs. master/worker cluster.
2. Producer language: Python (recommended) vs. Scala/Java.
3. Sink: Parquet only (recommended) vs. also Postgres/ClickHouse for SQL querying.
4. HVFHV volume: capped sample by default (recommended, e.g. 500k rows) vs. the full month.
5. Data acquisition: one-time download script on the host (recommended) vs. a Compose `data-init` service.
6. Batch history length: 24 months (recommended) vs. 12 (too short for several catalog KPIs) vs. 49 months back to 2022 (covers the Airport fee comparison, about twice the disk) (D25).
7. Query engines for the historic store: DuckDB + Spark SQL (recommended) vs. adding Trino or a database (D24, D25).
8. Medallion layout: Bronze not copied, Silver/Gold under `output/stream` and `output/batch` (D26).
9. Validation policy: keep flag-able rows in Silver with `dq_*` flags (recommended) vs. keep M3's reject policy (D27).
10. Which path builds which KPI marts: stream builds the daily marts, batch builds everything else (D26).
11. Stream and batch stay in separate stores and are combined only at query time with a cutover date (D28, decided).

## 10. Definition of Done
- `docker compose up --build` runs the whole stream pipeline unattended; `docker compose --profile batch up spark-batch` builds the historical Silver and Gold layers; `--profile ml up spark-ml` trains the models.
- Every KPI in `specs/3_KPI.html` has a Gold table that returns a result (or "not enough history"), and the stream and batch daily marts reconcile for the streamed month.
- Silver matches the medallion spec.
- The combined views return stream and batch days without overlap, and no stream data has been written into the batch store.
- All milestone acceptance criteria pass; reconciliation test shows no lost or duplicated records.
- README documents setup, configuration, and verification.
- Specs in `specs/` match the implemented behavior.
