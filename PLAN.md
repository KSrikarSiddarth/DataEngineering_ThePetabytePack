# PLAN.md — NYC Taxi Streaming Pipeline (Kafka + Spark + Docker Compose)

Spec-driven development: each milestone has a spec, acceptance criteria, and a verification command. Implement in order; do not start a milestone until the previous one's acceptance criteria pass.

## 1. Goal

A simple, locally runnable big data pipeline that replays NYC TLC trip records (Yellow, Green, FHV, and HVFHV) as streams through Kafka, processes them with Spark Structured Streaming, and writes aggregated results to a queryable sink. Everything runs with `docker compose up`.

## 2. Scope

**In scope**
- Config-driven producer that replays all four TLC datasets (Yellow, Green, FHV, HVFHV) into one Kafka topic each, simulating real-time via event time.
- Normalization of the four schemas into one unified trip model, so downstream applications can query across services.
- Kafka (KRaft mode, single broker) with a raw trips topic.
- Spark Structured Streaming job: parse, validate, enrich with taxi zones, aggregate in windows.
- Sink: Parquet files (and console) on a mounted volume; optional second topic for aggregates.
- Single-command startup, health checks, README with run instructions.

**Out of scope (initially)**
- Cloud deployment, multi-broker clusters, schema registry, auth, dashboards (stretch goal only).

## 3. Existing Inputs (in `data/`; fetch the trip files with `python scripts/download_data.py`)

**Data source:** NYC TLC Trip Record Data — https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page (monthly Parquet files for Yellow, Green, FHV, and HVFHV trips, plus the data dictionaries, zone lookup CSV, and taxi zone shapefile). Other months can be downloaded from there to extend the replay.

| File | Use |
|---|---|
| `yellow_tripdata_2026-01.parquet` | Yellow source stream (present) |
| `green_tripdata_2026-01.parquet` | Green source stream (to download into `data/`) |
| `fhv_tripdata_2026-01.parquet` | FHV source stream (to download into `data/`) |
| `fhvhv_tripdata_2026-01.parquet` | HVFHV source stream (to download into `data/`) |
| `taxi_zone_lookup.csv` | Static enrichment (LocationID → Borough, Zone) |
| `taxi_zones/` shapefile | Not needed for MVP (stretch: geo maps) |
| `data_dictionary_trip_records_yellow.pdf` | Field reference for schema |

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
                   /data/out/trips_clean   /data/out/agg_*  (Parquet)
                   (+ taxi.trips.invalid topic for rejects)
```

### Services (docker-compose.yml)

| Service | Image / Build | Notes |
|---|---|---|
| `kafka` | `apache/kafka` (KRaft, no ZooKeeper) | Ports 9092 (host), 29092 (internal) |
| `kafka-init` | same image | Creates topics, then exits |
| `spark-master` / `spark-worker` | `apache/spark` (or bitnami equivalent) | Or single `spark` container in local mode for MVP |
| `spark-job` | build from `./spark` | Submits streaming job with Kafka connector package |
| `producer-yellow`, `producer-green`, `producer-fhv`, `producer-hvfhv` | one image built from `./producer` (Python), different `DATASET` env | Each replays one dataset to its topic |
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
│   └── stream_job.py
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
2. Parse JSON per dataset with an explicit `StructType` (no schema inference); cast timestamps. Then **normalize** to one unified trip schema: `service, pickup_ts, dropoff_ts, PULocationID, DOLocationID, trip_distance, passenger_count, fare_amount, tip_amount, total_amount, payment_type, extras` (a JSON/map column holding dataset-specific fields such as `trip_type` or driver pay). Fields a dataset lacks (e.g. fares for FHV) are null.
3. **Validation** — rules that need a field apply only to datasets that have it (e.g. fare checks skip FHV; FHV rows with null location IDs are kept but flagged, not rejected). Reject (route to `taxi.trips.invalid` or `output/invalid`) rows where: dropoff < pickup, `trip_distance < 0`, `total_amount < 0`, null pickup time, pickup outside January 2026 (±tolerance), or null location IDs.
4. **Enrichment** — broadcast join with `taxi_zone_lookup.csv` for pickup and dropoff borough/zone.
5. **Aggregations** (event-time, watermark 10 minutes):
   - `agg_zone_hourly`: 1-hour tumbling window × pickup zone × service → trip count, avg distance, and avg fare / avg tip / total revenue (null where the service has no fare data).
   - `agg_service_hourly`: 1-hour window × service → trip count (cross-service demand comparison).
   - `agg_payment_5min`: 5-minute window × payment_type → count, revenue (Yellow and Green only).
6. **Sinks**: clean trips + aggregates as Parquet (append mode), with checkpoints in `/data/checkpoints/<query>`; console sink for aggregates in dev mode.
7. Must restart cleanly from checkpoint without duplicating output.

### 6.4 Config (`.env`)
`KAFKA_VERSION, SPARK_VERSION, SCALA_VERSION, TOPIC_INVALID, REPLAY_SPEEDUP, MAX_RECORDS, MAX_RECORDS_HVFHV` (raw topic names come from `producer/datasets.yaml`)

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
- **Accept:** with 1000 records produced per dataset (4000 total), `output/trips_clean` row count + invalid count = 4000, and the per-`service` counts reconcile with each producer; enriched rows have non-null `PUZone` for valid location IDs.

### M4 — Spark: windowed aggregations
- Implement aggregations with watermark and checkpoints.
- **Accept:** aggregate totals (sum of trip counts across windows) equal clean-trip count for fully-closed windows; spot-check one zone-hour against a pandas computation on the same parquet.

### M5 — End-to-end with Compose
- Add `spark-job` and `producer` services with `depends_on` (healthy/completed conditions).
- **Accept:** from a clean checkout, `docker compose up --build` processes the full file without manual steps; `output/` contains clean, invalid, and aggregate datasets; `docker compose down -v` leaves nothing behind.

### M6 — Hardening & docs
- Restart test: stop `spark-job` mid-run, restart, verify no duplicates/loss (compare counts).
- README: prerequisites (Docker Desktop on Windows, ≥6 GB RAM allotted), run, inspect (Kafka UI, reading Parquet with pandas/DuckDB), troubleshooting, config table.
- **Accept:** a new user can follow README and reproduce results.

### Stretch
- Late-data/out-of-order injection to demonstrate watermark behavior.
- Write aggregates to a Kafka topic and a simple dashboard (Streamlit/Grafana).
- Geo visualization with `taxi_zones` shapefile.
- Replay the full HVFHV month (no cap) on a bigger machine or cluster.
- Switch sink to Delta Lake/Iceberg.

## 8. Testing Strategy

| Level | What | How |
|---|---|---|
| Unit | Row→JSON serialization, validation rules | `pytest` (producer functions, Spark transforms on small DataFrames in local mode) |
| Integration | Producer → Kafka → Spark on 1k records | `tests/smoke_test.ps1` against compose stack |
| Data quality | Counts reconcile (raw = clean + invalid) | Assertion script reading output Parquet |
| Recovery | Checkpoint restart | Manual script in M6 |

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

**Decisions to confirm before M1**
1. Spark deployment: single local-mode container (simpler, recommended for MVP) vs. master/worker cluster.
2. Producer language: Python (recommended) vs. Scala/Java.
3. Sink: Parquet only (recommended) vs. also Postgres/ClickHouse for SQL querying.
4. HVFHV volume: capped sample by default (recommended, e.g. 500k rows) vs. the full month.
5. Data acquisition: one-time download script on the host (recommended) vs. a Compose `data-init` service.

## 10. Definition of Done
- `docker compose up --build` runs the whole pipeline unattended.
- All milestone acceptance criteria pass; reconciliation test shows no lost or duplicated records.
- README documents setup, configuration, and verification.
- Specs in `specs/` match the implemented behavior.
