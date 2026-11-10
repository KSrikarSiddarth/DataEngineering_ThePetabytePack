# Steps.md — The Story of the NYC Taxi Stream + Batch Pipeline

This project has two goals: **stream processing** of taxi trips through Kafka and Spark Structured Streaming (Chapters 1 to 4), and **batch processing of historic data** with queryable Silver and Gold layers and Spark ML models (Chapters 5 and 6). The data is organized as Bronze, Silver and Gold (`specs/2_nyc_taxi_medallion_architecture.html`) and the KPIs are defined in `specs/3_KPI.html`. This file tells the build as a story, one chapter per milestone from `PLAN.md`. Each chapter has the **why**, the **how** (implementation details), and **how we know it worked**. Status markers are updated as work progresses.

**Legend:** ✅ done · 🚧 in progress · ⬜ not started

| Chapter | Milestone | Status |
|---|---|---|
| 0 | Setting the stage | ✅ |
| 1 | Infrastructure up (Kafka) | 🚧 written, not yet run (Docker unavailable on this machine) |
| 2 | The producer | 🚧 built and tested without Kafka; Kafka path not run |
| 3 | Spark: parse, validate, enrich | 🚧 built and tested locally on file input; Kafka source not run |
| 4 | Silver conformance and stream Gold (daily KPI marts) | 🚧 built and tested locally on file input; Kafka source not run |
| 5 | Batch processing of historic data (Silver, monthly Gold, query benchmark) | 🚧 built and verified on 3 real months; full 24-month run not done |
| 6 | Spark ML on the historic data | ⬜ |
| 7 | End-to-end with Compose | ⬜ |
| 8 | Hardening and docs | ⬜ |

---

## Chapter 0 — Setting the stage ✅

**The story.** We started with a pile of static files: a month of yellow taxi trips (`yellow_tripdata_2026-01.parquet`), a zone lookup table, the taxi zone shapefile, and the data dictionaries. Everything sat in a single folder, and nothing moved. The goal was to make it move: to pretend the trips are happening right now, and to process them as they arrive.

**Where the data comes from.** The NYC Taxi & Limousine Commission publishes it at https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page: monthly Parquet trip files (Yellow, Green, FHV, HVFHV), data dictionaries, the zone lookup CSV, and the zone shapefile.

**What we did**
- Wrote `PLAN.md`, the spec for the whole pipeline: architecture, message schema, Spark behavior, milestones, and acceptance criteria. From here on, the plan is the source of truth and code follows it.
- Added a `.gitignore` for `data/` so large datasets never enter version control.
- Opened the project in VS Code. Git is not yet installed on this machine, so `git init` is pending (`winget install --id Git.Git -e`), followed by setting `user.name` and `user.email`.

**Still to do in this chapter**
- Install Git and run `git init`.
- Move the input files into `data/` so they match the plan and the `.gitignore`.

---

## Chapter 1 — Infrastructure up 🚧

**Status.** The files are written. The YAML parses and the init script compiles, but nothing has been run against Docker, because Docker can't run on this machine (see `installation.md` and the WSL test). Run the verification below on a machine with Docker before marking this chapter done.

**What was built**
- `stream-config.yaml`: the single configuration file for streaming. Chapter 1 uses its `kafka`, `topic_defaults` and `topics` sections. The rest is read in later chapters: `streaming` (volume, pacing, delivery), `data_quality` (null, invalid, duplicate, late, out-of-order, malformed and schema-drift rates, with `clean`, `realistic` and `messy` profiles), and `processing` (watermark, trigger, windows).
- `.env`: pinned versions (`KAFKA_VERSION=3.9.0`, Kafka UI `v0.7.2`) and host ports.
- `docker-compose.yml`: `kafka` (KRaft, single broker, auto topic creation off, healthcheck), `kafka-init`, and `kafka-ui` behind the `ui` profile.
- `kafka-init/`: a small Python image whose `create_topics.py` reads `stream-config.yaml`, creates every topic, treats existing topics as success, and fails if any topic is missing afterwards.

**Why topics come from the config file.** Auto-creation is disabled, so a typo in a topic name fails loudly instead of silently creating a new topic. Partitions and retention live in one place.

**Run it**
```
docker compose up -d kafka kafka-init
docker compose logs kafka-init
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:29092 --list
docker compose --profile ui up -d     # optional UI at http://localhost:8080
```

**Not yet verified:** the `apache/kafka:3.9.0` listener settings, the healthcheck, and the `confluent-kafka==2.6.1` admin calls. If the healthcheck or init fails, those are the first places to look.

---

## Chapter 1 (original plan)

**The story.** Before any taxi can ride, the road has to exist. Kafka is that road: a durable, ordered log that sits between whoever produces trips and whoever consumes them. We start with Kafka alone and prove it works before adding anything else.

**Implementation plan**
1. Create `.env` with pinned versions and names: `KAFKA_VERSION`, `TOPIC_INVALID=taxi.trips.invalid`. Raw topic names (`taxi.yellow.raw`, `taxi.green.raw`, `taxi.fhv.raw`, `taxi.hvfhv.raw`) are defined in the producer's `datasets.yaml`.
2. Create `docker-compose.yml` with:
   - `kafka`: the `apache/kafka` image in KRaft mode (no ZooKeeper), one broker. It has two listeners: `9092` for the host and `29092` for other containers. Add a healthcheck that runs `kafka-topics.sh --list`.
   - `kafka-init`: a one-shot container that waits for the broker to be healthy, creates all five topics from `stream-config.yaml` (four raw ones with 3 partitions, 6 for HVFHV, 1-day retention, plus the invalid topic), and exits.
   - `kafka-ui` (optional): the UI on port 8080 for looking at messages.
3. Start it: `docker compose up -d kafka kafka-init`.

**How we know it worked**
- `docker compose ps` shows `kafka` healthy and `kafka-init` exited with code 0.
- Listing topics shows the four raw topics and `taxi.trips.invalid`.

**Watch out for:** the two-listener setup. Getting the advertised listeners wrong is the most common reason containers can't reach Kafka.

---

## Chapter 2 — The producer 🚧

**Status.** The producer is written and tested against the real data, but only without Kafka. Docker isn't available on this machine, so the Kafka delivery path (`KafkaSink`, `wait_for_kafka`, the Compose services) has not been run.

**What happened**
- Downloaded the Green, FHV and HVFHV files with `scripts/download_data.py` (URL pattern checked, files validated as Parquet): Green 40,272 rows, FHV 1,941,722, HVFHV 20,940,373.
- Read the real schemas and profiled the files. This replaced the recalled column names in the plan, and turned up four things that changed the design:
  - No file is sorted by pickup time, so the producer sorts on indices, which keeps memory bounded even for HVFHV.
  - FHV's `PUlocationID` is about 85% null, so FHV messages are keyed by `dispatching_base_num` instead.
  - Yellow already has about 29% nulls in several columns, and a few Yellow and Green rows fall outside January. The data has real quality problems before any are injected.
  - FHV and HVFHV use different column names and have no `total_amount` (details in `specs/raw-schemas.md`).
- Built `producer/`: `datasets.yaml` (per-dataset spec), `config.py`, `replay.py` (sorted, capped, sampled reads), `faults.py` (the quality profiles from `stream-config.yaml`), `pacing.py`, `serialize.py`, `producer.py`, a Dockerfile, and four Compose services under the `producers` profile.
- Injected faults are tagged in a Kafka header (`fault`), so later steps can tell injected problems from natural ones. `--dry-run` writes NDJSON instead of sending to Kafka.

**Verified here:** 43 pytest tests pass (schemas against the real files, fault rates, ordering, pacing, malformed JSON, and a 1000-message dry run for each of the four datasets). Each dry run produced exactly 1000 messages, with keys, sorted by pickup time.

**Not verified:** sending to a real broker, the Compose producer services, and throughput against Kafka. Run on a machine with Docker:
```
docker compose up -d kafka kafka-init
MAX_RECORDS=1000 REPLAY_SPEEDUP=0 QUALITY_PROFILE=clean docker compose --profile producers up producer-yellow producer-green producer-fhv producer-hvfhv
```
Then check each topic's message count, for example with `kafka-get-offsets.sh` or Kafka UI.

**Known limit:** HVFHV with `max_records: 0` would sort all 20.9M rows in memory (the code prints a warning above 5M rows). Keep the default cap of 500,000.

### Original plan for this chapter

**The story.** The data is a historical file, but streaming needs events arriving over time. The producer plays the part of the taxis: it reads the file in order and publishes each trip to Kafka at a controllable speed.

**Implementation plan**
0. The pipeline covers all four TLC datasets: Yellow, Green, FHV, and HVFHV, since downstream applications may need any of them. Download the Green, FHV, and HVFHV files for 2026-01 into `data/` first.
1. Inspect each parquet schema with pyarrow and fix the final field lists in the specs (Section 6.1 of `PLAN.md`). Column names and file patterns in the plan were recalled from memory and must be checked here. New 2026 columns, such as `cbd_congestion_fee`, are expected. Write `producer/datasets.yaml` mapping each dataset to its topic, file pattern, event-time column, and key column.
2. Write `producer/producer.py`:
   - Read the file in record batches with `pyarrow.parquet.ParquetFile.iter_batches`, so memory stays flat.
   - Sort by `tpep_pickup_datetime`. If the file doesn't fit in memory, sort per day or per batch as a trade-off, and note it in the spec.
   - Serialize each row to JSON, with ISO-8601 timestamps and nulls preserved.
   - Send with a Kafka client (`confluent-kafka` or `kafka-python`), key = the dataset's pickup-zone column, topic = the dataset's topic. The `DATASET` env var picks which entry of `datasets.yaml` to use, so the same image serves all four, run as four Compose services. HVFHV is far larger than the others, so it gets 6 partitions and a capped `MAX_RECORDS` by default.
   - Pace using `REPLAY_SPEEDUP`: sleep for the gap between consecutive event times divided by the speedup. A value of `0` disables sleeping.
   - Retry the broker connection at startup, flush before exit, and log throughput every N records.
3. Add `producer/Dockerfile` and `requirements.txt`, and a `producer` service in Compose with env vars `KAFKA_BOOTSTRAP`, `TOPIC`, `REPLAY_SPEEDUP`, `MAX_RECORDS`, `INPUT_PATH`.
4. Add a unit test for row-to-JSON serialization.

**How we know it worked**
- For each dataset, with `MAX_RECORDS=1000 REPLAY_SPEEDUP=0`, a console consumer reads exactly 1000 messages from its topic that match that dataset's schema, each with a key.

---

## Chapter 3 — Spark: parse, validate, enrich 🚧

**Status.** The Spark job is written and has run end to end on this machine, but with a file source standing in for Kafka. The Kafka source, the Docker image and the `spark-job` Compose service have not been run, because Docker isn't available here.

**What was built**
- `spark/transform.py`: pure DataFrame functions, used unchanged by the streaming job and by tests.
  - **Parse:** each dataset's topic is parsed with an explicit schema, with no inference. Unparseable or empty messages go to the invalid output as `malformed_json`, with the raw payload kept.
  - **Normalize:** all four datasets map into one unified trip model, using the `normalize` section of `producer/datasets.yaml`. HVFHV `trip_miles`, `base_passenger_fare` and `tips` become distance, fare and tip. Fields a dataset doesn't have stay null, and everything unmapped is kept as JSON in `extras`.
  - **Enrich:** a broadcast left join with the zone lookup adds pickup and dropoff borough and zone.
  - **Validate:** a row can be rejected for null or out-of-range pickup time (January 2026 plus one day of tolerance), dropoff before pickup, trips over 24 hours, negative distance or amounts, a location ID missing from the lookup, or (Yellow, Green, HVFHV only) a null pickup or dropoff zone. Reasons are comma-joined in `reject_reason`. FHV rows with a missing zone are kept and flagged in `dq_flags`, since 85% of them have none.
- `spark/stream_job.py`: reads the four Kafka topics (or NDJSON files with `--source files` for development), writes `trips_clean` and `invalid` as Parquet partitioned by `service`, each with its own checkpoint. `available_now` or `processing_time` triggers, set in `stream-config.yaml` or by `TRIGGER_MODE`.
- `spark/Dockerfile`: Python 3.11, Java, PySpark 3.5.3, and the Kafka connector jars baked in (URLs checked against Maven Central).
- `scripts/reconcile.py`: compares each producer's `messages_sent` with the Spark output and lists reject reasons.

**Verified here**
- 8 Spark tests, within a total of 52 passing tests. They cover 24 rule cases one by one, the unified mapping and enrichment, the output layout, and a run over real data with the `messy` profile. Every injected fault of the five invalid kinds, and every malformed message, was rejected with the right reason, and rows with no injected fault were only rejected for problems already in the TLC data.
- The streaming job itself (file source, Parquet sinks, checkpoints, `available_now` trigger) ran to completion on 2,031 messages from the four producers. Clean + invalid matched messages sent for every service, and a second run from the same checkpoint added nothing.

**Not verified:** reading from a real Kafka broker, the Docker build (including the connector jar download), and the Compose service. On a machine with Docker:
```
docker compose up -d kafka kafka-init
MAX_RECORDS=1000 REPLAY_SPEEDUP=0 docker compose --profile producers up producer-yellow producer-green producer-fhv producer-hvfhv
TRIGGER_MODE=available_now docker compose --profile spark up spark-job
python scripts/reconcile.py
```

**Things to know**
- Duplicates injected by the producer are not removed in M3. They count as separate clean rows, so `clean + invalid` equals messages sent, not unique trips.
- The `taxi.trips.invalid` Kafka topic exists but is not written to yet. Rejected rows go to Parquet only (decision D23).
- A system-wide `SPARK_HOME` (this machine has Spark 3.3.2) overrides the pip-installed PySpark and causes a version mismatch. The tests and the commands above point it at the right place for their own process only (see `installation.md`).

### Original plan for this chapter

**The story.** Raw messages are just bytes, and real taxi data is messy: negative fares, trips that end before they start, dates from other years. Spark's first job is to turn raw bytes into trustworthy, readable rows and to set aside the ones it can't trust.

**Implementation plan**
1. Create `spark/stream_job.py` and `spark/Dockerfile`. Use a pinned Spark image and bake the Kafka connector jar (`spark-sql-kafka-0-10`, matching Spark and Scala versions) into the image so nothing is downloaded at runtime.
2. Read the four raw topics with `startingOffsets=earliest`, and tag each row with a `service` column derived from the topic name.
3. Parse `value` with `from_json` against an explicit `StructType` per dataset, and cast timestamp strings to `TimestampType`. No schema inference. Then normalize all four into one unified trip schema (service, pickup and dropoff times, location IDs, distance, fare fields, payment type, and an `extras` column for dataset-specific fields). Fields a dataset lacks, such as fares for FHV, stay null.
4. Validate. A row is invalid if any of these hold: null pickup time, dropoff before pickup, negative distance or total, null location IDs, or pickup far outside January 2026. Split the stream into valid and invalid and add a `reject_reason` column to the invalid side.
5. Enrich valid rows by joining `taxi_zone_lookup.csv` twice (pickup and dropoff). Use a broadcast join, since the table is small. This adds borough and zone names.
6. Write valid trips to Parquet (`output/trips_clean`) and invalid ones to `output/invalid`, each with its own checkpoint directory.

**How we know it worked**
- With 1000 records produced per dataset, clean rows + invalid rows = 4000, and per-service counts match each producer.
- Enriched rows have non-null zone names for valid location IDs.

---

## Chapter 4 — Silver conformance and stream Gold 🚧

**Status.** Built and verified on this machine with the file source standing in for Kafka. The Kafka source, the Docker image and the Compose service are still not run (no Docker here).

**What was built**
- `spark/transform.py` reworked. The unified model is now `silver.trips` (`specs/silver-schema.md`): typed columns for times, zones, distance, fares, driver pay, six fee columns and the shared-ride flags, plus `trip_id` (a hash, because TLC has no key), `duration_sec`, `payment_label`, `service_type` (`fhvhv` for HVFHV), `kafka_timestamp` and `silver_write_ts`. `producer/datasets.yaml` carries the wider column mapping.
- **Flag, don't reject (D27).** Ten boolean `dq_*` columns mark the problems the KPIs measure. Only malformed JSON and a null pickup time go to `silver_invalid`.
- **Deduplication (D30).** `dropDuplicatesWithinWatermark` on `trip_id`, with the watermark on Kafka ingest time, so late trips are not mistaken for expired state.
- `spark/dims.py`: `dim_zone`, `dim_payment_type`, `dim_ratecode`, `dim_hvfhs_company`, written once per run.
- `spark/gold.py`: the eight daily marts as hourly-grain, additive-measure tables (D29, `specs/gold-schema.md`). The stream job's second stage reads Silver back as a stream, with a 10-minute watermark.
- `spark/stream_job.py`: Silver and `silver_invalid` under `output/stream/`, Gold under `output/stream/gold/`, ten queries with their own checkpoints. `--stage silver|gold` runs one stage.
- `scripts/reconcile.py`, `scripts/show_results.py` moved to the new layout; new `scripts/evaluate_stream.py` for latency (S1) and throughput (S3).

**Verified here**
- 26 Spark tests (10 transform, 11 Gold, 5 job): 26 rule cases, trip_id behaviour, typed fees, real data with the `messy` profile (every injected fault is flagged or rejected, nothing else is rejected), every Gold mart on hand-computed values, and the whole job on NDJSON files, including the duplicate being removed and a second run from the same checkpoints adding nothing.
- A run on 80,000 real messages (20,000 per dataset, `realistic` profile): the accounting balances for all four services; Yellow, Green and HVFHV removed exactly the duplicates injected; every written Gold hour equals the Silver count (389 of 389); one zone-day matches DuckDB.

**Not verified:** reading from a real Kafka broker, the Docker build and the `spark-job` service, and the latency and throughput figures (the file source stamps rows with the processing time, so S1 reads 0 s there).

**Things to know**
- FHV rows that are identical in every field collapse into one. 25 of 20,000 FHV rows were removed beyond the injected duplicates (D30).
- About 18,500 of 80,000 Silver rows carry `dq_null_pu_location`. Almost all are FHV, where 85% of rows have no pickup zone in the TLC file.
- Gold windows are written only after the watermark passes them. In a short sample, a service whose events span less than an hour (the HVFHV sample covered 24 minutes) writes nothing to its own marts (`hvfhv_*`); the other marts are driven by all services.

### Original plan for this chapter

**The story.** Chapter 3 cleaned the stream, but we built it before we had the full picture of what the project must answer. Two documents now say it exactly: `specs/2_nyc_taxi_medallion_architecture.html` defines Bronze, Silver and Gold tables, and `specs/3_KPI.html` lists every KPI with its question, calculation and Gold table. This chapter brings the stream in line with them. First the cleaned trips become **Silver** (one schema for all four services, typed, deduplicated, with data-quality flags). Then the stream becomes insight: **Gold** daily marts such as trips per day, demand by hour, revenue, trip efficiency, HVFHV economics and data quality.

**Implementation plan**

*A. Silver conformance*
1. Rework the M3 output into `silver.trips`: renamed columns (`service_type`, `pu_location_id`, `distance_miles`, `base_fare`), plus `trip_id`, `duration_sec`, the typed fee columns, and the `dq_*` flags. The gaps between the spec and the current code are listed in `PLAN.md` §6.7.
2. Add the dimension tables (`dim_zone`, `dim_payment_type`, `dim_ratecode`, `dim_hvfhs_company`).
3. Derive `trip_id` as a deterministic hash (the TLC files have no key), then deduplicate with `dropDuplicatesWithinWatermark`, counting removed rows.
4. Apply the validation policy (D27: flag rule violations in Silver, reject only unusable rows). Move the outputs to `output/stream/silver/trips` and `output/stream/silver_invalid`.
5. Write `specs/silver-schema.md` from the medallion page, and a schema test against it.
6. Add `kafka_timestamp` and `silver_write_ts` to the stream's Silver so latency can be measured, and write `scripts/evaluate_stream.py` for the targets in `evaluation.md`.

*B. Stream Gold (daily marts)*
6. Set the watermark (10 minutes) on the pickup time, so Spark can finalize old windows.
7. Build `trips_daily`, `demand_hourly`, `zone_od_daily`, `revenue_daily`, `trip_efficiency_daily`, `hvfhv_economics_daily`, `hvfhv_service_daily` and `data_quality_daily` with event-time windows, in append mode to `output/stream/gold/`, with checkpoints. Services without the needed fields are excluded (no FHV in revenue). Set `spark.sql.shuffle.partitions` to about 8.
8. Add a console sink in dev mode for fast feedback.

**How we know it worked**
- The Silver schema test passes.
- Per service, Silver + invalid + duplicates removed = messages sent.
- For fully closed windows, the sum of `trips_daily` counts equals the Silver trip count for those days; one zone-day spot-checked in DuckDB matches.
- Every Gold table has the KPI columns that `specs/3_KPI.html` gives for its scope.

**Expect:** in append mode, a window's output appears only after the watermark passes it, so the last windows may not show until more data (or a trailing event) arrives. That is correct and should be documented. Duplicates are only removed within the watermark delay.

**Open decisions:** D26 (layers and which path builds which mart) and D27 (flag instead of reject) should be confirmed before this chapter starts.

---

## Chapter 5 — Batch processing of historic data 🚧

**Status.** Built and verified on this machine, on synthetic files and on real data for 2024-12, 2025-12 and 2026-01 (Yellow, Green, FHV, and a capped HVFHV). **Not done:** the full 24-month backfill (about 80 GB and about 9 hours on this machine, see `storage_requirements.md`), a full HVFHV month, and the Docker `spark-batch` service (no Docker here).

**What was built**
- `spark/batch_job.py`: one file at a time. `read_typed` (new in `transform.py`) reads the TLC Parquet, tolerating columns missing in older files; `conform` is the stream's function. Duplicate `trip_id`s are dropped per file. A file's partition `service_type/year/month` is deleted and rewritten, so a rerun is idempotent. Year and month are those of the source file (D31). Every load adds a row to `silver/batch_run_log`; a missing file or a Gold error is logged as failed and the run continues.
- Gold per month: the eight daily marts of Chapter 4 (the revenue mart gained airport, CBD and congestion fee sums), then seven monthly marts as sums of them, `data_quality_monthly` (with duplicates and late corrections from the log) and, at the end of a run, `batch_run_metrics` (reconciliation, freshness, schema drift, runtime, backfill).
- `scripts/queries/*.sql`: the Historical-trends and Batch-health KPIs as portable SQL (they run unchanged on DuckDB and Spark SQL), plus `live_vs_baseline.sql`. A value that needs months that are not loaded is NULL with a status "not enough history".
- `scripts/combined_views.py` (batch tables, and `combined_<mart>` views with the cutover), `scripts/benchmark_queries.py` (timings on both engines and pruning against a full scan), `scripts/reconcile.py --batch`, `scripts/compare_stream_batch.py`, `scripts/download_data.py --batch` (also records TLC publication dates), and the `spark-batch` Compose service.

**Verified here**
- 10 batch tests (synthetic files; about 4 minutes): reconciliation, one log row per load, idempotent rerun, a corrected file replacing only its own partition and counting late corrections, schema drift (a column added in a later month), a missing file logged as failed, a month with only HVFHV, every KPI query on DuckDB and on Spark SQL with equal row counts, "not enough history" where 2 months are loaded, and the combined views.
- Real data, 2024-12 and 2025-12, Yellow + Green + FHV, 12.2M rows, 10 minutes: `reconcile.py --batch` difference 0 for all six files (FHV removed 2,202 and 2,227 identical rows). Year-over-year for December 2025 computed: Yellow +17.4%, FHV +0.7%, Green -10.7%; the Airport fee comparison is "not enough history" and the CBD fee comparison runs (average CBD fee per trip 0.00 before, 0.53 after).
- Stream against batch for 2026-01 (clean profile, 20,000 trips per dataset, same cap): every stream row equals the batch row in all six marts that had rows (47,000 rows). HVFHV was checked separately with 100,000 trips: the two HVFHV marts match too.
- Benchmark on the real store: the KPI queries take 6–40 ms on DuckDB and 0.1–2.4 s on Spark SQL; the pruned Silver query needs 16 of 48 files and takes 45 ms against 486 ms for the full scan (Spark: 0.7 s against 1.9 s).

**Things to know**
- **Disk is much larger than planned.** Silver is 56–69 bytes per row (a third of it the `trip_id` string), so 24 months with HVFHV is about 72–85 GB, not 40. zstd compression saves 31% and a 16-character `trip_id` another 12 points; neither is applied (decision needed).
- Speed on this 2-core VM is about 1.2M rows per minute, so a full backfill is roughly 9 hours; a development range of 3 months is practical.
- A real run found a crash that tests missed: a month with no rows for a mart (no Yellow or Green file) broke the monthly roll-up. Fixed, and tested.
- The freshness KPI measures the gap to TLC's publication date at load time, so a backfill shows hundreds of days.
- Each month's Silver is written as 16 files per partition (the shuffle partition count), which is many small files for the tiny Green data.

### Original plan for this chapter

**The story.** The stream tells us what is happening right now. But a month of replayed data cannot say whether demand is growing year over year, how seasonality looks, or how fares moved after the congestion fee began. For that we need history, and with history comes the other big data problem: not speed, but size. Two years of HVFHV alone is about 500 million rows. This chapter adds the second path through the project: a batch job that processes many months at once into Silver, builds the monthly and pipeline-health Gold marts, and a query benchmark that shows how SQL behaves at that size.

**Implementation plan**
1. Download 24 months (2024-01 to 2025-12, ending before the streamed month) of all four datasets with the existing `scripts/download_data.py --months ...` (13–14 GB raw; check free disk first). Twelve months would not be enough for the year-over-year and fee-policy KPIs (D25). The months go in a new `batch:` section of `stream-config.yaml`. The download script also records each file's TLC publication date for `batch_run_log`.
2. Add a typed-Parquet entry point to `spark/transform.py` that skips `from_json`. Normalize, validate, enrich and deduplicate stay the same functions the stream uses, so a trip is judged identically in both paths.
3. Write `spark/batch_job.py`: process one month at a time (to keep memory in check), write `output/batch/silver/trips` partitioned by `service_type/year/month` and `output/batch/silver_invalid/`, with dynamic partition overwrite so rerunning a month is safe. Write one row per file to `silver.batch_run_log`.
4. Build the Gold marts in batch: the daily marts again over the full history, the monthly marts (`trips_monthly`, `revenue_monthly`, `trip_efficiency_monthly`, `demand_monthly_hourly`, `zone_od_monthly`, `hvfhv_economics_monthly`), and the pipeline-health marts (`batch_run_metrics`, `data_quality_monthly`), using the same mart definitions as the stream where they overlap.
5. Add the `spark-batch` Compose service (profile `batch`). It needs no Kafka.
6. Write the KPI queries as SQL files in `scripts/queries/`, and `scripts/benchmark_queries.py`: run each on DuckDB and Spark SQL, and compare a partition-pruned query against a full scan. Print a timing table.
7. Extend `reconcile.py` with `--batch`: per service and month, source rows against Silver + invalid + duplicates removed.
8. Combine the two paths at query time only (D28): `scripts/combined_views.py` builds views that take batch rows before the cutover date (2026-01-01) and stream rows from it on, and `scripts/queries/live_vs_baseline.sql` compares the streamed days with the batch history. The stream's output is never written into the batch store.

**How we know it worked**
- Per service and month, Silver + invalid + duplicates removed equal the source file's row count (the catalog's "Bronze-to-Silver row reconciliation" is 0).
- Rerunning one month changes nothing else, and adds a second `batch_run_log` row.
- For the streamed month, a throwaway batch run (`output/check/`, deleted afterwards) matches the stream's daily marts (clean profile, same HVFHV cap).
- In the combined views every date comes from exactly one side, and the combined row count is batch + stream.
- Every Historical-trends and Batch-pipeline-health KPI returns a result, or "not enough history" if the range is too short; the benchmark shows the pruned query reading fewer files than the full scan.

**Watch for:** HVFHV memory and time on a laptop (loop over months, keep shuffle partitions low, or start with 3 months), monthly files that contain rows from other months, column changes between months, and disk use (about 40–50 GB; see `storage_requirements.md`).

---

## Chapter 6 — Spark ML on the historic data ⬜

**The story.** History is also training data. With two years of Silver and Gold we can ask what will happen: how many trips each zone will see next hour, what a trip should cost, how long it will take, whether it will be tipped, and which trips look wrong. The KPI catalog lists the models and the metrics that show whether they are any good, and whether they stay good.

**Implementation plan**
1. Build `silver.ml_features` (lags, hour, day of week, holiday flag, zone profile). Leave out fields known only after the trip (dropoff time, total amount, tip) wherever they would leak the label. Zone-hour demand models use the small aggregated table; trip-level models train on a configurable sample.
2. Train and score, with Spark MLlib: demand forecast (`GBTRegressor`), fare prediction (Yellow and Green), duration and ETA, tip prediction (card payments only), wait time and shared-ride match (HVFHV), zone clustering and demand-pattern clustering (`KMeans`), anomaly detection, and `PCA`.
3. Write predictions to `output/batch/gold/ml_*` and the model KPIs (training time, error per scoring month, feature drift, lift over baseline, cross-validation spread) to `ml_model_metrics`.
4. Add the `spark-ml` Compose service (profile `ml`).
5. Write `scripts/evaluate_models.py`, which checks the metrics against the proposed targets in `evaluation.md`.

**How we know it worked**
- Each model trains, writes its predictions table, and adds a metrics row with the metric named in the catalog.
- A test shows no excluded column is in any feature list.
- Lift over the baseline ("same hour last week") is reported for each model.
- The same seed gives the same metrics.

**Known limits:** the TLC files have no weather or holidays (holidays can be added as a small table), no driver or passenger IDs (so no recommendation models), and FHV has no fares. Spark ML has no ARIMA or Prophet, so forecasting uses lag features with tree or linear models.

---

## Chapter 7 — End-to-end with Compose ⬜

**The story.** So far each piece was started by hand. Now we make the whole thing run with a single command, the way someone cloning the project would expect.

**Implementation plan**
1. Add the `spark-job` and `producer` services to `docker-compose.yml`.
2. Order startup with `depends_on`: Kafka healthy → `kafka-init` completed → Spark job started → producer started. Starting Spark before the producer means the stream is already listening.
3. Mount `./data` read-only and `./output` for results. If Windows file permissions cause trouble, switch to a named volume.
4. Add `tests/smoke_test.ps1`: bring the stack up with a small `MAX_RECORDS`, wait for completion, then check that the outputs exist and the counts reconcile.
5. Document how to run the batch backfill and the ML job next to the stream (`docker compose --profile batch up spark-batch`, `--profile ml up spark-ml`); they run on their own and do not wait for Kafka.

**How we know it worked**
- From a clean checkout, `docker compose up --build` processes the whole file with no manual steps.
- `docker compose down -v` leaves nothing behind.

---

## Chapter 8 — Hardening and docs ⬜

**The story.** A pipeline that works once is a demo. One that survives a restart and can be handed to someone else is a project.

**Implementation plan**
1. Recovery test: stop `spark-job` partway through, restart it, and compare final counts to a clean run. There should be no loss and no duplicates, thanks to checkpoints.
2. Write `README.md`: prerequisites (Docker Desktop, WSL2, at least 6 GB RAM), quick start, configuration table, how to inspect results (Kafka UI, pandas or DuckDB on the Parquet output), and troubleshooting.
3. Make sure the specs in the plan match what was actually built, and update them where they drifted.

**How we know it worked**
- A new person can follow the README and reproduce the results.

---

## Epilogue — Stretch goals

Late and out-of-order data to show the watermark at work (already partly built into the producer's fault injection); aggregates published back to Kafka with a small dashboard; the full uncapped HVFHV month; maps built from the taxi zone shapefile; a Delta Lake or Iceberg sink; live daily-mart KPIs compared with the historical baseline; minute-scale stream KPIs (not in the KPI catalog); and Trino or a database as an extra query engine over the Silver and Gold layers.

---

## Changelog

| Date | Change |
|---|---|
| 2026-11-10 | M5 built: batch job, monthly and health marts, KPI queries, combined views, benchmark, `reconcile --batch`, stream-vs-batch comparison, `spark-batch` service. Verified on synthetic files and real 2024-12, 2025-12 and 2026-01 data; stream and batch marts match exactly. Found that Silver is much larger than estimated (storage doc revised). Added D31. |
| 2026-11-09 | M4 built: Silver conformance (typed columns, `trip_id`, `dq_*` flags, dimensions), deduplication, the eight stream Gold marts, reworked `reconcile.py` and `show_results.py`, new `evaluate_stream.py`, `specs/silver-schema.md` and `specs/gold-schema.md`. 70 tests pass. Verified on file input only (no Kafka, no Docker). Added D29 and D30; D27 implemented. |
| 2026-11-05 | Stream and batch kept in separate stores and combined only at query time with a cutover date (D28, option A). Batch range is now 2024-01 to 2025-12. The live-against-baseline KPI stays, as a query over the combined views. |
| 2026-11-05 | Read the medallion and KPI specs and updated all planning docs. Chapter 4 is now Silver conformance plus stream Gold daily marts (the M3 output must be reworked to match Silver). Chapter 5 now builds Silver and the monthly and health marts, over 24 months instead of 12. New Chapter 6 (Spark ML); end-to-end Compose is Chapter 7 and hardening Chapter 8. Added D26, D27. Planning only, no code. |
| 2026-11-04 | Goal widened to stream + batch processing of historic data. Added the batch milestone as M5 (Chapter 5); end-to-end Compose is now M6 and hardening is M7. Added KPI lists, D24 and D25. Planning only, no code. |
| 2026-11-02 | Created `PLAN.md`, `.gitignore`, and this file. No pipeline code written yet. |
| 2026-11-04 | M3 built: Spark parse/normalize/enrich/validate job, Docker image and Compose service, reconcile script. Streaming run verified on file input only. Fixed a producer bug found by the Spark tests (null injection could erase an injected invalid value). |
| 2026-11-04 | M2 built: data download script, verified schemas (`specs/raw-schemas.md`), config-driven producer with fault injection, 43 passing tests. Kafka path not run (no Docker). |
| 2026-11-04 | M1 files written: `stream-config.yaml`, `.env`, `docker-compose.yml`, `kafka-init/`. Not run: no Docker on this machine. |
| 2026-11-02 | Moved Green, FHV, and HVFHV into the MVP: one topic and producer service per dataset, a unified trip schema in Spark, cross-service aggregations. |
