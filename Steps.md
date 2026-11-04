# Steps.md — The Story of the NYC Taxi Streaming Pipeline

This file tells the build as a story, one chapter per milestone from `PLAN.md`. Each chapter has the **why**, the **how** (implementation details), and **how we know it worked**. Status markers are updated as work progresses.

**Legend:** ✅ done · 🚧 in progress · ⬜ not started

| Chapter | Milestone | Status |
|---|---|---|
| 0 | Setting the stage | ✅ |
| 1 | Infrastructure up (Kafka) | 🚧 written, not yet run (Docker unavailable on this machine) |
| 2 | The producer | 🚧 built and tested without Kafka; Kafka path not run |
| 3 | Spark: parse, validate, enrich | 🚧 built and tested locally on file input; Kafka source not run |
| 4 | Spark: windowed aggregations | ⬜ |
| 5 | End-to-end with Compose | ⬜ |
| 6 | Hardening and docs | ⬜ |

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

## Chapter 4 — Spark: windowed aggregations ⬜

**The story.** One trip at a time is just data; the interesting part is what happens across many of them. Here the stream becomes insight, such as how busy each pickup zone is each hour and how people pay.

**Implementation plan**
1. Set a watermark of 10 minutes on `tpep_pickup_datetime`, so Spark knows how late data can arrive and can finalize old windows.
2. Add two aggregations:
   - `agg_zone_hourly`: 1-hour tumbling window × pickup zone × service, computing trip count, average distance, and fare, tip, and revenue figures (null for services without fare data).
   - `agg_service_hourly`: 1-hour window × service, computing trip count, for comparing demand across services.
   - `agg_payment_5min`: 5-minute window × payment type, computing count and revenue (Yellow and Green only).
3. Write both in append mode to Parquet, with checkpoints. Set `spark.sql.shuffle.partitions` to about 8 to suit a laptop.
4. Add a console sink in dev mode for fast feedback.

**How we know it worked**
- For fully closed windows, the sum of trip counts across windows equals the clean-trip count.
- One zone-hour spot-checked against a pandas computation on the same parquet matches.

**Expect:** in append mode, a window's output appears only after the watermark passes it. The last windows may not show until more data (or a trailing event) arrives. That is correct behavior and should be documented.

---

## Chapter 5 — End-to-end with Compose ⬜

**The story.** So far each piece was started by hand. Now we make the whole thing run with a single command, the way someone cloning the project would expect.

**Implementation plan**
1. Add the `spark-job` and `producer` services to `docker-compose.yml`.
2. Order startup with `depends_on`: Kafka healthy → `kafka-init` completed → Spark job started → producer started. Starting Spark before the producer means the stream is already listening.
3. Mount `./data` read-only and `./output` for results. If Windows file permissions cause trouble, switch to a named volume.
4. Add `tests/smoke_test.ps1`: bring the stack up with a small `MAX_RECORDS`, wait for completion, then check that the outputs exist and the counts reconcile.

**How we know it worked**
- From a clean checkout, `docker compose up --build` processes the whole file with no manual steps.
- `docker compose down -v` leaves nothing behind.

---

## Chapter 6 — Hardening and docs ⬜

**The story.** A pipeline that works once is a demo. One that survives a restart and can be handed to someone else is a project.

**Implementation plan**
1. Recovery test: stop `spark-job` partway through, restart it, and compare final counts to a clean run. There should be no loss and no duplicates, thanks to checkpoints.
2. Write `README.md`: prerequisites (Docker Desktop, WSL2, at least 6 GB RAM), quick start, configuration table, how to inspect results (Kafka UI, pandas or DuckDB on the Parquet output), and troubleshooting.
3. Make sure the specs in the plan match what was actually built, and update them where they drifted.

**How we know it worked**
- A new person can follow the README and reproduce the results.

---

## Epilogue — Stretch goals

Late and out-of-order data to show the watermark at work; aggregates published back to Kafka with a small dashboard; the full uncapped HVFHV month; maps built from the taxi zone shapefile; and a Delta Lake or Iceberg sink.

---

## Changelog

| Date | Change |
|---|---|
| 2026-11-02 | Created `PLAN.md`, `.gitignore`, and this file. No pipeline code written yet. |
| 2026-11-04 | M3 built: Spark parse/normalize/enrich/validate job, Docker image and Compose service, reconcile script. Streaming run verified on file input only. Fixed a producer bug found by the Spark tests (null injection could erase an injected invalid value). |
| 2026-11-04 | M2 built: data download script, verified schemas (`specs/raw-schemas.md`), config-driven producer with fault injection, 43 passing tests. Kafka path not run (no Docker). |
| 2026-11-04 | M1 files written: `stream-config.yaml`, `.env`, `docker-compose.yml`, `kafka-init/`. Not run: no Docker on this machine. |
| 2026-11-02 | Moved Green, FHV, and HVFHV into the MVP: one topic and producer service per dataset, a unified trip schema in Spark, cross-service aggregations. |
