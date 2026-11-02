# Steps.md — The Story of the NYC Taxi Streaming Pipeline

This file tells the build as a story, one chapter per milestone from `PLAN.md`. Each chapter has the **why**, the **how** (implementation details), and **how we know it worked**. Status markers are updated as work progresses.

**Legend:** ✅ done · 🚧 in progress · ⬜ not started

| Chapter | Milestone | Status |
|---|---|---|
| 0 | Setting the stage | ✅ |
| 1 | Infrastructure up (Kafka) | ⬜ |
| 2 | The producer | ⬜ |
| 3 | Spark: parse, validate, enrich | ⬜ |
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

## Chapter 1 — Infrastructure up ⬜

**The story.** Before any taxi can ride, the road has to exist. Kafka is that road: a durable, ordered log that sits between whoever produces trips and whoever consumes them. We start with Kafka alone and prove it works before adding anything else.

**Implementation plan**
1. Create `.env` with pinned versions and names: `KAFKA_VERSION`, `TOPIC_INVALID=taxi.trips.invalid`. Raw topic names (`taxi.yellow.raw`, `taxi.green.raw`, `taxi.fhv.raw`, `taxi.hvfhv.raw`) are defined in the producer's `datasets.yaml`.
2. Create `docker-compose.yml` with:
   - `kafka`: the `apache/kafka` image in KRaft mode (no ZooKeeper), one broker. It has two listeners: `9092` for the host and `29092` for other containers. Add a healthcheck that runs `kafka-topics.sh --list`.
   - `kafka-init`: a one-shot container that waits for the broker to be healthy, creates all five topics (four raw ones with 3 partitions, 6 for HVFHV, 1-day retention, plus the invalid topic), and exits.
   - `kafka-ui` (optional): the UI on port 8080 for looking at messages.
3. Start it: `docker compose up -d kafka kafka-init`.

**How we know it worked**
- `docker compose ps` shows `kafka` healthy and `kafka-init` exited with code 0.
- Listing topics shows the four raw topics and `taxi.trips.invalid`.

**Watch out for:** the two-listener setup. Getting the advertised listeners wrong is the most common reason containers can't reach Kafka.

---

## Chapter 2 — The producer ⬜

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

## Chapter 3 — Spark: parse, validate, enrich ⬜

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
| 2026-11-02 | Moved Green, FHV, and HVFHV into the MVP: one topic and producer service per dataset, a unified trip schema in Spark, cross-service aggregations. |
