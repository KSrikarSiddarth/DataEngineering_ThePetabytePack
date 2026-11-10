# decisions.md — Decision Log

Every significant choice in this project, the alternatives considered, and why we picked what we did. Decisions are numbered so other docs can reference them.

**Status legend:** ✅ Decided (agreed with the project owner) · 🟡 Proposed (recommended in the plan, awaiting confirmation) · ⬜ Open

| # | Decision | Choice | Status |
|---|---|---|---|
| D1 | Processing model | Streaming (replayed history); batch added for historic data in D24 | ✅ |
| D2 | Message broker | Apache Kafka | ✅ |
| D3 | Stream processor | Spark Structured Streaming | ✅ |
| D4 | Orchestration | Docker Compose | ✅ |
| D5 | Datasets in the MVP | Yellow, Green, FHV, HVFHV | ✅ |
| D6 | Kafka mode | KRaft (no ZooKeeper), single broker | 🟡 |
| D7 | Topic layout | One raw topic per dataset | 🟡 |
| D8 | Message format | JSON | 🟡 |
| D9 | Producer language and design | Python, one config-driven image | 🟡 |
| D10 | Spark deployment | Single local-mode container | 🟡 |
| D11 | Schema handling | Explicit schemas, normalized to a unified model | 🟡 |
| D12 | Sink | Parquet on a mounted volume | 🟡 |
| D13 | Inspection tools | DuckDB first, pandas for small checks | ✅ |
| D14 | HVFHV volume | Capped sample by default | 🟡 |
| D15 | Data acquisition | Host download script | 🟡 |
| D16 | Development method | Spec-driven, milestone by milestone | ✅ |
| D17 | Version control | Git, with `data/` ignored | ✅ |
| D18 | Replay ordering | Index-based sort by pickup time | 🟡 |
| D19 | FHV message key | `dispatching_base_num` | 🟡 |
| D20 | Fault injection | Seeded, config-driven, tagged in a Kafka header | 🟡 |
| D21 | Spark version and packaging | Spark 3.5.3 via pip in a Python image, connector jars baked in | 🟡 |
| D22 | Validation policy | Reject on rule violations, flag (not reject) missing FHV zones | 🟡 |
| D23 | Rejected rows | Parquet `invalid` output, not the Kafka invalid topic | 🟡 |
| D24 | Batch processing of historic data | A second path next to the stream, sharing one transform module | ✅ (goal) · 🟡 (design) |
| D25 | Historical store and query engines | Partitioned Parquet, 24 months, DuckDB + Spark SQL | 🟡 |
| D26 | Data layers | Bronze (not copied), Silver, Gold; stream builds daily marts, batch builds the rest | 🟡 |
| D28 | Stream and batch results | Separate stores; stream never merged into the historic store; combined only at query time with a cutover date | ✅ |
| D27 | Validation policy v2 | Flag KPI-relevant violations in Silver, reject only unusable rows (replaces D22; implemented in M4) | ✅ |
| D29 | Gold mart shape | Hourly grain, additive measures only; daily and ratios computed at query time | ✅ |
| D31 | Batch layout and monthly marts | Partition by source-file month; monthly marts hold additive sums and trend KPIs are computed by queries | 🟡 |
| D30 | Silver deduplication | `dropDuplicatesWithinWatermark` on `trip_id`, with the watermark (2 minutes) on Kafka ingest time | ✅ |

---

## D1 — Streaming over batch ✅

**Choice:** Replay the historical trip files as a real-time stream.

**Why:** The goal is a pipeline that streams NYC taxi data. The data is historical, so the replay (with an adjustable speed) is what lets us exercise streaming concepts such as event time, watermarks, and windowed aggregation on data we already have.

**Alternatives**
- *Plain batch ETL (read Parquet, transform, write):* simpler, but doesn't meet the goal and teaches nothing about streaming.
- *A live feed:* TLC publishes monthly files, not a real-time feed, so there is nothing live to connect to.

**Trade-off:** The stream is simulated, so delivery timing is artificial. Event-time logic is real, but latency numbers are not.

**Update (2026-11-04):** the project goal is now stream **and** batch processing of historic data (D24). This decision still holds for the live-KPI path: streaming remains the way the "what is happening now" questions are answered. The "plain batch ETL" alternative rejected above is no longer rejected outright; it is now the second path, for a different question.

---

## D2 — Apache Kafka as the broker ✅

**Choice:** Kafka.

**Why:** It is the standard durable, partitioned, replayable log for streaming pipelines. Replayability suits development: we can rerun Spark from the beginning without re-running the producer. Keys and partitions also give us ordering per pickup zone.

**Alternatives**
- *RabbitMQ:* a message queue, not a log. Messages are removed once consumed, so there is no replay, and it integrates less naturally with Spark.
- *Redpanda:* Kafka-compatible, lighter, and starts faster. A good option, but the project asks for Kafka, and the standard Kafka image keeps the learning transferable.
- *Redis Streams / Pulsar:* viable, but have a smaller ecosystem of Spark integrations and tutorials.
- *No broker (producer writes files, Spark watches a folder):* the simplest approach, but skips the part of the pipeline that is the point.

---

## D3 — Spark Structured Streaming for processing ✅

**Choice:** Spark Structured Streaming.

**Why:** It was named in the goal. It also handles the needs of this project well: event-time windows, watermarks, checkpointed exactly-once file sinks, broadcast joins for the zone lookup, and SQL/DataFrame APIs. The same code could scale beyond a laptop.

**Alternatives**
- *Apache Flink:* stronger at low-latency, true event-at-a-time streaming, but heavier to set up and less common for this kind of analytics work.
- *Kafka Streams / ksqlDB:* tightly tied to Kafka, great for simple transforms, but weaker for the file-based sinks and the batch-style analytics we want.
- *Plain Python consumers:* simple, but we would have to build windowing, watermarking, and fault tolerance ourselves.

**Trade-off:** Spark's micro-batch model has latency of seconds, not milliseconds. That is fine for this use case.

---

## D4 — Docker Compose for orchestration ✅

**Choice:** Docker Compose.

**Why:** The goal is a simple pipeline that runs with one command (`docker compose up`). Compose gives reproducible environments, pinned versions, startup ordering, and works on Windows with Docker Desktop.

**Alternatives**
- *Installing Kafka and Spark directly on Windows:* painful (Java, Hadoop native libraries, path issues), and hard to reproduce.
- *Kubernetes (minikube/kind):* far more machinery than a single-machine project needs.
- *Managed cloud (Confluent Cloud, Databricks, EMR):* costs money, needs accounts, and hides the parts we want to learn.

**Trade-off:** A single machine means no real distributed behavior, and resource limits (RAM under WSL 2) matter.

---

## D5 — All four TLC datasets in the MVP ✅

**Choice:** Yellow, Green, FHV, and HVFHV.

**Why:** The project owner decided that downstream applications may need all of them. Covering the four services also forces a design that generalizes: separate topics, per-dataset schemas, and a unified model.

**Alternatives**
- *Yellow only, with the others as extensions:* the original plan. Lower risk and faster to the first working pipeline, but adding datasets later would risk rework in the producer and Spark job.
- *Yellow and Green only:* nearly identical schemas, so little design value.

**Trade-off:** More schemas to verify, more producers to run, and HVFHV's size is a risk (see D14).

---

## D6 — Kafka in KRaft mode, single broker 🟡

**Choice:** `apache/kafka` image with KRaft, one broker, no ZooKeeper.

**Why:** KRaft is the current default and removes a whole service from Compose. One broker is enough for development and uses the least memory.

**Alternatives**
- *ZooKeeper-based Kafka:* older, needs an extra container, and is being phased out.
- *Multi-broker cluster:* demonstrates replication, but doubles resource use for no benefit in this project.
- *Bitnami or Confluent images:* workable, but the official Apache image has the fewest surprises. (Image availability and terms change over time, so check when implementing.)

---

## D7 — One raw topic per dataset 🟡

**Choice:** `taxi.yellow.raw`, `taxi.green.raw`, `taxi.fhv.raw`, `taxi.hvfhv.raw`, plus `taxi.trips.invalid`.

**Why:** Each dataset has a different schema, and keeping them separate means each topic holds one shape of message. Spark parsing stays clean, and each topic can have its own partitions and retention (HVFHV gets more).

**Alternatives**
- *One topic for all trips with a `service` field:* fewer topics, but every consumer has to handle four shapes, and one dataset's volume can crowd out the others.
- *Normalizing before Kafka:* would lose raw fidelity. Keeping raw data in Kafka and normalizing in Spark makes it possible to change normalization later and replay.

---

## D8 — JSON messages 🟡

**Choice:** JSON with ISO-8601 timestamps.

**Why:** Readable in Kafka UI and console consumers, easy to debug, and needs no extra infrastructure. Explicit Spark schemas keep parsing safe.

**Alternatives**
- *Avro or Protobuf with a Schema Registry:* smaller messages and enforced schema evolution. Better for production, but adds a service and more setup than a simple pipeline needs.
- *Raw Parquet bytes:* not suited to per-record streaming.

**Trade-off:** JSON is bigger and slower to parse. At this scale that is acceptable, and we can revisit it if HVFHV throughput demands it.

---

## D9 — Python producer, one config-driven image 🟡

**Choice:** A Python producer using pyarrow and a Kafka client, with a `DATASET` setting and `datasets.yaml`.

**Why:** pyarrow reads Parquet in batches without loading whole files, Python is quick to write and test, and one image reused four times avoids duplicated code.

**Alternatives**
- *Scala/Java producer:* faster and closer to Spark, but slower to develop and heavier to build.
- *Spark as the producer (batch read, write to Kafka):* possible and fast, but replaying with realistic timing is awkward, and it hides the producer's role.
- *Kafka Connect with a file source:* less control over pacing, and awkward with Parquet.
- *Four separate scripts:* simple at first, but they would drift.

---

## D10 — Spark in a single local-mode container 🟡

**Choice:** One `spark-job` container running Spark in local mode (`local[2]`), not a master/worker cluster.

**Why:** It uses the least memory, has the fewest moving parts, and behaves the same as a cluster for the API we use. The job code does not change if we move to a cluster later.

**Alternatives**
- *Master plus worker containers:* shows the cluster architecture, but costs RAM and startup complexity with no extra capability on one machine.
- *Spark on Kubernetes or YARN:* out of proportion for the project.

**Trade-off:** No real distribution, so we can't demonstrate scaling out.

---

## D11 — Explicit schemas and a unified trip model 🟡

**Choice:** Each dataset gets an explicit Spark `StructType`; all are normalized into one unified trip schema with an `extras` column for dataset-specific fields.

**Why:** Schema inference on streams is slow, unreliable, and hides drift. Explicit schemas fail loudly when files change (and 2026 files may have new columns). The unified model lets applications query across services, which was the reason for D5.

**Alternatives**
- *Inference:* easier up front, but fragile.
- *Keep four separate models only:* faithful to the sources, but every cross-service question becomes a custom union.
- *Common columns only (drop the rest):* simple, but loses information such as driver pay and trip type; `extras` avoids that.

---

## D12 — Parquet files as the sink 🟡

**Choice:** Append-mode Parquet on a mounted volume, with checkpoints.

**Why:** It's columnar and compact, readable by DuckDB, pandas, and Spark, and needs no additional service. File sinks with checkpoints give exactly-once output.

**Alternatives**
- *Postgres or ClickHouse:* good for SQL serving and dashboards, but add a service and schema management. Listed as an extension.
- *Delta Lake or Iceberg:* add ACID tables, schema evolution, and time travel; strong candidates for a later upgrade, but add dependencies.
- *Console only:* fine for debugging, not for keeping results.

---

## D13 — DuckDB first, pandas for small checks ✅

**Choice:** DuckDB for inspecting Parquet output and reconciliation checks; pandas for small samples, tests, and plotting.

**Why:** DuckDB queries Parquet in place with SQL, streams out-of-core, and aggregates far faster than pandas on millions of rows. HVFHV is too large to load comfortably in pandas. pandas remains convenient for small DataFrames and plots, and the two interoperate.

**Alternatives**
- *pandas only:* memory-bound on big files.
- *Spark for inspection:* works, but spinning up Spark to peek at a file is heavyweight.
- *Polars:* fast and a good alternative; DuckDB's SQL fits our reconciliation queries better.

---

## D14 — Cap HVFHV by default 🟡

**Choice:** A default `MAX_RECORDS_HVFHV` cap (suggested about 500k rows), with 6 partitions.

**Why:** The HVFHV month has tens of millions of rows, which risks overwhelming a laptop running Kafka and Spark in Docker. A cap lets the whole pipeline run end to end; the cap can be raised or removed.

**Alternatives**
- *Full month by default:* realistic volume, but likely to cause slow runs and out-of-memory failures.
- *Exclude HVFHV:* contradicts D5.
- *Random sampling:* keeps the time distribution, but needs a full scan; the cap takes the earliest records, so measure throughput in M2 before choosing.

---

## D15 — Host-side download script 🟡

**Choice:** `scripts/download_data.py` run once on the host, saving files to `data/`.

**Why:** Simple, cached after the first run, and no network access needed inside Docker. The file names follow a predictable pattern, so we build the URLs rather than scrape the TLC page.

**Alternatives**
- *A Compose `data-init` service:* fully self-contained, but needs internet in the container and re-downloads on clean volumes.
- *Manual download:* works, but isn't reproducible.
- *Scraping the TLC page:* brittle if the HTML changes.

---

## D16 — Spec-driven, milestone by milestone ✅

**Choice:** `PLAN.md` holds specs and acceptance criteria; `Steps.md` tracks progress; implement M1 to M8 in order.

**Why:** Each piece is verified before the next depends on it (Kafka, then producer, then Spark), which keeps failures easy to locate. Written specs also make the behavior testable.

**Alternatives**
- *Build everything and debug at the end:* faster to start, much harder to debug across Kafka, producers, and Spark together.
- *Notebook-first exploration:* good for discovery, poorer for a reproducible Compose project.

---

## D17 — Git, ignoring `data/` ✅

**Choice:** Use Git; add `data/` to `.gitignore`.

**Why:** Version control lets us commit per milestone. The datasets are large, public, and re-downloadable, so they stay out of the repository.

**Alternatives**
- *Git LFS or DVC for data:* useful when data must be versioned; unnecessary here since the source is public.
- *No version control:* rules out history and rollback.

---

## D18 — Index-based sort for replay 🟡

**Choice:** The producer reads only the pickup column, computes the sort order (after sampling and the record cap), then loads just the selected rows and sorts them.

**Why:** None of the four 2026-01 files is sorted by pickup time, so a replay needs a sort. This way a capped HVFHV replay sorts 500k rows, not 20.9M, and memory stays bounded.

**Alternatives**
- *Replay in file order:* simplest, but event times jump around and the watermark and window results become meaningless.
- *Load and sort the whole file:* fine for Green and FHV, but HVFHV needs several GB.
- *Per-day bucket files on disk:* handles the uncapped HVFHV case, but is more code. A fallback if the uncapped month is ever needed.

**Trade-off:** An uncapped HVFHV replay still sorts all selected rows in memory, so keep the cap (D14).

---

## D19 — FHV is keyed by `dispatching_base_num` 🟡

**Choice:** The other datasets use `PULocationID` as the Kafka key. FHV uses `dispatching_base_num`.

**Why:** In the real FHV file `PUlocationID` is null in about 85% of rows, so keying by it would send most messages with no key and lose partition affinity. `dispatching_base_num` is never null.

**Alternatives:** keep `PUlocationID` (mostly unkeyed), or no key at all (round-robin). Both give up ordering per key.

---

## D20 — Seeded, config-driven fault injection, tagged in a header 🟡

**Choice:** Quality faults come from `stream-config.yaml` profiles (`clean`, `realistic`, `messy`), are driven by a fixed random seed, and are recorded in a `fault` Kafka header.

**Why:** Repeatable runs make tests and reconciliation possible. Header tags let us tell injected faults from the natural ones in the data (which are substantial), without changing the message body that Spark parses.

**Alternatives:** inject in Spark (hides the fault from the broker and from the raw topic), put the tag in the JSON body (pollutes the schema), or no tagging (can't measure what the validation step should have caught).

---

## D21 — Spark 3.5.3 from pip, connector jars baked into the image 🟡

**Choice:** `python:3.11-slim` + Java + `pyspark==3.5.3`, with the Kafka connector jars (connector 3.5.3, kafka-clients 3.4.1, commons-pool2 2.11.1) downloaded from Maven Central at build time.

**Why:** Spark 3.5 runs on Java 11 and 17 and matches the Java already on the development machine, and the same pinned pyspark runs both in Docker and in local tests. Baking the jars in means the job doesn't download anything at start-up. The jar versions are the ones in the connector's own pom, and the URLs were checked.

**Alternatives**
- *Spark 4.x:* newer, but needs Java 17+ and a different Scala/Kafka connector line; no benefit for this job.
- *The `apache/spark` image:* less setup, but I could not verify the exact tags from here, and it can't be reused for local tests.
- *`--packages` at runtime:* simpler Dockerfile, but needs internet access and time on every cold start.

---

## D22 — Reject on rule violations, flag missing FHV zones 🟡

**Choice:** A row is rejected for a null/out-of-range pickup time, dropoff before pickup, trips over 24h, negative distance or amount, an unknown zone ID, or (Yellow, Green, HVFHV) a null zone. FHV rows whose zone is null are kept and flagged in `dq_flags`.

**Why:** In the real FHV file the pickup zone is null in about 85% of rows, so rejecting them would discard most of the dataset. For the other three, a missing zone is genuinely abnormal. The list is configurable (`location_required_services`).

**Alternatives:** reject every null zone (loses most FHV), or flag everything and reject nothing (turns validation into reporting only).

**Update (2026-11-05):** D27 proposes replacing the reject policy with flags in Silver, because the KPI catalog's data-quality KPIs are computed from flags. The FHV zone rule above stays as it is (flag, not reject).

---

## D23 — Rejected rows go to Parquet, not the Kafka invalid topic 🟡

**Choice:** `output/invalid` (Parquet, with `reject_reason` and the raw payload). `taxi.trips.invalid` is created but unused for now.

**Why:** It's the simplest to inspect and reconcile, and avoids a second Kafka sink and its checkpoint while M3 is being proven.

**Alternatives:** write rejects back to `taxi.trips.invalid` so another consumer could react to them (a reasonable extension), or both.

---

## D24 — Batch processing of historic data as a second path ✅ (goal) 🟡 (design)

**Choice:** Add a Spark batch job that processes many months of Parquet into a historical store, next to the streaming path. Both paths call the same normalize, validate and enrich functions in `spark/transform.py`. The batch path is its own milestone (M5) and its own Compose profile (`batch`), and needs no Kafka.

**Why:** The two paths answer different questions. The stream answers "what is happening now?" (the daily KPI marts, built incrementally). The batch path answers "what happened over time?" (trends, peak hours, top routes, fare economics) and shows how SQL queries behave on hundreds of millions of rows. The project owner asked for the goal to be stream + batch; the historic-query capability is a deliberate second goal, not a by-product.

**Alternatives**
- *Stream-only, replaying more months:* makes the stream run for hours and still cannot answer a year-long query quickly.
- *Batch by replaying history through Kafka into the same streaming job:* reuses the stream, but forces hundreds of millions of rows through a broker for no benefit, and hides the batch concepts (partitioning, pruning, scan speed) the goal is meant to show.
- *Separate batch code that re-implements validation:* faster to write, but the two paths would drift. Sharing `transform.py` and reconciling the streamed month against batch (M5) keeps them honest. Cost: `transform.py` needs a typed-Parquet entry point that skips `from_json`.
- *Lambda-style merged serving layer now:* combining live and historic results in one view is attractive but is left as a stretch goal.

**Trade-off:** more disk, more code, and a longer test and download cycle. Fault injection does not apply to batch (it reads the original files).

---

## D25 — Historical store and query engines 🟡

**Choice:** Parquet partitioned by `service_type/year/month` under `output/batch/silver/trips` (and Gold marts under `output/batch/gold/`); **24 months by default** (2024-01 to 2025-12, ending before the streamed month, D28); queried in place with DuckDB and Spark SQL; timings from a benchmark script.

**Why**
- *Partitioned Parquet:* the same format as the stream sink (D12), so no new storage service, and partition pruning and column selection are exactly the effects the benchmark should show.
- *24 months (changed from 12 on 2026-11-05):* the KPI catalog (`specs/3_KPI.html`) asks for year-over-year growth, a rolling 12-month average, a seasonality index, service-type share shift "over years", and fee impact around policy dates. Year-over-year growth needs at least 13 months (24 for a full series), seasonality needs two cycles, and the CBD congestion fee (January 2025) needs months before it, which 2025-02 to 2026-01 does not have. 24 months (2024-01 to 2025-12) covers all of these except the 2022 Airport fee comparison, with the whole of 2024 before the January 2025 fee. The streamed month is outside the range (D28), and the range is a setting, so a laptop can start with 3 months.
- *DuckDB:* already adopted (D13); handles hundreds of millions of rows on one machine without a server.
- *Spark SQL:* same data, same container image as the batch job, so it shows the distributed-engine side for comparison.

**Alternatives**
- *Trino, Postgres, ClickHouse:* real serving layers and a good stretch goal, but each adds a service, loading, and setup that is not needed to demonstrate the goal.
- *Delta Lake or Iceberg tables:* better updates and time travel, but more dependencies; also a stretch goal (see D12).
- *A single huge Parquet file:* simple, but gives no pruning and makes idempotent per-month reruns impossible.
- *Partition by day:* finer pruning, but roughly 365 partitions per service and many small files; month is the better default at this size.

- *12 months:* the original proposal; about half the disk, but several catalog KPIs could not be computed.
- *49 months (2022-01 to 2026-01):* also covers the 2022 Airport fee comparison, but roughly doubles disk again (about 80 GB) and the batch run time.

**Current setting (2026-11-10):** the range in `stream-config.yaml` is set to 3 months (2025-10 to 2025-12) for development; year-over-year, the rolling 12-month average and seasonality then report "not enough history". Set `batch.start_month` back to 2024-01 for the 24-month default.

**Open:** disk and time. Measured on 2 real months of Yellow, Green and FHV: Silver is 56–69 bytes per row, so 24 months with HVFHV is about 72–85 GB (about 55–60 GB with zstd and a 16-character `trip_id`), and the run speed of about 1.2M rows per minute on a 2-core VM means roughly 9 hours (see `storage_requirements.md`). Decide whether to cut months, apply the reductions, or skip HVFHV for older months.

---

## D26 — Medallion layers (Bronze, Silver, Gold) and which path builds what 🟡

**Choice:** Organize both paths as Bronze, Silver and Gold, following `specs/2_nyc_taxi_medallion_architecture.html`.
- **Bronze** is the landing layer and is not copied: the Kafka raw topics (stream) and the TLC files in `data/` (batch).
- **Silver** (`silver.trips`, dimensions, `batch_run_log`, `ml_features`) is written by both paths, to separate folders: `output/stream/silver/` and `output/batch/silver/`.
- **Gold** marts follow `specs/3_KPI.html`. The stream path builds the daily marts incrementally. The batch path rebuilds the daily marts and owns the monthly marts, pipeline health and ML outputs.

**Why:** The specs define the target tables, so using them directly avoids inventing a second data model. Separate folders per path keep the two independent (D28); they are combined only at query time. Not copying Bronze saves 7–14 GB. The catalog does not say which path builds which mart; building the daily marts in the stream keeps the live path using the same KPI definitions as the batch path.

**Alternatives**
- *Copy Bronze into Parquet tables:* matches the spec literally and gives replayable raw data, but doubles the raw storage. The Kafka log and the downloaded files already serve as replayable raw data.
- *One shared Silver for both paths:* less disk, but the paths would write over the same month and nothing could be reconciled.
- *Batch builds all Gold, stream builds only Silver:* simpler, but the streaming half of the project would then answer none of the KPIs.
- *Add minute-scale stream KPIs:* more "real-time", but they are not in the catalog; left as a stretch goal.

**Consequence:** the M3 output (`output/trips_clean`, `output/invalid`, unified column names) must be reworked to match Silver, as the first part of M4. The gaps are listed in `PLAN.md` §6.7.

---

## D27 — Flag, don't reject, rule violations that the KPIs measure ✅

**Choice:** Keep trips with dropoff before pickup, zero or negative distance, negative fare, unmatched zones, and pickup times outside the file's month in `silver.trips`, with a `dq_*` flag. Only rows that cannot be used at all (malformed JSON, null pickup time) go to `silver_invalid`. Gold marts filter on the flags (for example, the efficiency marts exclude rows with `dq_dropoff_before_pickup`). **This replaces D22 (confirmed by the project owner on 2026-11-10).**

**Why:** The catalog's data-quality KPIs ("dropoff-before-pickup share", "zero or negative distance and fare share", "unmatched zone share", null-rate drift) are computed from flags on Silver rows. Under D22 those rows are rejected and never reach Silver, so the KPIs would have to be rebuilt from the rejected data. Silver's description also says "validated", with `dq_*` columns in its schema.

**Alternatives**
- *Keep D22 (reject):* Silver stays clean, and the quality KPIs read from `silver_invalid`. It works, but splits one KPI across two tables, and the `dq_*` columns in the Silver spec would be unused.
- *Both:* flag in Silver and also copy to `silver_invalid`. Double counting makes reconciliation harder.

**Trade-off:** every Gold query must remember the filter; a shared view or the mart definitions in `spark/gold.py` apply it in one place. The M3 reconcile test (every injected fault ends in a reject) changes to "every injected fault is flagged or rejected as expected".

**Implemented in M4 (2026-11-09).** `silver_invalid` holds only `malformed_json` and `null_pickup_time`. Two flags beyond the medallion page, `dq_pickup_out_of_range` and `dq_trip_too_long`, replace the M3 rejects for those cases. On an 80,000-message run with the `realistic` profile, 58 rows were rejected and about 21,000 flags were set (mostly FHV's natural 85% null pickup zone).

---

## D29 — Gold marts: hourly grain, additive measures ✅

Confirmed by the project owner on 2026-11-10.

**Choice:** Every Gold daily mart is grouped by the pickup hour and holds only counts and sums (for example `trips`, `sum_total_amount`, `sum_distance_miles`), never averages or ratios. A day is the sum by `pickup_date`; a KPI such as average fare is `sum_total_amount / trips`, computed in the query. The formulas are in `specs/gold-schema.md`.

**Why:** The plan says the stream's grain is the hour, rolled up to the day. A stored average cannot be rolled up (the average of hourly averages is wrong), but sums can. The same property lets the stream marts and the batch marts be unioned and compared exactly (D28, M5).

**Alternatives**
- *Daily rows with averages:* matches the catalog wording but cannot be built incrementally in a stream, and cannot be rolled up from hourly rows.
- *Two sets of tables, hourly and daily:* more tables and more to keep consistent.

**Trade-off:** a dashboard needs a small formula per KPI instead of reading a ready-made column. The queries in `scripts/` hold them.

---

## D30 — Deduplication watermark on ingest time ✅

Confirmed by the project owner on 2026-11-10, with the window set to 2 minutes (first proposed as 5) to cut the dedup state in memory. The producer's duplicates appear within 100 records of the original, far inside 2 minutes.

**Choice:** Silver is deduplicated with `dropDuplicatesWithinWatermark(["trip_id"])`, with the watermark on `kafka_timestamp` (2 minutes, `processing.dedup_watermark_minutes`), not on the pickup time. The Gold stage keeps its own 10-minute watermark on `pickup_ts`.

**Why:** Spark drops rows whose event time is older than the watermark in a deduplication. With the watermark on pickup time, the injected late trips and out-of-range pickups would be silently dropped, and the project's accounting (sent = Silver + invalid + removed duplicates) and the "flag, don't reject" policy (D27) would not hold. Ingest time never goes backwards, so only real repeats are removed.

**Trade-offs:** A duplicate arriving more than 2 minutes (ingest time) after the first is kept. The count of removed rows is derived (sent - Silver - invalid) in `scripts/reconcile.py` and compared with the producer's injected duplicates, because a streaming query cannot count what it drops. Trips that are identical in every field (common in FHV, which has few fields) are removed too: on a 20,000-message FHV sample, 46 rows were removed against 21 injected duplicates, and the other 25 were identical rows already in the TLC file (gap 4 in `PLAN.md` §6.7).

---

## D31 — Batch partitions by source-file month; monthly marts are additive 🟡

**Choice:** (1) Silver, `silver_invalid` and the Gold marts of the batch store are partitioned by the month of the **source file** (`year=/month=`), not by each trip's pickup time. A file is loaded by deleting and rewriting its own partition. (2) The monthly marts (`trips_monthly`, `revenue_monthly`, `trip_efficiency_monthly`, `demand_monthly_hourly`, `zone_od_monthly`, `hvfhv_economics_monthly`, `data_quality_monthly`) are sums of the daily marts over one load month with fewer keys. Month-over-month and year-over-year growth, the rolling 12-month average, the seasonality index and the shares are computed by `scripts/queries/*.sql`, not stored.

**Why:** (1) A TLC file holds some rows with pickup times outside its month (flagged by `dq_pickup_out_of_range`). Partitioning by pickup time would put them in another month's partition, so rerunning one file could overwrite rows that another file contributed. Partitioning by file month makes a rerun touch exactly one partition per dataset, which is what the plan's acceptance test 2 needs. (2) D29 applied to the monthly grain: stored ratios cannot be rolled up or recomputed after a month is reloaded, and a query can return NULL ("not enough history") exactly where a window is incomplete, which the catalog asks for.

**Alternatives**
- *Partition by pickup month (the plan's first wording):* matches the date of each trip, but breaks idempotence as above.
- *Store growth, rolling and seasonality columns in the monthly marts (the medallion page's wording):* ready-made columns, but the whole table must be recomputed after every load, and a reload of an old month changes later rows.

**Trade-offs:** a trip picked up on Dec 31 and stored in the January file counts in January's monthly mart (the daily marts keep its real date). Dashboards need the query formulas. `trips_monthly` for a month can therefore differ by a few rows from a count by pickup month.

---

---

## D28 — Combine stream and batch at query time only ✅

**Choice:** The stream path writes only to `output/stream/` and the batch path only to `output/batch/`. The stream's output is **never appended to or merged into the historic store**. The two are combined only when queried, through DuckDB/Spark SQL views with a **cutover date** (`combined.cutover_date`, default 2026-01-01): batch rows before the cutover, stream rows from it on. Each date comes from exactly one side. The batch range therefore ends before the streamed month (2024-01 to 2025-12). The KPIs are unchanged, including the live-against-baseline comparison, which is now a query over the combined views.

**Why:** Decided by the project owner (option A). No data is copied or promoted, the historic store stays a clean, repeatable batch result, and each path can be run, reset and verified on its own. The cutover rule is a single config value.

**Alternatives**
- *Append the stream into the historic store, then overwrite it when the batch result is final:* the usual "streaming now, correct later" pattern, but it needs a promotion step and a winner rule, and it mixes the two paths' data.
- *One shared store both paths write to:* simplest to query, but risks duplicates and breaks the stream-vs-batch comparison.
- *No combination at all:* simplest, but the live-against-baseline KPI could not be answered.

**Consequences**
- The batch range changes from 2024-02..2026-01 to **2024-01..2025-12** (D25). The streamed month is no longer in the batch store.
- The stream/batch comparison for 2026-01 (M5) uses a throwaway batch run into `output/check/`, deleted afterwards.
- New in M5: `scripts/combined_views.py` and `scripts/queries/live_vs_baseline.sql`. Only the daily marts exist on both sides, so only those are combined (PLAN.md §6.8).
- Each date must be on exactly one side. A gap or overlap between the batch range and the cutover is a bug, and a test covers it.
- The cutover is a date, not a live pointer: if the stream replays a different month, change `cutover_date` and the batch range together.

---

## Open Questions

| # | Question | Needed by |
|---|---|---|
| Q1 | Confirm D6 to D12 and D14 to D15, or choose alternatives | Before M1 |
| Q2 | HVFHV cap size: measure throughput first, then fix the number | M2 |
| Q3 | ~~How many months to replay~~ Resolved: stream replays 2026-01; batch uses 24 months (D25, to confirm) | M2 / M5 |
| Q4 | Whether a dashboard or SQL serving layer is wanted (D12 extension) | After M7 |
| Q5 | Confirm D24 and D25 design: 24 months, 12, or 49; DuckDB + Spark SQL only | Before M5 |
| Q6 | Confirm D26 (medallion layout, stream builds daily marts). D27 is confirmed (2026-11-10). | Before M5 |
| Q7 | Spec details: the `fhvhv` vs `hvfhv` service value, and the extra Silver columns the KPIs need (`PLAN.md` §6.7 gaps) | Before M4 |
| Q8 | Is the 2022 Airport fee comparison required? If so the range must start in 2021 or 2022 (D25) | Before M5 |

## Change Log

| Date | Change |
|---|---|
| 2026-11-10 | M5 built. Added D31 (batch partitions by source-file month; monthly marts additive).
| 2026-11-10 | D27, D29 and D30 confirmed. The deduplication window (D30) changed from 5 to 2 minutes to reduce state memory. |
| 2026-11-09 | M4 built. D27 implemented. Added D29 (Gold marts as hourly additive measures) and D30 (deduplication watermark on Kafka ingest time). |
| 2026-11-05 | Added D28: stream and batch stay separate stores, combined only at query time with a cutover date (option A). Batch range changed to 2024-01..2025-12. Kept the live-against-baseline KPI, now a query over the combined views. |
| 2026-11-05 | Read `specs/2_…medallion…html` and `specs/3_KPI.html`. Added D26 (medallion layers, which path builds which mart) and D27 (flag instead of reject; would replace D22). Changed D25 from 12 to 24 months, because the catalog's multi-year KPIs need more history. Added Q6 to Q8. Added M4 rework, M6 (Spark ML); end-to-end is now M7, hardening M8. |
| 2026-11-04 | Added D24 (batch path alongside the stream) and D25 (historical store and query engines). Updated D1 and D16. Resolved Q3, added Q5. |
| 2026-11-02 | Initial log created from decisions in `PLAN.md`, `Steps.md`, and `installation.md`. |
