# decisions.md — Decision Log

Every significant choice in this project, the alternatives considered, and why we picked what we did. Decisions are numbered so other docs can reference them.

**Status legend:** ✅ Decided (agreed with the project owner) · 🟡 Proposed (recommended in the plan, awaiting confirmation) · ⬜ Open

| # | Decision | Choice | Status |
|---|---|---|---|
| D1 | Processing model | Streaming (replayed history) | ✅ |
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

---

## D1 — Streaming over batch ✅

**Choice:** Replay the historical trip files as a real-time stream.

**Why:** The goal is a pipeline that streams NYC taxi data. The data is historical, so the replay (with an adjustable speed) is what lets us exercise streaming concepts such as event time, watermarks, and windowed aggregation on data we already have.

**Alternatives**
- *Plain batch ETL (read Parquet, transform, write):* simpler, but doesn't meet the goal and teaches nothing about streaming.
- *A live feed:* TLC publishes monthly files, not a real-time feed, so there is nothing live to connect to.

**Trade-off:** The stream is simulated, so delivery timing is artificial. Event-time logic is real, but latency numbers are not.

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

**Choice:** `PLAN.md` holds specs and acceptance criteria; `Steps.md` tracks progress; implement M1 to M6 in order.

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

---

## D23 — Rejected rows go to Parquet, not the Kafka invalid topic 🟡

**Choice:** `output/invalid` (Parquet, with `reject_reason` and the raw payload). `taxi.trips.invalid` is created but unused for now.

**Why:** It's the simplest to inspect and reconcile, and avoids a second Kafka sink and its checkpoint while M3 is being proven.

**Alternatives:** write rejects back to `taxi.trips.invalid` so another consumer could react to them (a reasonable extension), or both.

---

## Open Questions

| # | Question | Needed by |
|---|---|---|
| Q1 | Confirm D6 to D12 and D14 to D15, or choose alternatives | Before M1 |
| Q2 | HVFHV cap size: measure throughput first, then fix the number | M2 |
| Q3 | How many months to replay (just 2026-01, or more) | M2 |
| Q4 | Whether a dashboard or SQL serving layer is wanted (D12 extension) | After M5 |

## Change Log

| Date | Change |
|---|---|
| 2026-11-02 | Initial log created from decisions in `PLAN.md`, `Steps.md`, and `installation.md`. |
