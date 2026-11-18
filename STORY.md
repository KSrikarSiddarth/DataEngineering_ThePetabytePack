# STORY.md — From a pile of taxi files to something worth knowing

A walk through the NYC Taxi data, told in the order we hit each problem. Every chapter starts with a
question, shows what the data did to us, names the data engineering concept that answers it, and points at the
file in this repo where it lives.

**Honesty note.** Numbers marked *(measured)* come from runs recorded in `storage_requirements.md` and
`evaluation.md`. Numbers marked *(estimate)* are plans. The two-machine cluster in Chapter 8 is the scale-out
step of the story; the repo today runs Spark in `local[2]` inside Docker (D10), so Chapter 8 is a runbook to
verify, not a result we have recorded.

---

## Cast

| Role | Machine | Job |
|---|---|---|
| **Master** | Local Windows 11 PC (Docker Desktop + WSL 2) | Holds the code, the Kafka broker, the Spark master, the DuckDB/Streamlit front end |
| **Worker** | One Ubuntu VM | Runs a Spark worker: extra cores and RAM to chew through partitions |
| **Data** | NYC TLC trip records | Yellow, Green, FHV, HVFHV, monthly Parquet files |

The goal in one sentence: *turn four differently shaped, dirty, very large trip datasets into trusted KPIs, live
and historical, plus a few models that earn their keep.*

---

## Chapter 1 — Opening the box (the raw data)

**Question:** what are we even dealing with?

We downloaded one month (2026-01) of each dataset with `scripts/download_data.py` and looked at the schemas
(`specs/raw-schemas.md`). Three things stood out:

1. **Four datasets, four shapes.** Yellow has full fares, Green uses `lpep_*` timestamps and `trip_type`, FHV has
   *no fares or distance at all*, HVFHV adds driver pay, wait times and shared-ride flags.
2. **Size is wildly uneven.** Per month (*measured*): Yellow ≈ 61 MB, Green ≈ 1 MB, FHV ≈ 21 MB, HVFHV ≈ 482 MB
   (about 20.9 M rows). HVFHV is more than 85% of all input. Over 24 months that is ≈ 14 GB raw.
3. **The files are unsorted and imperfect.** FHV's pickup zone is ~85% null, so it cannot be a partition key;
   TLC files contain negative fares, dropoffs before pickups, and out-of-range dates.

**Concepts**
- **Columnar storage (Parquet).** Compressed, typed, and readable by column. Asking "average fare" touches one
  column, not the whole file. This is why 14 GB of raw trips is manageable at all.
- **Schema drift / heterogeneity.** One business ("a trip") arrives in four dialects.

---

## Chapter 2 — Why one laptop stops being enough (the scale problem)

**Question:** can we just load it into pandas?

Do the arithmetic before writing any code:

| Fact | Value |
|---|---|
| Rows in the 24-month plan | ~500 M (almost all HVFHV) *(estimate)* |
| Silver size per row | 56–69 bytes, ~5× the source Parquet *(measured on 12 M rows)* |
| Silver for 24 months with HVFHV | ~72–85 GB *(estimate, HVFHV extrapolated)* |
| Throughput on a 2-core VM | ~1.2 M rows/min *(measured)* |
| Time for the full range on that VM | ~9 hours *(estimate from the measured rate)* |

pandas would need the data in RAM; the full range does not fit in a 16 GB PC. Even with Spark, one 2-core box
takes a working day. This is the *petabyte pack* lesson in miniature: **the question is never "is the data big?"
but "which resource runs out first — memory, disk, or time?"** Here it is time, then disk.

**Concepts**
- **Scale up vs scale out.** A bigger machine helps once; more machines help repeatedly.
- **Back-of-envelope sizing.** `storage_requirements.md` exists because the first measurement overturned the
  first guess (Silver was 5× the source, 47% of a Yellow file was the 32-char `trip_id`). We shrank the plan to
  3 months for development *(D25)* rather than discovering the disk was full at hour 7.

---

## Chapter 3 — Giving the data a shape (Medallion layers)

**Question:** where do raw, clean and useful data live, and who can touch them?

We adopted **Bronze → Silver → Gold** (`specs/2_nyc_taxi_medallion_architecture.html`, D26):

| Layer | What it is here | Rule |
|---|---|---|
| **Bronze** | Kafka topics + the files in `data/` | Never edited, never copied again (saves 7–14 GB) |
| **Silver** | `silver.trips` — one conformed trip model + dimensions (`dim_zone`, `dim_hvfhs_company`) | Cleaned, typed, enriched, deduplicated |
| **Gold** | KPI marts (`trips_daily`, `revenue_daily`, `hvfhv_economics_daily`, …) | Small, additive, business-shaped |

**Concepts**
- **Medallion architecture.** Each layer has one job, so a bug can be traced to the layer that introduced it.
- **Raw is replayable.** Because Bronze is immutable, any later fix to Silver is a re-run, not a re-download.

---

## Chapter 4 — One trip model for four dialects (schema on read, explicit schemas)

**Question:** how do we ask "how many trips yesterday?" across Yellow, Green, FHV and HVFHV in one query?

`spark/transform.py` normalizes every dataset into a unified trip model driven by `producer/datasets.yaml`.
Fields a service lacks (fares for FHV) become null; fields we do not map are kept as JSON in `extras` so nothing is
lost. Schemas are **explicit `StructType`s, never inferred** (inference on a 500 M-row file is a full extra pass
and silently changes types between months).

**Concepts**
- **Canonical data model.** Downstream code (Gold, ML, dashboard) is written once against Silver.
- **Explicit schema over inference.** Cheaper and safer at scale.
- **One code path for stream and batch.** Both call `transform.py`, so a trip is judged identically in both
  paths and the two can be reconciled (D24).

---

## Chapter 5 — Trust: data quality without losing data

**Question:** a trip with a negative fare — drop it, fix it, or keep it?

We split problems in two (D22, D27):

- **Reject** only what is unusable: malformed JSON, null pickup time → `silver_invalid`.
- **Flag, don't delete** everything the KPIs *measure*: `dq_dropoff_before_pickup`, `dq_zero_distance`,
  `dq_negative_fare`, `dq_unmatched_zone`, `dq_trip_over_24h`, … stay in Silver with a boolean.

Then `data_quality_daily` turns the flags into a KPI of its own: the pipeline reports how dirty its input is.

**Concepts**
- **Data quality as a measurement, not a filter.** Deleting bad rows hides the very signal (policy changes,
  broken feeds) an analyst wants.
- **Reconciliation.** `scripts/reconcile.py` proves `sent = Silver + invalid + deduplicated`. No row vanishes
  silently.

---

## Chapter 6 — "What is happening now?" (streaming)

**Question:** can we see the city's demand as it happens?

TLC publishes monthly files, not a feed, so we **replay** January 2026 as a stream (D1): four producers,
one per dataset, push JSON to Kafka at `replay_speedup: 3600` (one simulated hour per real second, a month in
about 12 minutes). Spark Structured Streaming reads the topics and writes Silver and the daily Gold marts.

| Problem | Concept | Where |
|---|---|---|
| Many producers, one broker, ordered per key | **Topics, partitions, keys.** Key = `PULocationID` keeps a zone's trips in one partition (FHV keys on `dispatching_base_num` since its zone is mostly null, D19). HVFHV gets 6 partitions, the others 3 | `stream-config.yaml`, `kafka-init/` |
| Trips arrive late and out of order | **Event time + watermark.** Windows are by pickup time, not arrival; a 10-minute watermark decides how late is too late | `spark/stream_job.py` |
| Producers retry, so duplicates appear | **Idempotent processing.** `dropDuplicatesWithinWatermark` on `trip_id`, watermark on ingest time (D30) | `spark/stream_job.py` |
| A burst could swamp Spark | **Backpressure.** `max_offsets_per_trigger: 50000` bounds each micro-batch | `stream-config.yaml` |
| Real feeds are dirty | **Fault injection.** Seeded, config-driven bad rows (D20) prove the pipeline catches them | `producer/faults.py` |
| Crash mid-run | **Checkpointing.** Restart resumes from offsets with no loss and no duplicates (target S5) | `output/checkpoints/` |

The streaming targets (p95 latency to Silver ≤ 45 s, ~7,000 msg/s) are in `evaluation.md` §2 and are
*proposed, not yet measured* — the Docker path had not been run when that file was written.

---

## Chapter 7 — "What happened over time?" (batch)

**Question:** year-over-year growth, seasonality, and the effect of the January 2025 congestion fee cannot come
from one replayed month. How do we process history?

A Spark batch job (`spark/batch_job.py`, D24) reads monthly files and writes Silver partitioned by
`service_type/year/month`, then rebuilds the daily marts and adds monthly marts. 24 months, because the KPI
catalog needs 13+ months for growth and a full year *before* the fee date (D25). It deliberately ends at
2025-12 so the streamed month never overlaps (D28).

**Concepts**
- **Partitioned storage and pruning.** A query on `service_type='yellow' AND month='2025-03'` opens one folder,
  not 24 months of four services.
- **Idempotent, per-month loads.** Re-running one month overwrites only that month's partition (D31); a failed
  file does not poison the rest. `batch_run_log` records every load.
- **Additive marts.** Gold stores sums and counts (`sum_total_amount`, `trips`), never averages. Daily →
  monthly is then a plain `SUM`, and ratios (average fare = `sum_total_amount / trips`) are computed at query
  time, so they stay correct at any grain (D29).
- **Stream and batch as two answers.** Stream answers "now", batch answers "over time". They write to
  separate folders and are joined only at query time through a cutover date (D28). It is a deliberate,
  simpler cousin of the Lambda architecture.

---

## Chapter 8 — Adding a second machine (Windows master, Ubuntu worker)

**Question:** the batch job is the long pole (~9 h on 2 cores). What changes when we add a worker?

Spark already knows how to split the work: a month of HVFHV is dozens of row groups and many files, i.e. many
**partitions**, each an independent task. A *master* hands tasks to *workers*; every worker core runs one at a
time. Going from 2 cores to 2 + N cores shrinks wall-clock time roughly in proportion — **until something other
than CPU becomes the limit.** This chapter is where the distributed-systems concepts show up.

### 8.1 Topology

```
 Windows PC (master)                         Ubuntu VM (worker)
 ┌──────────────────────────┐   LAN/bridge   ┌──────────────────────────┐
 │ Spark master  :7077 (UI 8080)│◄──────────►│ Spark worker  (cores, RAM)│
 │ Spark driver (batch_job.py) │              │ executors run tasks       │
 │ Kafka :9092, DuckDB, Streamlit│            │ local disk: partial output│
 └──────────────────────────┘                └──────────────────────────┘
          shared data + output path visible to BOTH (SMB/NFS, or identical copies)
```

### 8.2 Runbook (to verify on your machines)

1. **Network.** Put the VM on a *bridged* adapter so the PC and VM can reach each other. Open ports 7077
   (master), 8080/8081 (UIs), and the driver and block-manager ports Spark chooses (pin them with
   `spark.driver.port` and `spark.blockManager.port`, otherwise the firewall silently blocks executors).
2. **Same versions on both sides.** Spark **3.5.3** and Python **3.12** (D21). A Python or Spark mismatch fails
   with confusing py4j errors (the same trap `installation.md` §5 describes for a stray `SPARK_HOME`).
3. **Start the master** (Windows, inside the Spark container/WSL):
   `spark-class org.apache.spark.deploy.master.Master --host <PC-IP>`
4. **Start the worker** (Ubuntu VM):
   `spark-class org.apache.spark.deploy.worker.Worker spark://<PC-IP>:7077 --cores 4 --memory 6g`
   The master UI at `http://<PC-IP>:8080` should now list one worker.
5. **Point the batch job at the cluster.** `docker-compose.yml` already reads `BATCH_SPARK_MASTER`
   (default `local[2]`):
   `BATCH_SPARK_MASTER=spark://<PC-IP>:7077 docker compose --profile batch up spark-batch`
6. **Make the data visible to both machines.** Executors read `data/` and write `output/` themselves. Share
   the folders (SMB from Windows, mounted on the VM at the *same path*) or copy `data/` to both. Without this
   the job fails with "file not found" on whichever machine does not have the files.

### 8.3 Concepts you will actually see

| What you observe | Concept |
|---|---|
| Master UI shows tasks spread over local + remote cores | **Data parallelism**: one task per partition |
| HVFHV stage finishes last while Green is instant | **Skew**: the work is not evenly sized, so the slowest partition sets the finish time |
| Zone enrichment is cheap | **Broadcast join**: the 265-row zone lookup is copied to every executor instead of shuffling 500 M rows |
| Dedup and Gold group-bys pause between stages | **Shuffle**: data moves between machines over the network. This is the expensive step. A LAN or VM virtual NIC is far slower than RAM, so shuffles are why 2 machines are rarely 2× faster |
| Adding cores stops helping | **Amdahl's law / bottleneck shift**: the driver, the network, or the shared disk becomes the limit |
| A worker is killed mid-run | **Fault tolerance**: Spark re-runs the lost tasks elsewhere; per-month idempotent loads (Ch. 7) make a retry safe |
| Output written by the worker sits on the VM's disk | **Data locality / shared storage**: without a shared filesystem, "distributed" output is stranded. This is why real clusters use HDFS or object storage |

### 8.4 What to measure and write down

Run the same month (for example 2025-12 HVFHV) three ways and record the row `rows_per_sec` from
`gold/batch_run_metrics` (see `duckdb_queries.md` §2):

| Run | Cores | Expected lesson |
|---|---|---|
| A. `local[2]` on the PC | 2 | The baseline (~1.2 M rows/min measured on a 2-core VM) |
| B. Master + worker | 2 + worker cores | Near-linear gain on scan/transform stages |
| C. Same, but with `shuffle_partitions` too low or too high | same | Too few = idle cores and big spills; too many = scheduler overhead |

Fill the table with *your* numbers. The point of the story is the shape of the curve, not a promised speed-up.

---

## Chapter 9 — Asking questions cheaply (the serving layer)

**Question:** do we need a database server to query 80 GB of Parquet?

No. Parquet in place plus **DuckDB** answers Gold-mart questions on one machine, with no load step (D13, D25).
`scripts/combined_views.py` builds `output/combined.duckdb` containing *only views*: batch rows before the
cutover date, stream rows from it on. `scripts/benchmark_queries.py` times the KPI queries so partition pruning
and column selection are visible rather than asserted.

**Concepts**
- **Compute separated from storage.** The same Parquet is read by Spark (to build) and DuckDB (to serve).
- **Pre-aggregation.** Queries hit marts of thousands of rows, not 500 M trips.
- **Views as contracts.** The dashboard and the SQL in `scripts/queries/` never change when files are rebuilt.

---

## Chapter 10 — The payoff: what the data tells us

All of the above exists to answer questions. Three kinds of value came out.

### 10.1 Trusted KPIs (`specs/3_KPI.html`, `scripts/queries/`)

| Business question | Query | Needs |
|---|---|---|
| Which service is growing, which is shrinking? | `trips_growth.sql`, `service_share_shift.sql` | 13–24 months |
| Where is demand moving? | `zone_growth.sql`, `peak_hour_shift.sql` | monthly marts |
| Did the 2025 congestion fee change revenue and tips? | `fee_impact.sql`, `revenue_trends.sql` | months before and after Jan 2025 |
| Are ride-hail drivers getting a smaller share of fares? | `driver_pay_trend.sql` | HVFHV economics mart |
| Is today normal compared with history? | `live_vs_baseline.sql` | stream + batch (the cutover view) |
| Can we trust the pipeline? | `batch_health.sql`, `data_quality_drift.sql` | `batch_run_log` |

With the development setting of 3 months, growth, rolling 12-month and seasonality KPIs honestly report
"not enough history"; set `batch.start_month` back to 2024-01 and run the cluster to unlock them.

### 10.2 Predictions (Spark ML on the historic Silver store, `spark/ml_job.py`)

First run, 2 real months, 300,000-trip sample per service, random 20% holdout *(measured, `evaluation.md` §6)*:

| Model | Result | Vs. its simple baseline |
|---|---|---|
| **Demand per zone-hour** | WAPE 0.239 | **+41% better** than "same hour last week" (target met) |
| Zone clusters | silhouette 0.47 | meaningful structure; stability (ARI 0.67) below the 0.8 target |
| Demand-pattern clusters | silhouette 0.60 | target met |
| **Anomalous trips** | recall 0.86, precision 0.81, 1.05% flagged | beats a fixed 80-mph rule on injected bad trips; target met |
| Fare | MAE $3.99, R² 0.80 | beats distance-only line, **misses** $2.50 / 0.90 target |
| Duration | MAE 4.2 min | beats zone-pair mean, misses target |
| Tip ≥ 20% (cards) | AUC 0.646 | beats majority class, misses 0.70 |
| Shared-ride match | AUC 0.682 | beats overall rate, narrowly misses 0.70 |
| Wait time (HVFHV) | MAE 178 s | only +2% over baseline: zone, hour and company carry little signal |

We report the misses as misses. A model that barely beats its baseline is itself a finding: *pickup zone and hour
do not explain how long you wait for a ride-hail car; look at supply data next.* Cross-validation spread
(0.2–1.8%) and month-to-month drift stayed inside their limits, so the numbers that did pass can be trusted.

### 10.3 A place to look (`dashboard/app.py`)

Streamlit over DuckDB: each KPI page runs one of the SQL files unchanged, ML pages show model against baseline,
and a page whose data is missing says so instead of showing zeros.

```
streamlit run dashboard/app.py
```

---

## Chapter 11 — What the journey taught us

| Concept | The moment it mattered |
|---|---|
| Size the problem first | One measurement (56–69 B/row) overturned the disk plan before it failed |
| Immutable raw layer | Fixing validation rules is a re-run, not a re-download |
| Explicit schema | Four datasets, one model, no inference passes |
| Flag, don't delete | Data quality became a KPI rather than a silent loss |
| Event time + watermark | Late and duplicate trips handled by rule, not by hope |
| Partitioning and idempotent loads | Any month can be redone alone; queries read only what they need |
| Additive aggregates | Daily → monthly with a `SUM`; ratios stay correct |
| Scale out, honestly | A second machine helps scan and transform, but shuffles, skew, and shared storage decide how much |
| Compute/storage separation | Spark builds, DuckDB serves, same Parquet |
| Baselines before models | "Better than what?" kept us from celebrating a +2% wait-time model |

**The scale ladder we climbed:** one pandas script → one Dockerized Spark job (`local[2]`) → Spark master on the
PC plus a worker on the VM → (next rungs, not built) object storage instead of a shared folder, more workers,
Delta/Iceberg tables, and a real feed in place of the replay.

---

## Where each piece lives

| Topic | File |
|---|---|
| Plan, milestones, architecture | `PLAN.md` |
| Every design decision and trade-off | `decisions.md` (D1–D33) |
| Raw, Silver, Gold schemas | `specs/raw-schemas.md`, `silver-schema.md`, `gold-schema.md` |
| Streaming config | `stream-config.yaml` |
| Services and profiles | `docker-compose.yml` |
| Run it | `run.md`, `installation.md` |
| Disk and time estimates | `storage_requirements.md` |
| Targets vs. results | `evaluation.md` |
| Handy queries | `duckdb_queries.md` |
