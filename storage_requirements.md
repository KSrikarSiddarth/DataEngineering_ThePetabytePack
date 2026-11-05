# storage_requirements.md — Disk Space Needed

How much disk the project needs, and where it goes. **Every number below is an estimate, not a measurement.** The raw file sizes come from the 2026-01 files we downloaded (M2). Everything else is reasoning about the pipeline. Replace the estimates with real numbers once M5 (batch) has run on one month.

**What changed on 2026-11-05.** The KPI catalog (`specs/3_KPI.html`) needs more than 12 months of history (year-over-year growth, seasonality, fee-policy before/after), so the default batch range is now **24 months** (D25), and the project also gains Gold marts and Spark ML outputs (M6). The previous estimate (about 25 GB) was for 12 months.

## 1. Summary

**Plan for about 40 GB; keep 50 GB free.**

| Item | Estimate | Needed from | Freed afterwards? |
|---|---|---|---|
| Raw trip files, 24 months, all four datasets (Bronze for the batch path) | about 14 GB | M5 (1 month, about 0.6 GB, from M2) | No |
| Silver trips, batch (`output/batch/silver`, plus `silver_invalid`) | 8–16 GB | M5 | No |
| Gold marts, stream and batch (daily, monthly, health) | under 1 GB | M4, M5 | No |
| Spark ML: features (sample), predictions, models, metrics | 1–3 GB | M6 | No |
| Spark temporary files (sort and shuffle spill, mostly HVFHV) | about 5 GB | M5 | Yes, when the job ends |
| Stream path: Kafka data (Bronze), Silver, Gold, checkpoints | 2–3 GB | M1–M4 | Kafka data with `docker compose down -v` |
| Docker images (Kafka, Spark, producer, Kafka UI) and Docker/WSL disk overhead | about 3–4 GB | M1 | With `docker system prune` |
| Python virtualenv (PySpark, DuckDB, pyarrow, pytest) and Spark jars | about 1 GB | M2 | No |
| **Total** | **about 35–47 GB** | | |

## 2. Raw input files (`data/`) — Bronze

Per month, from the 2026-01 files:

| Dataset | Size per month | 24 months |
|---|---|---|
| Yellow | about 61 MB | about 1.5 GB |
| Green | about 1 MB | about 0.03 GB |
| FHV | about 21 MB | about 0.5 GB |
| HVFHV (about 20.9M rows per month) | about 482 MB | about 11.6 GB |
| **Total** | **about 565 MB** | **about 13.6 GB** |

- HVFHV is more than 85% of the input. Other months may be a little larger or smaller (summer and holiday months differ, and older months are usually smaller).
- The batch range (2024-01 to 2025-12) ends before the streamed month (D28), so the streamed month's files (about 0.6 GB) are extra and are not part of the batch store. The stream/batch check for that month (M5) writes a temporary `output/check/` of well under 1 GB, deleted afterwards.
- Bronze is not copied a second time (D26): the files in `data/` and the Kafka topics are the landing layer.
- The zone lookup CSV and the shapefile are small (a few MB).
- `data/` is gitignored. `scripts/download_data.py --months ...` fetches any range.

## 3. Output (`output/`) — Silver and Gold

| Folder | Estimate | Notes |
|---|---|---|
| `output/batch/silver/trips` (24 months) | 8–16 GB | Silver schema plus zone names and typed fee columns. The size depends on how many columns are kept and on compression. |
| `output/batch/silver_invalid/` | well under 0.5 GB | Unusable rows only (D27). |
| `output/batch/gold/` marts | under 1 GB | Aggregated, so small. |
| `output/batch/gold/ml_*`, features, models | 1–3 GB | Trip-level models train on a sample (for example 5M rows). Zone-hour features are small. |
| `output/stream/silver/trips`, `silver_invalid` | under 1 GB | One month, HVFHV capped at 500k records by default. |
| `output/stream/gold/` | tens of MB | |
| `output/checkpoints/` | tens of MB | Do not delete while a job is running. |
| `output/producer-stats/`, `output/batch-stats/` | a few MB | |

`output/` is gitignored.

## 4. Temporary and system storage

- **Spark spill (about 5 GB, batch only).** Sorting and shuffling a month of HVFHV spills to the local temp folder. The job processes one month at a time, so the peak does not grow with the number of months. It is freed when the job ends. Put it on a drive with room (`spark.local.dir`).
- **Kafka data (about 1–3 GB).** JSON messages are larger than the Parquet they came from. Retention is 1 day. `docker compose down -v` deletes it.
- **Docker images (about 3–4 GB).** The Spark image (Python, Java, PySpark, Kafka connector jars) is the largest, at roughly 1.5 GB. Kafka is about 0.4 GB. The `spark-batch` and `spark-ml` services reuse the Spark image, so they add no image size.
- **Docker/WSL disk file.** Docker Desktop keeps images, containers and volumes in a virtual disk (`.vhdx`) that grows but does not shrink by itself. Budget for it to stay at its high-water mark.

## 5. Scenarios

| Scenario | Months of history | Total disk |
|---|---|---|
| Stream only (M1–M4) | none | about 8–10 GB |
| Batch, small (development) | 3 | about 15–18 GB |
| Original proposal (too short for several catalog KPIs) | 12 | about 24–30 GB |
| **Default plan** | **24** | **about 35–47 GB** |
| Covers the 2022 Airport fee comparison too | 49 (2022-01 to 2026-01) | about 62–80 GB |
| Default plus the uncapped HVFHV month in the stream (stretch) | 24 | add several GB for Kafka and the stream output |

## 6. Reducing disk use

- Download fewer months: set the range in `stream-config.yaml` (`batch:` section) and pass `--months` to the download script. Check which catalog KPIs then show "not enough history".
- Skip HVFHV for batch runs (`datasets` in the same section). It removes about 85% of the input and Silver, and the HVFHV KPIs and models.
- Delete `output/` and `output/checkpoints` together when resetting (see `run.md`).
- Run `docker system prune` to remove unused images, and `docker compose down -v` to remove Kafka data.
- After the batch job, delete the Spark temp folder if it was placed outside the job's own cleanup.

## 7. To measure later

When M5 runs on one month, record: the raw size of that month, the size of its `output/batch/silver` partition, and the peak size of the Spark temp folder. When M6 runs, record the feature and prediction sizes. Multiply by the number of months and update the tables above.

## 8. Related files

`installation.md` (system requirements), `PLAN.md` §3, §6.7 and §9 (inputs, layers, risks), `decisions.md` D25 (months and layout) and D26 (layers).
