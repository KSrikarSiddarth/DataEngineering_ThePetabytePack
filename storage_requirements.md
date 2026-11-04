# storage_requirements.md — Disk Space Needed

How much disk the project needs, and where it goes. **Every number below is an estimate, not a measurement.** The raw file sizes come from the 2026-01 files we downloaded (M2). Everything else is reasoning about the pipeline. Replace the estimates with real numbers once M5 (batch) has run on one month.

## 1. Summary

**Plan for about 25 GB; keep 30 GB free.**

| Item | Estimate | Needed from | Freed afterwards? |
|---|---|---|---|
| Raw trip files, 12 months, all four datasets | about 7 GB | M5 (1 month, about 0.6 GB, from M2) | No |
| Historical store (`output/historical`, plus `output/historical_invalid`) | 4–8 GB | M5 | No |
| Spark temporary files (sort and shuffle spill, mostly HVFHV) | about 5 GB | M5 | Yes, when the job ends |
| Stream path: Kafka data, Parquet output, checkpoints | 2–3 GB | M1–M4 | Kafka data with `docker compose down -v` |
| Docker images (Kafka, Spark, producer, Kafka UI) and Docker/WSL disk overhead | about 3–4 GB | M1 | With `docker system prune` |
| Python virtualenv (PySpark, DuckDB, pyarrow, pytest) and Spark jars | about 1 GB | M2 | No |
| **Total** | **about 22–28 GB** | | |

## 2. Raw input files (`data/`)

Per month, from the 2026-01 files:

| Dataset | Size per month | 12 months |
|---|---|---|
| Yellow | about 61 MB | about 0.7 GB |
| Green | about 1 MB | about 0.01 GB |
| FHV | about 21 MB | about 0.25 GB |
| HVFHV (about 20.9M rows per month) | about 482 MB | about 5.8 GB |
| **Total** | **about 565 MB** | **about 6.8 GB** |

- HVFHV is more than 85% of the input. Other months may be a little larger or smaller (summer and holiday months differ).
- The zone lookup CSV and the shapefile are small (a few MB).
- `data/` is gitignored. `scripts/download_data.py --months ...` fetches any range.

## 3. Output (`output/`)

| Folder | Path | Estimate | Notes |
|---|---|---|---|
| Historical clean trips | `output/historical/service=*/year=*/month=*` | 4–8 GB for 12 months | Unified model plus zone names. The size depends on how much of the `extras` column is kept. |
| Historical rejects | `output/historical_invalid/` | well under 0.5 GB | A small share of rows is rejected. |
| Stream clean trips and rejects | `output/trips_clean`, `output/invalid` | under 1 GB | One month, HVFHV capped at 500k records by default. |
| Stream aggregates (M4) | `output/agg_*` | tens of MB | Small: windows, not trips. |
| Checkpoints | `output/checkpoints/` | tens of MB | Do not delete while a job is running. |
| Producer and batch stats | `output/producer-stats/`, `output/batch-stats/` | a few MB | |

`output/` is gitignored.

## 4. Temporary and system storage

- **Spark spill (about 5 GB, batch only).** Sorting and shuffling a month of HVFHV spills to the local temp folder. The job processes one month at a time, so the peak does not grow with the number of months. It is freed when the job ends. Put it on a drive with room (`spark.local.dir`).
- **Kafka data (about 1–3 GB).** JSON messages are larger than the Parquet they came from. Retention is 1 day. `docker compose down -v` deletes it.
- **Docker images (about 3–4 GB).** The Spark image (Python, Java, PySpark, Kafka connector jars) is the largest, at roughly 1.5 GB. Kafka is about 0.4 GB.
- **Docker/WSL disk file.** Docker Desktop keeps images, containers and volumes in a virtual disk (`.vhdx`) that grows but does not shrink by itself. Budget for it to stay at its high-water mark.

## 5. Scenarios

| Scenario | Months of history | Total disk |
|---|---|---|
| Stream only (M1–M4) | none | about 8–10 GB |
| Batch, small | 3 | about 12–14 GB |
| **Default plan** | **12** | **about 22–28 GB** |
| Default plus the uncapped HVFHV month in the stream (stretch) | 12 | add several GB for Kafka and the stream output |

## 6. Reducing disk use

- Download fewer months: set the range in `stream-config.yaml` (`batch:` section) and pass `--months` to the download script.
- Skip HVFHV for batch runs (`datasets` in the same section). It removes about 85% of the input.
- Delete `output/` and `output/checkpoints` together when resetting (see `run.md`).
- Run `docker system prune` to remove unused images, and `docker compose down -v` to remove Kafka data.
- After the batch job, delete the Spark temp folder if it was placed outside the job's own cleanup.

## 7. To measure later

When M5 runs on one month, record: the raw size of that month, the size of its `output/historical` partition, and the peak size of the Spark temp folder. Multiply by 12 and update the tables above.

## 8. Related files

`installation.md` (system requirements), `PLAN.md` §3 and §9 (inputs and risks), `decisions.md` D25 (months and layout).
