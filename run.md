# run.md — Start the project and view the results

Commands are for PowerShell from the project root. Steps 2 onward need Docker Desktop (see `installation.md`).

**What has been verified.** The producer, the Spark transformations and the streaming job were run on a dev machine **without Docker** (Section 7). The Docker path in Sections 2 to 5 (Kafka, the producer containers, the Spark container) is written but **has not been run yet**. If a command fails, check Section 9.

## 1. One-time setup

```powershell
# Python environment for the download script, tests and dry runs
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements-dev.txt

# Download the trip files into data/ (Yellow is ~61 MB, Green ~1 MB, FHV ~21 MB, HVFHV ~482 MB)
python scripts/download_data.py
```

`data/` must also hold `taxi_zones/taxi_zone_lookup.csv`. Check:

```powershell
docker --version; docker compose version
Get-ChildItem data
```

## 2. Start Kafka

```powershell
docker compose up -d kafka kafka-init
docker compose ps                      # kafka = healthy, kafka-init = exited (0)
docker compose logs kafka-init         # "created taxi.yellow.raw" ... "ok 5 topics ready"
```

Optional web UI at http://localhost:8080:

```powershell
docker compose --profile ui up -d kafka-ui
```

## 3. Send the taxi data (producers)

Quick test, 1000 messages per dataset with no faults and no waiting:

```powershell
$env:MAX_RECORDS="1000"; $env:REPLAY_SPEEDUP="0"; $env:QUALITY_PROFILE="clean"
docker compose --profile producers up producer-yellow producer-green producer-fhv producer-hvfhv
```

Realistic stream: leave the variables unset (`Remove-Item Env:MAX_RECORDS, Env:REPLAY_SPEEDUP, Env:QUALITY_PROFILE`). Defaults come from `stream-config.yaml`:

- `replay_speedup: 3600` replays one simulated hour per real second, so the full month takes about 12 minutes;
- HVFHV is capped at 500,000 records;
- the `realistic` quality profile is active (about 2% nulls, 0.5% rule violations, a few duplicates, late and out-of-order records, and rare malformed JSON).

Use `$env:QUALITY_PROFILE="messy"` for a heavier fault load, or `clean` for none.

Each producer prints a JSON summary at the end and writes it to `output/producer-stats/<dataset>.json`.

Run a single dataset: `docker compose --profile producers up producer-green`.

## 4. Check what is in Kafka

```powershell
# Message count per topic (end offsets)
docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:29092 --topic taxi.green.raw

# Read the first 3 messages, with keys
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:29092 `
  --topic taxi.green.raw --from-beginning --max-messages 3 --property print.key=true

# See injected-fault tags (the `fault` header) 
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:29092 `
  --topic taxi.yellow.raw --from-beginning --max-messages 20 --property print.headers=true
```

In Kafka UI: Topics → `taxi.*.raw` → Messages.

## 5. Run Spark (parse, validate, enrich)

Process everything currently in Kafka, then stop:

```powershell
$env:TRIGGER_MODE="available_now"
docker compose --profile spark up --build spark-job
```

Or keep it running as a live stream (new messages are picked up every 10 seconds), in the background:

```powershell
Remove-Item Env:TRIGGER_MODE -ErrorAction SilentlyContinue
docker compose --profile spark up -d --build spark-job
docker compose logs -f spark-job
```

Start the Spark job **before** the producers for a live stream, or after them to process the backlog. Either way it reads from the earliest offset the first time.

The first build downloads the Spark image layers and the Kafka connector jars, so it takes several minutes and needs internet access.

## 6. View the results

Results are Parquet files on the host, in `output/`:

| Path | Contents |
|---|---|
| `output/trips_clean/service=<name>/` | Valid trips in the unified model, with pickup/dropoff borough and zone |
| `output/invalid/service=<name>/` | Rejected rows with `reject_reason` and the raw message |
| `output/producer-stats/*.json` | What each producer sent, including injected-fault counts |
| `output/checkpoints/` | Spark progress (do not delete while the job is running) |

**Reconcile: every message sent must be in exactly one output**

```powershell
python scripts/reconcile.py
```

Prints sent / clean / invalid per service, the reject reasons, and `OK` or `MISMATCH` (exit code 1). Injected duplicates count as separate messages in M3.

**Summaries with DuckDB**

```powershell
python scripts/show_results.py
```

Prints trips per service (count, average distance and fare, revenue), top pickup boroughs and zones, rejected rows by service and reason, and the FHV rows flagged for a missing zone. Run your own query against the tables `clean` and `invalid`:

```powershell
python scripts/show_results.py --sql "SELECT service, count(*) AS n FROM clean WHERE PUBorough = 'Manhattan' GROUP BY service"
```

Add `--output-dir <path>` for output somewhere other than `output/`.

## 7. Try it without Docker (verified on the dev machine)

Needs Java 8, 11 or 17 and `pip install -r requirements-dev.txt`. This replays the real data into files instead of Kafka and runs the same Spark code.

```powershell
# Keep this path short: Windows' 260-character limit breaks reads in deep folders
$run = "$env:TEMP\m3run"; New-Item -ItemType Directory -Force "$run\in","$run\out" | Out-Null

# 1. Producers in dry-run mode: write NDJSON instead of sending to Kafka
$env:DATA_DIR="data"; $env:REPLAY_SPEEDUP="0"; $env:MAX_RECORDS="500"; $env:QUALITY_PROFILE="messy"
$env:STATS_DIR="$run\out\producer-stats"
foreach ($d in "yellow","green","fhv","hvfhv") { python producer\producer.py --dataset $d --dry-run --out "$run\in\$d.ndjson" }

# 2. Spark streaming job on those files (a machine-wide SPARK_HOME would clash: point it at pip's pyspark)
$env:SPARK_HOME = (python -c "import os,pyspark;print(os.path.dirname(pyspark.__file__))")
$env:PYSPARK_PYTHON = (Get-Command python).Source
$env:OUTPUT_DIR="$run\out"; $env:CHECKPOINT_DIR="$run\out\checkpoints"; $env:ZONE_LOOKUP="data\taxi_zones\taxi_zone_lookup.csv"
python spark\stream_job.py --source files --input-dir "$run\in"

# 3. Check the result
python scripts\reconcile.py --output-dir "$run\out"
python scripts\show_results.py --output-dir "$run\out"
```

HVFHV takes about 12 seconds to start because it sorts 20.9 million timestamps. Unset the variables afterwards with `Remove-Item Env:DATA_DIR,Env:REPLAY_SPEEDUP,Env:MAX_RECORDS,Env:QUALITY_PROFILE,Env:STATS_DIR,Env:SPARK_HOME,Env:PYSPARK_PYTHON,Env:OUTPUT_DIR,Env:CHECKPOINT_DIR,Env:ZONE_LOOKUP`.

## 8. Tests, stop and reset

```powershell
python -m pytest tests -q              # ~3 minutes (the Spark tests start a local Spark)
python -m pytest tests -q -k "not spark and not real_data"    # fast subset (a few seconds)

docker compose stop                    # stop everything, keep data
docker compose down                    # remove containers, keep Kafka data (volume)
docker compose down -v                 # also delete Kafka data
```

**Start completely fresh** (Kafka data and Spark progress must be reset together, otherwise Spark's checkpoint will not match the new topics):

```powershell
docker compose --profile ui --profile producers --profile spark down -v
Remove-Item -Recurse -Force output
```

## 9. Troubleshooting

| Symptom | Likely cause and fix |
|---|---|
| `kafka` never becomes healthy | Not enough memory for Docker. Give WSL at least 6 GB (8 GB recommended) in `%UserProfile%\.wslconfig`, then `wsl --shutdown`. |
| `kafka-init` exits with an error | `docker compose logs kafka-init`. The listener settings and the `confluent-kafka` admin calls were not run yet. |
| Producer: "input file(s) missing" | Run `python scripts/download_data.py`. |
| Producer waits for Kafka then gives up | `docker compose ps` shows whether `kafka-init` finished. Topics must exist before producers start. |
| Producer is slow | `REPLAY_SPEEDUP` is paced on purpose. Use `$env:REPLAY_SPEEDUP="0"` for full speed. HVFHV also takes a while to sort at start-up. |
| `spark-job` build fails on `curl` | The Maven Central download needs internet. Check proxy and firewall. |
| `reconcile.py`: MISMATCH | Spark has not finished, or Kafka was reused with an old checkpoint. Wait for `available_now` to exit, or reset (Section 8). |
| Spark tests fail with `legacyInferArrayTypeFromFirstElement` | A machine-wide `SPARK_HOME` overrides PySpark. See `installation.md`. |
| `FileNotFoundError` on a file that exists | Windows path longer than 260 characters. Use a shorter folder. |
| Docker Desktop will not start on an Azure VM | B-series sizes cannot run nested virtualization. See `installation.md`. |

## 10. Not built yet

- **Silver conformance and stream Gold (M4):** the stream output moves from `output/trips_clean` and `output/invalid` to `output/stream/silver/trips`, `output/stream/silver_invalid` and `output/stream/gold/`, and the columns are renamed to the Silver schema. Sections 5 to 7 and the scripts (`reconcile.py`, `show_results.py`) will change then.
- **Batch processing of historic data (M5):** the 24-month download, the `spark-batch` job, Silver and Gold under `output/batch/`, the KPI queries and the query benchmark.
- **Spark ML (M6):** the `spark-ml` job and the model metrics.
- The single-command `docker compose up --build` for the whole pipeline (M7) and the restart test (M8). Until M7, start the services in the order above.
- Commands for M4 to M6 will be added here when they are built. Until then, everything in this file describes the M3 outputs.
