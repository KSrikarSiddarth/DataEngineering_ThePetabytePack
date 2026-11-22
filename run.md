# run.md: run the project and view the results

Everything runs directly on the machine: Python, Java and Spark. Kafka is only needed for the live stream (section 7).
Run every command from the project root. The paths in `config.yaml` (`data/`, `output/`) are relative to it.

Commands are bash, as on the Ubuntu VMs. In Windows PowerShell:

| Bash | PowerShell |
|---|---|
| `source .venv/bin/activate` | `.venv\Scripts\Activate.ps1` |
| `X=1 python ...` | `$env:X="1"; python ...` (unset with `Remove-Item Env:X`) |

To run on a cluster instead of one machine, see `Run_SetupE.md` (SetupE) or `../SetupF.md` (SetupC with MinIO).

## 1. One-time setup

Needs Python 3.12 and Java 17 (`installation.md`).

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
python scripts/download_data.py --zones --months 2025-12 --datasets yellow green fhv   # ~83 MB, plus the zone lookup
```

- **File sizes:** Yellow is about 61 MB per month, Green about 1 MB, FHV about 21 MB and HVFHV about 482 MB.
- **Default range:** `python scripts/download_data.py --batch` downloads the whole range in `config.yaml` (2025-10 to 2025-12, about 1.7 GB).

## 2. Batch: monthly files to Silver and Gold

The batch path needs no Kafka. It reads `data/` and writes `output/batch/{silver,silver_invalid,gold}`.

```bash
python spark/batch_job.py --months 2025-12 --datasets yellow green fhv
python spark/batch_job.py                                   # the whole batch range in config.yaml
```

- **Speed:** about 1 minute per small file on a 2 core machine. A month of HVFHV (about 21 million trips) takes much longer.
- **Reruns:** rerunning a month replaces only that month's partitions and adds a row to `silver/batch_run_log`.
- **Failures:** a missing file is logged as failed and the run continues. The exit code is 1 if any file failed.
- **Settings:** cores, driver memory and shuffle partitions are in `config.yaml` (`batch:`). The default `spark_master: local[2]` uses two cores.

## 3. Check and query the batch output

```bash
python scripts/reconcile.py --batch                 # source rows = silver + invalid + duplicates, per file (diff 0)
python scripts/combined_views.py                    # writes output/combined.duckdb
python scripts/benchmark_queries.py --no-spark      # every KPI query on DuckDB (drop --no-spark to add Spark SQL)
```

- **The KPI queries** are in `scripts/queries/*.sql`, covering growth, service share, revenue, fees, efficiency, peak
  hours, zones, driver pay, batch health, data-quality drift and live versus baseline.
- **Missing history:** a value that needs months that aren't loaded is NULL, with the status "not enough history".
- **Ratios:** Gold holds sums, so compute ratios in the query, for example `sum(sum_total_amount) / sum(trips)`.

## 4. Spark ML

The ML job reads the batch store and writes `output/batch/silver/ml_features` and `output/batch/gold/ml_*`. Load at
least two months first, because the lag features look back 168 hours.

```bash
python spark/ml_job.py --models fare anomaly        # a subset (anomaly needs fare)
python spark/ml_job.py                              # all models
python scripts/evaluate_models.py                   # the latest run against evaluation.md
```

Settings are in `config.yaml` (`ml:`). On a 2 core machine, two months with the defaults take about 25 minutes.

## 5. Dashboard

```bash
streamlit run dashboard/app.py --server.address localhost      # http://localhost:8501
```

- **What it reads:** the Parquet files in `output/`, through DuckDB. A page without data says what to run.
- **Reload data:** use the button after each new run.
- **On a VM:** view it through `ssh -L 8501:localhost:8501 <vm>`.

## 6. Stream without Kafka (verified)

The producers write NDJSON files instead of sending to Kafka, and the same Spark streaming code reads those files.
This is the simplest way to see the stream path on one machine, including Windows.

```bash
python scripts/download_data.py --months 2026-01               # the streamed month (config.yaml streaming.months)

mkdir -p /tmp/streamrun/in
for d in yellow green fhv hvfhv; do
  MAX_RECORDS=500 REPLAY_SPEEDUP=0 QUALITY_PROFILE=messy STATS_DIR=/tmp/streamrun/out/producer-stats \
    python producer/producer.py --dataset $d --dry-run --out /tmp/streamrun/in/$d.ndjson
done

OUTPUT_DIR=/tmp/streamrun/out/stream CHECKPOINT_DIR=/tmp/streamrun/out/checkpoints \
  python spark/stream_job.py --source files --input-dir /tmp/streamrun/in

python scripts/reconcile.py --output-dir /tmp/streamrun/out
python scripts/show_results.py --output-dir /tmp/streamrun/out
```

- **On Windows:** use a short folder such as `$env:TEMP\m3run`, because the 260-character path limit breaks reads.
- **Start-up time:** HVFHV takes about 12 seconds to start, because it sorts 20.9 million timestamps.
- **Without HVFHV:** to skip the large download, leave `hvfhv` out of both lists.

## 7. Stream with Kafka (Linux)

Run Kafka natively on a Linux machine, such as the Ubuntu VM. It needs Java 17 and about 1 GB of RAM. This path has not been run.

**Install and start a single broker (KRaft, no ZooKeeper):**

```bash
cd ~ && wget https://archive.apache.org/dist/kafka/3.9.0/kafka_2.13-3.9.0.tgz
tar xzf kafka_2.13-3.9.0.tgz && mv kafka_2.13-3.9.0 kafka && cd kafka
cat >> config/kraft/server.properties <<EOF
log.dirs=$HOME/kafka-data
auto.create.topics.enable=false
advertised.listeners=PLAINTEXT://localhost:9092,CONTROLLER://localhost:9093
EOF
bin/kafka-storage.sh format -t "$(bin/kafka-storage.sh random-uuid)" -c config/kraft/server.properties
KAFKA_HEAP_OPTS="-Xms512m -Xmx1g" bin/kafka-server-start.sh -daemon config/kraft/server.properties
```

- **Duplicate keys:** the appended lines override the file's earlier values, because the last value wins.
- **On a cluster:** Spark executors on other machines need to reach Kafka. Replace `localhost` in `advertised.listeners` with the VM's private IP, and pass `KAFKA_BOOTSTRAP=<VM IP>:9092` to the commands below.
- **Stopping:** `~/kafka/bin/kafka-server-stop.sh`.

**Create the topics, send the data, run Spark:**

```bash
pip install -r producer/requirements.txt                     # confluent-kafka
python scripts/create_topics.py                              # "ok 5 topics ready"

# Quick test: 1000 messages per dataset, no faults, no waiting (drop the three variables for the realistic stream)
for d in yellow green fhv hvfhv; do
  MAX_RECORDS=1000 REPLAY_SPEEDUP=0 QUALITY_PROFILE=clean STATS_DIR=output/producer-stats \
    python producer/producer.py --dataset $d &
done; wait

SPARK_PACKAGES=org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 TRIGGER_MODE=available_now python spark/stream_job.py
```

- **Live stream:** leave out `TRIGGER_MODE=available_now`. The job then picks up new messages every 10 seconds.
- **First run:** it reads from the earliest offset, so starting before or after the producers both work.
- **Producer defaults** (`config.yaml`):
  - `replay_speedup: 3600` replays the month in about 12 minutes.
  - HVFHV is capped at 500,000 records.
  - The `realistic` quality profile is used (set `QUALITY_PROFILE=messy` or `clean` to change it).
- **Gold timing:** a Gold hour is written only after the 10-minute watermark passes it, so the newest hours appear later.
- **Producer stats:** each producer writes a summary to `output/producer-stats/<dataset>.json`.

**Look inside Kafka:**

```bash
~/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic taxi.green.raw
~/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic taxi.yellow.raw \
  --from-beginning --max-messages 5 --property print.key=true --property print.headers=true
```

**Check the stream:**

```bash
python scripts/reconcile.py                # every message sent is in exactly one output: OK or MISMATCH
python scripts/show_results.py             # summaries; add --sql "SELECT ..." for your own query
python scripts/evaluate_stream.py          # latency and throughput against evaluation.md
```

**Compare with the batch for the streamed month.** Run the producers with `QUALITY_PROFILE=clean` and the same `MAX_RECORDS` first:

```bash
python spark/batch_job.py --months 2026-01 --output-dir output/check --max-records yellow=1000 green=1000 fhv=1000 hvfhv=1000
python scripts/compare_stream_batch.py --stream output/stream/gold --batch output/check/gold
rm -rf output/check
```

**Start fresh.** Reset Kafka and Spark's checkpoints together, otherwise the checkpoint won't match the new topics:

```bash
~/kafka/bin/kafka-server-stop.sh && rm -rf ~/kafka-data
cd ~/kafka && bin/kafka-storage.sh format -t "$(bin/kafka-storage.sh random-uuid)" -c config/kraft/server.properties && cd -
rm -rf output/stream output/checkpoints output/producer-stats
```

Then start Kafka and create the topics again.

## 8. Where the results are

| Path | Contents |
|---|---|
| `output/batch/silver/trips/` | Silver trips, one schema for all four services, with `dq_*` flags (`specs/silver-schema.md`) |
| `output/batch/silver_invalid/` | Unusable rows with the reason |
| `output/batch/silver/dim_*/`, `batch_run_log/` | Lookups, and one row per file load |
| `output/batch/gold/<mart>/` | Daily, monthly and health marts (`specs/gold-schema.md`), ML results (`ml_*`) |
| `output/stream/...` | The same layout for the stream path |
| `output/producer-stats/*.json` | What each producer sent, including injected-fault counts |
| `output/checkpoints/` | Spark stream progress (don't delete while the job runs) |

## 9. Tests

```bash
python -m pytest tests -q                                   # ~16 minutes on a 2 core machine (Spark tests start a local Spark)
python -m pytest tests -q -k "not spark and not real_data"  # fast subset, a few seconds
```

Two tests depend on the machine, not the code:
- `test_zone_hour_lags_and_grid`: run with `TZ=UTC` if the machine isn't on UTC.
- `test_kpi_queries_answer_on_duckdb_and_spark_sql`: expects the 24-month batch range (`start_month: 2024-01`).

## 10. Troubleshooting

| Symptom | Likely cause and fix |
|---|---|
| `batch_job.py`: a file is `failed` with `not found` | The month isn't in `data/`: `python scripts/download_data.py --months <month>` |
| `PATH_NOT_FOUND ... taxi_zone_lookup.csv` | Download it: `python scripts/download_data.py --zones --months <any month>` |
| `Path does not exist` for `data/` or `output/` | Run commands from the project root (the config paths are relative) |
| Py4J errors such as `legacyInferArrayTypeFromFirstElement does not exist` | A machine-wide `SPARK_HOME` points at another Spark version. Unset it for local runs, or point it at pip's pyspark: `export SPARK_HOME=$(python -c "import os,pyspark;print(os.path.dirname(pyspark.__file__))")` |
| `JAVA_HOME is not set` / `Java gateway process exited` | Install Java 17 and put it on PATH |
| `No module named 'distutils'` in the ML job | `pip install setuptools==80.9.0` (Python 3.12 removed distutils) |
| Out of memory | Lower `driver_memory` in `config.yaml`, load fewer months, or leave out HVFHV |
| `reconcile.py`: MISMATCH | Spark hasn't finished, or Kafka was reused with an old checkpoint: reset (section 7) |
| Producer: "input file(s) missing" | Download the streamed month (section 6) |
| Producer waits for Kafka then gives up | Kafka isn't running, or the topics don't exist yet: `python scripts/create_topics.py` |
| Stream job: `Failed to find data source: kafka` | Set `SPARK_PACKAGES=org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3` |
| `FileNotFoundError` for a file that exists (Windows) | A path longer than 260 characters: use a shorter folder |
