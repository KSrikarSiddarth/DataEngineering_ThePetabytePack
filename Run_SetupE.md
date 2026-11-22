# Run_SetupE.md: running DataEngineering_ThePetabytePack on SetupE

How to run the project on the SetupE cluster (`../SetupE.md`). The Ubuntu laptop is **only the master and driver**.
It downloads no data and processes no data. The Azure VMs download, process and analyse, and ADLS holds everything.

**Status.** The code changes were checked with the project's tests on this laptop (section 10). Nothing has been run
against ADLS, the cluster or Kafka yet, so treat sections 3 to 7 as "to verify on your machines".

Placeholders: `<SA>` storage account name, `<LAPTOP_TS_IP>` and `<VM1_TS_IP>` Tailscale addresses (SetupE section 3).
`$P` is this project folder, `~/siddu/MTech/DataEngineeringAtScale/DataEngineering_ThePetabytePack` on the laptop and
`~/DataEngineering_ThePetabytePack` on VM1.

## 1. Who does what

```
  Laptop (master + driver)           VM1 (Ubuntu worker)                 VM2 (Windows worker)
  scripts/run_on_cluster.sh          scripts/land_data.sh  (download)    executors only
  plans and sends tasks              executors                            
  holds .env.setupE (the key)        scripts/pull_output.sh + analysis
                                     dashboard, optional Kafka
        \                                   |                                  /
         +--------------- ADLS Gen2:  bronze (input files)  and  lake (output) -------+
```

| Step | Where it runs | Command |
| --- | --- | --- |
| Download TLC files, upload to `bronze` | VM1 | `scripts/land_data.sh` |
| Batch job (Silver, Gold) | Executors on VM1 and VM2, driven from the laptop | `scripts/run_on_cluster.sh batch` |
| ML job | Same | `scripts/run_on_cluster.sh ml` |
| Stream job (Kafka to Silver and Gold) | Same, Kafka and producers on VM1 | `scripts/run_on_cluster.sh stream` |
| Reconcile, queries, benchmark, dashboard | VM1, on a copy pulled from ADLS | `scripts/pull_output.sh`, then the scripts |

The driver reads only file listings and Parquet footers. Table data goes between the executors and ADLS inside Azure.

## 2. What changed in the project

| File | Change |
| --- | --- |
| `spark/storage.py` (new) | `join`, `exists`, `delete`, `mkdirs`, `read_text`, `write_text` through Hadoop's FileSystem, so a path can be local or `abfss://` |
| `spark/batch_job.py`, `ml_job.py`, `stream_job.py` | Use those helpers instead of `Path`, `shutil` and `.exists()`; paths are strings. Local paths still work, so `run.md` is unchanged |
| `spark/stream_job.py` `build_spark` | Reads `AZURE_STORAGE_ACCOUNT` and `AZURE_STORAGE_KEY` (the account key goes to the executors with the Spark conf, and Spark's redaction hides it in the UI and event log) and `SPARK_PACKAGES` |
| `scripts/download_data.py` | New `--zones` flag downloads `taxi_zone_lookup.csv` |
| `spark/requirements.txt`, `requirements-dev.txt` | Add `setuptools`: `pyspark.ml` imports `distutils`, which Python 3.12 removed, so the ML job fails on every 3.12 node without it |
| `scripts/land_data.sh`, `pull_output.sh`, `run_on_cluster.sh` (new) | The three commands in the table above |
| `setupE.env.example`, `.gitignore` | Settings template; `.env.setupE` (the key) is ignored by git |
| `tests/test_storage.py` (new) | Tests of the helpers |

The analysis scripts and the dashboard are unchanged: they read a local `output/` folder, so they run on VM1 over a copy.

## 3. One-time setup

### 3.1 Storage

Containers `bronze` and `lake` (SetupE created `bronze`; `lake` is new). Input files go to `bronze`, output to `lake`.

```bash
SA=<SA>
for c in bronze lake; do az storage container create --account-name $SA -n $c --auth-mode login; done
az role assignment create --assignee <your-user-email> --role "Storage Blob Data Contributor" \
  --scope $(az storage account show -n $SA --query id -o tsv)      # lets `az ... --auth-mode login` work on the VMs
```

### 3.2 Laptop (master and driver)

```bash
cd $P
source ~/venv/bin/activate                                           # Python 3.12 + pyspark 3.5.3 (SetupE section 4)
uv pip install --python ~/venv/bin/python -r spark/requirements.txt  # pyspark, PyYAML, numpy, setuptools: nothing else is needed here
cp setupE.env.example .env.setupE && chmod 600 .env.setupE           # fill in the account, key and MASTER_TS_IP
```

`run_on_cluster.sh` relies on SetupE section 6: `~/spark/conf/spark-defaults.conf` (driver host and ports, Python
settings) and `SPARK_HOME=~/spark`. The laptop never needs `data/` or `output/`.

### 3.3 VM1 (landing, analysis, executors)

```bash
# from the laptop: copy the code only (no data)
rsync -av --exclude data --exclude output --exclude .git --exclude __pycache__ --exclude .env.setupE \
  $P/ azureuser@<VM1_TS_IP>:~/DataEngineering_ThePetabytePack/

# on VM1
curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash && az login --use-device-code
pip install --break-system-packages -r ~/DataEngineering_ThePetabytePack/requirements-dev.txt
echo 'AZURE_STORAGE_ACCOUNT=<SA>' > ~/DataEngineering_ThePetabytePack/.env.setupE      # the VM scripts need only this
```

`requirements-dev.txt` covers the download script, the analysis scripts and the dashboard, and includes `numpy` and `setuptools`,
which the ML job needs on every executor.

### 3.4 VM2 (Windows executors)

Per SetupE section 5 (winutils, the `python3.exe` copy, firewall), plus numpy and setuptools for the ML job:

```powershell
C:\Python312\python.exe -m pip install numpy==2.2.6 setuptools==80.9.0
```

### 3.5 The cluster is up with both workers

SetupE section 8: the master UI (`http://<LAPTOP_TS_IP>:8080`) lists VM1 and VM2 as ALIVE. The earlier limit of "VM1 only"
is gone, because the data is no longer on a local disk. For VM1 only, stop VM2's worker and set `EXECUTOR_MEMORY=4g`,
`EXECUTOR_CORES=3`, `TOTAL_CORES=3` in `.env.setupE`.

## 4. Land the data (on VM1)

Start with one month of the three small datasets (about 83 MB), run from the laptop over SSH so the download happens on VM1:

```bash
ssh azureuser@<VM1_TS_IP> 'cd ~/DataEngineering_ThePetabytePack && scripts/land_data.sh --months 2025-12 --datasets yellow green fhv'
ssh azureuser@<VM1_TS_IP> 'az storage blob list --account-name <SA> -c bronze --auth-mode login --query "[].name" -o tsv'
```

`land_data.sh` downloads into `data/` on VM1 (including the zone lookup), then uploads `data/` to `bronze`. `data/` stays on
VM1 because `reconcile.py --batch` compares the source files with the Silver counts. Widen later:

```bash
scripts/land_data.sh --months 2025-12 --datasets hvfhv        # about 482 MB; Silver is an estimated 1.5 to 2 GB per month
scripts/land_data.sh --batch                                   # 2025-10 to 2025-12, about 1.7 GB
```

VM1 has about 27 GB of disk: check `df -h` before adding HVFHV months (data/ plus any pulled Silver).

## 5. Run the jobs (on the laptop)

```bash
tmux new -s taxi
cd $P && source ~/venv/bin/activate
systemd-inhibit --what=sleep:idle --why="spark job" scripts/run_on_cluster.sh batch --months 2025-12 --datasets yellow green fhv
```

`run_on_cluster.sh` sets the master URL, executor size, the ADLS account key and the `hadoop-azure` package, points the job at
`abfss://bronze@<SA>...` (input) and `abfss://lake@<SA>...` (output), and runs `spark/batch_job.py`. Any other option of the
job passes through. Output lands under `lake/batch/{silver,silver_invalid,gold}`.

- **Watch it:** master UI `http://<LAPTOP_TS_IP>:8080` (executors on VM1 and VM2), job UI `http://localhost:4040`.
- **Each file prints** `status`, row counts, seconds and rows/s. A failed file is logged and the run continues; exit code 1.
- **Rerunning a month** replaces only that month's partitions.
- **Shuffle partitions** come from `config.yaml` (`batch.shuffle_partitions`), not from a flag.
- **ML** (needs two or more months of Silver): `scripts/run_on_cluster.sh ml --models fare anomaly`, then without `--models` for all ten.
- **Run time** was about 10 minutes for 12M rows on a 2-core VM in the project's own test. Record `runtime_sec` and `rows_per_sec`
  from `gold/batch_run_metrics` for each run and cluster size (the scaling and Silver-versus-Gold demos in `../DEMO.md`).

## 6. Check and view the results (on VM1)

```bash
ssh azureuser@<VM1_TS_IP>
cd ~/DataEngineering_ThePetabytePack

# 1. Reconcile every source file with its Silver and invalid rows (needs Silver trips, so pull them for the months you check)
scripts/pull_output.sh 'batch/silver/*' 'batch/silver_invalid/*'
python3 scripts/reconcile.py --batch                      # expect OK, diff 0; data/ on VM1 holds the source files

# 2. Queries and the dashboard (Gold, dimensions and the run log are small)
scripts/pull_output.sh 'batch/gold/*' 'batch/silver/dim_*' 'batch/silver/batch_run_log/*'
python3 scripts/combined_views.py                         # writes output/combined.duckdb
python3 scripts/benchmark_queries.py --no-spark           # every KPI query on DuckDB
streamlit run dashboard/app.py --server.address 127.0.0.1
```

View the dashboard from the laptop through a tunnel, and open `http://localhost:8501` in the laptop's browser (it only displays):

```bash
ssh -L 8501:localhost:8501 azureuser@<VM1_TS_IP>
```

Pulling is a copy inside Azure (free). Pull again after each run. To look at one table by hand, use
`python3 scripts/show_results.py --sql "SELECT ..."` after a stream run (it reads `output/stream`), or DuckDB over `output/batch/gold`.
The Gold tables hold sums, so compute ratios in the query (`sum(sum_total_amount) / sum(trips)`).

## 7. The stream path (Kafka)

Kafka and the producers run natively on VM1, and the stream job runs on the cluster, reading Kafka over Tailscale and writing Silver, Gold
and checkpoints to `lake/stream` and `lake/checkpoints`. This path has not been run.

```bash
# on VM1: install and start Kafka as in run.md section 7, with advertised.listeners=PLAINTEXT://<VM1_TS_IP>:9092,...
cd ~/DataEngineering_ThePetabytePack
scripts/land_data.sh --months 2026-01                      # the streamed month, which the producers read from data/
pip install --break-system-packages -r producer/requirements.txt
export KAFKA_BOOTSTRAP=<VM1_TS_IP>:9092
python3 scripts/create_topics.py
for d in yellow green fhv hvfhv; do STATS_DIR=output/producer-stats python3 producer/producer.py --dataset $d & done; wait
```

Then on the laptop, with `KAFKA_BOOTSTRAP=<VM1_TS_IP>:9092` in `.env.setupE`:

```bash
scripts/run_on_cluster.sh stream --trigger-mode available_now      # process what is in Kafka, then stop
```

Pull `'stream/*'` to VM1 and use `reconcile.py`, `show_results.py` and `evaluate_stream.py` there. To start fresh, reset Kafka and the
checkpoints together:

```bash
~/kafka/bin/kafka-server-stop.sh && rm -rf ~/kafka-data                         # on VM1, then format and start again (run.md section 7)
az storage blob delete-batch --account-name <SA> -s lake --pattern 'stream/*' --auth-mode login
az storage blob delete-batch --account-name <SA> -s lake --pattern 'checkpoints/*' --auth-mode login
```

## 8. Stop and clean up

```bash
stop-history-server.sh 2>/dev/null; stop-master.sh              # laptop; stop-worker.sh on VM1; Stop-ScheduledTask on VM2
az vm deallocate -g <RG> -n <vm>                                 # each VM, so they stop billing
az storage blob delete-batch --account-name <SA> -s lake --pattern 'batch/*' --auth-mode login      # to rebuild the batch store
```

Storage is billed per GB-month, so delete `lake` data you no longer need. Nothing large reaches the laptop, so there is no egress.

## 9. Troubleshooting

| Symptom | Likely cause / fix |
| --- | --- |
| `No FileSystem for scheme "abfss"` or `ClassNotFoundException ...azurebfs` | The job was not started through `run_on_cluster.sh` (no `SPARK_PACKAGES`), or the laptop cannot reach Maven |
| `403` or `AuthorizationFailure` from storage | Wrong account or key in `.env.setupE` |
| `Initial job has not accepted any resources` | No worker registered, or `EXECUTOR_MEMORY`/`EXECUTOR_CORES` exceed what the workers offer (VM2's worker is 2g and 2 cores) |
| Tasks fail only on VM2 | winutils or the `python3.exe` copy is missing (SetupE section 5), or numpy or setuptools is not installed there |
| `Python in worker has different version` | A machine is not on Python 3.12 |
| `ModuleNotFoundError: numpy` in the ML job | Install `numpy==2.2.6` on the workers |
| `ModuleNotFoundError: No module named 'distutils'` | Python 3.12 without `setuptools`: `pip install setuptools==80.9.0` on that machine (laptop venv, VM1, VM2) |
| Executors exit, "connection refused" to the driver | `spark.driver.host` must be `<LAPTOP_TS_IP>`; see SetupE section 11 |
| `az` says "not authorized" on VM1 | `az login` not done, or the Storage Blob Data Contributor role is missing |
| `reconcile.py`: no `batch_run_log` | Pull `'batch/silver/*'` first (section 6) |
| `reconcile.py`: MISMATCH | A job was interrupted: rerun the month (it replaces only that month) |
| `reconcile.py`: source file not found | `data/` on VM1 lacks that month: run `land_data.sh` for it |
| Driver killed or out of memory on the laptop | Keep `DRIVER_MEMORY=1g`, close the browser, avoid large `collect()` |
| Job dies when the laptop sleeps | Run under `systemd-inhibit` in `tmux` |

## 10. What was checked

On the laptop, with a synthetic 265-row zone file (deleted afterwards) and no downloaded data:

| Check | Result |
| --- | --- |
| `spark/storage.py` on a local folder (`tests/test_storage.py`) | 3 passed |
| Batch and stream jobs, through `tests/test_batch.py` and `tests/test_stream_job.py` (they now run the string-path code) | 14 passed, 1 failed: see below |
| ML job, through `tests/test_ml.py` (after adding `setuptools`) | 8 passed, 1 failed: see below |
| `py_compile` of the Spark code, `bash -n` of the three scripts, `run_on_cluster.sh` with a stub `python` | OK |

The two failures are not caused by these changes, and both pass once the cause is removed:

- `test_kpi_queries_answer_on_duckdb_and_spark_sql` expects a backfill of 3 of 24 months, but `config.yaml` is set to a 3-month range (2025-10
  to 2025-12), so the job reports 3 of 3. It passes with a copy of the config whose `batch.start_month` is `2024-01`.
- `test_zone_hour_lags_and_grid` builds naive timestamps, and this laptop's timezone is IST (UTC+5:30), which shifts the result by 5:30. It passes
  with `TZ=UTC`.

Not checked: any `abfss://` path, the cluster, the shell scripts' behaviour on VM1 (`az`, the downloads), Kafka, the ML job on the cluster,
and the redaction of the account key in the Spark UI.
