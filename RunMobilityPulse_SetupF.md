# Running MobilityPulse on SetupF in distributed mode

Project: `MobilityPulse_Submission-V2-Chandana-Windows.zip`, modified for the cluster. The modified project is
**`MobilityPulse_Submission-V2-Chandana-Windows-SetupF.zip`** (source: `MobilityPulse_SetupF/MobilityPulse_Submission/`).
Infrastructure: `SetupF.md` (Windows VM1 `spark-master`, Ubuntu VM2 `spark-worker` with MinIO, one Azure VNet).

## 0. How it runs

| Machine | Role for MobilityPulse |
|---|---|
| **VM1** (Windows) | Spark **master** and the **driver**: you run `.\mp.cmd <task> -Cluster` here. Downloads the raw data, keeps the pipeline state table and the serving DB (SQLite), runs the dashboard and API |
| **VM2** (Ubuntu) | Spark **worker** (the executors do the work), **MinIO** (the data lake), optional **Kafka** and the Kafka producer |

```
 VM1 (Windows)                                   VM2 (Ubuntu)
  .\mp.cmd pipeline -Cluster                      Spark worker -> executors (Python 3.12 + numpy/pandas/pyarrow)
  driver  :40000-40002  <---------------------->  MinIO :9000   s3a://lake/mobilitypulse/{raw,bronze,silver,gold,
  master  :7077                                                   quarantine,models,checkpoints}
  local: raw downloads, state, serving DB          Kafka :9092 (optional, stream path)
```

- **Data flow:** `ingest` downloads on VM1, then uploads the raw files to MinIO. Every later Spark stage (bronze,
  silver, demand, features, analytics, ml, anomaly, stream) runs on the executors on VM2, which read and write
  `s3a://lake/mobilitypulse/...` over loopback. `export` reads Gold from MinIO (pyarrow) into the SQLite file on VM1.
- **Credentials:** the driver sends the MinIO keys to the executors with the Spark config. VM2's Spark needs no MinIO settings.
- **Same code, two modes:** without `-Cluster` the project still runs as `local[*]` on one machine, as before.

### What was verified, and what was not

Verified on one Linux machine with the same code path (a real Spark 3.5.3 standalone master and worker, and a real
MinIO built from source, `spark://127.0.0.1:7077`, executors running a Python that does **not** have `mobilitypulse`
installed, like VM2):
- the full local test suite (97 pass: the original 87 plus 10 new cluster-mode tests) and `cluster-check`;
- the full synthetic pipeline `demo`: sample, bronze, silver, demand, features, analytics, ml, anomaly, export, all on
  the executors with all lake data in MinIO (the ML stage took about 9 minutes on that 4-core, 6 GB machine);
- `incremental` (second run reports "up to date");
- the stream path: native Kafka, the producer reading bronze from MinIO, the stream job with its checkpoint on `s3a://`,
  then `export` showing the "Live stream" table.

**Not verified** (no access to the VMs): the Azure VMs themselves, a **Windows driver** talking to a **Linux executor**
(SetupF section 4.2 describes this pattern for the other project, and it needs the matching Python 3.12), real-data
run times (`pipeline -Months medium`), and the dashboard on VM1. The steps for those are written from SetupF and the
project's Windows docs. Where something is an estimate it says so.

---

## 1. Before you start: what SetupF must already have

SetupF sections 1 to 7 done: both VMs, SSH, Spark 3.5.3 master on VM1 and worker on VM2, **MinIO on VM2 with the bucket
`lake`** (section 7.4 creates `bronze` and `lake`), cluster check in section 6 shows the worker **ALIVE**.

Two things to know about SetupF:

1. **MinIO download (section 7.1):** `https://dl.min.io/server/minio/release/linux-amd64/minio` now answers
   **410 Gone** (checked). Use the build-from-source fallback in 7.1. I ran exactly that
   (`go install github.com/minio/minio@latest` with Go 1.24.4) and it built in about 9 minutes, so it works.
2. **Memory:** VM1 and VM2 have 8 GB each. The project's own default driver memory (6g) does not fit VM1, so the profile
   uses the SetupF values: driver 2g, executor 3g, 3 cores.

---

## 2. One-time setup

### 2.1 Copy the project (your machine)

```bash
cd ~/siddu/MTech/DataEngineeringAtScale
scp MobilityPulse_Submission-V2-Chandana-Windows-SetupF.zip spark-master:C:/mp.zip
scp MobilityPulse_Submission-V2-Chandana-Windows-SetupF.zip spark-worker:~/mp.zip      # only needed for the stream producer (4.2) or a Linux driver
```

**[VM1 PowerShell]**:

```powershell
Expand-Archive C:\mp.zip C:\mp -Force
cd C:\mp\MobilityPulse_Submission
```

### 2.2 VM2: packages for the executors

The executors run Python workers, so VM2's Python needs the same packages and the **same Python version as the driver
(3.12, which is Ubuntu 24.04's `python3`)**. The `mobilitypulse` package itself is sent by the driver, so it is not installed here.

**[VM2 bash]**:

```bash
unzip -q ~/mp.zip -d ~/mp && cd ~/mp/MobilityPulse_Submission
pip install --break-system-packages -r requirements-executor.txt     # as SetupF 8.3: the worker runs as your user
python3 -c "import numpy, pandas, pyarrow, pyspark; print('executor packages ok')"
```

`python3` must be the interpreter the worker finds. If you prefer a venv, use its `bin/python` path as
`EXECUTOR_PYTHON` in `.env.setupF` (step 2.4).

### 2.3 VM1: Python 3.12 environment for the driver

SetupF section 3 already installed Java 17, Spark, Python 3.12 (`C:\Python312`), `C:\hadoop\bin\winutils.exe` and the
machine-wide variables. The project's setup script creates a separate venv and installs the project and its packages.

**[VM1 PowerShell]**, in `C:\mp\MobilityPulse_Submission`:

```powershell
Set-ExecutionPolicy -Scope CurrentUser RemoteSigned -Force
.\mp.cmd setup -Cluster                 # venv %USERPROFILE%\.venvs\mp312 (Python 3.12), project + pyspark 3.5.3, imports check
```

- `-Cluster` makes the setup use Python **3.12** (the default is 3.11). Use `py -3.12`, which is on PATH after SetupF section 3.
- `winutils.exe` and `hadoop.dll`: SetupF put them in `C:\hadoop\bin` and set `HADOOP_HOME`. The project respects an
  existing `HADOOP_HOME`, and its setup script only downloads its own copy into `tools\hadoop` if you have none.
- If `setup` prints a warning about JDK 17 you already installed: `$env:JAVA_HOME` is set machine-wide by SetupF, so ignore it.

### 2.4 VM1: the settings file `.env.setupF`

Same format and names as SetupF 8.2. Notepad doesn't work over SSH, so use a here-string and fill in the values:

```powershell
Copy-Item setupF.env.example .env.setupF      # to see every setting; or write it directly:
Set-Content .env.setupF @"
S3_ENDPOINT=http://<VM2_IP>:9000
S3_ACCESS_KEY=sparkadmin
S3_SECRET_KEY=<MINIO_PASSWORD>
MASTER_IP=<MASTER_IP>
DRIVER_MEMORY=2g
EXECUTOR_MEMORY=3g
EXECUTOR_CORES=3
TOTAL_CORES=3
EXECUTOR_PYTHON=python3
KAFKA_BOOTSTRAP=
"@
Get-Content .env.setupF
```

- **Private IPs, never `localhost`:** the executors receive the same addresses.
- **Values unquoted.** The file holds the MinIO password and is ignored by git (`.gitignore`).
- `MASTER_IP` gives `spark://<MASTER_IP>:7077` and, because the driver runs on VM1, the address the executors call back on.
- Open the driver ports once, if SetupF 4.1 is not done: `New-NetFirewallRule -DisplayName "Spark" -Direction Inbound -Protocol TCP -LocalPort 7077,8080,4040,18080,40000-40002 -RemoteAddress <VNET_RANGE> -Action Allow`.

### 2.5 Check the cluster

**[VM1 PowerShell]**:

```powershell
.\mp.cmd check -Cluster
```

Expected, in about 1 to 2 minutes (the first run downloads the S3A jars, about 300 MB, from Maven into `%USERPROFILE%\.ivy2`, so VM1 needs internet):

```
master   : spark://<MASTER_IP>:7077
data dir : s3a://lake/mobilitypulse
  OK    read/write on s3a://lake/mobilitypulse
  OK    tasks ran on: spark-worker               <- the executors run on VM2, not on VM1
  OK    1 executor(s) registered
  OK    shuffle (groupBy) works
  OK    Python UDF using the mobilitypulse package works on the executors
  OK    parquet write + read on s3a://lake/mobilitypulse/_check
cluster check: PASSED
```

If a line fails, see section 6 before running anything else. `check` only writes under `s3a://lake/mobilitypulse/_check` and deletes it.

---

## 3. Run the batch pipeline

**[VM1 PowerShell]**:

```powershell
cd C:\mp\MobilityPulse_Submission

# 3.1 Synthetic data, no internet needed: every stage, on the cluster
.\mp.cmd demo -Cluster                    # data in s3a://lake/mobilitypulse_sample

# 3.2 Real data: Jan to Mar 2024 (downloads NYC taxi + NOAA weather to VM1, uploads to MinIO, runs every stage)
.\mp.cmd pipeline -Months medium -Cluster
```

Single stages work too: `.\mp.cmd silver -Months medium -Cluster`, `.\mp.cmd ml -Cluster`, and
`.\mp.cmd incremental -Months 2024-01..2024-06 -Cluster` (only new or changed months).

- **`ingest`** downloads to `%USERPROFILE%\mobilitypulse_data\raw` and then uploads new or changed files to
  `s3a://lake/mobilitypulse/raw`. Running `bronze` alone also uploads anything not there yet.
- **Where it ends up** (check on VM2 with `mc ls --recursive local/lake/mobilitypulse | head`):
  `raw/ bronze/ silver/ quarantine/ gold/ models/` under `lake/mobilitypulse`.
- **Watch it:** open the tunnels from SetupF section 6 (`ssh -N -L 8080:<MASTER_IP>:8080 -L 4040:<MASTER_IP>:4040 spark-master`), then
  `http://localhost:8080` (the application) and `http://localhost:4040` (jobs, stages, Executors tab: the executors are on `<VM2_IP>`).
  On VM2: `htop`, `free -h`, `df -h /`.
- **Time:** the synthetic `demo` took about 11 minutes on my small test machine, mostly the ML stage (GBT, random forest).
  With 3 cores on SetupF expect the same order of magnitude. For `pipeline -Months medium` I have **no measurement**; the
  project's docs give 10 to 20 minutes on a 16 GB laptop, and the cluster has less memory and 3 cores, so plan for longer.
- **Long runs survive an SSH disconnect** with the scheduled-task pattern of SetupF 10.3:

  ```powershell
  $run = 'cd C:\mp\MobilityPulse_Submission; .\mp.cmd pipeline -Months medium -Cluster *> C:\spark-logs\mp-pipeline.log'
  $action = New-ScheduledTaskAction -Execute "powershell.exe" -Argument "-ExecutionPolicy Bypass -Command `"$run`""
  Register-ScheduledTask -TaskName MobilityPulse -Action $action -User SYSTEM -RunLevel Highest -Force
  Start-ScheduledTask -TaskName MobilityPulse
  Get-Content C:\spark-logs\mp-pipeline.log -Wait
  ```

  The task runs as SYSTEM, so it needs its own `.ivy2` download, and its venv and `HOME` are SYSTEM's. If `mp.cmd` can't
  find the venv, set `$env:MP_VENV` to the full path in `$run`.

### Tuning

| Setting | Where | Default |
|---|---|---|
| Driver and executor memory, cores | `.env.setupF` (`DRIVER_MEMORY`, `EXECUTOR_MEMORY`, `EXECUTOR_CORES`, `TOTAL_CORES`) | 2g, 3g, 3, 3 |
| Shuffle partitions | `$env:MP__SPARK__SHUFFLE_PARTITIONS = "16"` or `configs/profile_setupf.yaml` | 16 |
| Fewer or lighter models | `$env:MP__ML__MODELS = "[linear_regression, gbt]"`, `$env:MP__ML__GBT__MAX_ITER = "60"` | 3 models, 150 iterations |
| Any config value | `MP__SECTION__KEY` environment variable | see `configs/config.yaml` |

If the ML stage runs out of memory on the executor (`java.lang.OutOfMemoryError`, executor lost), lower the number of
models or trees first. Executor memory can't exceed the worker's 4g (`SPARK_WORKER_MEMORY`).

---

## 4. Results, dashboard and API

`export` (the last pipeline stage) reads Gold from MinIO and writes the SQLite DB on VM1:
`%USERPROFILE%\mobilitypulse_data\serving\mobilitypulse.db`. Dashboard and API read that file, so they run on VM1 without `-Cluster`:

**[VM1 PowerShell]**, in two windows:

```powershell
.\mp.cmd dashboard        # Streamlit on 127.0.0.1:8501 (VM1 only)
.\mp.cmd api              # FastAPI on 127.0.0.1:8000
```

**[your machine]**: both listen on VM1's loopback, so tunnel to VM1:

```bash
ssh -N -L 8501:127.0.0.1:8501 -L 8000:127.0.0.1:8000 spark-master
```

Open http://localhost:8501 (dashboard) and http://localhost:8000/docs (API).

- **Demo data on the dashboard:** the demo's local files are in `%USERPROFILE%\mobilitypulse_sample`, so run
  `$env:MOBILITYPULSE_DATA_DIR = "$HOME\mobilitypulse_sample"; .\mp.cmd dashboard`.
- **If `export` says the DB is open in another process:** stop the dashboard and API, run `.\mp.cmd export -Cluster`, then restart them.
- **Look at the lake:** `mc ls --recursive local/lake/mobilitypulse/gold` on VM2, or DuckDB as in SetupF 11.2.

---

## 5. The stream path (optional): Kafka on VM2

Memory on VM2 is tight with MinIO, the worker (4g) and Kafka (1g). Set `EXECUTOR_MEMORY=2g` in `.env.setupF` while streaming.

### 5.1 Kafka on VM2

**[VM2 bash]**, in `~/mp/MobilityPulse_Submission`. This is SetupF 12.1 as a script (no Docker):

```bash
infra/setupF/kafka_native.sh install <VM2_IP>      # downloads Kafka 3.9.0 to ~/kafka, advertises <VM2_IP>:9092, formats KRaft storage
infra/setupF/kafka_native.sh start
infra/setupF/kafka_native.sh topics                # taxi-trips (4 partitions) and taxi-trips-dlq
```

Kafka doesn't restart by itself after a reboot: run `start` again. Add `KAFKA_BOOTSTRAP=<VM2_IP>:9092` to `.env.setupF` on **both** VMs.

### 5.2 Producer (VM2)

The producer replays January 2024 trips from bronze (read from MinIO) into Kafka. It needs the project in a venv with
`confluent-kafka`, and a `.env.setupF` with the S3 settings and `KAFKA_BOOTSTRAP`:

```bash
sudo apt install -y python3-venv make
make setup CLUSTER=1 PYTHON311=python3.12          # venv ~/.venvs/mp312
cp setupF.env.example .env.setupF && nano .env.setupF   # S3_ENDPOINT=http://<VM2_IP>:9000, keys, KAFKA_BOOTSTRAP=<VM2_IP>:9092
make produce CLUSTER=1 RATE=2000 MAX=50000         # needs bronze for 2024-01 (the batch pipeline above)
```

### 5.3 Stream job on the cluster (VM1)

**[VM1 PowerShell]**:

```powershell
.\mp.cmd stream -Cluster                    # Ctrl+C to stop; run the producer on VM2 before or while it runs
.\mp.cmd export -Cluster                    # afterwards: the dashboard's "Live stream" page
```

- Output and checkpoint: `s3a://lake/mobilitypulse/gold/stream_zone_metrics`, `.../quarantine/stream_dlq`, `.../checkpoints/<tag>`.
  The per-batch CSVs (`progress.csv`, `batches.csv`, `latency_samples.csv`) are written on VM1 in `results\stream\`.
- The first run downloads the Kafka connector package from Maven.
- **Start fresh** (Kafka and checkpoint together, otherwise they don't match): on VM2 `infra/setupF/kafka_native.sh reset`, start, `topics`;
  on VM1 `$env:MP_PROFILE="setupf"; & "$env:USERPROFILE\.venvs\mp312\Scripts\python.exe" -m mobilitypulse.streaming.stream_job --reset --duration 1`.

---

## 6. Troubleshooting

| Problem | Cause and fix |
|---|---|
| `.env.setupF` ignored, job runs locally | The profile is only used with `-Cluster` (or `MP_PROFILE=setupf`). `check` prints `master : spark://...` and `data dir : s3a://...`; if it shows `local[*]`, the flag or `MASTER_IP` is missing |
| `object store: ... NETWORK_CONNECTION / Couldn't connect` | MinIO not reachable on `<VM2_IP>:9000` from VM1: `Test-NetConnection <VM2_IP> -Port 9000`, `systemctl status minio` on VM2 |
| `NO_SUCH_BUCKET` | Create it on VM2: `mc mb local/lake` (`check` creates it for you when missing) |
| `InvalidAccessKeyId` / `SignatureDoesNotMatch` | Wrong `S3_ACCESS_KEY` / `S3_SECRET_KEY`, or a password with special characters: see SetupF 7.6 |
| `no executor registered`, job hangs at "Initial job has not accepted any resources" | Worker not ALIVE in the master UI, or the executors can't reach the driver: check `Test-NetConnection` from VM2 (`nc -zv <MASTER_IP> 40000`) and the firewall rule for 40000-40002; check `MASTER_IP` is VM1's **private** IP |
| `Python in worker has different version 3.x than that in driver 3.12` | VM2's `python3` is not 3.12, or `EXECUTOR_PYTHON` points elsewhere. Both machines need the same minor version |
| `ModuleNotFoundError: numpy / pandas / pyarrow` in an executor | Step 2.2 not done on VM2 (or done for a different Python than `EXECUTOR_PYTHON`) |
| `Failed to bind ... port 40000` | A previous driver is still running (Task Manager, `python.exe`), or another job holds 40000-40002. Only one Spark application at a time |
| `java.lang.NoSuchMethodError` / `ClassNotFoundException` for `S3AFileSystem` | The `hadoop-aws` jar doesn't match Spark's Hadoop 3.3.4. The config uses `hadoop-aws:3.3.4`; don't mix other versions into `C:\spark\jars` |
| Slow first start | Maven download of the S3A jars (about 300 MB); later runs reuse `%USERPROFILE%\.ivy2` |
| Spark UI on 4040 not reachable | Only exists while a job runs; the tunnel goes to `<MASTER_IP>:4040` |
| Everything else | The Windows-specific list in `README_WINDOWS.md` (winutils, `Java gateway process exited`, ...) still applies on VM1 |

---

## 7. What changed in the project

| File | Change |
|---|---|
| `src/mobilitypulse/common/storage.py` (new) | `RemotePath` (an `s3a://bucket/key` that behaves like `pathlib.Path`: `/`, `exists`, `read_text`, `write_text`, `glob`) and helpers for pyarrow/pandas reads, writes, delete and upload, so one code path serves a local folder and MinIO |
| `common/config.py`, `configs/config.yaml` | `paths.data_dir` may be an `s3a://` URI. New `paths.local_dir` (raw downloads, state, serving DB, benchmark output stay on the driver machine) and `paths.raw_lake`. New `storage` and cluster `spark` settings. Profiles: `MP_PROFILE=setupf` layers `configs/profile_setupf.yaml` and `.env.setupF` |
| `common/spark.py` | Cluster mode when `spark.master` is `spark://...`: driver host and ports 40000-40002, executor memory/cores, `PYSPARK_PYTHON` for the (Linux) executors, S3A settings for MinIO and the `hadoop-aws` jars, and the `mobilitypulse` package shipped to the executors for the Python UDFs. Local mode is unchanged |
| `batch/bronze.py`, `ingestion/download.py`, `ingestion/sample.py` | Bronze reads raw files from `raw_lake`. `ingest`, `sample` and `bronze` upload new or changed raw files to MinIO |
| `ml/train.py`, `ml/anomaly.py`, `serving/export.py`, `streaming/producer.py`, `streaming/stream_job.py`, `common/utils.py` | The few non-Spark file operations (small parquet files, `best_model.json`, delete, reads with pyarrow) go through the storage layer |
| `cluster_check.py` (new), `mp.ps1`, `mp.cmd`, `setup_windows.ps1`, `Makefile` | `-Cluster` / `CLUSTER=1`, `check` / `cluster-check`, Python 3.12 venv for the cluster |
| `infra/setupF/kafka_native.sh` (new) | Native single-broker Kafka on VM2 instead of Docker |
| `setupF.env.example`, `requirements-executor.txt` (new), `.gitignore` | Settings template (`.env.setupF` is git-ignored), executor packages |
| `tests/unit/test_cluster_mode.py` (new) | 10 tests for the storage layer, profile, settings mapping and Spark settings (no cluster needed) |

## 8. Not covered

- **Benchmarks** (`bench-batch`, `bench-stream`, `concepts`) and `test` / `scorecard`: they build their own `local[n]` sessions
  and temporary folders to compare core counts and partitions, so they keep running locally as before (without `-Cluster`).
  Run them on any one machine, not on the cluster.
- **Task-level numbers in the report** (`results/`) come from the original local runs; a cluster run writes new
  `results\measurements.json` entries on VM1.
- **HTTPS to MinIO / multiple workers:** the profile assumes `http://` and one worker. More workers only need the
  same `requirements-executor.txt` on each and a higher `TOTAL_CORES`.
