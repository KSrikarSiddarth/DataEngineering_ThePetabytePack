# Running NYC MobilityPulse on Windows (after setup)

These steps assume `setup_windows.ps1` has finished (see [README_WINDOWS.md](README_WINDOWS.md)).
Run everything from PowerShell inside the `MobilityPulse_Submission` folder.

## Disk space and memory

| Scenario | Install | Data | Temporary (peak) | **Total** |
|---|---|---|---|---|
| Setup + tests + demo only | 1.5 GB | 0.2 GB | - | **~2 GB** |
| Main pipeline (`pipeline -Months medium`), dashboard, API | 1.5 GB | 1.5 GB | 3 GB | **~6 GB** (keep 8 GB free) |
| Everything: streaming with Docker, plus all benchmarks | 1.5 + 4 GB (Docker) | 3 GB | 5 GB | **~14 GB** (keep 15-20 GB free) |

* **Install** is Python 3.11 (~100 MB), JDK 17 (~300 MB), the venv at `%USERPROFILE%\.venvs\mp311` (~830 MB, PySpark
  alone is 340 MB), the pip cache (~300-400 MB, safe to delete after setup) and the project folder with winutils (~30 MB).
* **Data** is stored in `%USERPROFILE%\mobilitypulse_data`. The demo uses `%USERPROFILE%\mobilitypulse_sample`.
* **Temporary** is Spark's shuffle files in `%TEMP%`, which are removed when each job finishes. After a run, only the
  install and data columns stay on disk: about 3 GB for the main pipeline, or about 8.5 GB for everything.
  If disk fills up over time, delete the leftover `spark-*` and `blockmgr-*` folders in `%TEMP%`.
* **Memory:** 16 GB RAM is recommended. The Spark driver uses 6 GB by default, the large benchmarks need
  `$env:MP__SPARK__DRIVER_MEMORY="10g"`, and Docker/Kafka adds about 2 GB. On an 8 GB machine, `demo` and `test` work,
  but the real pipeline is likely to run out of memory.

The install figures were measured on a Linux machine. The data and temporary figures are estimates from the project's
docs and benchmark results.

## 1. Check that it works (no internet needed)

```powershell
.\mp.cmd test-fast     # unit tests, about 1 min
.\mp.cmd demo          # synthetic data through every stage, writes to %USERPROFILE%\mobilitypulse_sample
```

If the demo prints `finished: {...}` at the end, Spark, Java and winutils are all working.

## 2. Run the real pipeline (downloads NYC taxi data)

```powershell
.\mp.cmd pipeline -Months medium     # Jan-Mar 2024: ingest -> bronze -> silver -> demand -> features -> analytics -> ml -> anomaly -> export
```

This takes roughly 10-20 minutes and writes to `%USERPROFILE%\mobilitypulse_data`.
You can also run a single stage, e.g. `.\mp.cmd silver -Months medium` or `.\mp.cmd ml`.

## 3. Open the dashboard and API

These need step 2 (or `export`) to have run. Use two terminals:

```powershell
.\mp.cmd dashboard     # http://127.0.0.1:8501
.\mp.cmd api           # http://127.0.0.1:8000/docs
```

To view the demo data instead, run this in that terminal before starting the dashboard:

```powershell
$env:MOBILITYPULSE_DATA_DIR = "$HOME\mobilitypulse_sample"
.\mp.cmd dashboard
```

## 4. Streaming (optional, needs Docker Desktop running)

```powershell
.\mp.cmd kafka-up      # start Kafka and create the topics
.\mp.cmd stream        # terminal 1: Spark streaming job, Ctrl+C to stop
.\mp.cmd produce       # terminal 2: replays January trips into Kafka
.\mp.cmd export        # afterwards: shows results on the dashboard's "Live stream" page
.\mp.cmd kafka-down    # when finished
```

## 5. Benchmarks and scorecard (optional)

```powershell
.\mp.cmd bronze -Months large   # needed by bench-batch
.\mp.cmd bench-batch            # about 20 min
.\mp.cmd bench-stream           # about 35 min, needs Kafka running
.\mp.cmd test                   # full test suite with coverage
.\mp.cmd scorecard              # writes results\evaluation_scorecard.md
```

## Notes

* `.\mp.cmd help` lists every command.
* If `export` says the DB is open in another process, stop the dashboard or API first, then run it again.
* [README_WINDOWS.md](README_WINDOWS.md) covers fixes for other common errors.
