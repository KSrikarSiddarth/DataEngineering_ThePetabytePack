# 05 · Runbook

## 1. First-time setup

Full, verified instructions are in the [README](../README.md) sections 1–3. Short version:

```bash
brew install openjdk@17 python@3.11 colima docker docker-compose
# ~/.docker/config.json -> "cliPluginsExtraDirs": ["/opt/homebrew/lib/docker/cli-plugins"]
cd Complete_Project_Implementation
make setup            # creates ~/.venvs/mp311 and installs the package in editable mode
make test
```

## 2. Daily commands

| Goal | Command |
|---|---|
| Offline end-to-end demo | `make demo` (data in `~/mobilitypulse_sample`) |
| Real data, one month | `make pipeline MONTHS=small` |
| Re-run one stage | `make silver MONTHS=2024-01` |
| Start Kafka | `colima start --cpu 4 --memory 6 && make kafka-up` |
| Streaming demo | `make stream` + `make produce RATE=2000` in a second terminal |
| Serve | `make export && make api` / `make dashboard` |
| Evaluate | `make bench-batch`, `make bench-stream`, `make test`, `make scorecard` |
| Stop Kafka | `make kafka-down` |

Override any config value without editing YAML, e.g.
`MP__SPARK__SHUFFLE_PARTITIONS=64 MP__SPARK__DRIVER_MEMORY=8g make silver`.

## 3. Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `Unable to locate a Java Runtime` | `export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home` (the Makefile and `get_spark` set it automatically) |
| PySpark fails with Python 3.13/3.14 | use the 3.11 venv `~/.venvs/mp311` |
| `docker: unknown command: docker compose` | compose plugin path missing in `~/.docker/config.json` (see setup) |
| `Cannot connect to the Docker daemon` | `colima start` |
| Spark binds to a VPN address / `Service 'sparkDriver' could not bind` | already handled: driver host/bind address forced to 127.0.0.1 |
| Port 4040 busy | another Spark app is running; it moves to 4041 automatically (benchmark trials use 4050) |
| Kafka connector download fails | first `make stream` needs internet to fetch `spark-sql-kafka-0-10_2.12:3.5.3` from Maven (cached in `~/.ivy2`) |
| Stream job restarts from old offsets | it resumes from `checkpoints/<tag>`; use `python -m mobilitypulse.streaming.stream_job --reset` to start clean |
| `RuntimeError ... already holds downloaded (real) data` | `make demo` refuses to mix synthetic data into the real data dir; use `SAMPLE_DIR` |
| OneDrive "file in use" / slow writes | keep `MOBILITYPULSE_DATA_DIR` outside OneDrive (default `~/mobilitypulse_data`) |
| Out of memory on `large` | raise `MP__SPARK__DRIVER_MEMORY`, or lower to `MONTHS=medium`; check spill in the Spark UI |
| `SSLCertVerificationError` on download (corporate Zscaler proxy) | handled: `truststore` makes Python verify against the macOS keychain (which trusts the corporate root CA); verification stays on |
| NOAA file is a few KB of HTML | NOAA moved `global-hourly` to `https://noaa-global-hourly-pds.s3.amazonaws.com/<year>/<station>.csv` (config updated); the downloader now rejects HTML/non-Parquet payloads, and a changed URL forces a re-fetch |
| Streaming p50 latency > 5 s | keep `streaming.shuffle_partitions` small (8): each micro-batch runs one state-store task per partition; 32 partitions gave p50 6.6 s, 8 gave 4.7 s at 2k ev/s. Changing it requires a new checkpoint (`--reset`) |

## 4. Resetting

```bash
rm -rf ~/mobilitypulse_data/{bronze,silver,gold,quarantine,models,checkpoints,serving}   # keep raw downloads
make clean-sample
make kafka-down && docker volume prune     # Kafka keeps no persistent volume; topics are recreated by kafka-init
```
