# installation.md: software setup

Everything installs directly on the machine. There is no Docker and no WSL.

| Software | Needed for | Required? |
|---|---|---|
| Python 3.12 | Everything (download, Spark jobs through PySpark, analysis, dashboard, tests) | Yes |
| Java 17 | Spark (batch, stream, ML, Spark tests) | Yes |
| Kafka 3.9 | The live stream only (`run.md` section 7), on Linux | Optional |
| Git | Version control | Optional |

Spark comes with `pip install pyspark`, so a separate Spark download is only needed for a standalone cluster
(`Run_SetupE.md`, `../SetupF.md`).

## Ubuntu 24.04

```bash
sudo apt update && sudo apt install -y openjdk-17-jdk python3-venv python3-pip git
java -version && python3 --version          # 17.x and 3.12.x
cd DataEngineering_ThePetabytePack
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
pip install -r producer/requirements.txt    # only for the Kafka stream (confluent-kafka)
```

## Windows 11

```powershell
winget install -e --id EclipseAdoptium.Temurin.17.JDK
winget install -e --id Python.Python.3.12
winget install -e --id Git.Git               # optional
```

Reopen the terminal so PATH updates, then:

```powershell
java -version; python --version
cd DataEngineering_ThePetabytePack
python -m venv .venv; .venv\Scripts\Activate.ps1
pip install -r requirements-dev.txt
```

- **Without admin rights:** the Temurin `.zip` and the per-user Python installer don't need admin. Set `JAVA_HOME` and PATH for your user only.
- **Kafka:** run it on a Linux machine. On Windows, use the stream's file source (`run.md` section 6).
- **Spark writes on Windows:** writing local files needs `winutils.exe` and `hadoop.dll` (Hadoop 3.3.x) in `%HADOOP_HOME%\bin`. If a write fails with `HADOOP_HOME unset` or a `winutils` error, add them (see `../SetupC.md` section 4).
- **Long paths:** keep the project in a short path such as `C:\proj`. The 260-character path limit breaks file reads in deep folders.

## Requirements files

| File | Contents | Where |
|---|---|---|
| `requirements-dev.txt` | pyspark, pyarrow, PyYAML, numpy, setuptools, duckdb, streamlit, pytest | Any machine that downloads, analyses or runs tests |
| `spark/requirements.txt` | pyspark, PyYAML, numpy, setuptools | A Spark driver or worker that does nothing else |
| `producer/requirements.txt` | confluent-kafka, pyarrow, PyYAML, numpy | The machine running the Kafka producers |

`setuptools` is needed because `pyspark.ml` imports `distutils`, which Python 3.12 removed.

## System requirements

- **RAM:** 8 GB works for one or a few months. The batch job's driver uses 4 GB by default (`config.yaml`).
- **Disk:** 2 to 5 GB for a few months. About 80 GB for the full 24-month range (`storage_requirements.md`).
- **Ports:** 9092 for Kafka, if used. 4040 for the Spark UI while a job runs. 8501 for the dashboard.

## Readiness check

```bash
java -version && python --version && python -c "import pyspark; print(pyspark.__version__)"    # 17.x, 3.12.x, 3.5.3
python -m pytest tests -q -k "not spark and not real_data"                                      # a few seconds
```
