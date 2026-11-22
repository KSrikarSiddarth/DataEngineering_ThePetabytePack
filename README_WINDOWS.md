# Running NYC MobilityPulse on Windows 10 / 11

Everything in `README.md` works the same way on Windows. The only difference is that you use
**`.\mp.cmd <task>`** instead of `make <task>`. The task names are the same.

## 1. Prerequisites (one time)

Install these from PowerShell, then **open a new terminal** so the PATH changes take effect:

```powershell
winget install Python.Python.3.11              # Python 3.11 (tested version; 3.10-3.12 accepted)
winget install EclipseAdoptium.Temurin.17.JDK  # Java 17 for Spark (auto-detected, no JAVA_HOME needed)
winget install Microsoft.VCRedist.2015+.x64    # runtime needed by hadoop.dll (often already installed)
winget install Docker.DockerDesktop            # only for the Kafka / streaming part
```

## 2. Setup (one time, from the project folder)

```powershell
powershell -ExecutionPolicy Bypass -File .\setup_windows.ps1
```

This script:

* creates the venv at `%USERPROFILE%\.venvs\mp311` and installs the project with its dev tools
* checks that Java 17 is installed
* downloads `winutils.exe` and `hadoop.dll` into `tools\hadoop\bin`. Spark needs these on Windows to write
  Parquet files and streaming checkpoints. The code finds them automatically, so you don't set `HADOOP_HOME`.

## 3. Commands

For a step-by-step run order after setup, see [RUN_WINDOWS.md](RUN_WINDOWS.md).

| Linux / macOS                     | Windows (PowerShell or cmd)                 |
|-----------------------------------|---------------------------------------------|
| `make help`                       | `.\mp.cmd help`                             |
| `make test-fast` / `make test`    | `.\mp.cmd test-fast` / `.\mp.cmd test`      |
| `make demo`                       | `.\mp.cmd demo`                             |
| `make pipeline MONTHS=medium`     | `.\mp.cmd pipeline -Months medium`          |
| `make incremental MONTHS=2024-01..2024-06 FORCE=1` | `.\mp.cmd incremental -Months 2024-01..2024-06 -Force` |
| `make bronze MONTHS=2024-01,2024-02` | `.\mp.cmd bronze -Months 2024-01,2024-02` |
| `make kafka-up` / `make kafka-down` | `.\mp.cmd kafka-up` / `.\mp.cmd kafka-down` (Docker Desktop must be running) |
| `make stream` (terminal 1)        | `.\mp.cmd stream`                           |
| `make produce RATE=5000 MAX=500000` (terminal 2) | `.\mp.cmd produce -Rate 5000 -Max 500000` |
| `make concepts EXPERIMENTS="c1 c2"` | `.\mp.cmd concepts -Experiments "c1 c2"`  |
| `make api` / `make dashboard`     | `.\mp.cmd api` / `.\mp.cmd dashboard`       |
| `make scorecard`                  | `.\mp.cmd scorecard`                        |
| `MP__SPARK__DRIVER_MEMORY=8g make silver` | `$env:MP__SPARK__DRIVER_MEMORY="8g"; .\mp.cmd silver` |
| `MOBILITYPULSE_DATA_DIR=~/x make dashboard` | `$env:MOBILITYPULSE_DATA_DIR="$HOME\x"; .\mp.cmd dashboard` |

Data goes to `%USERPROFILE%\mobilitypulse_data`, and the demo writes to `%USERPROFILE%\mobilitypulse_sample`.
`~` paths in `configs\config.yaml` work unchanged.

## 4. What was changed for Windows

| File | Change |
|------|--------|
| `src/mobilitypulse/common/spark.py` | `time.tzset()` doesn't exist on Windows, so the code now pins the process time zone through the C runtime instead. It also finds JDK 17 under `C:\Program Files\...` and sets `HADOOP_HOME` to `tools\hadoop`. |
| `src/mobilitypulse/serving/export.py` | The SQLite connection is closed before the atomic `os.replace` (Windows can't replace an open file). The replace retries, and if the API or dashboard is holding the DB open it fails with a clear message. |
| `src/mobilitypulse/benchmark/stream_bench.py` | Replaced `signal.SIGKILL`, which doesn't exist on Windows, with `Popen.kill()`. A graceful stop now sends `CTRL_BREAK_EVENT` to the stream job's own process group. |
| `src/mobilitypulse/streaming/stream_job.py` | Also handles `SIGBREAK`, which is how Windows delivers a graceful stop. |
| `src/**`, `tests/**` | Text files are read and written as UTF-8 explicitly. The Windows default is cp1252. |
| `infra/kafka/Dockerfile`, `.gitattributes` | Strips CRLF from `create-topics.sh`, so the Kafka topic-init container still works after a Windows checkout or edit. |
| `mp.ps1`, `mp.cmd`, `setup_windows.ps1` | New files: the Windows equivalents of the Makefile and `make setup`. |

These changes don't affect Linux or macOS, and the full suite (87 tests) still passes there.

## 5. Troubleshooting

* **`running scripts is disabled on this system`**: use `.\mp.cmd` (it bypasses the policy), or run
  `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned` once.
* **`UnsatisfiedLinkError ... NativeIO$Windows.access0`** or **`HADOOP_HOME and hadoop.home.dir are unset`**:
  `tools\hadoop\bin\winutils.exe` or `hadoop.dll` is missing. Re-run `setup_windows.ps1`. If they are present,
  install the VC++ redistributable listed above.
* **`Java gateway process exited before sending its port number`**: Java 17 isn't installed, or an older Java is
  first on PATH. Set it explicitly with `$env:JAVA_HOME="C:\Program Files\Eclipse Adoptium\jdk-17..."`.
* **`ERROR ShutdownHookManager: Exception while deleting Spark temp dir`** at exit: this is a known harmless Spark
  warning on Windows. Results are not affected.
* **Windows Firewall prompt for Java / Python**: allow it on *private* networks. Spark and the API only bind to
  127.0.0.1.
* **`cannot replace ...mobilitypulse.db: it is open in another process`**: stop `api` / `dashboard`, run
  `.\mp.cmd export` again, then restart them.
* **Very long paths** (deep OneDrive folders): keep the data dir at its default under `%USERPROFILE%`, or enable
  long paths (admin PowerShell):
  `New-ItemProperty HKLM:\SYSTEM\CurrentControlSet\Control\FileSystem -Name LongPathsEnabled -Value 1 -PropertyType DWORD -Force`.
* **Kafka not reachable on `localhost:9092`**: start Docker Desktop first, then run `.\mp.cmd kafka-up`, and check with
  `.\mp.cmd kafka-topics`.
* Spark tests are slower on Windows than on Linux (Python workers are not forked/reused there). Expect `.\mp.cmd test`
  to take roughly 10-15 minutes.
