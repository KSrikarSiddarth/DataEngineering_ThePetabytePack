# installation.md — Software Setup

What to install on a Windows 11 PC, and which step of the project (see `PLAN.md` and `Steps.md`) needs it. Install only what you need for the step you're about to start.

## 1. Summary

| Software | Needed for | Required? | Install |
|---|---|---|---|
| Virtualization (BIOS) | Docker Desktop / WSL 2 | Yes | Enable VT-x / AMD-V in BIOS |
| WSL 2 | M1 onward (Docker backend) | Yes | `wsl --install` |
| Docker Desktop (with Compose v2) | M1 onward | Yes | `winget install -e --id Docker.DockerDesktop` |
| Git | Chapter 0 (version control), all later steps | Yes | `winget install --id Git.Git -e` |
| Python 3.10+ | M2 (data download, schema inspection), tests | Yes | `winget install -e --id Python.Python.3.12` |
| VS Code | Editing, all steps | Already installed | — |
| Docker extension for VS Code | Viewing containers | Optional | VS Code Extensions: "Docker" |
| Java / Spark / Kafka on the host | Nothing | **No** | They run inside containers |

## 2. Step-by-step: what each step needs

| Step | Needs | Notes |
|---|---|---|
| Chapter 0 — Setup | Git, VS Code | `git init`, set `user.name` and `user.email` |
| M1 — Kafka up | WSL 2, Docker Desktop, internet | First run pulls the Kafka image |
| M2 — Producer | Python (host), Docker Desktop | Python is used to download the Green, FHV, and HVFHV files, inspect the parquet schemas, and run schema tests. The producer itself runs in a container. |
| M3 — Spark parse/validate/enrich | Docker Desktop, internet | The Spark image and Kafka connector jar are downloaded at build time |
| M4 — Windowed aggregations | Docker Desktop, Python (optional) | Python with pandas is used to spot-check results |
| M5 — Batch processing of historic data | Docker Desktop (for the container run), or Python + Java 8/11/17 on the host; DuckDB; ~20 GB free disk | Downloads 12 months of the four datasets (~6 GB raw, most of it HVFHV). Needs no Kafka. Python with `duckdb` runs the query benchmark. |
| M6 — End-to-end Compose | Docker Desktop, PowerShell | Runs `tests/smoke_test.ps1` |
| M7 — Hardening and docs | Docker Desktop | Restart/recovery test |
| Reading the output (any time after M3) | Python with pandas/pyarrow, or DuckDB | Optional; for inspecting Parquet files |

## 3. Installation instructions

### 3.1 Enable virtualization
Check Task Manager → Performance → CPU → "Virtualization: Enabled". If disabled, turn on Intel VT-x or AMD-V in the BIOS/UEFI settings.

### 3.2 WSL 2
Run in an **administrator** PowerShell, then reboot:
```powershell
wsl --install
```
Verify:
```powershell
wsl --status
```

### 3.3 Docker Desktop
```powershell
winget install -e --id Docker.DockerDesktop
```
Start Docker Desktop and wait until the engine shows as running. In Settings → General, make sure **Use the WSL 2 based engine** is on.

Verify:
```powershell
docker --version
docker compose version
docker run --rm hello-world
```

**Licensing:** free for personal use and small businesses; larger organizations need a paid subscription. A work-managed PC may need IT approval or admin rights.

**Resources:** create `%UserProfile%\.wslconfig` to give Docker enough memory (Kafka, Spark, and four producers run together):
```
[wsl2]
memory=8GB
processors=4
```
Then run `wsl --shutdown` and restart Docker Desktop. Minimum is 6 GB; 8 GB is recommended.

### 3.4 Git
```powershell
winget install --id Git.Git -e
```
Close and reopen the terminal, then:
```powershell
git --version
git config --global user.name "Your Name"
git config --global user.email "you@example.com"
git init
```

### 3.5 Python
```powershell
winget install -e --id Python.Python.3.12
```
Reopen the terminal and verify with `python --version`. Then install the host-side tools (use a virtual environment):
```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install pyarrow pandas pytest duckdb
```
Add `.venv/` to `.gitignore`.

## 4. System requirements

- **RAM:** 16 GB total on the PC is comfortable; 8 GB allotted to WSL/Docker.
- **Disk:** 15–20 GB free for images, input data, and output; **25 GB or more** once the batch milestone (M5) adds 12 months of input data (~6 GB) and the historical store.
- **Ports free:** 9092 (Kafka), 8080 (Kafka UI, optional).
- **Network:** internet access for image pulls and data downloads. A corporate proxy or firewall may block these.
- **Location:** keep the project on a local drive (C:), not a network drive.

## 5. Windows-specific notes

- Shell scripts with CRLF line endings fail inside Linux containers. Add a `.gitattributes` with `*.sh text eol=lf` once scripts exist.
- Bind mounts from Windows are slower than Linux filesystems, but fine at this project's scale. If permissions cause trouble, use a named Docker volume for `output/`.
- Restart the terminal after installing anything that changes PATH.
- **Running the Spark tests or `stream_job.py` on the host** needs Java 8, 11 or 17 on PATH and `pip install -r requirements-dev.txt`. A machine-wide `SPARK_HOME` pointing at a different Spark version (this VM has Spark 3.3.2) overrides the pip-installed PySpark and fails with py4j errors such as `legacyInferArrayTypeFromFirstElement does not exist`. The test suite sets `SPARK_HOME` and `PYSPARK_PYTHON` for its own process; for manual runs set them in that shell only: `$env:SPARK_HOME = (python -c "import os,pyspark;print(os.path.dirname(pyspark.__file__))")`.
- **Long paths:** Windows' 260-character path limit breaks file reads in deeply nested folders (it made pyarrow report "path not found" for files that exist). Keep test output in a short path such as `%TEMP%\m3run`.

## 6. Readiness check

Run before starting M1:
```powershell
wsl --status
docker --version
docker compose version
docker run --rm hello-world
git --version
python --version
```
If all of these succeed, the machine is ready.

## 7. Current status on this machine

| Software | Status |
|---|---|
| VS Code | Installed (`code .` worked) |
| Git | **Not installed** (`git` not recognized) |
| WSL 2 | Not checked |
| Docker Desktop | Not checked |
| Python | Not checked |
