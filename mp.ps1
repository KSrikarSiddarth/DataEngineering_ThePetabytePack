<#
  NYC MobilityPulse - Windows task runner (PowerShell equivalent of the Makefile).

  Usage (from this folder):   .\mp <task> [-Months <spec>] [-Rate N] [-Max N] [-Force] [-Experiments "c1 c2"]
  Examples:
      .\mp help
      .\mp test-fast
      .\mp demo
      .\mp pipeline -Months medium
      .\mp incremental -Months 2024-01..2024-06 -Force
      .\mp produce -Rate 5000 -Max 500000

  Environment overrides work as on Linux/macOS, e.g.
      $env:MP__SPARK__DRIVER_MEMORY = "8g"; .\mp silver -Months medium
#>
param(
    [Parameter(Position = 0)][string]$Task = "help",
    [string[]]$Months = @("default"),   # comma lists: -Months 2024-01,2024-02
    [int]$Rate = 0,
    [int]$Max = 0,
    [switch]$Force,
    [switch]$Cluster,                   # run on the SetupF Spark cluster + MinIO (settings in setupF.env)
    [string]$Experiments = "",
    [string]$SampleDir = (Join-Path $env:USERPROFILE "mobilitypulse_sample")
)
$ErrorActionPreference = "Stop"
Set-Location -LiteralPath $PSScriptRoot

# ------------------------------------------------------------------ environment
if ($Cluster) { $env:MP_PROFILE = "setupf" }
$ClusterMode = ($env:MP_PROFILE -eq "setupf")
# the cluster venv must have the same Python minor version as the executors (Ubuntu 24.04: 3.12)
$VenvName = if ($ClusterMode) { "mp312" } else { "mp311" }
$Venv = if ($env:MP_VENV) { $env:MP_VENV } else { Join-Path $env:USERPROFILE ".venvs\$VenvName" }
$Py = Join-Path $Venv "Scripts\python.exe"
$env:TZ = "UTC0"          # all timestamps are handled as UTC wall-clock (Makefile/Linux sets TZ=UTC in code)
$env:PYTHONUTF8 = "1"     # UTF-8 file IO instead of the Windows code page
$env:PYTHONIOENCODING = "utf-8"
$hadoop = Join-Path $PSScriptRoot "tools\hadoop"
if (-not $env:HADOOP_HOME -and (Test-Path (Join-Path $hadoop "bin\winutils.exe"))) { $env:HADOOP_HOME = $hadoop }
if ($env:HADOOP_HOME) { $env:PATH = (Join-Path $env:HADOOP_HOME "bin") + ";" + $env:PATH }
$Compose = @("compose", "-f", "infra/docker-compose.yml")

function Need-Venv {
    if (-not (Test-Path -LiteralPath $Py)) { throw "venv not found at $Venv - run: powershell -ExecutionPolicy Bypass -File .\setup_windows.ps1" }
}
$M = $Months -join ","
# explicit argument arrays (not $args) so tokens like --cov-report=json:path reach the exe unchanged
function Run-Py([string[]]$A) {
    Need-Venv
    & $Py @A
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}
function Run-Docker([string[]]$A) {
    if ($ClusterMode) { throw "Kafka runs natively on VM2 in cluster mode (RunMobilityPulse_SetupF.md section 7), not in Docker." }
    & docker @A
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}

$help = @"
  (add -Cluster to any task to run it on the SetupF cluster; see RunMobilityPulse_SetupF.md)
  setup          create the Python 3.11 venv (3.12 with -Cluster), install the package, fetch winutils (runs setup_windows.ps1)
  ingest         download TLC/zones/NOAA for -Months into raw/
  pipeline       ingest + bronze..export for -Months
  incremental    process only new/unfinished/changed -Months (add -Force to redo), then refresh features + analytics
  bronze|silver|demand|features|analytics   single batch stage for -Months (default: datasets.default)
  ml|anomaly|export                         single ML / serving stage
  demo           synthetic data end-to-end in $SampleDir (no internet needed)
  clean-sample   delete the synthetic sample data dir
  kafka-up       start Kafka (KRaft) and create topics   [needs Docker Desktop]
  kafka-ui       Kafka + web UI on http://localhost:8080
  kafka-down     stop Kafka
  kafka-topics   list topics
  stream         run the Structured Streaming job (Ctrl+C to stop)
  produce        replay bronze trips into Kafka (-Rate 2000 -Max 200000)
  bench-batch    batch experiments E1-E5 (needs bronze for datasets.large months)
  bench-stream   streaming experiments (needs Kafka + bronze for producer.replay_month)
  concepts       Spark internals evidence pack -> results/concepts  (-Experiments "c1 c2" to select)
  test           full test suite with coverage -> results/tests
  test-fast      unit tests only
  scorecard      evaluate all targets -> results/evaluation_scorecard.md
  check          (with -Cluster) SetupF smoke test: MinIO, executors, shuffle, UDF, s3a round trip
  api            FastAPI on http://127.0.0.1:8000/docs
  dashboard      Streamlit dashboard on http://127.0.0.1:8501
"@

switch ($Task) {
    "help"         { Write-Host $help }
    "setup"        { $pyv = if ($ClusterMode) { "3.12" } else { "3.11" }
                     & powershell -NoProfile -ExecutionPolicy Bypass -File (Join-Path $PSScriptRoot "setup_windows.ps1") -Venv $Venv -PythonVersion $pyv }
    "check"        { Run-Py @("-m", "mobilitypulse.cluster_check") }

    # ---------------------------------------------------------------- batch pipeline (real data)
    "ingest"       { Run-Py @("-m", "mobilitypulse.cli", "--stage", "ingest", "--months", $M) }
    "pipeline"     { Run-Py @("-m", "mobilitypulse.cli", "--stage", "all", "--months", $M) }
    "incremental"  { $extra = @(); if ($Force) { $extra += "--force" }
                     Run-Py (@("-m", "mobilitypulse.cli", "--stage", "incremental", "--months", $M) + $extra) }
    { $_ -in "bronze", "silver", "demand", "features", "analytics" } { Run-Py @("-m", "mobilitypulse.cli", "--stage", $Task, "--months", $M) }
    { $_ -in "ml", "anomaly", "export" } { Run-Py @("-m", "mobilitypulse.cli", "--stage", $Task) }

    "demo"         { if ($ClusterMode) { $env:MOBILITYPULSE_DATA_DIR = "s3a://lake/mobilitypulse_sample"; $env:MP__PATHS__LOCAL_DIR = $SampleDir }
                     else { $env:MOBILITYPULSE_DATA_DIR = $SampleDir }
                     Run-Py @("-m", "mobilitypulse.cli", "--stage", "demo") }
    "clean-sample" { if (Test-Path -LiteralPath $SampleDir) { Remove-Item -LiteralPath $SampleDir -Recurse -Force } }

    # ---------------------------------------------------------------- Kafka + streaming
    "kafka-up"     { Run-Docker ($Compose + @("up", "-d", "--build", "kafka", "kafka-init")) }
    "kafka-ui"     { Run-Docker ($Compose + @("--profile", "ui", "up", "-d", "--build")) }
    "kafka-down"   { Run-Docker ($Compose + @("--profile", "ui", "down")) }
    "kafka-topics" { Run-Docker @("exec", "mp-kafka", "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "localhost:9092", "--describe") }
    "stream"       { Run-Py @("-m", "mobilitypulse.streaming.stream_job") }
    "produce"      { $extra = @(); if ($Rate) { $extra += "--rate", "$Rate" }; if ($Max) { $extra += "--max-events", "$Max" }
                     Run-Py (@("-m", "mobilitypulse.streaming.producer") + $extra) }

    # ---------------------------------------------------------------- evaluation
    "bench-batch"  { Run-Py @("-m", "mobilitypulse.benchmark.batch_bench") }
    "bench-stream" { Run-Py @("-m", "mobilitypulse.benchmark.stream_bench") }
    "concepts"     { $extra = @(); if ($Experiments) { $extra = $Experiments -split "\s+" }
                     Run-Py (@("-m", "mobilitypulse.benchmark.concepts") + $extra) }
    "test"         { New-Item -ItemType Directory -Force -Path "results\tests" | Out-Null
                     Run-Py @("-m", "pytest", "--cov", "--cov-report=term", "--cov-report=json:results/tests/coverage.json", "--junitxml=results/tests/junit.xml") }
    "test-fast"    { Run-Py @("-m", "pytest", "-m", "not integration") }
    "scorecard"    { Run-Py @("-m", "mobilitypulse.benchmark.scorecard") }

    # ---------------------------------------------------------------- serving
    "api"          { Run-Py @("-m", "uvicorn", "mobilitypulse.serving.api:app", "--host", "127.0.0.1", "--port", "8000") }
    "dashboard"    { Run-Py @("-m", "streamlit", "run", "src/mobilitypulse/serving/dashboard.py", "--server.address", "127.0.0.1",
                             "--server.port", "8501", "--server.headless", "true", "--browser.gatherUsageStats", "false") }

    default        { Write-Host "Unknown task '$Task'.`n" -ForegroundColor Red; Write-Host $help; exit 2 }
}
