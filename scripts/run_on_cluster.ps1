# Run one Spark job of the project on the SetupF cluster (SetupC with MinIO). Run it in PowerShell on VM1, the Windows
# master, which is only the driver: the executors on VM2 read and write the MinIO buckets (bronze = input, lake = output).
#     scripts\run_on_cluster.ps1 batch --months 2025-12 --datasets yellow green fhv
#     scripts\run_on_cluster.ps1 ml --models fare anomaly
#     scripts\run_on_cluster.ps1 stream --trigger-mode available_now     # Kafka and the producers run on VM2
# Settings come from .env.setupF (copy setupF.env.example); `python` on PATH must be Python 3.12 with pyspark 3.5.3.
$ErrorActionPreference = "Stop"
Set-Location (Split-Path $PSScriptRoot)
if (-not (Test-Path .env.setupF)) { throw "create .env.setupF from setupF.env.example" }

# Load KEY=VALUE lines into this process's environment (comments and blank lines are skipped).
foreach ($line in Get-Content .env.setupF) {
    if ($line -match '^\s*([A-Z_][A-Z0-9_]*)=(.*)$') {
        [Environment]::SetEnvironmentVariable($Matches[1], $Matches[2].Trim(), "Process")
    }
}
foreach ($name in "S3_ENDPOINT", "S3_ACCESS_KEY", "S3_SECRET_KEY", "MASTER_IP") {
    if (-not [Environment]::GetEnvironmentVariable($name)) { throw "set $name in .env.setupF" }
}
# Fall back to the defaults below for any memory or core setting left empty.
function Setting($name, $default) { if ($v = [Environment]::GetEnvironmentVariable($name)) { $v } else { $default } }

$job = $args[0]
$rest = @($args | Select-Object -Skip 1)
$packages = "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"

$env:SPARK_MASTER = "spark://$($env:MASTER_IP):7077"
$env:SPARK_DRIVER_MEMORY = Setting DRIVER_MEMORY 2g
$env:PYSPARK_PYTHON = "python3"                       # the executors run on Ubuntu, never the Windows python.exe
$env:PYSPARK_SUBMIT_ARGS = "--conf spark.executor.memory=$(Setting EXECUTOR_MEMORY 3g) " +
    "--conf spark.executor.cores=$(Setting EXECUTOR_CORES 3) --conf spark.cores.max=$(Setting TOTAL_CORES 3) pyspark-shell"
$env:ZONE_LOOKUP = "s3a://bronze/taxi_zones/taxi_zone_lookup.csv"

switch ($job) {
    "batch"  { $script = @("spark/batch_job.py", "--input-dir", "s3a://bronze", "--output-dir", "s3a://lake/batch") }
    "ml"     { $script = @("spark/ml_job.py", "--batch-dir", "s3a://lake/batch") }
    "stream" { $script = @("spark/stream_job.py")
               $packages += ",org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"
               $env:OUTPUT_DIR = "s3a://lake/stream"; $env:CHECKPOINT_DIR = "s3a://lake/checkpoints" }
    default  { throw "usage: scripts\run_on_cluster.ps1 batch|ml|stream [job options]" }
}
$env:SPARK_PACKAGES = $packages
& python @script @rest
exit $LASTEXITCODE
