#!/usr/bin/env bash
# Run one Spark job of the project on the SetupE cluster. Run it on the master, which is only the driver: the data is
# read and written by the executors on the worker VMs, in ADLS (bronze = input, lake = output).
#     scripts/run_on_cluster.sh batch --months 2025-12 --datasets yellow green fhv
#     scripts/run_on_cluster.sh ml --models fare anomaly
#     scripts/run_on_cluster.sh stream --source kafka       # Kafka and the producers run on a VM (see Run_SetupE.md)
# Settings come from .env.setupE (copy setupE.env.example); the python on PATH must be the Spark 3.5.3 virtualenv.
set -euo pipefail
cd "$(dirname "$0")/.."
if [ ! -f .env.setupE ]; then echo "create .env.setupE from setupE.env.example" >&2; exit 2; fi
set -a; . ./.env.setupE; set +a
: "${AZURE_STORAGE_ACCOUNT:?} ${AZURE_STORAGE_KEY:?} ${MASTER_TS_IP:?}"

job="${1:?usage: $0 batch|ml|stream [job options]}"; shift
bronze="abfss://bronze@${AZURE_STORAGE_ACCOUNT}.dfs.core.windows.net"
lake="abfss://lake@${AZURE_STORAGE_ACCOUNT}.dfs.core.windows.net"
packages="org.apache.hadoop:hadoop-azure:3.3.4"

export SPARK_MASTER="spark://${MASTER_TS_IP}:7077"
export SPARK_DRIVER_MEMORY="${DRIVER_MEMORY:-1g}"
export PYSPARK_SUBMIT_ARGS="--conf spark.executor.memory=${EXECUTOR_MEMORY:-2g} --conf spark.executor.cores=${EXECUTOR_CORES:-2} --conf spark.cores.max=${TOTAL_CORES:-4} pyspark-shell"
export ZONE_LOOKUP="$bronze/taxi_zones/taxi_zone_lookup.csv"

case "$job" in
  batch)  args=(spark/batch_job.py --input-dir "$bronze" --output-dir "$lake/batch") ;;
  ml)     args=(spark/ml_job.py --batch-dir "$lake/batch") ;;
  stream) args=(spark/stream_job.py)
          packages="$packages,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"
          export OUTPUT_DIR="$lake/stream" CHECKPOINT_DIR="$lake/checkpoints" ;;
  *)      echo "unknown job '$job' (batch, ml or stream)" >&2; exit 2 ;;
esac
export SPARK_PACKAGES="$packages"
exec python "${args[@]}" "$@"
