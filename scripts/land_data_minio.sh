#!/usr/bin/env bash
# SetupF: run on VM2, the Ubuntu worker that hosts MinIO. Downloads the TLC files into data/ and copies them into the
# MinIO "bronze" bucket, where the Spark executors read them. Options are passed to download_data.py, for example:
#     scripts/land_data_minio.sh --months 2025-12 --datasets yellow green fhv
# Needs the mc alias "local" (minIOInstall.md step 4). data/ stays on VM2, because scripts/reconcile.py --batch compares
# the source files with the Silver counts.
set -euo pipefail
cd "$(dirname "$0")/.."

python3 scripts/download_data.py --zones "$@"
mc mirror --overwrite --exclude "*.part" data local/bronze
