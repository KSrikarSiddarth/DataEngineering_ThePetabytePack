#!/usr/bin/env bash
# Run on a worker VM, never on the master: download the TLC files into data/ and upload them to the ADLS "bronze"
# container, where the Spark executors read them. Options are passed to download_data.py, for example:
#     scripts/land_data.sh --months 2025-12 --datasets yellow green fhv
# Needs `az login` on this VM, the Storage Blob Data Contributor role, and AZURE_STORAGE_ACCOUNT (see setupE.env.example).
# data/ stays on the VM, because scripts/reconcile.py --batch compares the source files with the Silver counts.
set -euo pipefail
cd "$(dirname "$0")/.."
if [ -f .env.setupE ]; then set -a; . ./.env.setupE; set +a; fi
: "${AZURE_STORAGE_ACCOUNT:?set AZURE_STORAGE_ACCOUNT}"

python3 scripts/download_data.py --zones "$@"
az storage blob upload-batch --account-name "$AZURE_STORAGE_ACCOUNT" --auth-mode login \
    --destination bronze --source data --overwrite
