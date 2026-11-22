#!/usr/bin/env bash
# Run on a worker VM, never on the master: copy the pipeline output from the ADLS "lake" container into output/, so
# reconcile.py, show_results.py, combined_views.py and the dashboard can read it in place. The copy stays inside
# Azure (same region), so it costs no egress. Each argument is a blob-name pattern; with none, everything is copied.
# Silver trips can be large, so pull only what a script needs:
#     scripts/pull_output.sh 'batch/gold/*' 'batch/silver/dim_*' 'batch/silver/batch_run_log/*'   # dashboard, queries
#     scripts/pull_output.sh 'batch/silver/*' 'batch/silver_invalid/*'                           # reconcile.py --batch
set -euo pipefail
cd "$(dirname "$0")/.."
if [ -f .env.setupE ]; then set -a; . ./.env.setupE; set +a; fi
: "${AZURE_STORAGE_ACCOUNT:?set AZURE_STORAGE_ACCOUNT}"

mkdir -p output
for pattern in "${@:-*}"; do
  az storage blob download-batch --account-name "$AZURE_STORAGE_ACCOUNT" --auth-mode login \
      --source lake --destination output --pattern "$pattern" --overwrite
done
