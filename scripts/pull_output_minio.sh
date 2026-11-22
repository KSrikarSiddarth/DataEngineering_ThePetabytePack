#!/usr/bin/env bash
# SetupF: run on VM2. Copies pipeline output from the MinIO "lake" bucket into output/, so reconcile.py, show_results.py,
# combined_views.py and the dashboard read it in place. Each argument is a folder under lake/; with none, everything is
# copied. Silver trips can be large, so pull only what a script needs:
#     scripts/pull_output_minio.sh batch/gold batch/silver/batch_run_log batch/silver/dim_zone batch/silver/dim_payment_type \
#         batch/silver/dim_ratecode batch/silver/dim_hvfhs_company                     # dashboard, queries
#     scripts/pull_output_minio.sh batch/silver batch/silver_invalid                  # reconcile.py --batch
# Needs the mc alias "local" (minIOInstall.md step 4).
set -euo pipefail
cd "$(dirname "$0")/.."

for folder in "${@:-}"; do
  mkdir -p "output/$folder"
  mc mirror --overwrite "local/lake/$folder" "output/$folder"
done
