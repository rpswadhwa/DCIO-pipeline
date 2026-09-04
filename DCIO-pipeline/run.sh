#!/bin/bash
set -e
cd /home/ec2-user/DCIO-pipeline/DCIO-pipeline
source /home/ec2-user/DCIO-pipeline/venv/bin/activate

echo "=== DEPLOY PROVENANCE ==="
if [ -f .deployed_commit ]; then
  echo "Code deployed via deploy_to_ec2.sh from local commit:"
  cat .deployed_commit
else
  echo "!!! WARNING: no .deployed_commit marker found."
  echo "!!! This tree was not deployed via deploy_to_ec2.sh (hand-edited on EC2,"
  echo "!!! or deployed the old ad hoc way). Do not trust results until this is"
  echo "!!! reconciled back into the local git repo and redeployed properly."
fi
echo "=========================="

export $(grep -v ^# .env | xargs)

# boto3 clients in STEP 10 (Athena/Glue) need an explicit region; the shell
# `aws --region` flag does not propagate to Python. Default it if .env omits it.
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"
export AWS_REGION="${AWS_REGION:-$AWS_DEFAULT_REGION}"

# Only consumed by run_pipeline.py's USE_OCR=1 branch (ocr_passes.py / classify_pages.py);
# has no effect on the default pdfplumber/camelot text-extraction path.
export TESSERACT_CMD="${TESSERACT_CMD:-/home/ec2-user/micromamba/envs/ocr/bin/tesseract}"
export TESSDATA_PREFIX="${TESSDATA_PREFIX:-/home/ec2-user/micromamba/envs/ocr/share/tessdata}"
export OMP_THREAD_LIMIT="${OMP_THREAD_LIMIT:-1}"

# Input follows the download job's run-dated layout:
#   filings_5500_pdf/year=<run_year>/batch_date=<YYYY-MM-DD>/
# BATCH_DATE defaults to today; override to target a specific batch:
#   BATCH_DATE=2026-06-28 bash run.sh
BATCH_DATE="${BATCH_DATE:-$(date +%F)}"
RUN_YEAR="${BATCH_DATE:0:4}"
S3_INPUT_PATH="s3://retirementinsights-bronze/filings_5500_pdf/year=${RUN_YEAR}/batch_date=${BATCH_DATE}/"

echo "[STEP 0] Syncing PDFs from S3: $S3_INPUT_PATH"
# --delete makes data/inputs/ an exact mirror of this batch's S3 folder. Without it,
# PDFs from every previous batch ever synced on this box accumulate forever and get
# reprocessed on every run (this silently turned 10-plan runs into 130+-plan runs).
aws s3 sync "$S3_INPUT_PATH" data/inputs/ --exclude "*" --include "*.pdf" --delete
echo "Sync complete: $(ls data/inputs/*.pdf 2>/dev/null | wc -l) PDFs"

PYTHONPATH=. python3.11 -m src.run_pipeline

echo "[CLASSIFICATION] Running asset class classification pipeline..."
PYTHONPATH=. python3.11 run_classification.py
