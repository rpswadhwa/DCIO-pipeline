#!/bin/bash
# Processes a single page_requests entry from the Undercapture Desk UI
# (https://claude.ai/artifact/BsaQseekvLh7zLueRicx3H): downloads just that
# plan's PDF, scopes the LLM full-row-extraction path to the requested pages
# via LLM_PAGE_OVERRIDES_FILE/LLM_FULL_EXTRACT, then classifies only that
# ack_id. Unlike run.sh, this does NOT touch the rest of data/inputs/ or sweep
# the full plan_mf_history_v3 table.
#
# Required env vars:
#   ACK_ID    - the plan's ack_id (also the PDF filename stem and page_requests doc id)
#   S3_KEY    - full s3:// URI of the plan's source PDF
#   PAGES_CSV - comma-separated 1-indexed page numbers to extract, e.g. "23,24,25,26"
#
# Usage (on EC2):
#   ACK_ID=... S3_KEY=s3://... PAGES_CSV=23,24,25 bash process_page_request.sh
#
# Prints "PROCESS_PAGE_REQUEST: SUCCESS ack_id=<id>" on success or
# "PROCESS_PAGE_REQUEST: FAILURE ack_id=<id>" on failure, so a caller
# (SSM invocation output) can grep for the result without parsing full logs.

set -euo pipefail
cd /home/ec2-user/DCIO-pipeline/DCIO-pipeline
source /home/ec2-user/DCIO-pipeline/venv/bin/activate

: "${ACK_ID:?ACK_ID is required}"
: "${S3_KEY:?S3_KEY is required}"
: "${PAGES_CSV:?PAGES_CSV is required}"

fail() {
  echo "PROCESS_PAGE_REQUEST: FAILURE ack_id=${ACK_ID} reason=$1"
  exit 1
}
trap 'fail "unexpected error at line $LINENO"' ERR

echo "=== DEPLOY PROVENANCE ==="
if [ -f .deployed_commit ]; then
  cat .deployed_commit
else
  echo "!!! WARNING: no .deployed_commit marker found."
fi
echo "=========================="

export $(grep -v '^#' .env | xargs)
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"
export AWS_REGION="${AWS_REGION:-$AWS_DEFAULT_REGION}"
export TESSERACT_CMD="${TESSERACT_CMD:-/home/ec2-user/micromamba/envs/ocr/bin/tesseract}"
export TESSDATA_PREFIX="${TESSDATA_PREFIX:-/home/ec2-user/micromamba/envs/ocr/share/tessdata}"
export OMP_THREAD_LIMIT="${OMP_THREAD_LIMIT:-1}"

echo "[STEP 1] Scoping data/inputs/ to ${ACK_ID}.pdf only"
rm -f data/inputs/*.pdf
aws s3 cp "$S3_KEY" "data/inputs/${ACK_ID}.pdf" --region "$AWS_REGION" || fail "s3 download failed"
[ -s "data/inputs/${ACK_ID}.pdf" ] || fail "downloaded PDF is missing or empty"

echo "[STEP 2] Writing llm_page_overrides.json for pages: ${PAGES_CSV}"
ACK_ID="$ACK_ID" PAGES_CSV="$PAGES_CSV" python3.11 -c '
import json, os
ack_id = os.environ["ACK_ID"]
pages = [int(p.strip()) for p in os.environ["PAGES_CSV"].split(",") if p.strip()]
if not pages:
    raise SystemExit("PAGES_CSV parsed to an empty page list")
json.dump({ack_id: pages}, open("llm_page_overrides.json", "w"))
print(f"Wrote override for {ack_id}: {pages}")
' || fail "failed to write llm_page_overrides.json"

echo "[STEP 3] Running pipeline scoped to ${ACK_ID} with LLM_FULL_EXTRACT=1"
# run_pipeline.py itself now runs classification (run_classification.py scoped to
# the override ack_id) as its own Step 12 whenever LLM_PAGE_OVERRIDES_FILE is set,
# so it can't be silently skipped if this script's invocation changes -- no
# separate classification step needed here anymore.
SYNC_S3_INPUTS=0 \
LLM_PAGE_OVERRIDES_FILE=llm_page_overrides.json \
LLM_FULL_EXTRACT=1 \
PYTHONPATH=. python3.11 -m src.run_pipeline || fail "run_pipeline.py failed"

echo "PROCESS_PAGE_REQUEST: SUCCESS ack_id=${ACK_ID}"
