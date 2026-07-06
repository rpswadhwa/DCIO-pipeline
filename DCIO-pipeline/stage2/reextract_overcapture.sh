#!/bin/bash
# Stage-2 exception re-run: apply the section-typing correction to a flagged
# over-capture ACK list by running the pipeline with SECTION_TYPING=1.
#
# Usage: stage2/reextract_overcapture.sh <ack_list_file> <workdir>
#
# Does NOT swap into the table -- run stage2/pilot_and_size.py first to VALIDATE
# (new MF total vs certified), then do the per-ACK swap (see stage2/RUNBOOK.md).
set -euo pipefail

ACK_LIST="${1:?usage: reextract_overcapture.sh <ack_list_file> <workdir>}"
WORKDIR="${2:?usage: reextract_overcapture.sh <ack_list_file> <workdir>}"
BRONZE="${BRONZE_PREFIX:-s3://retirementinsights-bronze/filings_5500_pdf/year=2026/batch_date=2026-06-28}"
PROJ="/home/ec2-user/DCIO-pipeline/DCIO-pipeline"

mkdir -p "$WORKDIR/inputs" "$WORKDIR/outputs"

# 1. download the flagged ACK PDFs
tr -d '\r' < "$ACK_LIST" | grep . | while read -r ack; do
  [ -f "$WORKDIR/inputs/$ack.pdf" ] || aws s3 cp "$BRONZE/$ack.pdf" "$WORKDIR/inputs/$ack.pdf" --quiet 2>/dev/null || echo "MISS $ack"
done
echo "downloaded $(ls "$WORKDIR/inputs" | wc -l) PDFs"

# 2. run the pipeline with the exception stage ON (extraction only; no LLM)
cd "$PROJ"
# shellcheck disable=SC1091
source ../venv/bin/activate
SECTION_TYPING=1 USE_LLM=0 USE_POST_LLM=0 SYNC_S3_INPUTS=0 VALIDATION_ENABLED=0 \
  INPUT_DIR="$WORKDIR/inputs" OUTPUT_DIR="$WORKDIR/outputs" \
  PYTHONPATH=.:stage2 python3.11 -m src.run_pipeline

echo "corrected rows -> $WORKDIR/outputs/  (validate with stage2/pilot_and_size.py, then swap per RUNBOOK.md)"
