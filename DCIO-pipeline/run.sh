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

# (2026-10-21) Last step of each extraction: one holding lives in one table (MF / CIT / alternatives).
# Runs after classification because classification is what tags MF rows Alternatives. Scoped to the
# filings in data/inputs (PDF stems); every change is logged to default.alt_holding_dedup_log first.
dedup_inputs() {
  echo "[DEDUP] Cross-table alternative holding de-duplication for this batch..."
  DEDUP_ACKS=data/dedup_acks.txt
  ls data/inputs/*.pdf 2>/dev/null | xargs -r -n1 basename | sed 's/\.pdf$//' > "$DEDUP_ACKS" || true
  if [ -s "$DEDUP_ACKS" ]; then
    PYTHONPATH=. python3.11 -m src.dedup_alt_holdings --ack-file "$DEDUP_ACKS" --apply \
      || echo "!!! WARNING: dedup_alt_holdings failed -- duplicates for this batch are NOT removed; rerun it by hand"
  else
    echo "[DEDUP] no PDFs in data/inputs, skipping"
  fi
}

# (2026-11-02) Chunk mode. A full DOL release (2026-11-02: 60,658 PDFs, ~79 GB) cannot go through one
# sync + one run_pipeline: the disk has ~7 GB free, run_pipeline writes nothing until every PDF is done,
# and SSM kills the job at 48h. Chunk mode plans ordered chunks (src/batch_chunks.py: Health & Welfare
# dropped; DC/DB >= $100M first, largest first), then per chunk fetches only its PDFs, extracts, writes,
# classifies + dedups that chunk, and deletes the PDFs. Finished chunks are marked done, so rerunning
# the same command resumes. CHUNK_TIER picks which chunks run: priority (default) | rest | all.
#   CHUNK_MODE=auto (default: chunk when the batch has more than CHUNK_SIZE PDFs) | 1 | 0
CHUNK_SIZE="${CHUNK_SIZE:-500}"
CHUNK_MODE="${CHUNK_MODE:-auto}"
CHUNK_TIER="${CHUNK_TIER:-priority}"
PDF_COUNT=$( (aws s3 ls "$S3_INPUT_PATH" || true) | grep -c '\.pdf$' || true)
if [ "$CHUNK_MODE" = "auto" ]; then
  if [ "$PDF_COUNT" -gt "$CHUNK_SIZE" ]; then CHUNK_MODE=1; else CHUNK_MODE=0; fi
fi
echo "[STEP 0] $PDF_COUNT PDFs in $S3_INPUT_PATH (chunk mode=$CHUNK_MODE, size=$CHUNK_SIZE, tier=$CHUNK_TIER)"

if [ "$CHUNK_MODE" = "1" ]; then
  CHUNK_DIR="data/chunks/${BATCH_DATE}"
  mkdir -p "$CHUNK_DIR/done"
  if [ ! -f "$CHUNK_DIR/.planned" ]; then
    PYTHONPATH=. python3.11 -m src.batch_chunks plan --batch-date "$BATCH_DATE" --out "$CHUNK_DIR" --chunk-size "$CHUNK_SIZE"
    touch "$CHUNK_DIR/.planned"
  fi
  # (2026-11-03) One run_pipeline per 500-filing chunk was OOM-killed (p0001, 2.5h lost): the runner
  # splits each chunk into micro-batches (CHUNK_MICRO, 25) with memory/time caps, CHUNK_WORKERS (3) in
  # parallel with serialized table writes, retries failures one filing at a time, and quarantines
  # filings that still fail (data/chunks/<batch>/quarantine.tsv). Each run_pipeline gets its own
  # INPUT_DIR/OUTPUT_DIR and SYNC_S3_INPUTS=0 (otherwise it re-syncs the whole batch and fills the disk).
  FAILED_CHUNKS=""
  PYTHONPATH=. python3.11 -m src.batch_chunks run --batch-date "$BATCH_DATE" --chunk-dir "$CHUNK_DIR" --tier "$CHUNK_TIER" \
    || FAILED_CHUNKS=" (chunk runner exited non-zero; see the log above)"
  echo "[CLASSIFICATION] end-of-run whole-table sweep..."
  PYTHONPATH=. python3.11 run_classification.py
else
  echo "[STEP 0] Syncing PDFs from S3: $S3_INPUT_PATH"
  # --delete makes data/inputs/ an exact mirror of this batch's S3 folder. Without it,
  # PDFs from every previous batch ever synced on this box accumulate forever and get
  # reprocessed on every run (this silently turned 10-plan runs into 130+-plan runs).
  aws s3 sync "$S3_INPUT_PATH" data/inputs/ --exclude "*" --include "*.pdf" --delete
  echo "Sync complete: $(ls data/inputs/*.pdf 2>/dev/null | wc -l) PDFs"

  PYTHONPATH=. python3.11 -m src.run_pipeline

  echo "[CLASSIFICATION] Running asset class classification pipeline..."
  PYTHONPATH=. python3.11 run_classification.py

  dedup_inputs
fi

# (2026-10-27) Whole-table sweep of the ALT manager cache. The per-run fill only applies answers to
# this batch's acks, so a name looked up here (or decided via --import-review) never reached the
# same fund in older filings -- 151 lines/$1.6B were stuck that way on 2026-10-27. One MERGE, fills
# blank managers only.
echo "[ALT MANAGER SWEEP] cached manager answers -> all managerless alternatives rows..."
PYTHONPATH=. python3.11 -m src.alt_manager_llm --apply-only \
  || echo "!!! WARNING: alt_manager_llm --apply-only failed -- cached managers NOT applied table-wide; rerun it by hand"
# provider_id for every labelled row whose manager has an approved ALT: alt_family alias (fills blanks only)
PYTHONPATH=. python3.11 -m src.alt_manager_fuzzy --stamp-ids-only \
  || echo "!!! WARNING: alt_manager_fuzzy --stamp-ids-only failed -- provider_id NOT stamped table-wide; rerun it by hand"

# (2026-10-27) Today's parent firm (approved default.firm_acquisitions deals) into manager_parent_name.
# Whole table (not just this batch) so rows the sweep above filled in older filings get a parent too.
echo "[FIRM PARENT] manager_parent_name, all filings..."
PYTHONPATH=. python3.11 -m src.apply_firm_parents --all \
  || echo "!!! WARNING: apply_firm_parents failed -- manager_parent_name NOT filled; rerun it by hand"

# (2026-11-02) Chunk mode keeps going past a failed chunk; fail the run at the end so the Step Function
# alerts. Rerunning the same command retries only the chunks without a done marker.
if [ -n "${FAILED_CHUNKS:-}" ]; then
  echo "!!! FAILED CHUNKS:$FAILED_CHUNKS -- rerun BATCH_DATE=$BATCH_DATE CHUNK_TIER=$CHUNK_TIER bash run.sh to retry them"
  exit 1
fi
