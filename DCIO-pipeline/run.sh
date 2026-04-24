#!/bin/bash
set -e
cd /home/ec2-user/DCIO-pipeline/DCIO-pipeline
source /home/ec2-user/DCIO-pipeline/venv/bin/activate

export $(grep -v ^# .env | xargs)

if [ -n "$S3_INPUT_PATH" ]; then
    echo "[STEP 0] Syncing PDFs from S3: $S3_INPUT_PATH"
    aws s3 sync "$S3_INPUT_PATH" data/inputs/ --exclude "*" --include "*.pdf"
    echo "Sync complete"
fi

PYTHONPATH=. python3.11 -m src.run_pipeline
