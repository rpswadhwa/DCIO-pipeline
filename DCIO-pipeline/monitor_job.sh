#!/bin/bash

# Monitor AWS Batch Job

JOB_ID="${1:-9e79b139-7438-4f4f-ab4c-d045ac55c5a6}"
REGION="us-east-1"
PROFILE="admin"

echo "=========================================="
echo "AWS Batch Job Monitor"
echo "=========================================="
echo "Job ID: $JOB_ID"
echo "Updating every 5 seconds..."
echo ""

while true; do
    clear
    
    # Get job details
    aws batch describe-jobs \
        --jobs "$JOB_ID" \
        --region "$REGION" \
        --profile "$PROFILE" \
        --query 'jobs[0].{
            jobId:jobId,
            jobName:jobName,
            status:status,
            statusReason:statusReason,
            createdAt:createdAt,
            startedAt:startedAt,
            stoppedAt:stoppedAt,
            container:container.image,
            exitCode:container.exitCode
        }' \
        --output table
    
    echo ""
    echo "Last updated: $(date)"
    echo "Press Ctrl+C to exit"
    
    # Get job status once more to check if completed
    STATUS=$(aws batch describe-jobs \
        --jobs "$JOB_ID" \
        --region "$REGION" \
        --profile "$PROFILE" \
        --query 'jobs[0].status' \
        --output text)
    
    if [[ "$STATUS" == "SUCCEEDED" || "$STATUS" == "FAILED" ]]; then
        echo ""
        echo "Job completed with status: $STATUS"
        break
    fi
    
    sleep 5
done
