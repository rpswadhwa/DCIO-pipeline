#!/bin/bash

# AWS Batch Setup with Fargate (Simpler Option)

set -e

AWS_REGION="us-east-1"
AWS_PROFILE="admin"
AWS_ACCOUNT_ID="575531351306"
JOB_DEF_NAME="form5500-ocr-pipeline"
ECR_IMAGE="575531351306.dkr.ecr.us-east-1.amazonaws.com/form5500-ocr-pipeline:latest"

echo "=========================================="
echo "AWS Batch Fargate Setup"
echo "=========================================="

# Step 1: Create ECS Task Execution Role
echo ""
echo "[1/3] Creating ECS Task Execution Role..."

ROLE_NAME="ecsTaskExecutionRole-form5500"
TRUST_POLICY='{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {"Service": "ecs-tasks.amazonaws.com"},
    "Action": "sts:AssumeRole"
  }]
}'

if aws iam get-role --role-name "$ROLE_NAME" --profile "$AWS_PROFILE" 2>/dev/null; then
    echo "✓ Role already exists"
else
    aws iam create-role \
        --role-name "$ROLE_NAME" \
        --assume-role-policy-document "$TRUST_POLICY" \
        --profile "$AWS_PROFILE"
    
    aws iam attach-role-policy \
        --role-name "$ROLE_NAME" \
        --policy-arn arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy \
        --profile "$AWS_PROFILE"
    
    # Allow pulling from ECR
    aws iam attach-role-policy \
        --role-name "$ROLE_NAME" \
        --policy-arn arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly \
        --profile "$AWS_PROFILE"
    
    echo "✓ Role created: $ROLE_NAME"
fi

# Step 2: Register Fargate Job Definition
echo ""
echo "[2/3] Registering Fargate job definition..."

aws batch register-job-definition \
    --job-definition-name "$JOB_DEF_NAME" \
    --type container \
    --platform-capabilities FARGATE \
    --container-properties "{
        \"image\": \"${ECR_IMAGE}\",
        \"vcpus\": 2,
        \"memory\": 4096,
        \"jobRoleArn\": \"arn:aws:iam::${AWS_ACCOUNT_ID}:role/${ROLE_NAME}\",
        \"environment\": [
            {\"name\": \"INPUT_DIR\", \"value\": \"data/inputs\"},
            {\"name\": \"OUTPUT_DIR\", \"value\": \"data/outputs\"},
            {\"name\": \"DPI\", \"value\": \"350\"},
            {\"name\": \"USE_LLM\", \"value\": \"1\"}
        ],
        \"fargatePlatformConfiguration\": {
            \"platformVersion\": \"LATEST\"
        }
    }" \
    --region "$AWS_REGION" \
    --profile "$AWS_PROFILE"

echo "✓ Job definition registered"

# Step 3: Create Fargate Compute Environment
echo ""
echo "[3/3] Creating Fargate compute environment..."

COMPUTE_ENV="form5500-fargate-compute"
if aws batch describe-compute-environments --compute-environments "$COMPUTE_ENV" --region "$AWS_REGION" --profile "$AWS_PROFILE" 2>/dev/null | grep -q "$COMPUTE_ENV"; then
    echo "✓ Compute environment already exists"
else
    aws batch create-compute-environment \
        --compute-environment-name "$COMPUTE_ENV" \
        --type MANAGED \
        --state ENABLED \
        --service-role "arn:aws:iam::${AWS_ACCOUNT_ID}:role/AWSBatchServiceRole" \
        --compute-resources "{
            \"type\": \"FARGATE\",
            \"maxvCpus\": 256,
            \"subnets\": [\"SUBNET_ID_HERE\"],
            \"securityGroupIds\": [\"SECURITY_GROUP_ID_HERE\"]
        }" \
        --region "$AWS_REGION" \
        --profile "$AWS_PROFILE" \
        2>/dev/null || echo "⚠️  Note: You need to update subnets and security groups"
fi

# Create Job Queue
JOB_QUEUE="form5500-fargate-queue"
if aws batch describe-job-queues --job-queues "$JOB_QUEUE" --region "$AWS_REGION" --profile "$AWS_PROFILE" 2>/dev/null | grep -q "$JOB_QUEUE"; then
    echo "✓ Job queue already exists"
else
    aws batch create-job-queue \
        --job-queue-name "$JOB_QUEUE" \
        --state ENABLED \
        --priority 1 \
        --compute-environment-order "order=1,computeEnvironment=$COMPUTE_ENV" \
        --region "$AWS_REGION" \
        --profile "$AWS_PROFILE" \
        2>/dev/null || echo "Note: Job queue creation may need compute environment setup"
fi

echo ""
echo "=========================================="
echo "✓ Batch setup complete!"
echo "=========================================="
echo ""
echo "Job Definition: $JOB_DEF_NAME"
echo "Image: $ECR_IMAGE"
echo ""
echo "To submit a job, run:"
echo "aws batch submit-job \\"
echo "  --job-name form5500-ocr-job-1 \\"
echo "  --job-queue form5500-fargate-queue \\"
echo "  --job-definition $JOB_DEF_NAME \\"
echo "  --region us-east-1 \\"
echo "  --profile admin"
echo ""
