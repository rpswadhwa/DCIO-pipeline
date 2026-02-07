#!/bin/bash

# AWS Batch Setup Script for form5500-ocr-pipeline

set -e

# Configuration
AWS_ACCOUNT_ID="575531351306"
AWS_REGION="us-east-1"
AWS_PROFILE="admin"
COMPUTE_ENV_NAME="form5500-compute-env"
JOB_QUEUE_NAME="form5500-job-queue"
JOB_DEF_NAME="form5500-ocr-pipeline"
ECR_IMAGE="575531351306.dkr.ecr.us-east-1.amazonaws.com/form5500-ocr-pipeline:latest"

echo "=========================================="
echo "AWS Batch Setup for form5500-ocr-pipeline"
echo "=========================================="

# Step 1: Create IAM Role for Batch (if needed)
echo ""
echo "[1/5] Setting up IAM role..."

ROLE_NAME="AWSBatchServiceRole-form5500"
TRUST_POLICY='{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "batch.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}'

# Check if role exists
if ! aws iam get-role --role-name "$ROLE_NAME" --profile "$AWS_PROFILE" 2>/dev/null; then
    echo "Creating IAM role: $ROLE_NAME"
    aws iam create-role \
        --role-name "$ROLE_NAME" \
        --assume-role-policy-document "$TRUST_POLICY" \
        --profile "$AWS_PROFILE"
    
    # Attach policy
    aws iam attach-role-policy \
        --role-name "$ROLE_NAME" \
        --policy-arn arn:aws:iam::aws:policy/service-role/AWSBatchServiceRole \
        --profile "$AWS_PROFILE"
    
    echo "✓ IAM role created"
else
    echo "✓ IAM role already exists"
fi

# Step 2: Create EC2 Instance Role (for container execution)
echo ""
echo "[2/5] Setting up EC2 instance role..."

INSTANCE_ROLE_NAME="ecsInstanceRole-form5500"
INSTANCE_PROFILE_NAME="ecsInstanceProfile-form5500"

if ! aws iam get-role --role-name "$INSTANCE_ROLE_NAME" --profile "$AWS_PROFILE" 2>/dev/null; then
    INSTANCE_TRUST_POLICY='{
      "Version": "2012-10-17",
      "Statement": [
        {
          "Effect": "Allow",
          "Principal": {
            "Service": "ec2.amazonaws.com"
          },
          "Action": "sts:AssumeRole"
        }
      ]
    }'
    
    aws iam create-role \
        --role-name "$INSTANCE_ROLE_NAME" \
        --assume-role-policy-document "$INSTANCE_TRUST_POLICY" \
        --profile "$AWS_PROFILE"
    
    # Attach policies
    aws iam attach-role-policy \
        --role-name "$INSTANCE_ROLE_NAME" \
        --policy-arn arn:aws:iam::aws:policy/service-role/AmazonEC2ContainerServiceforEC2Role \
        --profile "$AWS_PROFILE"
    
    aws iam attach-role-policy \
        --role-name "$INSTANCE_ROLE_NAME" \
        --policy-arn arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly \
        --profile "$AWS_PROFILE"
    
    # Create instance profile
    aws iam create-instance-profile \
        --instance-profile-name "$INSTANCE_PROFILE_NAME" \
        --profile "$AWS_PROFILE"
    
    aws iam add-role-to-instance-profile \
        --instance-profile-name "$INSTANCE_PROFILE_NAME" \
        --role-name "$INSTANCE_ROLE_NAME" \
        --profile "$AWS_PROFILE"
    
    echo "✓ EC2 instance role created"
else
    echo "✓ EC2 instance role already exists"
fi

# Step 3: Create Compute Environment
echo ""
echo "[3/5] Creating compute environment..."

if aws batch describe-compute-environments --compute-environments "$COMPUTE_ENV_NAME" --region "$AWS_REGION" --profile "$AWS_PROFILE" 2>/dev/null | grep -q "$COMPUTE_ENV_NAME"; then
    echo "✓ Compute environment already exists"
else
    aws batch create-compute-environment \
        --compute-environment-name "$COMPUTE_ENV_NAME" \
        --type MANAGED \
        --state ENABLED \
        --compute-resources "{
            \"type\": \"EC2\",
            \"minvCpus\": 0,
            \"maxvCpus\": 256,
            \"desiredvCpus\": 0,
            \"instanceTypes\": [\"optimal\"],
            \"subnets\": [\"subnet-0000000000000000\"],
            \"securityGroupIds\": [\"sg-0000000000000000\"],
            \"instanceRole\": \"arn:aws:iam::${AWS_ACCOUNT_ID}:instance-profile/${INSTANCE_PROFILE_NAME}\"
        }" \
        --service-role "arn:aws:iam::${AWS_ACCOUNT_ID}:role/${ROLE_NAME}" \
        --region "$AWS_REGION" \
        --profile "$AWS_PROFILE" \
        || echo "Note: Compute environment creation may need VPC configuration"
fi

echo "⚠️  Note: You need to configure the compute environment with your VPC subnet and security group"
echo "    Update: aws batch update-compute-environment --compute-environment $COMPUTE_ENV_NAME ..."

# Step 4: Create Job Queue
echo ""
echo "[4/5] Creating job queue..."

if aws batch describe-job-queues --job-queues "$JOB_QUEUE_NAME" --region "$AWS_REGION" --profile "$AWS_PROFILE" 2>/dev/null | grep -q "$JOB_QUEUE_NAME"; then
    echo "✓ Job queue already exists"
else
    aws batch create-job-queue \
        --job-queue-name "$JOB_QUEUE_NAME" \
        --state ENABLED \
        --priority 1 \
        --compute-environment-order "order=1,computeEnvironment=$COMPUTE_ENV_NAME" \
        --region "$AWS_REGION" \
        --profile "$AWS_PROFILE" \
        || echo "Note: Job queue will be created once compute environment is ready"
fi

# Step 5: Register Job Definition
echo ""
echo "[5/5] Registering job definition..."

aws batch register-job-definition \
    --job-definition-name "$JOB_DEF_NAME" \
    --type container \
    --container-properties "{
        \"image\": \"${ECR_IMAGE}\",
        \"vcpus\": 2,
        \"memory\": 4096,
        \"environment\": [
            {\"name\": \"INPUT_DIR\", \"value\": \"data/inputs\"},
            {\"name\": \"OUTPUT_DIR\", \"value\": \"data/outputs\"},
            {\"name\": \"USE_LLM\", \"value\": \"1\"}
        ],
        \"mountPoints\": [],
        \"volumes\": []
    }" \
    --region "$AWS_REGION" \
    --profile "$AWS_PROFILE"

echo ""
echo "=========================================="
echo "✓ AWS Batch setup complete!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "1. Configure compute environment with your VPC:"
echo "   aws batch update-compute-environment \\"
echo "     --compute-environment $COMPUTE_ENV_NAME \\"
echo "     --compute-resources subnet=subnet-xxxxx,securityGroupIds=sg-xxxxx"
echo ""
echo "2. Submit a job:"
echo "   aws batch submit-job \\"
echo "     --job-name form5500-job \\"
echo "     --job-queue $JOB_QUEUE_NAME \\"
echo "     --job-definition $JOB_DEF_NAME"
echo ""
