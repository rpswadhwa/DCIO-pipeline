# AWS Batch Deployment Guide for form5500-ocr-pipeline

## Overview
This guide will help you launch your Docker container on AWS Batch.

## Prerequisites
- AWS Account with admin credentials configured in CLI
- ECR image already pushed: `575531351306.dkr.ecr.us-east-1.amazonaws.com/form5500-ocr-pipeline:latest`
- VPC with subnets and security groups configured

## Option 1: Quick Start (Easiest - Using ECS)

If you want the fastest setup without managing Batch infrastructure, use Amazon ECS directly.

### Step 1: Get VPC Information
```bash
# List your VPCs
aws ec2 describe-vpcs --profile admin --region us-east-1

# List subnets
aws ec2 describe-subnets --filters "Name=vpc-id,Values=vpc-xxxxx" --profile admin --region us-east-1

# List security groups
aws ec2 describe-security-groups --filters "Name=vpc-id,Values=vpc-xxxxx" --profile admin --region us-east-1
```

### Step 2: Create ECS Task Definition
```bash
aws ecs register-task-definition \
  --family form5500-ocr-task \
  --network-mode awsvpc \
  --requires-compatibilities FARGATE \
  --cpu 2048 \
  --memory 4096 \
  --execution-role-arn arn:aws:iam::575531351306:role/ecsTaskExecutionRole \
  --container-definitions '[{
    "name": "form5500-ocr",
    "image": "575531351306.dkr.ecr.us-east-1.amazonaws.com/form5500-ocr-pipeline:latest",
    "essential": true,
    "environment": [
      {"name": "INPUT_DIR", "value": "data/inputs"},
      {"name": "OUTPUT_DIR", "value": "data/outputs"},
      {"name": "USE_LLM", "value": "1"}
    ],
    "logConfiguration": {
      "logDriver": "awslogs",
      "options": {
        "awslogs-group": "/ecs/form5500-ocr",
        "awslogs-region": "us-east-1",
        "awslogs-stream-prefix": "ecs"
      }
    }
  }]' \
  --region us-east-1 \
  --profile admin
```

### Step 3: Run Task
```bash
aws ecs run-task \
  --cluster default \
  --task-definition form5500-ocr-task:1 \
  --launch-type FARGATE \
  --network-configuration "awsvpcConfiguration={subnets=[subnet-xxxxx],securityGroups=[sg-xxxxx],assignPublicIp=ENABLED}" \
  --region us-east-1 \
  --profile admin
```

---

## Option 2: Using AWS Batch (Recommended for Production)

### Step 1: Set VPC Details
```bash
# Store your VPC details
VPC_ID="vpc-xxxxx"
SUBNET_ID="subnet-xxxxx"
SECURITY_GROUP_ID="sg-xxxxx"
```

### Step 2: Create IAM Roles
```bash
# Create Batch Service Role
aws iam create-role \
  --role-name AWSBatchServiceRole-form5500 \
  --assume-role-policy-document file://batch-trust-policy.json \
  --profile admin

aws iam attach-role-policy \
  --role-name AWSBatchServiceRole-form5500 \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSBatchServiceRole \
  --profile admin
```

### Step 3: Create Compute Environment
```bash
aws batch create-compute-environment \
  --compute-environment-name form5500-compute-env \
  --type MANAGED \
  --state ENABLED \
  --compute-resources \
    type=FARGATE,\
    maxvCpus=256,\
    subnets=subnet-xxxxx,\
    securityGroupIds=sg-xxxxx \
  --service-role arn:aws:iam::575531351306:role/AWSBatchServiceRole-form5500 \
  --region us-east-1 \
  --profile admin
```

### Step 4: Create Job Queue
```bash
aws batch create-job-queue \
  --job-queue-name form5500-job-queue \
  --state ENABLED \
  --priority 1 \
  --compute-environment-order order=1,computeEnvironment=form5500-compute-env \
  --region us-east-1 \
  --profile admin
```

### Step 5: Register Job Definition
```bash
aws batch register-job-definition \
  --job-definition-name form5500-ocr-pipeline \
  --type container \
  --platform-capabilities FARGATE \
  --container-properties file://job-definition.json \
  --region us-east-1 \
  --profile admin
```

### Step 6: Submit Job
```bash
aws batch submit-job \
  --job-name form5500-ocr-run-1 \
  --job-queue form5500-job-queue \
  --job-definition form5500-ocr-pipeline \
  --region us-east-1 \
  --profile admin
```

---

## Monitoring Jobs

### Check Job Status
```bash
# List all jobs
aws batch describe-jobs \
  --jobs <JOB_ID> \
  --region us-east-1 \
  --profile admin

# Watch job status
watch -n 5 'aws batch describe-jobs --jobs <JOB_ID> --region us-east-1 --profile admin --query "jobs[0].{id:jobId,status:status,reason:statusReason}"'
```

### View Logs
```bash
# List log streams
aws logs describe-log-streams \
  --log-group-name /batch/form5500 \
  --region us-east-1 \
  --profile admin

# Get log events
aws logs get-log-events \
  --log-group-name /batch/form5500 \
  --log-stream-name <STREAM_NAME> \
  --region us-east-1 \
  --profile admin
```

---

## Environment Variables
You can override environment variables when submitting the job:

```bash
aws batch submit-job \
  --job-name form5500-ocr-run-2 \
  --job-queue form5500-job-queue \
  --job-definition form5500-ocr-pipeline \
  --container-overrides environment=[{name=USE_LLM,value=0},{name=DPI,value=200}] \
  --region us-east-1 \
  --profile admin
```

---

## Troubleshooting

### Job stays in RUNNABLE state
- Check compute environment is ENABLED
- Verify vCPU and memory resources are available
- Check security group allows outbound traffic

### Image pull errors
- Verify ECR image exists: `aws ecr describe-images --repository-name form5500-ocr-pipeline`
- Check task execution role has ECR read permissions
- Verify task role has ECR permissions

### Job fails immediately
- Check CloudWatch logs for errors
- Verify .env file is properly configured in the container
- Check environment variables are set correctly

---

## Quick Commands

```bash
# Register job definition
aws batch register-job-definition --job-definition-name form5500-ocr-pipeline --type container --platform-capabilities FARGATE --container-properties '{"image":"575531351306.dkr.ecr.us-east-1.amazonaws.com/form5500-ocr-pipeline:latest","vcpus":2,"memory":4096}' --region us-east-1 --profile admin

# Submit job
aws batch submit-job --job-name form5500-ocr-run --job-queue form5500-job-queue --job-definition form5500-ocr-pipeline --region us-east-1 --profile admin

# Check job status
aws batch list-jobs --job-queue form5500-job-queue --region us-east-1 --profile admin
```
