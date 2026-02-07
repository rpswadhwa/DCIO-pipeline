#!/bin/bash

# ECR Deployment Script for form5500-ocr-pipeline

set -e

# Configuration
AWS_ACCOUNT_ID="575531351306"
AWS_REGION="us-east-1"
ECR_REPO_NAME="form5500-ocr-pipeline"
IMAGE_TAG="latest"
AWS_PROFILE="admin"

# Full ECR URI
ECR_URI="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"
IMAGE_NAME="${ECR_URI}/${ECR_REPO_NAME}:${IMAGE_TAG}"

echo "=========================================="
echo "Deploying to ECR"
echo "=========================================="
echo "AWS Account: ${AWS_ACCOUNT_ID}"
echo "AWS Region: ${AWS_REGION}"
echo "ECR Repository: ${ECR_REPO_NAME}"
echo "Image URI: ${IMAGE_NAME}"
echo "=========================================="

# Step 1: Login to ECR
echo ""
echo "[1/4] Logging in to ECR..."
aws ecr get-login-password --region ${AWS_REGION} --profile ${AWS_PROFILE} | \
  docker login --username AWS --password-stdin ${ECR_URI}
echo "✓ ECR login successful"

# Step 2: Build Docker image
echo ""
echo "[2/4] Building Docker image..."
docker build -t ${ECR_REPO_NAME}:${IMAGE_TAG} .
docker tag ${ECR_REPO_NAME}:${IMAGE_TAG} ${IMAGE_NAME}
echo "✓ Docker image built successfully"

# Step 3: Push to ECR
echo ""
echo "[3/4] Pushing image to ECR..."
docker push ${IMAGE_NAME}
echo "✓ Image pushed to ECR successfully"

# Step 4: Display image details
echo ""
echo "[4/4] Image deployment summary:"
echo "=========================================="
aws ecr describe-images --repository-name ${ECR_REPO_NAME} \
  --region ${AWS_REGION} \
  --profile ${AWS_PROFILE} \
  --query 'imageDetails[0]' \
  --output table
echo "=========================================="
echo ""
echo "✓ Deployment complete!"
echo ""
echo "To run the container:"
echo "docker run ${IMAGE_NAME}"
echo ""
echo "To push to ECS, use:"
echo "  Image URI: ${IMAGE_NAME}"
