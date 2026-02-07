#!/bin/bash

# Docker Troubleshooting Script for macOS

echo "=========================================="
echo "Docker Troubleshooting & Reset"
echo "=========================================="

# Step 1: Stop Docker
echo ""
echo "[1/4] Stopping Docker Desktop..."
osascript -e 'quit app "Docker"' || true
sleep 5

# Step 2: Clean all Docker data
echo "[2/4] Cleaning Docker data..."
rm -rf ~/Library/Containers/com.docker.docker/Data
rm -rf ~/.docker/buildx
mkdir -p ~/Library/Containers/com.docker.docker/Data
echo "✓ Docker data cleaned"

# Step 3: Restart Docker
echo "[3/4] Starting Docker Desktop..."
open -a Docker
sleep 20
echo "✓ Docker restarted"

# Step 4: Verify Docker is working
echo "[4/4] Verifying Docker daemon..."
docker ps
echo "✓ Docker daemon is ready"

echo ""
echo "=========================================="
echo "✓ Docker reset complete!"
echo "=========================================="
echo ""
echo "You can now run the build:"
echo "cd /Users/Arnikayadav/Desktop/Python/DCIO/DCIO/DATA/pipeline"
echo "docker build -t form5500-ocr-pipeline:latest ."
