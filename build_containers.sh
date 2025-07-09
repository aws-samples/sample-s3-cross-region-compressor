#!/bin/bash
set -e

# Utility function to check if a command exists
command_exists() {
  command -v "$1" >/dev/null 2>&1
}

# Check for UV tool
if ! command_exists uv; then
  echo "Warning: UV package manager not found"
  echo "UV is recommended for dependency management: https://github.com/astral-sh/uv"
  echo "Continuing without UV - will use existing requirements.txt files"
  USE_UV=false
else
  USE_UV=true
fi

# Check for container engine (Finch or Docker)
CONTAINER_ENGINE=""
if command_exists finch; then
  CONTAINER_ENGINE="finch"
  echo "Using Finch as container engine"
elif command_exists docker; then
  CONTAINER_ENGINE="docker"
  echo "Using Docker as container engine"
else
  echo "Error: Neither Finch nor Docker found"
  echo "Please install either Finch (https://github.com/runfinch/finch) or Docker (https://www.docker.com/)"
  exit 1
fi

# Check if container engine is running and start if needed
if [ "$CONTAINER_ENGINE" = "finch" ]; then
  if ! finch info > /dev/null 2>&1; then
    echo "Finch does not seem to be running, starting it now..."
    finch vm start
    if [ $? -ne 0 ]; then
      echo "Failed to start Finch VM"
      exit 1
    fi
  fi
elif [ "$CONTAINER_ENGINE" = "docker" ]; then
  if ! docker info > /dev/null 2>&1; then
    echo "Docker does not seem to be running, please start it manually"
    exit 1
  fi
fi

# Create dist directory if it doesn't exist
mkdir -p ./bin/dist

# Build the ARM containers
echo "Building ARM64 containers..."

# Refresh dependencies
if [ "$USE_UV" = true ]; then
  echo "Refreshing dependencies with UV..."
  uv export --format requirements.txt --project ./bin/source_region -o ./bin/source_region/requirements.txt -q
  if [ $? -ne 0 ]; then
    echo "Failed to compile source_region dependencies"
    exit 1
  fi

  uv export --format requirements.txt --project ./bin/target_region -o ./bin/target_region/requirements.txt -q
  if [ $? -ne 0 ]; then
    echo "Failed to compile target_region dependencies"
    exit 1
  fi
else
  echo "Using existing requirements.txt files..."
fi

# Building containers with the appropriate engine
echo "Building containers using $CONTAINER_ENGINE..."
$CONTAINER_ENGINE build --platform=linux/arm64 -t source_region ./bin/source_region/
if [ $? -ne 0 ]; then
  echo "Failed to build source_region container"
  exit 1
fi

$CONTAINER_ENGINE build --platform=linux/arm64 -t target_region ./bin/target_region/
if [ $? -ne 0 ]; then
  echo "Failed to build target_region container"
  exit 1
fi

# Save the images to tar files using the appropriate engine
echo "Saving container images to tar files..."
if [ "$CONTAINER_ENGINE" = "finch" ]; then
  finch save source_region:latest > ./bin/dist/source_region.tar
  finch save target_region:latest > ./bin/dist/target_region.tar
elif [ "$CONTAINER_ENGINE" = "docker" ]; then
  docker save source_region:latest -o ./bin/dist/source_region.tar
  docker save target_region:latest -o ./bin/dist/target_region.tar
fi

echo "Container build complete!"
