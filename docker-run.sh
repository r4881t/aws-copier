#!/bin/bash

# Build the Docker image
docker build -t aws-copier .

# Check if the data directory exists, if not create it
if [ ! -d "./data" ]; then
  mkdir -p ./data
  echo "Created data directory for database persistence"
fi

# Run the Docker container with the provided arguments
# This will mount the ./data directory to /data in the container
docker run -v "$(pwd)/data:/data" aws-copier "$@"

# Example usage:
# ./docker-run.sh migrate --old-key KEY1 --old-secret SECRET1 --new-key KEY2 --new-secret SECRET2
# ./docker-run.sh check my-bucket-name
# ./docker-run.sh clean