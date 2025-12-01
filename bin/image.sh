#!/bin/bash
#
# Copyright (c) 2012 - 2020 YCSB contributors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you
# may not use this file except in compliance with the License. You
# may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied. See the License for the specific language governing
# permissions and limitations under the License. See accompanying
# LICENSE file.
#
# -----------------------------------------------------------------------------
# YCSB Docker Image Build Script
#
# This script builds a Docker image containing YCSB with specified bindings.
# Usage: bin/image.sh [binding1] [binding2] ... [bindingN]
#
# Example: bin/image.sh cassandra swiftpaxos
#          Creates a Docker image with Cassandra and SwiftPaxos bindings.
#
# If no bindings are specified, only core is included.
# -----------------------------------------------------------------------------

set -e

# Get script directory
SCRIPT_DIR=$(dirname "$0" 2>/dev/null)
YCSB_HOME=$(cd "$SCRIPT_DIR/.." && pwd)

# Default image name
IMAGE_NAME="ycsb"
IMAGE_TAG="latest"

# Function to display usage
usage() {
    echo "Usage: $0 [OPTIONS] [binding1] [binding2] ... [bindingN]"
    echo ""
    echo "Build a Docker image containing YCSB with specified bindings."
    echo ""
    echo "Options:"
    echo "  -t, --tag TAG         Tag for the Docker image (default: latest)"
    echo "  -n, --name NAME       Name for the Docker image (default: ycsb)"
    echo "  -h, --help            Display this help message"
    echo ""
    echo "Examples:"
    echo "  $0 cassandra swiftpaxos"
    echo "  $0 -t v1.0 -n my-ycsb redis mongodb"
    echo ""
    echo "Available bindings can be found in bin/bindings.properties"
    exit 1
}

# Parse options
BINDINGS=()
while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            usage
            ;;
        -t|--tag)
            IMAGE_TAG="$2"
            shift 2
            ;;
        -n|--name)
            IMAGE_NAME="$2"
            shift 2
            ;;
        -*)
            echo "Unknown option: $1"
            usage
            ;;
        *)
            BINDINGS+=("$1")
            shift
            ;;
    esac
done

# Check if Docker is available
if ! command -v docker &> /dev/null; then
    echo "[ERROR] Docker is not installed or not in PATH. Please install Docker first."
    exit 1
fi

# Validate bindings exist in bindings.properties
BINDINGS_FILE="$YCSB_HOME/bin/bindings.properties"
INVALID_BINDINGS=()

for binding in "${BINDINGS[@]}"; do
    # Extract the base binding name (before any dash)
    base_binding=$(echo "$binding" | cut -d'-' -f1)
    
    # Check if binding exists in bindings.properties
    if ! grep -q "^${binding}:" "$BINDINGS_FILE" 2>/dev/null; then
        INVALID_BINDINGS+=("$binding")
    fi
done

if [ ${#INVALID_BINDINGS[@]} -gt 0 ]; then
    echo "[ERROR] The following bindings were not found in $BINDINGS_FILE:"
    for invalid in "${INVALID_BINDINGS[@]}"; do
        echo "  - $invalid"
    done
    echo ""
    echo "Available bindings:"
    grep "^[a-z]" "$BINDINGS_FILE" | cut -d':' -f1 | sort
    exit 1
fi

# Build Maven module list for Docker build
MAVEN_MODULES=""
if [ ${#BINDINGS[@]} -eq 0 ]; then
    echo "[INFO] No bindings specified, building image with core only."
    MAVEN_MODULES=""
else
    echo "[INFO] Building Docker image with the following bindings: ${BINDINGS[*]}"
    for binding in "${BINDINGS[@]}"; do
        # Extract the base binding directory (before any dash)
        base_binding=$(echo "$binding" | cut -d'-' -f1)
        MAVEN_MODULES="${MAVEN_MODULES},site.ycsb:${base_binding}-binding"
    done
fi

# Full image name
FULL_IMAGE_NAME="${IMAGE_NAME}:${IMAGE_TAG}"

# Build Docker image
echo "[INFO] Building Docker image: $FULL_IMAGE_NAME"
echo "[INFO] This may take several minutes..."

cd "$YCSB_HOME"

docker build \
    --build-arg BINDINGS="$MAVEN_MODULES" \
    -t "$FULL_IMAGE_NAME" \
    -f Dockerfile \
    .

if [ $? -eq 0 ]; then
    echo ""
    echo "[SUCCESS] Docker image built successfully: $FULL_IMAGE_NAME"
    echo ""
    echo "To run YCSB in the container:"
    echo "  docker run $FULL_IMAGE_NAME load <binding> [options]"
    echo "  docker run $FULL_IMAGE_NAME run <binding> [options]"
    echo ""
    echo "Example:"
    if [ ${#BINDINGS[@]} -gt 0 ]; then
        echo "  docker run $FULL_IMAGE_NAME load ${BINDINGS[0]} -P workloads/workloada"
    else
        echo "  docker run $FULL_IMAGE_NAME load basic -P workloads/workloada"
    fi
else
    echo "[ERROR] Failed to build Docker image"
    exit 1
fi
