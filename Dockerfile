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

# Build stage
FROM maven:3.9-eclipse-temurin-21 AS builder

# Update CA certificates and install git, golang, build tools, and docker CLI
RUN apt-get update && apt-get install -y ca-certificates git golang make build-essential docker.io && update-ca-certificates

# Copy system library directory from 0track/tiga-suite container for Maven fallback
COPY --from=0track/tiga-suite:latest /usr/local/lib/ /usr/local/lib/

# Set working directory
WORKDIR /ycsb

# Copy the entire YCSB source
COPY . .

# Build argument for specifying which bindings to build
ARG BINDINGS=""

# Check if swiftpaxos binding is requested and build the dependency first
RUN if echo "$BINDINGS" | grep -q "swiftpaxos"; then \
        echo "SwiftPaxos binding requested, building swiftpaxos-client first..."; \
        cd /tmp && \
        git clone --branch container https://github.com/imdea-software/swiftpaxos.git && \
        cd swiftpaxos && \
        make && \
        cd /ycsb; \
    fi

# Build YCSB with specified bindings
# Use -Psource-run profile to copy dependencies to target/dependency
RUN if [ -z "$BINDINGS" ]; then \
        echo "Building core only..."; \
        mvn -Psource-run -pl site.ycsb:core -am clean package -DskipTests; \
    else \
        echo "Building core and bindings: $BINDINGS"; \
        mvn -Psource-run -pl site.ycsb:core${BINDINGS} -am clean package -DskipTests; \
    fi

# Create distribution structure
RUN mkdir -p /ycsb-dist/bin /ycsb-dist/workloads /ycsb-dist/core/target /ycsb-dist/conf && \
    cp -r /ycsb/bin/* /ycsb-dist/bin/ && \
    cp -r /ycsb/workloads/* /ycsb-dist/workloads/ && \
    # Copy pom.xml so ycsb.sh detects this as source checkout \
    cp /ycsb/pom.xml /ycsb-dist/ && \
    # Copy core JARs \
    cp -r /ycsb/core/target/*.jar /ycsb-dist/core/target/ && \
    # Copy core dependencies if they exist \
    mkdir -p /ycsb-dist/core/target/dependency && \
    if [ -d /ycsb/core/target/dependency ] && [ "$(ls -A /ycsb/core/target/dependency 2>/dev/null)" ]; then \
        cp -r /ycsb/core/target/dependency/* /ycsb-dist/core/target/dependency/; \
    fi && \
    # Copy all built binding targets with dependencies \
    for dir in /ycsb/*/target; do \
        if [ -d "$dir" ]; then \
            binding=$(basename $(dirname "$dir")); \
            if [ "$binding" != "core" ] && [ "$binding" != "distribution" ] && [ "$binding" != "binding-parent" ]; then \
                echo "Copying binding: $binding"; \
                mkdir -p "/ycsb-dist/$binding/target"; \
                cp "$dir"/*.jar "/ycsb-dist/$binding/target/" 2>/dev/null || true; \
                if [ -d "$dir/dependency" ] && [ "$(ls -A "$dir/dependency" 2>/dev/null)" ]; then \
                    mkdir -p "/ycsb-dist/$binding/target/dependency"; \
                    cp -r "$dir/dependency"/* "/ycsb-dist/$binding/target/dependency/"; \
                fi; \
            fi; \
        fi; \
    done

# Runtime stage
FROM eclipse-temurin:21-jre

# Set working directory
WORKDIR /ycsb

# Install ping utility and native JNI dependencies
RUN apt-get update && apt-get install -y \
    iputils-ping \
    libgflags2.2 \
    libgoogle-glog0v6 \
    libyaml-cpp0.8 \
    libboost-filesystem1.83.0 \
    libboost-thread1.83.0 \
    libboost-coroutine1.83.0 \
    libboost-context1.83.0 \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Copy the distribution from builder
COPY --from=builder /ycsb-dist /ycsb

# Make scripts executable
RUN chmod +x /ycsb/bin/ycsb.sh && \
    if [ -f /ycsb/bin/ycsb ]; then chmod +x /ycsb/bin/ycsb; fi

# Set environment variables
ENV YCSB_HOME=/ycsb

# Define environment variables for YCSB parameters
# These can be overridden at container runtime using docker run -e
ENV YCSB_COMMAND=""
ENV YCSB_BINDING=""
ENV YCSB_WORKLOAD=""
ENV YCSB_RECORDCOUNT=""
ENV YCSB_OPERATIONCOUNT=""
ENV YCSB_THREADS=""
ENV YCSB_TARGET=""
ENV YCSB_OPTS=""

# Create entrypoint script
RUN echo '#!/bin/bash\n\
set -e\n\
\n\
# Build command from environment variables if no arguments provided\n\
if [ $# -eq 0 ]; then\n\
    # Check if environment variables are set for running YCSB\n\
    if [ -n "$YCSB_COMMAND" ] && [ -n "$YCSB_BINDING" ]; then\n\
        # Validate YCSB_COMMAND\n\
        case "$YCSB_COMMAND" in\n\
            load|run|shell)\n\
                ;;\n\
            *)\n\
                echo "[ERROR] Invalid YCSB_COMMAND: $YCSB_COMMAND"\n\
                echo "[ERROR] Expected one of: load, run, shell"\n\
                exit 1\n\
                ;;\n\
        esac\n\
        \n\
        # Build command arguments using an array for safe handling\n\
        CMD_ARGS=("$YCSB_COMMAND" "$YCSB_BINDING")\n\
        \n\
        # Add workload file if specified\n\
        if [ -n "$YCSB_WORKLOAD" ]; then\n\
            CMD_ARGS+=("-P" "$YCSB_WORKLOAD")\n\
        fi\n\
        \n\
        # Add recordcount if specified (validate it is numeric)\n\
        if [ -n "$YCSB_RECORDCOUNT" ]; then\n\
            if ! echo "$YCSB_RECORDCOUNT" | grep -qE "^[0-9]+$"; then\n\
                echo "[ERROR] YCSB_RECORDCOUNT must be a non-negative integer"\n\
                exit 1\n\
            fi\n\
            CMD_ARGS+=("-p" "recordcount=$YCSB_RECORDCOUNT")\n\
        fi\n\
        \n\
        # Add operationcount if specified (validate it is numeric)\n\
        if [ -n "$YCSB_OPERATIONCOUNT" ]; then\n\
            if ! echo "$YCSB_OPERATIONCOUNT" | grep -qE "^[0-9]+$"; then\n\
                echo "[ERROR] YCSB_OPERATIONCOUNT must be a non-negative integer"\n\
                exit 1\n\
            fi\n\
            CMD_ARGS+=("-p" "operationcount=$YCSB_OPERATIONCOUNT")\n\
        fi\n\
        \n\
        # Add threads if specified (validate it is numeric)\n\
        if [ -n "$YCSB_THREADS" ]; then\n\
            if ! echo "$YCSB_THREADS" | grep -qE "^[0-9]+$"; then\n\
                echo "[ERROR] YCSB_THREADS must be a non-negative integer"\n\
                exit 1\n\
            fi\n\
            CMD_ARGS+=("-threads" "$YCSB_THREADS")\n\
        fi\n\
        \n\
        # Add target if specified (validate it is numeric)\n\
        if [ -n "$YCSB_TARGET" ]; then\n\
            if ! echo "$YCSB_TARGET" | grep -qE "^[0-9]+$"; then\n\
                echo "[ERROR] YCSB_TARGET must be a non-negative integer"\n\
                exit 1\n\
            fi\n\
            CMD_ARGS+=("-target" "$YCSB_TARGET")\n\
        fi\n\
        \n\
        # Log and execute the command\n\
        echo "Running: /ycsb/bin/ycsb.sh ${CMD_ARGS[*]} $YCSB_OPTS"\n\
        exec /ycsb/bin/ycsb.sh "${CMD_ARGS[@]}" $YCSB_OPTS\n\
    else\n\
        echo "YCSB Docker Container"\n\
        echo ""\n\
        echo "Usage: docker run [docker-options] <image> <ycsb-command> <ycsb-options>"\n\
        echo ""\n\
        echo "YCSB Commands:"\n\
        echo "  load    - Load data into the database"\n\
        echo "  run     - Run the benchmark"\n\
        echo "  shell   - Interactive YCSB shell"\n\
        echo ""\n\
        echo "Environment Variables:"\n\
        echo "  YCSB_COMMAND        - YCSB command (load, run, shell)"\n\
        echo "  YCSB_BINDING        - Database binding (e.g., basic, redis, cassandra-cql)"\n\
        echo "  YCSB_WORKLOAD       - Workload file path (e.g., workloads/workloada)"\n\
        echo "  YCSB_RECORDCOUNT    - Number of records to load/use"\n\
        echo "  YCSB_OPERATIONCOUNT - Number of operations to perform"\n\
        echo "  YCSB_THREADS        - Number of client threads"\n\
        echo "  YCSB_TARGET         - Target operations per second"\n\
        echo "  YCSB_OPTS           - Additional YCSB options"\n\
        echo ""\n\
        echo "Example (command line):"\n\
        echo "  docker run <image> load basic -P workloads/workloada"\n\
        echo "  docker run <image> run basic -P workloads/workloada"\n\
        echo ""\n\
        echo "Example (environment variables):"\n\
        echo "  docker run -e YCSB_COMMAND=load -e YCSB_BINDING=basic -e YCSB_WORKLOAD=workloads/workloada <image>"\n\
        echo "  docker run -e YCSB_COMMAND=run -e YCSB_BINDING=redis -e YCSB_WORKLOAD=workloads/workloada -e YCSB_RECORDCOUNT=10000 <image>"\n\
        echo ""\n\
        exit 0\n\
    fi\n\
fi\n\
\n\
# Execute ycsb.sh with all passed arguments\n\
exec /ycsb/bin/ycsb.sh "$@"' > /entrypoint.sh && \
    chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
CMD []
