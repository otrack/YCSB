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
FROM maven:3.9-eclipse-temurin-17 AS builder

# Update CA certificates and install git, golang, and build tools for swiftpaxos support
RUN apt-get update && apt-get install -y ca-certificates git golang make build-essential && update-ca-certificates

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
FROM eclipse-temurin:17-jre

# Set working directory
WORKDIR /ycsb

# Copy the distribution from builder
COPY --from=builder /ycsb-dist /ycsb

# Make scripts executable
RUN chmod +x /ycsb/bin/ycsb.sh && \
    if [ -f /ycsb/bin/ycsb ]; then chmod +x /ycsb/bin/ycsb; fi

# Set environment variables
ENV YCSB_HOME=/ycsb

# Create entrypoint script
RUN echo '#!/bin/bash\n\
set -e\n\
\n\
# If no arguments provided, show help\n\
if [ $# -eq 0 ]; then\n\
    echo "YCSB Docker Container"\n\
    echo ""\n\
    echo "Usage: docker run [docker-options] <image> <ycsb-command> <ycsb-options>"\n\
    echo ""\n\
    echo "YCSB Commands:"\n\
    echo "  load    - Load data into the database"\n\
    echo "  run     - Run the benchmark"\n\
    echo "  shell   - Interactive YCSB shell"\n\
    echo ""\n\
    echo "Example:"\n\
    echo "  docker run <image> load basic -P workloads/workloada"\n\
    echo "  docker run <image> run basic -P workloads/workloada"\n\
    echo ""\n\
    exit 0\n\
fi\n\
\n\
# Execute ycsb.sh with all passed arguments\n\
exec /ycsb/bin/ycsb.sh "$@"' > /entrypoint.sh && \
    chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
CMD []
