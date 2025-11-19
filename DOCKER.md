# YCSB Docker Image

This directory contains the Docker setup for running YCSB in containers.

## Building a Docker Image

Use the `bin/image.sh` script to build a Docker image with specific YCSB bindings:

```bash
# Build with one or more bindings
bin/image.sh cassandra swiftpaxos

# Build with custom image name and tag
bin/image.sh -n my-ycsb -t v1.0 redis mongodb

# Build with just core (basic binding)
bin/image.sh
```

### Available Options

- `-n, --name NAME` - Name for the Docker image (default: ycsb)
- `-t, --tag TAG` - Tag for the Docker image (default: latest)
- `-h, --help` - Display help message

## Running YCSB in Docker

Once you've built an image, you can run YCSB commands in the container:

```bash
# Run the load phase
docker run ycsb:latest load basic -P workloads/workloada

# Run the benchmark phase
docker run ycsb:latest run basic -P workloads/workloada

# Run with a specific binding (e.g., cassandra)
docker run --network=host ycsb:latest load cassandra-cql \
  -P workloads/workloada \
  -p hosts=localhost

# Run with custom parameters
docker run ycsb:latest run redis \
  -P workloads/workloada \
  -p redis.host=redis-server \
  -p redis.port=6379
```

## Networking

When running YCSB in Docker to connect to databases running on the host or in other containers:

### Connecting to Host Database

```bash
# Use host networking
docker run --network=host ycsb:latest load cassandra-cql -p hosts=localhost
```

### Connecting to Database in Docker Network

```bash
# Create a network
docker network create ycsb-net

# Run your database (example with Redis)
docker run -d --name redis --network=ycsb-net redis:latest

# Run YCSB
docker run --network=ycsb-net ycsb:latest load redis -p redis.host=redis
```

## Volume Mounting

You can mount custom workload files or configuration:

```bash
# Mount custom workload
docker run -v /path/to/workloads:/workloads ycsb:latest \
  load basic -P /workloads/my-workload.properties

# Mount custom configuration
docker run -v /path/to/conf:/ycsb/conf ycsb:latest \
  load cassandra-cql -P workloads/workloada
```

## Examples

### Cassandra Example

```bash
# Build image with Cassandra binding
bin/image.sh cassandra

# Start Cassandra container
docker network create ycsb-net
docker run -d --name cassandra --network=ycsb-net cassandra:latest

# Wait for Cassandra to start, then load data
docker run --network=ycsb-net ycsb:latest load cassandra-cql \
  -P workloads/workloada \
  -p hosts=cassandra

# Run benchmark
docker run --network=ycsb-net ycsb:latest run cassandra-cql \
  -P workloads/workloada \
  -p hosts=cassandra
```

### Redis Example

```bash
# Build image with Redis binding
bin/image.sh redis

# Start Redis container
docker network create ycsb-net
docker run -d --name redis --network=ycsb-net redis:latest

# Load data
docker run --network=ycsb-net ycsb:latest load redis \
  -P workloads/workloada \
  -p redis.host=redis

# Run benchmark
docker run --network=ycsb-net ycsb:latest run redis \
  -P workloads/workloada \
  -p redis.host=redis
```

## Available Bindings

See `bin/bindings.properties` for a complete list of available bindings. Common bindings include:

- `cassandra-cql` - Cassandra 2.1+ CQL
- `mongodb` - MongoDB
- `redis` - Redis
- `basic` - Basic in-memory database (for testing)
- `dynamodb` - Amazon DynamoDB
- `elasticsearch` - Elasticsearch
- `hbase1` - Apache HBase 1.x
- `jdbc` - JDBC-compliant databases
- And many more...

## Image Size Optimization

Each binding adds dependencies to the image. Build images with only the bindings you need to minimize image size:

```bash
# Minimal image (core only)
bin/image.sh

# Image with specific bindings only
bin/image.sh cassandra redis mongodb
```

## Troubleshooting

### Container shows help but doesn't run

Make sure you're passing the correct YCSB command format:

```bash
docker run ycsb:latest <command> <binding> [options]
```

### Connection refused errors

- Ensure databases are accessible from the container
- Use `--network=host` for host networking
- Or create a Docker network and connect both containers

### Binding not found

Ensure the binding was included when building the image:

```bash
# Rebuild with the required binding
bin/image.sh your-binding-name
```
