# YCSB Docker Image

This directory contains the Docker setup for running YCSB in containers.

## Quick Start Example

Here's a complete example to get started with YCSB in Docker:

```bash
# 1. Build a Docker image with the basic binding (no external database needed)
bin/image.sh

# 2. Test with the basic in-memory database
docker run ycsb:latest load basic -P workloads/workloada
docker run ycsb:latest run basic -P workloads/workloada

# 3. Build an image with specific bindings
bin/image.sh cassandra redis mongodb

# 4. Use the image with a real database (example with Redis)
docker network create ycsb-net
docker run -d --name redis --network=ycsb-net redis:latest
docker run --network=ycsb-net ycsb:latest load redis -P workloads/workloada -p redis.host=redis
docker run --network=ycsb-net ycsb:latest run redis -P workloads/workloada -p redis.host=redis
```

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

Once you've built an image, you can run YCSB commands in the container.

### Using Command Line Arguments

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

### Using Environment Variables

YCSB Docker images support environment variables for convenient configuration, especially useful with Docker Compose or Kubernetes:

| Environment Variable | Description | Example |
|---------------------|-------------|---------|
| `YCSB_COMMAND` | YCSB command (`load`, `run`, `shell`) | `load` |
| `YCSB_BINDING` | Database binding | `basic`, `redis`, `cassandra-cql` |
| `YCSB_WORKLOAD` | Path to workload file | `workloads/workloada` |
| `YCSB_RECORDCOUNT` | Number of records | `10000` |
| `YCSB_OPERATIONCOUNT` | Number of operations | `100000` |
| `YCSB_THREADS` | Number of client threads | `4` |
| `YCSB_TARGET` | Target operations per second | `1000` |
| `YCSB_OPTS` | Additional YCSB options (space-separated) | `-p redis.host=localhost` |

**Note:** `YCSB_OPTS` is split by whitespace, so option values containing spaces are not supported directly. Use individual `-p key=value` pairs without spaces in values.

```bash
# Using environment variables
docker run \
  -e YCSB_COMMAND=load \
  -e YCSB_BINDING=basic \
  -e YCSB_WORKLOAD=workloads/workloada \
  ycsb:latest

# With custom record count and threads
docker run \
  -e YCSB_COMMAND=run \
  -e YCSB_BINDING=basic \
  -e YCSB_WORKLOAD=workloads/workloada \
  -e YCSB_RECORDCOUNT=50000 \
  -e YCSB_OPERATIONCOUNT=100000 \
  -e YCSB_THREADS=8 \
  ycsb:latest

# With additional options for database connection
docker run --network=ycsb-net \
  -e YCSB_COMMAND=load \
  -e YCSB_BINDING=redis \
  -e YCSB_WORKLOAD=workloads/workloada \
  -e YCSB_OPTS="-p redis.host=redis -p redis.port=6379" \
  ycsb:latest
```

### Docker Compose Example

Environment variables make it easy to use YCSB with Docker Compose:

```yaml
version: '3.8'
services:
  redis:
    image: redis:latest
    networks:
      - ycsb-net

  ycsb-load:
    image: ycsb:latest
    depends_on:
      - redis
    environment:
      - YCSB_COMMAND=load
      - YCSB_BINDING=redis
      - YCSB_WORKLOAD=workloads/workloada
      - YCSB_RECORDCOUNT=10000
      - YCSB_OPTS=-p redis.host=redis
    networks:
      - ycsb-net

  ycsb-run:
    image: ycsb:latest
    depends_on:
      - ycsb-load
    environment:
      - YCSB_COMMAND=run
      - YCSB_BINDING=redis
      - YCSB_WORKLOAD=workloads/workloada
      - YCSB_OPERATIONCOUNT=100000
      - YCSB_THREADS=4
      - YCSB_OPTS=-p redis.host=redis
    networks:
      - ycsb-net

networks:
  ycsb-net:
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
- `swiftpaxos` - SwiftPaxos distributed consensus
- And many more...

## Special Bindings

### SwiftPaxos Binding

The SwiftPaxos binding requires building the swiftpaxos-client dependency from source. When you include `swiftpaxos` in your bindings list, the Docker build will automatically:

1. Clone the swiftpaxos repository (container branch)
2. Build the client library using the Makefile
3. Install the JAR to the local Maven repository
4. Build the YCSB swiftpaxos binding

```bash
# Build image with SwiftPaxos binding
bin/image.sh swiftpaxos

# Run with SwiftPaxos
docker run --network=host ycsb:latest load swiftpaxos \
  -P workloads/workloada \
  -p swiftpaxos.hosts=host1:port1,host2:port2
```

Note: Building with SwiftPaxos requires golang and takes longer due to the additional build steps.

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
