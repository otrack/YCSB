# YCSB-T: Closed Economy Workload

## Overview

The Closed Economy Workload implements the YCSB-T benchmark as described in the paper "YCSB+T: Benchmarking Web-scale Transactional Databases" by Akon Dey, Alan Fekete, Raghunath Nambiar, and Uwe Rohm.

This workload models a closed economy system where the total amount of "money" in the system remains constant. It is designed to test the transactional capabilities of databases, particularly their ability to maintain consistency across multi-object transactions.

## Use Case

The Closed Economy Workload simulates banking applications where:
- Multiple accounts hold monetary values
- Transactions transfer money between accounts
- The total amount of money in the system must remain constant
- Each transaction reads two accounts, decrements one, and increments the other

This workload is ideal for benchmarking:
- ACID transaction support
- Distributed consensus mechanisms
- Transaction isolation levels
- Consistency validation

## Configuration

The workload is configured through properties files or command-line parameters.

### Core Properties

| Property | Description | Default |
|----------|-------------|---------|
| `totalcash` | Total amount of money in the economy | 1000000 |
| `recordcount` | Number of records (accounts) | Required |
| `operationcount` | Number of operations to perform | Required |
| `readproportion` | Proportion of read-only transactions | 0.95 |
| `updateproportion` | Proportion of update transactions | 0.05 |
| `insertproportion` | Proportion of insert transactions | 0.0 |
| `scanproportion` | Proportion of scan transactions | 0.0 |
| `readmodifywriteproportion` | Proportion of transfer transactions | 0.0 |
| `requestdistribution` | Distribution for selecting records | uniform |
| `validatebyquery` | Use query-based validation | false |

### Request Distribution Options

- `uniform`: All records have equal probability
- `zipfian`: Skewed distribution favoring popular records
- `hotspot`: Some records are "hot" (frequently accessed)
- `exponential`: Exponential decay distribution
- `latest`: Favor recently inserted records

## Example Workload Files

### workloadce
Pure transfer workload (100% read-modify-write transactions):
```properties
recordcount=1000
operationcount=1000
workload=site.ycsb.workloads.ClosedEconomyWorkload

totalcash=1000000

readproportion=0
updateproportion=0
insertproportion=0
scanproportion=0
readmodifywriteproportion=1.0

requestdistribution=uniform
fieldcount=1
validatebyquery=false
```

### workloadce-mixed
Mixed workload (50% reads, 50% transfers):
```properties
recordcount=1000
operationcount=1000
workload=site.ycsb.workloads.ClosedEconomyWorkload

totalcash=1000000

readproportion=0.5
updateproportion=0
insertproportion=0
scanproportion=0
readmodifywriteproportion=0.5

requestdistribution=zipfian
fieldcount=1
validatebyquery=false
```

## Running the Workload

### Loading Data
```bash
./bin/ycsb load <database> -P workloads/workloadce -p recordcount=1000
```

### Running Transactions
```bash
./bin/ycsb run <database> -P workloads/workloadce -p operationcount=10000
```

### With Validation
```bash
./bin/ycsb run <database> -P workloads/workloadce -p operationcount=10000 -p validatebyquery=true
```

## Validation

The workload includes built-in validation to ensure consistency:

1. **Initial State**: Each record is initialized with `totalcash / recordcount` value
2. **During Execution**: Transactions transfer value from one record to another
3. **Final Validation**: The sum of all values should equal `totalcash`

If validation fails, it indicates:
- Lost updates
- Uncommitted reads (dirty reads)
- Non-repeatable reads
- Phantom reads
- Other consistency violations

### Validation Methods

Two validation methods are supported:

1. **By Query** (`validatebyquery=true`): Uses database-specific aggregation queries
2. **By Read Operations** (`validatebyquery=false`): Reads each record individually

## Transaction Support

Database clients must implement the following transaction methods:

```java
public void start() throws DBException {
    // Begin a transaction
}

public void commit() throws DBException {
    // Commit the current transaction
}

public void abort() throws DBException {
    // Abort the current transaction
}

public long validate() throws DBException {
    // Return the sum of all values in the database
    // Return -1 if validation is not supported
}
```

## Metrics

The workload reports the following metrics:

- **TX-READ**: Read transaction latency
- **TX-UPDATE**: Update transaction latency
- **TX-INSERT**: Insert transaction latency
- **TX-SCAN**: Scan transaction latency
- **TX-READMODIFYWRITE**: Transfer transaction latency

### Validation Metrics

- **VALIDATE STATUS**: SUCCESS or FAILED
- **TOTAL CASH**: Expected total
- **COUNTED CASH**: Actual total after operations
- **ACTUAL OPERATIONS**: Number of operations executed
- **ANOMALY SCORE**: Deviation per operation

## Implementation Notes

1. **Deadlock Prevention**: Keys are ordered before locking to prevent deadlocks
2. **Initial Distribution**: Cash is distributed equally across all records
3. **Thread Safety**: All operations are thread-safe
4. **Configurable**: Supports various request distributions and operation mixes

## References

- Dey, A., Fekete, A., Nambiar, R., & Rohm, U. (2014). YCSB+T: Benchmarking Web-scale Transactional Databases. In Proceedings of the IEEE International Conference on Data Engineering Workshops (ICDEW).
- Blog post: https://medium.com/@i.gorton/ycsb-with-transactions-measuring-the-costs-of-distributed-consensus-ea19ed53a086
- Reference implementation: https://github.com/kkty39/YCSB-Transactions

## License

Copyright (c) 2010-2016 Yahoo! Inc., 2017 YCSB contributors. All rights reserved.

Licensed under the Apache License, Version 2.0.
