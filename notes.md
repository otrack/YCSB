# 27.02 - copilot

The Cassandra driver is using an old CQL driver.
The goal of this work is to adjust the code to use a more recent version.

A first point that matters is that, since Datastax donated the Java client to ASF, it is now maintained by the community.
This is the repo with the code of the new driver: https://github.com/apache/cassandra-java-driver

To complete the work, you should these steps:

- Change pom.xml to use the new driver.
We will use version 4.19.2 because it is a few months old, and thus stable.

- Change the dependencies in cassandra/pom.xml as follows:

	<dependency>
      <groupId>org.apache.cassandra</groupId>
      <artifactId>java-driver-core</artifactId>
      <version>${cassandra.cql.version}</version>
    </dependency>
    <dependency>
      <groupId>org.apache.cassandra</groupId>
      <artifactId>java-driver-query-builder</artifactId>
      <version>${cassandra.cql.version}</version>
    </dependency>
    <dependency>
      <groupId>org.apache.cassandra</groupId>
      <artifactId>java-driver-mapper-runtime</artifactId>
      <version>${cassandra.cql.version}</version>
    </dependency>

- Adjust the CassandraCQLClient class to use the new driver.
Pay attention to *not* change the logic of this code.

- Regarding the LoadBalancingPolicy, we want that this policy 
1) uses any available local node (that is, a node with HostDistance.LOCAL), then
2) when such nodes are no more responsive, switches to the other nodes (i.e., nodes at distance HostDistance.REMOTE or HostDistance.IGNORED).

# 01.03 - copilot

The goal of this task is to enhance performance analysis in YCSB by enabling tracing in databases supporting it.
In detail, 
- Add a 'db.tracing' flag in DBWrapper whose default value is false.
- In Cassandra, replace trhe 'cassandra.tracing' flag with 'db.tracing'.
  If tracing is on, before returning Status.OK in an operation, output the tracing result.
- In JDBC, add a tracing flag to DBFlavor using 'db.tracing'.
  For CockroachDB, activate tracing when the flag is set.
  If tracing is on, before returning Status.OK in an operation, output the tracing result.
  Moreover, before closing the database, if tracing is on, output ggregated stats stored under 'crdb_internal.cluster_statement_statistics'.

# 01.03 - copilot

Fix tracing in the JDBC and Cassandra drivers.
Currently, the JDBC driver does not output anything when CockroachDB is used.
In Cassandra, when tracing is activated, some operations are failing.
Also, please move the tracing property to DBWrapper because it is something common to several databases (even if for the moment, two databases support it).

# 09.03 - copilot

Your goal is to create a new synthetic workload that generalizes the closed economy one.
The workload is implemented in the class site.ycsb.workloads.SwapWorkload.
It uses a new parameter called swapsize, hereafter abbreviated in S.
The workload relies on a new (read-modify-write) method called "swap" and implemented in site.ycsb.DB class.
The logic of this operation is as follows:
- Choose randomly S users (keys)
- Let user[0], ..., user[S-1] be such users
- For all the i in 0..S-1, replace user[i+1 mod S].field0 with user[i].field0
The implementation of swap in site.ycsb.DB is the default, interactive, one.
As with the transfer method, this method is called in DBWrapper and databases may propose a more efficient implementation.
You already provide two efficient implemnentations:
- one using the JDBC driver with the CockroachDB,
- and another for Cassandra, using the Accord protocol.
For both of these implementations, you should look at the code of transfer() which provide a good basis for implementiing efficiently the swap operation.
