/**
 * Copyright (c) 2013-2015 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License. See accompanying LICENSE file.
 *
 * Submitted by Chrisjan Matser on 10/11/2010.
 */
package site.ycsb.db;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.MessageFormatter;
import site.ycsb.*;

import javax.net.ssl.SSLContext;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Cassandra 4.x CQL client.
 *
 * See {@code cassandra2/README.md} for details.
 *
 * @author cmatser
 */
public class CassandraCQLClient extends DB {

  private static Logger logger = LoggerFactory.getLogger(CassandraCQLClient.class);

  private static CqlSession session = null;

  private static ConcurrentMap<Set<String>, PreparedStatement> readStmts =
      new ConcurrentHashMap<Set<String>, PreparedStatement>();
  private static ConcurrentMap<Set<String>, PreparedStatement> scanStmts =
      new ConcurrentHashMap<Set<String>, PreparedStatement>();
  private static ConcurrentMap<Set<String>, PreparedStatement> insertStmts =
      new ConcurrentHashMap<Set<String>, PreparedStatement>();
  private static ConcurrentMap<Set<String>, PreparedStatement> updateStmts =
      new ConcurrentHashMap<Set<String>, PreparedStatement>();
  private static AtomicReference<PreparedStatement> readAllStmt =
      new AtomicReference<PreparedStatement>();
  private static AtomicReference<PreparedStatement> scanAllStmt =
      new AtomicReference<PreparedStatement>();
  private static AtomicReference<PreparedStatement> deleteStmt =
      new AtomicReference<PreparedStatement>();

  private static DefaultConsistencyLevel readConsistencyLevel = DefaultConsistencyLevel.ONE;
  private static DefaultConsistencyLevel writeConsistencyLevel = DefaultConsistencyLevel.ONE;

  public static final String YCSB_KEY = "y_id";

  public static final String FIELD0 = "field0";

  public static final String KEYSPACE_PROPERTY = "cassandra.keyspace";
  public static final String KEYSPACE_PROPERTY_DEFAULT = "ycsb";
  public static final String USERNAME_PROPERTY = "cassandra.username";
  public static final String PASSWORD_PROPERTY = "cassandra.password";

  public static final String HOSTS_PROPERTY = "hosts";
  public static final String PORT_PROPERTY = "port";
  public static final String PORT_PROPERTY_DEFAULT = "9042";

  public static final String READ_CONSISTENCY_LEVEL_PROPERTY =
      "cassandra.readconsistencylevel";
  public static final String READ_CONSISTENCY_LEVEL_PROPERTY_DEFAULT = readConsistencyLevel.name();
  public static final String WRITE_CONSISTENCY_LEVEL_PROPERTY =
      "cassandra.writeconsistencylevel";
  public static final String WRITE_CONSISTENCY_LEVEL_PROPERTY_DEFAULT = writeConsistencyLevel.name();

  public static final String MAX_CONNECTIONS_PROPERTY =
      "cassandra.maxconnections";
  public static final String CORE_CONNECTIONS_PROPERTY =
      "cassandra.coreconnections";
  public static final String CONNECT_TIMEOUT_MILLIS_PROPERTY =
      "cassandra.connecttimeoutmillis";
  public static final String READ_TIMEOUT_MILLIS_PROPERTY =
      "cassandra.readtimeoutmillis";
  public static final Duration DEFAULLT_TIEOUT_MILLIS_PROPERTY =
      Duration.ofSeconds(10);

  public static final String TRACING_PROPERTY = "db.tracing";
  public static final String TRACING_PROPERTY_DEFAULT = "false";

  public static final String USE_SSL_CONNECTION = "cassandra.useSSL";
  private static final String DEFAULT_USE_SSL_CONNECTION = "false";

  /**
   * Count the number of times initialized to teardown on the last
   * {@link #cleanup()}.
   */
  private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);

  private static boolean trace = false;

  /**
   * Initialize any state for this DB. Called once per DB instance; there is one
   * DB instance per client thread.
   */
  @Override
  public void init() throws DBException {

    // Keep track of number of calls to init (for later cleanup)
    INIT_COUNT.incrementAndGet();

    // Synchronized so that we only have a single
    // session instance for all the threads.
    synchronized (INIT_COUNT) {

      // Check if the session has already been initialized
      if (session != null) {
        return;
      }

      try {

        trace = Boolean.valueOf(getProperties().getProperty(TRACING_PROPERTY, TRACING_PROPERTY_DEFAULT));

        String host = getProperties().getProperty(HOSTS_PROPERTY);
        if (host == null) {
          throw new DBException(String.format(
              "Required property \"%s\" missing for CassandraCQLClient",
              HOSTS_PROPERTY));
        }
        String[] hosts = host.split(",");
        String port = getProperties().getProperty(PORT_PROPERTY, PORT_PROPERTY_DEFAULT);

        String username = getProperties().getProperty(USERNAME_PROPERTY);
        String password = getProperties().getProperty(PASSWORD_PROPERTY);

        String keyspace = getProperties().getProperty(KEYSPACE_PROPERTY,
            KEYSPACE_PROPERTY_DEFAULT);

        readConsistencyLevel = DefaultConsistencyLevel.valueOf(
            getProperties().getProperty(READ_CONSISTENCY_LEVEL_PROPERTY,
                READ_CONSISTENCY_LEVEL_PROPERTY_DEFAULT));

        writeConsistencyLevel = DefaultConsistencyLevel.valueOf(
            getProperties().getProperty(WRITE_CONSISTENCY_LEVEL_PROPERTY,
                WRITE_CONSISTENCY_LEVEL_PROPERTY_DEFAULT));

        logger.info("Using read/write consistency level = " + readConsistencyLevel + " / " + writeConsistencyLevel);

        boolean useSSL = Boolean.parseBoolean(getProperties().getProperty(USE_SSL_CONNECTION,
            DEFAULT_USE_SSL_CONNECTION));

        // Build programmatic driver configuration
        // Store contact points in config so that LocalFirstLoadBalancingPolicy
        // can detect the local datacenter from them.
        List<String> contactPointStrings = new ArrayList<>();
        for (String h : hosts) {
          contactPointStrings.add(h + ":" + port);
        }
        ProgrammaticDriverConfigLoaderBuilder configBuilder = DriverConfigLoader.programmaticBuilder()
            .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, DEFAULLT_TIEOUT_MILLIS_PROPERTY)
            //
            .withClass(DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS, LocalFirstLoadBalancingPolicy.class)
            .withStringList(DefaultDriverOption.CONTACT_POINTS, contactPointStrings)
            // access local but not remote
            .withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE,
                Runtime.getRuntime().availableProcessors())
            .withInt(DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE,
                Runtime.getRuntime().availableProcessors());

        String maxConnections = getProperties().getProperty(MAX_CONNECTIONS_PROPERTY);
        if (maxConnections != null) {
          configBuilder.withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE,
              Integer.valueOf(maxConnections));
        }

        String coreConnections = getProperties().getProperty(CORE_CONNECTIONS_PROPERTY);
        if (coreConnections != null) {
          configBuilder.withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE,
              Integer.valueOf(coreConnections));
        }

        String connectTimeoutMillis = getProperties().getProperty(CONNECT_TIMEOUT_MILLIS_PROPERTY);
        if (connectTimeoutMillis != null) {
          configBuilder.withDuration(DefaultDriverOption.CONNECTION_CONNECT_TIMEOUT,
              Duration.ofMillis(Long.valueOf(connectTimeoutMillis)));
        }

        String readTimeoutMillis = getProperties().getProperty(READ_TIMEOUT_MILLIS_PROPERTY);
        if (readTimeoutMillis != null) {
          configBuilder.withDuration(DefaultDriverOption.REQUEST_TIMEOUT,
              Duration.ofMillis(Long.valueOf(readTimeoutMillis)));
        }

        CqlSessionBuilder builder = CqlSession.builder()
            .withConfigLoader(configBuilder.build())
            .withKeyspace(keyspace);

        for (String h : hosts) {
          builder.addContactPoint(new InetSocketAddress(h, Integer.valueOf(port)));
        }

        if (username != null && !username.isEmpty()) {
          builder.withAuthCredentials(username, password);
        }

        if (useSSL) {
          try {
            builder.withSslContext(SSLContext.getDefault());
          } catch (NoSuchAlgorithmException e) {
            throw new DBException(e);
          }
        }

        session = builder.build();

        Metadata metadata = session.getMetadata();
        logger.info("Connected to cluster: {}\n",
            metadata.getClusterName().orElse("unknown"));

        for (Node node : metadata.getNodes().values()) {
          logger.info("Datacenter: {}; Host: {}; Rack: {}; Distance: {}\n",
              node.getDatacenter(),
              node.getEndPoint().toString(),
              node.getRack(),
              node.getDistance()
          );
        }

        if (writeConsistencyLevel != DefaultConsistencyLevel.SERIAL
            || readConsistencyLevel != DefaultConsistencyLevel.SERIAL) {
          throw new IllegalArgumentException("Invalid consistency levels");
        }

      } catch (Exception e) {
        throw new DBException(e);
      }
    } // synchronized
  }

  /**
   * Cleanup any state for this DB. Called once per DB instance; there is one DB
   * instance per client thread.
   */
  @Override
  public void cleanup() throws DBException {
    synchronized (INIT_COUNT) {
      final int curInitCount = INIT_COUNT.decrementAndGet();
      if (curInitCount <= 0) {
        readStmts.clear();
        scanStmts.clear();
        insertStmts.clear();
        updateStmts.clear();
        readAllStmt.set(null);
        scanAllStmt.set(null);
        deleteStmt.set(null);
        session.close();
        session = null;
      }
      if (curInitCount < 0) {
        // This should never happen.
        throw new DBException(
            String.format("initCount is negative: %d", curInitCount));
      }
    }
  }

  /**
   * Read a record from the database. Each field/value pair from the result will
   * be stored in a HashMap.
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to read.
   * @param fields
   *          The list of fields to read, or null for all of them
   * @param result
   *          A HashMap of field/value pairs for the result
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status read(String table, String key, Set<String> fields,
      Map<String, ByteIterator> result) {
    try {
      PreparedStatement stmt = (fields == null) ? readAllStmt.get() : readStmts.get(fields);

      // Prepare statement on demand
      if (stmt == null) {
        Select select;

        if (fields == null) {
          select = QueryBuilder.selectFrom(table).all()
              .whereColumn(YCSB_KEY).isEqualTo(QueryBuilder.bindMarker());
        } else {
          select = QueryBuilder.selectFrom(table).columns(fields.toArray(new String[0]))
              .whereColumn(YCSB_KEY).isEqualTo(QueryBuilder.bindMarker());
        }

        stmt = session.prepare(
          select.build()
              .setSerialConsistencyLevel(DefaultConsistencyLevel.SERIAL)
              .setConsistencyLevel(DefaultConsistencyLevel.QUORUM)
        );

        PreparedStatement prevStmt = (fields == null) ?
                                     readAllStmt.getAndSet(stmt) :
                                     readStmts.putIfAbsent(new HashSet(fields), stmt);
        if (prevStmt != null) {
          stmt = prevStmt;
        }

      }

      logger.debug(stmt.getQuery());
      logger.debug("key = {}", key);

      BoundStatement boundStatement = stmt.bind(key);
      if (trace) {
        boundStatement = boundStatement.setTracing(true);
      }
      ResultSet rs = session.execute(boundStatement);

      // Should be only 1 row
      Row row = rs.one();
      if (row == null) {
        return Status.NOT_FOUND;
      }
      ColumnDefinitions cd = row.getColumnDefinitions();

      for (int i = 0; i < cd.size(); i++) {
        String name = cd.get(i).getName().asInternal();
        ByteBuffer val = row.getBytesUnsafe(i);
        if (val != null) {
          result.put(name, new ByteArrayByteIterator(val.array()));
        } else {
          result.put(name, null);
        }
      }

      if (trace) {
        outputTrace(rs);
      }
      return Status.OK;

    } catch (Exception e) {
      if (logger.isDebugEnabled()) {
        logger.debug(MessageFormatter.format("Error reading key: {}", key).getMessage(), e);
      }
      return Status.ERROR;
    }

  }

  /**
   * Perform a range scan for a set of records in the database. Each field/value
   * pair from the result will be stored in a HashMap.
   *
   * Cassandra CQL uses "token" method for range scan which doesn't always yield
   * intuitive results.
   *
   * @param table
   *          The name of the table
   * @param startkey
   *          The record key of the first record to read.
   * @param recordcount
   *          The number of records to read
   * @param fields
   *          The list of fields to read, or null for all of them
   * @param result
   *          A Vector of HashMaps, where each HashMap is a set field/value
   *          pairs for one record
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status scan(String table, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {

    try {
      PreparedStatement stmt = (fields == null) ? scanAllStmt.get() : scanStmts.get(fields);

      // Prepare statement on demand
      if (stmt == null) {
        // Build the token-based range scan CQL manually
        String cql;
        if (fields == null) {
          cql = "SELECT * FROM " + table;
        } else {
          cql = "SELECT " + String.join(", ", fields) + " FROM " + table;
        }
        cql += " WHERE token(" + YCSB_KEY + ") >= token(?) LIMIT ?";

        stmt = session.prepare(SimpleStatement.newInstance(cql)
            .setSerialConsistencyLevel(DefaultConsistencyLevel.SERIAL)
            .setConsistencyLevel(DefaultConsistencyLevel.QUORUM));

        PreparedStatement prevStmt = (fields == null) ?
                                     scanAllStmt.getAndSet(stmt) :
                                     scanStmts.putIfAbsent(new HashSet(fields), stmt);
        if (prevStmt != null) {
          stmt = prevStmt;
        }
      }

      logger.debug(stmt.getQuery());
      logger.debug("startKey = {}, recordcount = {}", startkey, recordcount);

      BoundStatementBuilder builder = stmt.boundStatementBuilder()
          .setString(0, startkey)
          .setInt(1, recordcount);
      if (trace) {
        builder.setTracing(true);
      }
      ResultSet rs = session.execute(builder.build());

      HashMap<String, ByteIterator> tuple;
      for (Row row : rs) {
        tuple = new HashMap<String, ByteIterator>();

        ColumnDefinitions cd = row.getColumnDefinitions();

        for (int i = 0; i < cd.size(); i++) {
          String name = cd.get(i).getName().asInternal();
          ByteBuffer val = row.getBytesUnsafe(i);
          if (val != null) {
            tuple.put(name, new ByteArrayByteIterator(val.array()));
          } else {
            tuple.put(name, null);
          }
        }

        result.add(tuple);
      }

      if (trace) {
        outputTrace(rs);
      }
      return Status.OK;

    } catch (Exception e) {
      if (logger.isDebugEnabled()) {
        logger.debug(MessageFormatter.format("Error scanning with startkey: {}", startkey).getMessage(), e);
      }
      return Status.ERROR;
    }

  }

  /**
   * Update a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key, overwriting any existing values with the same field name.
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to write.
   * @param values
   *          A HashMap of field/value pairs to update in the record
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {

    try {
      Set<String> fields = values.keySet();
      PreparedStatement stmt = updateStmts.get(fields);

      // Prepare statement on demand
      if (stmt == null) {
        // Build UPDATE statement manually to support conditional IF
        StringBuilder updateCQL = new StringBuilder("UPDATE ").append(table).append(" SET ");
        List<String> setterList = new ArrayList<>();
        for (String field : fields) {
          setterList.add(field + " = ?");
        }
        updateCQL.append(String.join(", ", setterList));
        updateCQL.append(" WHERE ").append(YCSB_KEY).append(" = ?");
        updateCQL.append(" IF ").append(FIELD0).append(" != 'test'");

        stmt = session.prepare(SimpleStatement.newInstance(updateCQL.toString())
            .setSerialConsistencyLevel(DefaultConsistencyLevel.SERIAL)
            .setConsistencyLevel(DefaultConsistencyLevel.QUORUM));

        PreparedStatement prevStmt = updateStmts.putIfAbsent(new HashSet(fields), stmt);
        if (prevStmt != null) {
          stmt = prevStmt;
        }

      }

      if (logger.isDebugEnabled()) {
        logger.debug(stmt.getQuery());
        logger.debug("key = {}", key);
        for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
          logger.debug("{} = {}", entry.getKey(), entry.getValue());
        }
      }

      // Add fields
      ColumnDefinitions vars = stmt.getVariableDefinitions();
      BoundStatementBuilder builder = stmt.boundStatementBuilder();
      for (int i = 0; i < vars.size() - 1; i++) {
        String colName = vars.get(i).getName().asInternal();
        builder.setString(i, values.get(colName).toString());
      }

      // Add key
      builder.setString(vars.size() - 1, key);
      if (trace) {
        builder.setTracing(true);
      }

      ResultSet updateRs = session.execute(builder.build());

      if (trace) {
        outputTrace(updateRs);
      }
      return Status.OK;
    } catch (Exception e) {
      if (logger.isDebugEnabled()) {
        logger.debug(MessageFormatter.format("Error updating key: {}", key).getMessage(), e);
      }
    }

    return Status.ERROR;
  }

  /**
   * Insert a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key.
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to insert.
   * @param values
   *          A HashMap of field/value pairs to insert in the record
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {

    try {
      Set<String> fields = values.keySet();
      PreparedStatement stmt = insertStmts.get(fields);

      // Prepare statement on demand
      if (stmt == null) {
        // Build INSERT statement manually to support IF NOT EXISTS
        StringBuilder insertCQL = new StringBuilder("INSERT INTO ").append(table).append(" (");
        List<String> cols = new ArrayList<>();
        List<String> markers = new ArrayList<>();
        cols.add(YCSB_KEY);
        markers.add("?");
        for (String field : fields) {
          cols.add(field);
          markers.add("?");
        }
        insertCQL.append(String.join(", ", cols))
            .append(") VALUES (")
            .append(String.join(", ", markers))
            .append(")")
            .append(" IF NOT EXISTS");

        stmt = session.prepare(SimpleStatement.newInstance(insertCQL.toString())
            .setSerialConsistencyLevel(DefaultConsistencyLevel.SERIAL)
            .setConsistencyLevel(DefaultConsistencyLevel.QUORUM));

        PreparedStatement prevStmt = insertStmts.putIfAbsent(new HashSet(fields), stmt);
        if (prevStmt != null) {
          stmt = prevStmt;
        }
      }

      if (logger.isDebugEnabled()) {
        logger.debug(stmt.getQuery());
        logger.debug("key = {}", key);
        for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
          logger.debug("{} = {}", entry.getKey(), entry.getValue());
        }
      }

      // Add key
      ColumnDefinitions vars = stmt.getVariableDefinitions();
      BoundStatementBuilder builder = stmt.boundStatementBuilder();
      builder.setString(0, key);

      // Add fields
      for (int i = 1; i < vars.size(); i++) {
        String colName = vars.get(i).getName().asInternal();
        builder.setString(i, values.get(colName).toString());
      }
      if (trace) {
        builder.setTracing(true);
      }

      ResultSet insertRs = session.execute(builder.build());

      if (trace) {
        outputTrace(insertRs);
      }
      return Status.OK;
    } catch (Exception e) {
      if (logger.isDebugEnabled()) {
        logger.debug(MessageFormatter.format("Error inserting key: {}", key).getMessage(), e);
      }
    }

    return Status.ERROR;
  }

  /**
   * Delete a record from the database.
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to delete.
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status delete(String table, String key) {

    try {
      PreparedStatement stmt = deleteStmt.get();

      // Prepare statement on demand
      if (stmt == null) {
        stmt = session.prepare(QueryBuilder.deleteFrom(table)
            .whereColumn(YCSB_KEY).isEqualTo(QueryBuilder.bindMarker())
            .build()
            .setSerialConsistencyLevel(DefaultConsistencyLevel.SERIAL)
            .setConsistencyLevel(DefaultConsistencyLevel.QUORUM));

        PreparedStatement prevStmt = deleteStmt.getAndSet(stmt);
        if (prevStmt != null) {
          stmt = prevStmt;
        }
      }

      logger.debug(stmt.getQuery());
      logger.debug("key = {}", key);

      BoundStatement bound = stmt.bind(key);
      if (trace) {
        bound = bound.setTracing(true);
      }
      ResultSet deleteRs = session.execute(bound);

      if (trace) {
        outputTrace(deleteRs);
      }
      return Status.OK;
    } catch (Exception e) {
      if (logger.isDebugEnabled()) {
        logger.debug(MessageFormatter.format("Error deleting key: {}", key).getMessage(), e);
      }
    }

    return Status.ERROR;
  }

  /**
   * Transfer operation using a proper two-phase transaction with hand-written CQL.
   * This implements Cassandra Accord transactions using LET statements to read values
   * within the transaction scope, then conditionally updates both accounts.
   * The entire operation is executed as a single CQL statement.
   * 
   * Note: This method returns Status.OK even if the conditional update doesn't execute
   * (e.g., when accounts don't exist). This is consistent with the transaction semantics
   * where the transaction succeeds but the conditional updates are skipped.
   * 
   * Based on: https://github.com/pmcfadin/awesome-accord/blob/main/examples/inventory/transaction.cql
   */
  @Override
  public Status transfer(String table, String key1, String key2, String field) {
    try {

      if (!isValidIdentifier(table) || !isValidIdentifier(field)) {
        logger.error("Invalid table or field name: table={}, field={}", table, field);
        return Status.ERROR;
      }

      StringBuilder cql = new StringBuilder();
      cql.append("BEGIN TRANSACTION\n");
      

      cql.append("  LET account1 = (SELECT ").append(field)
         .append(" FROM ").append(table)
         .append(" WHERE ").append(YCSB_KEY).append(" = '").append(key1).append("');\n");
      
      cql.append("  LET account2 = (SELECT ").append(field)
         .append(" FROM ").append(table)
         .append(" WHERE ").append(YCSB_KEY).append(" = '").append(key2).append("');\n");
      
      cql.append("  IF account1 IS NOT NULL AND account2 IS NOT NULL ")
         .append("AND account1.").append(field).append(" IS NOT NULL ")
         .append("AND account2.").append(field).append(" IS NOT NULL THEN\n");
      cql.append("    UPDATE ").append(table)
         .append(" SET ").append(field).append(" -= 1")
         .append(" WHERE ").append(YCSB_KEY).append(" = '").append(key1).append("';\n");
      
      cql.append("    UPDATE ").append(table)
         .append(" SET ").append(field).append(" += 1")
         .append(" WHERE ").append(YCSB_KEY).append(" = '").append(key2).append("';\n");
      cql.append("  END IF\n");
      
      cql.append("COMMIT TRANSACTION;");
      
      if (logger.isDebugEnabled()) {
        logger.debug("Executing transaction CQL: {}", cql.toString());
      }
      
      // Execute the hand-written transaction as a single statement
      SimpleStatement txnStmt = SimpleStatement.newInstance(cql.toString());
      txnStmt = txnStmt.setSerialConsistencyLevel(DefaultConsistencyLevel.SERIAL)
          .setConsistencyLevel(DefaultConsistencyLevel.QUORUM);

      if (trace) {
        txnStmt = txnStmt.setTracing(true);
      }

      ResultSet transferRs = session.execute(txnStmt);

      if (trace) {
        outputTrace(transferRs);
      }
      return Status.OK;
      
    } catch (Exception e) {
      if (logger.isDebugEnabled()) {
        logger.debug("Error in transfer operation", e);
      }
      return Status.ERROR;
    }
  }
  
  /**
   * Swap operation using Cassandra's Accord replication protocol.
   * Performs a cyclic rotation of {@code field} values among S users:
   * for each i in 0..S-1, {@code keys[(i+1) % S].field} is set to {@code keys[i].field}.
   *
   * <p>This is implemented as a single Accord transaction using {@code BEGIN TRANSACTION} /
   * {@code COMMIT TRANSACTION} CQL syntax, analogous to the {@link #transfer} method.
   *
   * @param table The name of the table
   * @param keys  The S record keys to swap among
   * @param field The field whose values are to be rotated
   * @return The result of the operation.
   */
  @Override
  public Status swap(String table, String[] keys, String field) {
    try {
      if (!isValidIdentifier(table) || !isValidIdentifier(field)) {
        logger.error("Invalid table or field name: table={}, field={}", table, field);
        return Status.ERROR;
      }
      for (String key : keys) {
        if (!isValidIdentifier(key)) {
          logger.error("Invalid key in swap operation: {}", key);
          return Status.ERROR;
        }
      }

      int s = keys.length;
      StringBuilder cql = new StringBuilder();
      cql.append("BEGIN TRANSACTION\n");

      // Declare a variable for each key's current value
      for (int i = 0; i < s; i++) {
        cql.append("  LET v").append(i).append(" = (SELECT ").append(field)
           .append(" FROM ").append(table)
           .append(" WHERE ").append(YCSB_KEY).append(" = '").append(keys[i]).append("');\n");
      }

      // Guard: all variables must be non-null
      cql.append("  IF ");
      for (int i = 0; i < s; i++) {
        if (i > 0) {
          cql.append(" AND ");
        }
        cql.append("v").append(i).append(" IS NOT NULL")
           .append(" AND v").append(i).append(".").append(field).append(" IS NOT NULL");
      }
      cql.append(" THEN\n");

      // Perform cyclic rotation: keys[(i+1)%s].field = keys[i].field
      for (int i = 0; i < s; i++) {
        cql.append("    UPDATE ").append(table)
           .append(" SET ").append(field).append(" = v").append(i).append(".").append(field)
           .append(" WHERE ").append(YCSB_KEY).append(" = '").append(keys[(i + 1) % s]).append("';\n");
      }
      cql.append("  END IF\n");

      cql.append("COMMIT TRANSACTION;");

      if (logger.isDebugEnabled()) {
        logger.debug("Executing swap transaction CQL: {}", cql.toString());
      }

      SimpleStatement txnStmt = SimpleStatement.newInstance(cql.toString());
      txnStmt = txnStmt.setSerialConsistencyLevel(DefaultConsistencyLevel.SERIAL)
          .setConsistencyLevel(DefaultConsistencyLevel.QUORUM);

      if (trace) {
        txnStmt = txnStmt.setTracing(true);
      }

      ResultSet swapRs = session.execute(txnStmt);

      if (trace) {
        outputTrace(swapRs);
      }
      return Status.OK;

    } catch (Exception e) {
      if (logger.isDebugEnabled()) {
        logger.debug("Error in swap operation", e);
      }
      return Status.ERROR;
    }
  }

  /**
   * Validates that an identifier (table or field name) contains only safe characters.
   * This prevents CQL injection through identifier names.
   */
  private boolean isValidIdentifier(String identifier) {
    if (identifier == null || identifier.isEmpty()) {
      return false;
    }
    // Allow alphanumeric characters, underscores, and hyphens
    return identifier.matches("^[a-zA-Z0-9_-]+$");
  }

  /**
   * Outputs the query trace from a ResultSet to stdout.
   */
  private void outputTrace(ResultSet rs) {
    ExecutionInfo ei = rs.getExecutionInfo();
    QueryTrace qt = ei.getQueryTrace();
    if (qt != null) {
      System.out.println("Trace ID: " + qt.getTracingId()
          + ", type: " + qt.getRequestType()
          + ", duration: " + qt.getDurationMicros() + "us");
      for (TraceEvent event : qt.getEvents()) {
        System.out.println("  [" + event.getTimestamp() + "] "
            + event.getActivity() + " @ " + event.getSource());
      }
    } else {
      System.out.println("Tracing enabled but no trace data available.");
    }
  }

}
