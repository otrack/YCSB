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

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.querybuilder.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.MessageFormatter;
import site.ycsb.*;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Cassandra 2.x CQL client.
 *
 * See {@code cassandra2/README.md} for details.
 *
 * @author cmatser
 */
public class CassandraCQLClient extends DB {

  private static Logger logger = LoggerFactory.getLogger(CassandraCQLClient.class);

  private static Cluster cluster = null;
  private static Session session = null;

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

  private static ConsistencyLevel readConsistencyLevel = ConsistencyLevel.ONE;
  private static ConsistencyLevel writeConsistencyLevel = ConsistencyLevel.ONE;

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

  public static final String TRACING_PROPERTY = "cassandra.tracing";
  public static final String TRACING_PROPERTY_DEFAULT = "true";

  public static final String USE_SSL_CONNECTION = "cassandra.useSSL";
  private static final String DEFAULT_USE_SSL_CONNECTION = "false";

  /**
   * Count the number of times initialized to teardown on the last
   * {@link #cleanup()}.
   */
  private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);

  private static boolean debug = false;

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
    // cluster/session instance for all the threads.
    synchronized (INIT_COUNT) {

      // Check if the cluster has already been initialized
      if (cluster != null) {
        return;
      }

      try {

        debug =
            Boolean.parseBoolean(getProperties().getProperty("debug", "false"));
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

        readConsistencyLevel = ConsistencyLevel.valueOf(
            getProperties().getProperty(READ_CONSISTENCY_LEVEL_PROPERTY,
                READ_CONSISTENCY_LEVEL_PROPERTY_DEFAULT));

        writeConsistencyLevel = ConsistencyLevel.valueOf(
            getProperties().getProperty(WRITE_CONSISTENCY_LEVEL_PROPERTY,
                WRITE_CONSISTENCY_LEVEL_PROPERTY_DEFAULT));

        logger.info("Using read/write consistency level = "+ readConsistencyLevel + " / "+ writeConsistencyLevel);

        Boolean useSSL = Boolean.parseBoolean(getProperties().getProperty(USE_SSL_CONNECTION,
            DEFAULT_USE_SSL_CONNECTION));

        if ((username != null) && !username.isEmpty()) {
          Cluster.Builder clusterBuilder = Cluster.builder().withCredentials(username, password)
              .withPort(Integer.valueOf(port)).addContactPoints(hosts);
          if (useSSL) {
            clusterBuilder = clusterBuilder.withSSL();
          } 
          cluster = clusterBuilder.build();
        } else {
          cluster = Cluster.builder().withPort(Integer.valueOf(port))
              .addContactPoints(hosts).build();
        }

        String maxConnections = getProperties().getProperty(
            MAX_CONNECTIONS_PROPERTY);
        if (maxConnections != null) {
          cluster.getConfiguration().getPoolingOptions()
              .setMaxConnectionsPerHost(HostDistance.LOCAL,
              Integer.valueOf(maxConnections));
        }

        String coreConnections = getProperties().getProperty(
            CORE_CONNECTIONS_PROPERTY);
        if (coreConnections != null) {
          cluster.getConfiguration().getPoolingOptions()
              .setCoreConnectionsPerHost(HostDistance.LOCAL,
              Integer.valueOf(coreConnections));
        }

        String connectTimoutMillis = getProperties().getProperty(
            CONNECT_TIMEOUT_MILLIS_PROPERTY);
        if (connectTimoutMillis != null) {
          cluster.getConfiguration().getSocketOptions()
              .setConnectTimeoutMillis(Integer.valueOf(connectTimoutMillis));
        }

        String readTimoutMillis = getProperties().getProperty(
            READ_TIMEOUT_MILLIS_PROPERTY);
        if (readTimoutMillis != null) {
          cluster.getConfiguration().getSocketOptions()
              .setReadTimeoutMillis(Integer.valueOf(readTimoutMillis));
        }

        Metadata metadata = cluster.getMetadata();
        logger.info("Connected to cluster: {}\n",
            metadata.getClusterName());

        LoadBalancingPolicy loadBalancingPolicy =
            cluster.getConfiguration().getPolicies().getLoadBalancingPolicy();

        for (Host discoveredHost : metadata.getAllHosts()) {
          logger.info("Datacenter: {}; Host: {}; Rack: {}; Distance: {}\n",
              discoveredHost.getDatacenter(),
              discoveredHost.getAddress(),
              discoveredHost.getRack(),
              loadBalancingPolicy.distance(discoveredHost));
        }

        // access local but not remote
        cluster.getConfiguration().getPoolingOptions()
            .setCoreConnectionsPerHost(HostDistance.REMOTE, 0)
            .setConnectionsPerHost(HostDistance.LOCAL, 1, Runtime.getRuntime().availableProcessors());

        session = cluster.connect(keyspace);

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
        cluster.close();
        cluster = null;
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
   * Start a database transaction.
   * Uses Cassandra 5's transaction support via BEGIN TRANSACTION statement.
   */
  @Override
  public void start() throws DBException {
    try {
      session.execute("BEGIN TRANSACTION");
      if (debug) {
        logger.debug("Started transaction");
      }
    } catch (Exception e) {
      throw new DBException("Error starting transaction: " + e);
    }
  }

  /**
   * Commit the current database transaction.
   * Uses Cassandra 5's transaction support via COMMIT TRANSACTION statement.
   */
  @Override
  public void commit() throws DBException {
    try {
      session.execute("COMMIT TRANSACTION");
      if (debug) {
        logger.debug("Committed transaction");
      }
    } catch (Exception e) {
      throw new DBException("Error committing transaction: " + e);
    }
  }

  /**
   * Abort the current database transaction.
   * Uses Cassandra 5's transaction support via ABORT TRANSACTION statement.
   */
  @Override
  public void abort() throws DBException {
    try {
      session.execute("ABORT TRANSACTION");
      if (debug) {
        logger.debug("Aborted transaction");
      }
    } catch (Exception e) {
      throw new DBException("Error aborting transaction: " + e);
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
        Select.Builder selectBuilder;

        if (fields == null) {
          selectBuilder = QueryBuilder.select().all();
        } else {
          selectBuilder = QueryBuilder.select();
          for (String col : fields) {
            ((Select.Selection) selectBuilder).column(col);
          }
        }

        Select.Where readStmt = selectBuilder.from(table)
            .where(QueryBuilder.eq(YCSB_KEY, QueryBuilder.bindMarker()));

        readStmt.setConsistencyLevel(readConsistencyLevel);

        stmt = session.prepare(readStmt);

        if (trace) {
          stmt.enableTracing();
        }

        PreparedStatement prevStmt = (fields == null) ?
                                     readAllStmt.getAndSet(stmt) :
                                     readStmts.putIfAbsent(new HashSet(fields), stmt);
        if (prevStmt != null) {
          stmt = prevStmt;
        }

      }

      logger.debug(stmt.getQueryString());
      logger.debug("key = {}", key);

      BoundStatement boundStatement = stmt.bind(key);
      ResultSet rs = session.execute(boundStatement);

      if (rs.isExhausted()) {
        return Status.NOT_FOUND;
      }

      // Should be only 1 row
      Row row = rs.one();
      ColumnDefinitions cd = row.getColumnDefinitions();

      for (ColumnDefinitions.Definition def : cd) {
        ByteBuffer val = row.getBytesUnsafe(def.getName());
        if (val != null) {
          result.put(def.getName(), new ByteArrayByteIterator(val.array()));
        } else {
          result.put(def.getName(), null);
        }
      }

      return Status.OK;

    } catch (Exception e) {
      e.printStackTrace();
      logger.error(MessageFormatter.format("Error reading key: {}", key).getMessage(), e);
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
        Select.Builder selectBuilder;

        if (fields == null) {
          selectBuilder = QueryBuilder.select().all();
        } else {
          selectBuilder = QueryBuilder.select();
          for (String col : fields) {
            ((Select.Selection) selectBuilder).column(col);
          }
        }

        Select selectStmt = selectBuilder.from(table);

        // The statement builder is not setup right for tokens.
        // So, we need to build it manually.
        String initialStmt = selectStmt.toString();
        StringBuilder scanStmt = new StringBuilder();
        scanStmt.append(initialStmt.substring(0, initialStmt.length() - 1));
        scanStmt.append(" WHERE ");
        scanStmt.append(QueryBuilder.token(YCSB_KEY));
        scanStmt.append(" >= ");
        scanStmt.append("token(");
        scanStmt.append(QueryBuilder.bindMarker());
        scanStmt.append(")");
        scanStmt.append(" LIMIT ");
        scanStmt.append(QueryBuilder.bindMarker());

        stmt = session.prepare(scanStmt.toString());
        stmt.setConsistencyLevel(readConsistencyLevel);
        if (trace) {
          stmt.enableTracing();
        }

        PreparedStatement prevStmt = (fields == null) ?
                                     scanAllStmt.getAndSet(stmt) :
                                     scanStmts.putIfAbsent(new HashSet(fields), stmt);
        if (prevStmt != null) {
          stmt = prevStmt;
        }
      }

      logger.debug(stmt.getQueryString());
      logger.debug("startKey = {}, recordcount = {}", startkey, recordcount);

      ResultSet rs = session.execute(stmt.bind(startkey, Integer.valueOf(recordcount)));

      HashMap<String, ByteIterator> tuple;
      while (!rs.isExhausted()) {
        Row row = rs.one();
        tuple = new HashMap<String, ByteIterator>();

        ColumnDefinitions cd = row.getColumnDefinitions();

        for (ColumnDefinitions.Definition def : cd) {
          ByteBuffer val = row.getBytesUnsafe(def.getName());
          if (val != null) {
            tuple.put(def.getName(), new ByteArrayByteIterator(val.array()));
          } else {
            tuple.put(def.getName(), null);
          }
        }

        result.add(tuple);
      }

      return Status.OK;

    } catch (Exception e) {
      logger.error(
          MessageFormatter.format("Error scanning with startkey: {}", startkey).getMessage(), e);
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
        Update updateStmt = QueryBuilder.update(table);

        // Add fields
        for (String field : fields) {
          updateStmt.with(QueryBuilder.set(field, QueryBuilder.bindMarker()));
        }

        // Add key
        updateStmt.where(QueryBuilder.eq(YCSB_KEY, QueryBuilder.bindMarker()));
        if (writeConsistencyLevel.equals(ConsistencyLevel.SERIAL)) {
          updateStmt.onlyIf(QueryBuilder.ne(FIELD0, "test")); // serializable updates require a "conditional if"
          updateStmt.setSerialConsistencyLevel(ConsistencyLevel.SERIAL);
        }
        updateStmt.setConsistencyLevel(writeConsistencyLevel);

        System.out.println(updateStmt);

        stmt = session.prepare(updateStmt);

        if (trace) {
          stmt.enableTracing();
        }

        PreparedStatement prevStmt = updateStmts.putIfAbsent(new HashSet(fields), stmt);
        if (prevStmt != null) {
          stmt = prevStmt;
        }

      }

      if (logger.isDebugEnabled()) {
        logger.debug(stmt.getQueryString());
        logger.debug("key = {}", key);
        for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
          logger.debug("{} = {}", entry.getKey(), entry.getValue());
        }
      }

      // Add fields
      ColumnDefinitions vars = stmt.getVariables();
      BoundStatement boundStmt = stmt.bind();
      for (int i = 0; i < vars.size() - 1; i++) {
        boundStmt.setString(i, values.get(vars.getName(i)).toString());
      }

      // Add key
      boundStmt.setString(vars.size() - 1, key);

      session.execute(boundStmt);

      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
      logger.error(MessageFormatter.format("Error updating key: {}", key).getMessage(), e);
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
        Insert insertStmt = QueryBuilder.insertInto(table);

        // Add key
        insertStmt.value(YCSB_KEY, QueryBuilder.bindMarker());

        // Add fields
        for (String field : fields) {
          insertStmt.value(field, QueryBuilder.bindMarker());
        }

        if (writeConsistencyLevel.equals(ConsistencyLevel.SERIAL)) {
          insertStmt.ifNotExists();
          insertStmt.setSerialConsistencyLevel(ConsistencyLevel.SERIAL);
        }
        insertStmt.setConsistencyLevel(writeConsistencyLevel);

        System.out.println(insertStmt);

        stmt = session.prepare(insertStmt);

        if (trace) {
          stmt.enableTracing();
        }

        PreparedStatement prevStmt = insertStmts.putIfAbsent(new HashSet(fields), stmt);
        if (prevStmt != null) {
          stmt = prevStmt;
        }
      }

      if (logger.isDebugEnabled()) {
        logger.debug(stmt.getQueryString());
        logger.debug("key = {}", key);
        for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
          logger.debug("{} = {}", entry.getKey(), entry.getValue());
        }
      }

      // Add key
      BoundStatement boundStmt = stmt.bind().setString(0, key);

      // Add fields
      ColumnDefinitions vars = stmt.getVariables();
      for (int i = 1; i < vars.size(); i++) {
        boundStmt.setString(i, values.get(vars.getName(i)).toString());
      }

      session.execute(boundStmt);

      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
      logger.error(MessageFormatter.format("Error inserting key: {}", key).getMessage(), e);
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
        stmt = session.prepare(QueryBuilder.delete().from(table)
                               .where(QueryBuilder.eq(YCSB_KEY, QueryBuilder.bindMarker())));
        stmt.setConsistencyLevel(writeConsistencyLevel);

        if (trace) {
          stmt.enableTracing();
        }

        PreparedStatement prevStmt = deleteStmt.getAndSet(stmt);
        if (prevStmt != null) {
          stmt = prevStmt;
        }
      }

      logger.debug(stmt.getQueryString());
      logger.debug("key = {}", key);

      session.execute(stmt.bind(key));

      return Status.OK;
    } catch (Exception e) {
      logger.error(MessageFormatter.format("Error deleting key: {}", key).getMessage(), e);
    }

    return Status.ERROR;
  }

  /**
   * Transfer operation using a proper two-phase transaction.
   * This method attempts to use Cassandra transactions if supported (Cassandra 5+),
   * and falls back to using BATCH for atomicity on older versions.
   */
  @Override
  public Status transfer(String table, String key1, String key2, String field) {
    try {
      // Try to use transaction support if available (Cassandra 5+)
      try {
        session.execute("BEGIN TRANSACTION");
      } catch (Exception e) {
        // If transactions are not supported, fall back to BATCH
        return transferWithBatch(table, key1, key2, field);
      }
      
      // Read both account balances
      PreparedStatement readStmt = readAllStmt.get();
      if (readStmt == null) {
        Select.Where selectStmt = QueryBuilder.select().all().from(table)
            .where(QueryBuilder.eq(YCSB_KEY, QueryBuilder.bindMarker()));
        selectStmt.setConsistencyLevel(readConsistencyLevel);
        readStmt = session.prepare(selectStmt);
        readAllStmt.set(readStmt);
      }
      
      // Read first account
      ResultSet rs1 = session.execute(readStmt.bind(key1));
      if (rs1.isExhausted()) {
        session.execute("ABORT TRANSACTION");
        return Status.NOT_FOUND;
      }
      Row row1 = rs1.one();
      ByteBuffer val1 = row1.getBytesUnsafe(field);
      if (val1 == null) {
        session.execute("ABORT TRANSACTION");
        return Status.NOT_FOUND;
      }
      long balance1 = Long.parseLong(new String(val1.array()));
      
      // Read second account
      ResultSet rs2 = session.execute(readStmt.bind(key2));
      if (rs2.isExhausted()) {
        session.execute("ABORT TRANSACTION");
        return Status.NOT_FOUND;
      }
      Row row2 = rs2.one();
      ByteBuffer val2 = row2.getBytesUnsafe(field);
      if (val2 == null) {
        session.execute("ABORT TRANSACTION");
        return Status.NOT_FOUND;
      }
      long balance2 = Long.parseLong(new String(val2.array()));
      
      // Transfer 1 unit
      balance1--;
      balance2++;
      
      // Prepare update statement
      Set<String> fields = new HashSet<>();
      fields.add(field);
      PreparedStatement updateStmt = updateStmts.get(fields);
      if (updateStmt == null) {
        Update update = QueryBuilder.update(table);
        update.with(QueryBuilder.set(field, QueryBuilder.bindMarker()));
        update.where(QueryBuilder.eq(YCSB_KEY, QueryBuilder.bindMarker()));
        update.setConsistencyLevel(writeConsistencyLevel);
        updateStmt = session.prepare(update);
        updateStmts.putIfAbsent(new HashSet<>(fields), updateStmt);
      }
      
      // Update both accounts
      session.execute(updateStmt.bind(Long.toString(balance1), key1));
      session.execute(updateStmt.bind(Long.toString(balance2), key2));
      
      // Commit transaction
      session.execute("COMMIT TRANSACTION");
      
      return Status.OK;
      
    } catch (Exception e) {
      logger.error("Error in transfer operation", e);
      try {
        session.execute("ABORT TRANSACTION");
      } catch (Exception abortEx) {
        // Ignore abort errors
      }
      return Status.ERROR;
    }
  }

  /**
   * Transfer implementation using BATCH for Cassandra versions without transaction support.
   */
  private Status transferWithBatch(String table, String key1, String key2, String field) {
    try {
      // Read both account balances (outside the batch)
      PreparedStatement readStmt = readAllStmt.get();
      if (readStmt == null) {
        Select.Where selectStmt = QueryBuilder.select().all().from(table)
            .where(QueryBuilder.eq(YCSB_KEY, QueryBuilder.bindMarker()));
        selectStmt.setConsistencyLevel(readConsistencyLevel);
        readStmt = session.prepare(selectStmt);
        readAllStmt.set(readStmt);
      }
      
      // Read first account
      ResultSet rs1 = session.execute(readStmt.bind(key1));
      if (rs1.isExhausted()) {
        return Status.NOT_FOUND;
      }
      Row row1 = rs1.one();
      ByteBuffer val1 = row1.getBytesUnsafe(field);
      if (val1 == null) {
        return Status.NOT_FOUND;
      }
      long balance1 = Long.parseLong(new String(val1.array()));
      
      // Read second account
      ResultSet rs2 = session.execute(readStmt.bind(key2));
      if (rs2.isExhausted()) {
        return Status.NOT_FOUND;
      }
      Row row2 = rs2.one();
      ByteBuffer val2 = row2.getBytesUnsafe(field);
      if (val2 == null) {
        return Status.NOT_FOUND;
      }
      long balance2 = Long.parseLong(new String(val2.array()));
      
      // Transfer 1 unit
      balance1--;
      balance2++;
      
      // Create a batch statement with both updates
      Batch batch = QueryBuilder.batch();
      
      Update update1 = QueryBuilder.update(table);
      update1.with(QueryBuilder.set(field, Long.toString(balance1)));
      update1.where(QueryBuilder.eq(YCSB_KEY, key1));
      
      Update update2 = QueryBuilder.update(table);
      update2.with(QueryBuilder.set(field, Long.toString(balance2)));
      update2.where(QueryBuilder.eq(YCSB_KEY, key2));
      
      batch.add(update1);
      batch.add(update2);
      batch.setConsistencyLevel(writeConsistencyLevel);
      
      // Execute the batch atomically
      session.execute(batch);
      
      return Status.OK;
      
    } catch (Exception e) {
      logger.error("Error in batch transfer operation", e);
      return Status.ERROR;
    }
  }

}
