/**
 * Copyright (c) 2010 - 2016 Yahoo! Inc., 2016, 2019 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package site.ycsb.db;

import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.ByteIterator;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import site.ycsb.db.flavors.DBFlavor;

/**
 * A class that wraps a JDBC compliant database to allow it to be interfaced
 * with YCSB. This class extends {@link DB} and implements the database
 * interface used by YCSB client.
 *
 * <br>
 * Each client will have its own instance of this class. This client is not
 * thread safe.
 *
 * <br>
 * This interface expects a schema <key> <field1> <field2> <field3> ... All
 * attributes are of type TEXT. All accesses are through the primary key.
 * Therefore, only one index on the primary key is needed.
 */
public class JdbcDBClient extends DB {

  /** The class to use as the jdbc driver. */
  public static final String DRIVER_CLASS = "db.driver";

  /** The URL to connect to the database. */
  public static final String CONNECTION_URL = "db.url";

  /** The user name to use to connect to the database. */
  public static final String CONNECTION_USER = "db.user";

  /** The password to use for establishing the connection. */
  public static final String CONNECTION_PASSWD = "db.passwd";

  /** The batch size for batched inserts. Set to >0 to use batching */
  public static final String DB_BATCH_SIZE = "db.batchsize";

  /** The JDBC fetch size hinted to the driver. */
  public static final String JDBC_FETCH_SIZE = "jdbc.fetchsize";

  /** The JDBC connection auto-commit property for the driver. */
  public static final String JDBC_AUTO_COMMIT = "jdbc.autocommit";

  public static final String JDBC_BATCH_UPDATES = "jdbc.batchupdateapi";

  /** The name of the property for the number of fields in a record. */
  public static final String FIELD_COUNT_PROPERTY = "fieldcount";

  /** Default number of fields in a record. */
  public static final String FIELD_COUNT_PROPERTY_DEFAULT = "10";

  /** Representing a NULL value. */
  public static final String NULL_VALUE = "NULL";

  /** The primary key in the user table. */
  public static final String PRIMARY_KEY = "YCSB_KEY";

  /** The field name prefix in the table. */
  public static final String COLUMN_PREFIX = "FIELD";

  /** The connection timeout in seconds (applied to both primary and backup connections). */
  public static final String CONNECTION_TIMEOUT = "db.timeout";
  public static final int CONNECTION_TIMEOUT_DEFAULT = 10;

  /** The tracing property (shared with DBWrapper). */
  public static final String TRACING_PROPERTY = "db.tracing";
  public static final String TRACING_PROPERTY_DEFAULT = "false";

  /** SQL:2008 standard: FETCH FIRST n ROWS after the ORDER BY. */
  private boolean sqlansiScans = false;
  /** SQL Server before 2012: TOP n after the SELECT. */
  private boolean sqlserverScans = false;

  private List<Connection> conns;
  private List<Connection> backupConns;
  private String[] backupUrls;
  private String user;
  private String passwd;
  private boolean initialized = false;
  private Properties props;
  private int jdbcFetchSize;
  private int batchSize;
  private boolean autoCommit;
  private boolean batchUpdates;
  private int connectionTimeout;
  private int connectionTimeoutMs;
  private ExecutorService timeoutExecutor;
  private static final String DEFAULT_PROP = "";
  private ConcurrentMap<StatementType, PreparedStatement> cachedStatements;
  private long numRowsInBatch = 0;
  /** DB flavor defines DB-specific syntax and behavior for the
   * particular database. Current database flavors are: {default, phoenix} */
  private DBFlavor dbFlavor;
  
  /** Track whether we're currently in a transaction. */
  private boolean inTransaction = false;

  /**
   * Ordered field information for insert and update statements.
   */
  private static class OrderedFieldInfo {
    private String fieldKeys;
    private List<String> fieldValues;

    OrderedFieldInfo(String fieldKeys, List<String> fieldValues) {
      this.fieldKeys = fieldKeys;
      this.fieldValues = fieldValues;
    }

    String getFieldKeys() {
      return fieldKeys;
    }

    List<String> getFieldValues() {
      return fieldValues;
    }
  }

  /**
   * For the given key, returns what shard contains data for this key.
   *
   * @param key Data key to do operation on
   * @return Shard index
   */
  private int getShardIndexByKey(String key) {
    int ret = Math.abs(key.hashCode()) % conns.size();
    return ret;
  }

  /**
   * For the given key, returns Connection object that holds connection to the
   * shard that contains this key.
   *
   * @param key Data key to get information for
   * @return Connection object
   */
  private Connection getShardConnectionByKey(String key) {
    return conns.get(getShardIndexByKey(key));
  }

  private void cleanupAllConnections() throws SQLException {
    for (Connection conn : conns) {
      if (!autoCommit) {
        conn.commit();
      }
      conn.close();
    }

    // Close any pre-created backup connections that were never used.
    if (backupConns != null) {
      for (Connection backupConn : backupConns) {
        if (backupConn != null) {
          try {
            if (!backupConn.isClosed()) {
              backupConn.close();
            }
          } catch (SQLException e) {
            System.out.println("Error closing unused backup connection: " + e.getMessage()
                + " [SQLState: " + e.getSQLState()
                + ", ErrorCode: " + e.getErrorCode() + "]");
          }
        }
      }
    }

    if (timeoutExecutor != null) {
      timeoutExecutor.shutdownNow();
    }
  }

  /** Returns parsed int value from the properties if set, otherwise returns -1. */
  private static int getIntProperty(Properties props, String key) throws DBException {
    String valueStr = props.getProperty(key);
    if (valueStr != null) {
      try {
        return Integer.parseInt(valueStr);
      } catch (NumberFormatException nfe) {
        System.err.println("Invalid " + key + " specified: " + valueStr);
        throw new DBException(nfe);
      }
    }
    return -1;
  }

  /** Returns parsed int value from the properties if set, otherwise returns defaultVal. */
  private static int getIntPropertyWithDefault(Properties props, String key, int defaultVal) throws DBException {
    String valueStr = props.getProperty(key);
    if (valueStr != null) {
      try {
        return Integer.parseInt(valueStr);
      } catch (NumberFormatException nfe) {
        System.err.println("Invalid " + key + " specified: " + valueStr);
        throw new DBException(nfe);
      }
    }
    return defaultVal;
  }

  /** Returns parsed boolean value from the properties if set, otherwise returns defaultVal. */
  private static boolean getBoolProperty(Properties props, String key, boolean defaultVal) {
    String valueStr = props.getProperty(key);
    if (valueStr != null) {
      return Boolean.parseBoolean(valueStr);
    }
    return defaultVal;
  }

  @Override
  public void init() throws DBException {
    if (initialized) {
      System.err.println("Client connection already initialized.");
      return;
    }
    props = getProperties();
    String urls = props.getProperty(CONNECTION_URL, DEFAULT_PROP);
    this.user = props.getProperty(CONNECTION_USER, DEFAULT_PROP);
    this.passwd = props.getProperty(CONNECTION_PASSWD, DEFAULT_PROP);
    String driver = props.getProperty(DRIVER_CLASS);

    this.jdbcFetchSize = getIntProperty(props, JDBC_FETCH_SIZE);
    this.batchSize = getIntProperty(props, DB_BATCH_SIZE);

    this.autoCommit = getBoolProperty(props, JDBC_AUTO_COMMIT, true);
    this.batchUpdates = getBoolProperty(props, JDBC_BATCH_UPDATES, false);
    this.connectionTimeout = getIntPropertyWithDefault(props, CONNECTION_TIMEOUT, CONNECTION_TIMEOUT_DEFAULT);
    this.connectionTimeoutMs = this.connectionTimeout * 1000;
    this.timeoutExecutor = Executors.newSingleThreadExecutor();

    try {
//  The SQL Syntax for Scan depends on the DB engine
//  - SQL:2008 standard: FETCH FIRST n ROWS after the ORDER BY
//  - SQL Server before 2012: TOP n after the SELECT
//  - others (MySQL,MariaDB, PostgreSQL before 8.4)
//  TODO: check product name and version rather than driver name
      if (driver != null) {
        if (driver.contains("sqlserver")) {
          sqlserverScans = true;
          sqlansiScans = false;
        }
        if (driver.contains("oracle")) {
          sqlserverScans = false;
          sqlansiScans = true;
        }
        if (driver.contains("postgres")) {
          sqlserverScans = false;
          sqlansiScans = true;
        }
        Class.forName(driver);
      }
      int shardCount = 0;
      conns = new ArrayList<Connection>(3);
      // for a longer explanation see the README.md
      // semicolons aren't present in JDBC urls, so we use them to delimit
      // multiple JDBC connections to shard across.
      // Within each shard entry, a comma separates the primary URL from an
      // optional backup URL used for runtime connection failover.
      final String[] urlArr = urls.split(";");
      backupUrls = new String[urlArr.length];
      backupConns = new ArrayList<Connection>(urlArr.length);
      for (int i = 0; i < urlArr.length; i++) {
        backupConns.add(null);
      }
      for (int i = 0; i < urlArr.length; i++) {
        String shardEntry = urlArr[i];
        String primaryUrl;
        String backupUrl = null;
        int commaIdx = shardEntry.indexOf(',');
        if (commaIdx >= 0) {
          primaryUrl = shardEntry.substring(0, commaIdx).trim();
          String candidate = shardEntry.substring(commaIdx + 1).trim();
          if (!candidate.isEmpty()) {
            backupUrl = candidate;
          }
        } else {
          primaryUrl = shardEntry.trim();
        }
        System.out.println("Adding shard node URL: " + primaryUrl);
        try {
          DriverManager.setLoginTimeout(connectionTimeout);
          Connection conn = DriverManager.getConnection(primaryUrl, user, passwd);
          // Since there is no explicit commit method in the DB interface, all
          // operations should auto commit, except when explicitly told not to
          // (this is necessary in cases such as for PostgreSQL when running a
          // scan workload with fetchSize)
          conn.setAutoCommit(autoCommit);
          conn.setNetworkTimeout(timeoutExecutor, connectionTimeoutMs);
          conns.add(conn);
        } catch (SQLException e) {
          System.out.println("Failed to establish primary connection for shard " + i
              + " (URL: " + primaryUrl + ", user: " + user + ")"
              + ": " + e.getMessage()
              + " [SQLState: " + e.getSQLState()
              + ", ErrorCode: " + e.getErrorCode() + "]");
          throw e;
        }

        backupUrls[i] = backupUrl;
        shardCount++;

        // Eagerly create backup connection during initialization so it is
        // ready to use immediately when the primary connection fails.
        if (backupUrl != null) {
          System.out.println("Creating backup connection for shard " + i + " (URL: " + backupUrl + ")");
          try {
            DriverManager.setLoginTimeout(connectionTimeout);
            Connection backupConn = DriverManager.getConnection(backupUrl, user, passwd);
            backupConn.setAutoCommit(autoCommit);
            backupConn.setNetworkTimeout(timeoutExecutor, connectionTimeoutMs);
            backupConns.set(i, backupConn);
          } catch (SQLException e) {
            System.out.println("Warning: Failed to create backup connection for shard " + i
                + " (URL: " + backupUrl + ", user: " + user + ")"
                + ": " + e.getMessage()
                + " [SQLState: " + e.getSQLState()
                + ", ErrorCode: " + e.getErrorCode() + "]"
                + ". Backup will be retried on failover.");
          }
        }
      }

      System.out.println("Using shards: " + shardCount
          + ", batchSize:" + batchSize
          + ", fetchSize: " + jdbcFetchSize
          + ", backups: " + backupUrls);

      cachedStatements = new ConcurrentHashMap<StatementType, PreparedStatement>();

      this.dbFlavor = DBFlavor.fromJdbcUrl(urlArr[0].split(",")[0].trim());

      boolean tracingEnabled = getBoolProperty(props, TRACING_PROPERTY,
          Boolean.parseBoolean(TRACING_PROPERTY_DEFAULT));
      if (tracingEnabled) {
        this.dbFlavor.setTracingEnabled(true);
        for (Connection conn : conns) {
          try {
            this.dbFlavor.activateTracing(conn);
          } catch (SQLException e) {
            System.err.println("Error activating tracing: " + e);
          }
        }
      }
    } catch (ClassNotFoundException e) {
      timeoutExecutor.shutdownNow();
      System.err.println("Error in initializing the JDBS driver: " + e);
      throw new DBException(e);
    } catch (SQLException e) {
      timeoutExecutor.shutdownNow();
      System.err.println("Error in database operation: " + e);
      throw new DBException(e);
    } catch (NumberFormatException e) {
      timeoutExecutor.shutdownNow();
      System.err.println("Invalid value for fieldcount property. " + e);
      throw new DBException(e);
    }

    initialized = true;
  }

  @Override
  public void cleanup() throws DBException {
    if (batchSize > 0) {
      try {
        // commit un-finished batches
        for (PreparedStatement st : cachedStatements.values()) {
          if (!st.getConnection().isClosed() && !st.isClosed() && (numRowsInBatch % batchSize != 0)) {
            st.executeBatch();
          }
        }
      } catch (SQLException e) {
        System.err.println("Error in cleanup execution. " + e);
        throw new DBException(e);
      }
    }

    try {
      if (dbFlavor.isTracingEnabled()) {
        for (Connection conn : conns) {
          try {
            dbFlavor.outputAggregatedStats(conn);
          } catch (SQLException e) {
            System.err.println("Error outputting aggregated stats: " + e);
          }
        }
      }
      cleanupAllConnections();
    } catch (SQLException e) {
      System.err.println("Error in closing the connection. " + e);
      throw new DBException(e);
    }
  }

  /**
   * Start a database transaction.
   */
  @Override
  public void start() throws DBException {
    try {
      // If autoCommit was originally true, we need to disable it for the transaction
      if (autoCommit && !inTransaction) {
        for (Connection conn : conns) {
          conn.setAutoCommit(false);
        }
      }
      inTransaction = true;
    } catch (SQLException e) {
      if (isConnectionError(e)) {
        for (int i = 0; i < conns.size(); i++) {
          attemptFailover(i);
        }
      }
      throw new DBException("Error starting transaction: " + e);
    }
  }

  /**
   * Commit the current database transaction.
   */
  @Override
  public void commit() throws DBException {
    try {
      if (inTransaction) {
        for (Connection conn : conns) {
          conn.commit();
        }
        // Restore autoCommit if it was originally enabled
        if (autoCommit) {
          for (Connection conn : conns) {
            conn.setAutoCommit(true);
          }
        }
        inTransaction = false;
      }
    } catch (SQLException e) {
      if (isConnectionError(e)) {
        for (int i = 0; i < conns.size(); i++) {
          attemptFailover(i);
        }
      }
      throw new DBException("Error committing transaction: " + e);
    }
  }

  /**
   * Abort the current database transaction.
   */
  @Override
  public void abort() throws DBException {
    try {
      if (inTransaction) {
        for (Connection conn : conns) {
          conn.rollback();
        }
        // Restore autoCommit if it was originally enabled
        if (autoCommit) {
          for (Connection conn : conns) {
            conn.setAutoCommit(true);
          }
        }
        inTransaction = false;
      }
    } catch (SQLException e) {
      if (isConnectionError(e)) {
        for (int i = 0; i < conns.size(); i++) {
          attemptFailover(i);
        }
      }
      throw new DBException("Error aborting transaction: " + e);
    }
  }

  private PreparedStatement createAndCacheInsertStatement(StatementType insertType, String key)
      throws SQLException {
    String insert = dbFlavor.createInsertStatement(insertType, key);
    PreparedStatement insertStatement = getShardConnectionByKey(key).prepareStatement(insert);
    PreparedStatement stmt = cachedStatements.putIfAbsent(insertType, insertStatement);
    if (stmt == null) {
      return insertStatement;
    }
    return stmt;
  }

  private PreparedStatement createAndCacheReadStatement(StatementType readType, String key)
      throws SQLException {
    String read = dbFlavor.createReadStatement(readType, key);
    PreparedStatement readStatement = getShardConnectionByKey(key).prepareStatement(read);
    PreparedStatement stmt = cachedStatements.putIfAbsent(readType, readStatement);
    if (stmt == null) {
      return readStatement;
    }
    return stmt;
  }

  private PreparedStatement createAndCacheDeleteStatement(StatementType deleteType, String key)
      throws SQLException {
    String delete = dbFlavor.createDeleteStatement(deleteType, key);
    PreparedStatement deleteStatement = getShardConnectionByKey(key).prepareStatement(delete);
    PreparedStatement stmt = cachedStatements.putIfAbsent(deleteType, deleteStatement);
    if (stmt == null) {
      return deleteStatement;
    }
    return stmt;
  }

  private PreparedStatement createAndCacheUpdateStatement(StatementType updateType, String key)
      throws SQLException {
    String update = dbFlavor.createUpdateStatement(updateType, key);
    PreparedStatement insertStatement = getShardConnectionByKey(key).prepareStatement(update);
    PreparedStatement stmt = cachedStatements.putIfAbsent(updateType, insertStatement);
    if (stmt == null) {
      return insertStatement;
    }
    return stmt;
  }

  private PreparedStatement createAndCacheScanStatement(StatementType scanType, String key)
      throws SQLException {
    String select = dbFlavor.createScanStatement(scanType, key, sqlserverScans, sqlansiScans);
    PreparedStatement scanStatement = getShardConnectionByKey(key).prepareStatement(select);
    if (this.jdbcFetchSize > 0) {
      scanStatement.setFetchSize(this.jdbcFetchSize);
    }
    PreparedStatement stmt = cachedStatements.putIfAbsent(scanType, scanStatement);
    if (stmt == null) {
      return scanStatement;
    }
    return stmt;
  }

  @Override
  public Status read(String tableName, String key, Set<String> fields, Map<String, ByteIterator> result) {
    try {
      StatementType type = new StatementType(StatementType.Type.READ, tableName, 1, "", getShardIndexByKey(key));
      PreparedStatement readStatement = cachedStatements.get(type);
      if (readStatement == null) {
        readStatement = createAndCacheReadStatement(type, key);
      }
      readStatement.setString(1, key);
      ResultSet resultSet = readStatement.executeQuery();
      if (!resultSet.next()) {
        resultSet.close();
        return Status.NOT_FOUND;
      }
      if (result != null && fields != null) {
        for (String field : fields) {
          String value = resultSet.getString(field);
          result.put(field, new StringByteIterator(value));
        }
      }
      resultSet.close();
      if (dbFlavor.isTracingEnabled()) {
        dbFlavor.outputTraceResult(getShardConnectionByKey(key));
      }
      return Status.OK;
    } catch (SQLException e) {
      if (isConnectionError(e)) {
        int shardIdx = getShardIndexByKey(key);
        if (attemptFailover(shardIdx)) {
          System.err.println("Connection failed over for shard " + shardIdx + ". Retry the operation.");
        }
      }
      System.err.println("Error in processing read of table " + tableName + ": " + e);
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String tableName, String startKey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    try {
      StatementType type = new StatementType(StatementType.Type.SCAN, tableName, 1, "", getShardIndexByKey(startKey));
      PreparedStatement scanStatement = cachedStatements.get(type);
      if (scanStatement == null) {
        scanStatement = createAndCacheScanStatement(type, startKey);
      }
      // SQL Server TOP syntax is at first
      if (sqlserverScans) {
        scanStatement.setInt(1, recordcount);
        scanStatement.setString(2, startKey);
      // FETCH FIRST and LIMIT are at the end
      } else {
        scanStatement.setString(1, startKey);
        scanStatement.setInt(2, recordcount);
      }
      ResultSet resultSet = scanStatement.executeQuery();
      for (int i = 0; i < recordcount && resultSet.next(); i++) {
        if (result != null && fields != null) {
          HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
          for (String field : fields) {
            String value = resultSet.getString(field);
            values.put(field, new StringByteIterator(value));
          }
          result.add(values);
        }
      }
      resultSet.close();
      if (dbFlavor.isTracingEnabled()) {
        dbFlavor.outputTraceResult(getShardConnectionByKey(startKey));
      }
      return Status.OK;
    } catch (SQLException e) {
      if (isConnectionError(e)) {
        int shardIdx = getShardIndexByKey(startKey);
        if (attemptFailover(shardIdx)) {
          System.err.println("Connection failed over for shard " + shardIdx + ". Retry the operation.");
        }
      }
      System.err.println("Error in processing scan of table: " + tableName + e);
      return Status.ERROR;
    }
  }

  @Override
  public Status update(String tableName, String key, Map<String, ByteIterator> values) {
    try {
      int numFields = values.size();
      OrderedFieldInfo fieldInfo = getFieldInfo(values);
      StatementType type = new StatementType(StatementType.Type.UPDATE, tableName,
          numFields, fieldInfo.getFieldKeys(), getShardIndexByKey(key));
      PreparedStatement updateStatement = cachedStatements.get(type);
      if (updateStatement == null) {
        updateStatement = createAndCacheUpdateStatement(type, key);
      }
      int index = 1;
      for (String value: fieldInfo.getFieldValues()) {
        updateStatement.setString(index++, value);
      }
      updateStatement.setString(index, key);
      int result = updateStatement.executeUpdate();
      if (result == 1) {
        if (dbFlavor.isTracingEnabled()) {
          dbFlavor.outputTraceResult(getShardConnectionByKey(key));
        }
        return Status.OK;
      }
      return Status.UNEXPECTED_STATE;
    } catch (SQLException e) {
      if (isConnectionError(e)) {
        int shardIdx = getShardIndexByKey(key);
        if (attemptFailover(shardIdx)) {
          System.err.println("Connection failed over for shard " + shardIdx + ". Retry the operation.");
        }
      }
      System.err.println("Error in processing update to table: " + tableName + e);
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(String tableName, String key, Map<String, ByteIterator> values) {
    try {
      int numFields = values.size();
      OrderedFieldInfo fieldInfo = getFieldInfo(values);
      StatementType type = new StatementType(StatementType.Type.INSERT, tableName,
          numFields, fieldInfo.getFieldKeys(), getShardIndexByKey(key));
      PreparedStatement insertStatement = cachedStatements.get(type);
      if (insertStatement == null) {
        insertStatement = createAndCacheInsertStatement(type, key);
      }
      insertStatement.setString(1, key);
      int index = 2;
      for (String value: fieldInfo.getFieldValues()) {
        insertStatement.setString(index++, value);
      }
      // Using the batch insert API
      if (batchUpdates) {
        insertStatement.addBatch();
        // Check for a sane batch size
        if (batchSize > 0) {
          // Commit the batch after it grows beyond the configured size
          if (++numRowsInBatch % batchSize == 0) {
            int[] results = insertStatement.executeBatch();
            for (int r : results) {
              // Acceptable values are 1 and SUCCESS_NO_INFO (-2) from reWriteBatchedInserts=true
              if (r != 1 && r != -2) { 
                return Status.ERROR;
              }
            }
            // If autoCommit is off, make sure we commit the batch
            if (!autoCommit) {
              getShardConnectionByKey(key).commit();
            }
            if (dbFlavor.isTracingEnabled()) {
              dbFlavor.outputTraceResult(getShardConnectionByKey(key));
            }
            return Status.OK;
          } // else, the default value of -1 or a nonsense. Treat it as an infinitely large batch.
        } // else, we let the batch accumulate
        // Added element to the batch, potentially committing the batch too.
        return Status.BATCHED_OK;
      } else {
        // Normal update
        int result = insertStatement.executeUpdate();
        // If we are not autoCommit, we might have to commit now
        if (!autoCommit) {
          // Let updates be batcher locally
          if (batchSize > 0) {
            if (++numRowsInBatch % batchSize == 0) {
              // Send the batch of updates
              getShardConnectionByKey(key).commit();
            }
            // uhh
            if (dbFlavor.isTracingEnabled()) {
              dbFlavor.outputTraceResult(getShardConnectionByKey(key));
            }
            return Status.OK;
          } else {
            // Commit each update
            getShardConnectionByKey(key).commit();
          }
        }
        if (result == 1) {
          if (dbFlavor.isTracingEnabled()) {
            dbFlavor.outputTraceResult(getShardConnectionByKey(key));
          }
          return Status.OK;
        }
      }
      return Status.UNEXPECTED_STATE;
    } catch (SQLException e) {
      if (isConnectionError(e)) {
        int shardIdx = getShardIndexByKey(key);
        if (attemptFailover(shardIdx)) {
          System.err.println("Connection failed over for shard " + shardIdx + ". Retry the operation.");
        }
      }
      System.err.println("Error in processing insert to table: " + tableName + e);
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String tableName, String key) {
    try {
      StatementType type = new StatementType(StatementType.Type.DELETE, tableName, 1, "", getShardIndexByKey(key));
      PreparedStatement deleteStatement = cachedStatements.get(type);
      if (deleteStatement == null) {
        deleteStatement = createAndCacheDeleteStatement(type, key);
      }
      deleteStatement.setString(1, key);
      int result = deleteStatement.executeUpdate();
      if (result == 1) {
        if (dbFlavor.isTracingEnabled()) {
          dbFlavor.outputTraceResult(getShardConnectionByKey(key));
        }
        return Status.OK;
      }
      return Status.UNEXPECTED_STATE;
    } catch (SQLException e) {
      if (isConnectionError(e)) {
        int shardIdx = getShardIndexByKey(key);
        if (attemptFailover(shardIdx)) {
          System.err.println("Connection failed over for shard " + shardIdx + ". Retry the operation.");
        }
      }
      System.err.println("Error in processing delete to table: " + tableName + e);
      return Status.ERROR;
    }
  }

  /**
   * Transfer operation using a proper two-phase transaction.
   * This method implements: BEGIN, read balance1, read balance2, update balance1, update balance2, COMMIT.
   * Note: This implementation only supports transfers within the same shard. For cross-shard transfers,
   * it falls back to the default interactive implementation in the base DB class.
   */
  @Override
  public Status transfer(String tableName, String key1, String key2, String field) {
    // Check if the database flavor provides a custom transfer statement
    String transferStmt = dbFlavor.createTransferStatement(tableName, key1, key2, field);

    // If flavor returns null, use the default implementation from DB class
    if (transferStmt == null) {
      return super.transfer(tableName, key1, key2, field);
    }

    Connection conn = null;
    PreparedStatement stmt = null;
    ResultSet rs = null;
    try {
      // Both keys are on the same shard, use that connection
      conn = getShardConnectionByKey(key1);

      // Execute the flavor-provided transfer statement
      stmt = conn.prepareStatement(transferStmt);
      
      // Bind parameters: key1, key2, key1, key2 (for SELECT, SELECT, UPDATE, UPDATE)
      stmt.setString(1, key1);
      stmt.setString(2, key2);
      stmt.setString(3, key1);
      stmt.setString(4, key2);
      
      // Execute the statement
      rs = stmt.executeQuery();

      // Check if the transfer was successful
      boolean success = false;
      if (rs.next()) {
        int affectedRows = rs.getInt("affected_rows");
        success = (affectedRows == 2); // Both updates should succeed
      }

      if (success && dbFlavor.isTracingEnabled()) {
        dbFlavor.outputTraceResult(conn);
      }
      return success ? Status.OK : Status.UNEXPECTED_STATE;
      
    } catch (SQLException e) {
      if (isConnectionError(e)) {
        int shardIdx = getShardIndexByKey(key1);
        if (attemptFailover(shardIdx)) {
          System.err.println("Connection failed over for shard " + shardIdx + ". Retry the operation.");
        }
      }
      System.err.println("Error in processing transfer on table: " + tableName + " - " + e);
      return Status.ERROR;
    } catch (NumberFormatException e) {
      System.err.println("Error in processing transfer on table: " + tableName + " - " + e);
      return Status.ERROR;
    } finally {
      // Clean up resources
      try {
        if (rs != null) {
          rs.close();
        }
        if (stmt != null) {
          stmt.close();
        }
      } catch (SQLException e) {
        System.err.println("Error closing resources: " + e);
      }
    }
  }

  /**
   * Returns true if the given SQLException indicates a broken or lost connection.
   */
  private boolean isConnectionError(SQLException e) {
    String sqlState = e.getSQLState();
    if (sqlState != null) {
      // 08000 = connection exception
      // 08001 = unable to establish SQL connection
      // 08003 = connection does not exist
      // 08004 = connection rejected by server
      // 08006 = connection failure
      // 08S01 = communication link failure (vendor-specific)
      // 57P01 = admin shutdown (PostgreSQL/CockroachDB)
      return sqlState.equals("08000") || sqlState.equals("08001") || sqlState.equals("08003")
          || sqlState.equals("08004") || sqlState.equals("08006") || sqlState.equals("08S01")
          || sqlState.equals("57P01");
    }
    String msg = e.getMessage();
    if (msg != null) {
      String msgLower = msg.toLowerCase();
      return msgLower.contains("connection") || msgLower.contains("closed")
          || msgLower.contains("reset") || msgLower.contains("broken")
          || msgLower.contains("terminated") || msgLower.contains("refused");
    }
    return false;
  }

  /**
   * Attempts to failover the connection for the given shard index to its backup URL.
   * Uses the pre-created backup connection from init() if available; otherwise creates a new one.
   * Returns true if failover succeeded, false otherwise.
   */
  private boolean attemptFailover(int shardIndex) {
    if (backupUrls == null || shardIndex < 0 || shardIndex >= backupUrls.length) {
      return false;
    }
    String backupUrl = backupUrls[shardIndex];
    if (backupUrl == null) {
      return false;
    }

    // Use the pre-created backup connection if it is available and open.
    Connection newConn = null;
    if (backupConns != null && shardIndex < backupConns.size()) {
      Connection preCreated = backupConns.get(shardIndex);
      try {
        if (preCreated != null && !preCreated.isClosed()) {
          newConn = preCreated;
          backupConns.set(shardIndex, null);
          System.out.println("Failing over shard " + shardIndex
              + " to pre-created backup connection (URL: " + backupUrl + ")");
        }
      } catch (SQLException ex) {
        System.out.println("Pre-created backup connection for shard " + shardIndex
            + " is no longer usable: " + ex.getMessage()
            + " [SQLState: " + ex.getSQLState()
            + ", ErrorCode: " + ex.getErrorCode() + "]"
            + ". Attempting to create a new backup connection.");
        newConn = null;
      }
    }

    if (newConn == null) {
      // Fall back to creating a new backup connection on demand.
      System.out.println("Creating new backup connection for shard " + shardIndex
          + " (URL: " + backupUrl + ", user: " + user + ")");
      try {
        DriverManager.setLoginTimeout(connectionTimeout);
        newConn = DriverManager.getConnection(backupUrl, user, passwd);
        newConn.setAutoCommit(autoCommit);
        newConn.setNetworkTimeout(timeoutExecutor, connectionTimeoutMs);
      } catch (SQLException ex) {
        System.out.println("Failover connection attempt failed for shard " + shardIndex
            + " (URL: " + backupUrl + ", user: " + user + ")"
            + ": " + ex.getMessage()
            + " [SQLState: " + ex.getSQLState()
            + ", ErrorCode: " + ex.getErrorCode() + "]");
        return false;
      }
    }

    Connection oldConn = conns.set(shardIndex, newConn);
    try {
      if (oldConn != null && !oldConn.isClosed()) {
        oldConn.close();
      }
    } catch (SQLException ex) {
      System.out.println("Error closing failed primary connection for shard " + shardIndex
          + ": " + ex.getMessage()
          + " [SQLState: " + ex.getSQLState()
          + ", ErrorCode: " + ex.getErrorCode() + "]");
    }
    cachedStatements.clear();
    System.out.println("Shard " + shardIndex + " successfully failed over to backup URL: " + backupUrl);
    return true;
  }

  private OrderedFieldInfo getFieldInfo(Map<String, ByteIterator> values) {
    String fieldKeys = "";
    List<String> fieldValues = new ArrayList<>();
    int count = 0;
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      fieldKeys += entry.getKey();
      if (count < values.size() - 1) {
        fieldKeys += ",";
      }
      fieldValues.add(count, entry.getValue().toString());
      count++;
    }

    return new OrderedFieldInfo(fieldKeys, fieldValues);
  }

  /**
   * Swap operation: cyclically rotates field values among S keys.
   * For each i in 0..S-1, keys[(i+1) % S].field is set to keys[i].field.
   */
  @Override
  public Status swap(String tableName, String[] keys, String field) {
    // Check if the database flavor provides a custom swap statement
    String swapStmt = dbFlavor.createSwapStatement(tableName, keys, field);

    // If flavor returns null, use the default implementation from DB class
    if (swapStmt == null) {
      return super.swap(tableName, keys, field);
    }

    int s = keys.length;
    Connection conn = null;
    PreparedStatement stmt = null;
    ResultSet rs = null;
    try {
      conn = getShardConnectionByKey(keys[0]);
      stmt = conn.prepareStatement(swapStmt);

      // Bind parameters for the swap statement.
      // Expected parameter order (for a CTE-based swap):
      //   1..s   : keys for the IN clause in the reads CTE
      //   s+1..3s: for each i in 0..s-1: keys[(i+1)%s] (target), keys[i] (source) in the CASE WHEN
      //   3s+1..4s: keys for the IN clause in the UPDATE WHERE
      int paramIndex = 1;
      // IN clause for reads CTE
      for (int i = 0; i < s; i++) {
        stmt.setString(paramIndex++, keys[i]);
      }
      // CASE WHEN expressions: target key = ?, source value from ?, for each i
      for (int i = 0; i < s; i++) {
        stmt.setString(paramIndex++, keys[(i + 1) % s]); // target key
        stmt.setString(paramIndex++, keys[i]);            // source key
      }
      // IN clause for UPDATE WHERE
      for (int i = 0; i < s; i++) {
        stmt.setString(paramIndex++, keys[i]);
      }

      rs = stmt.executeQuery();

      boolean success = false;
      if (rs.next()) {
        int affectedRows = rs.getInt("affected_rows");
        success = (affectedRows == s);
      }

      if (success && dbFlavor.isTracingEnabled()) {
        dbFlavor.outputTraceResult(conn);
      }
      return success ? Status.OK : Status.UNEXPECTED_STATE;

    } catch (SQLException e) {
      if (isConnectionError(e)) {
        int shardIdx = getShardIndexByKey(keys[0]);
        if (attemptFailover(shardIdx)) {
          System.err.println("Connection failed over for shard " + shardIdx + ". Retry the operation.");
        }
      }
      System.err.println("Error in processing swap on table: " + tableName + " - " + e);
      return Status.ERROR;
    } finally {
      try {
        if (rs != null) {
          rs.close();
        }
        if (stmt != null) {
          stmt.close();
        }
      } catch (SQLException e) {
        System.err.println("Error closing resources: " + e);
      }
    }
  }
}
