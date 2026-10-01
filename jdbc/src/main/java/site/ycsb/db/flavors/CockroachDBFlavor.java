/**
 * Copyright (c) 2016, 2019 YCSB contributors. All rights reserved.
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
package site.ycsb.db.flavors;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import site.ycsb.db.JdbcDBClient;

/**
 * A flavor for CockroachDB that provides optimized two-phase transaction support.
 * Uses a single UPDATE statement in a CTE to reduce lock footprint and round trips.
 */
public class CockroachDBFlavor extends DefaultDBFlavor {
  
  public CockroachDBFlavor() {
    super(DBName.COCKROACHDB);
  }

  /**
   * CockroachDB supports optimized two-phase transactions with SELECT FOR UPDATE.
   */
  @Override
  public boolean supportsOptimizedTransfer() {
    return true;
  }

  @Override
  public String createTransferStatement(String tableName, String key1, String key2, String field) {
    StringBuilder sql = new StringBuilder();

    sql.append("WITH update_rows AS (");
    sql.append("UPDATE ").append(tableName);
    sql.append(" SET ").append(field).append(" = CASE");
    sql.append(" WHEN ").append(JdbcDBClient.PRIMARY_KEY).append(" = ? THEN ").append(field);
    sql.append(" WHEN ").append(JdbcDBClient.PRIMARY_KEY).append(" = ? THEN ").append(field);
    sql.append(" ELSE ").append(field);
    sql.append(" END");
    sql.append(" WHERE ").append(JdbcDBClient.PRIMARY_KEY).append(" IN (?, ?)");
    sql.append(" RETURNING 1");
    sql.append(") ");
    sql.append("SELECT COUNT(*) AS affected_rows FROM update_rows");
    
    return sql.toString();
  }

  /**
   * Creates an efficient CTE-based SQL statement for the swap operation.
   *
   * <p>The statement reads all S values in a CTE, then updates each
   * {@code keys[(i+1) % S]} with the value read from {@code keys[i]}.
   *
   * <p>Parameter binding order (total = 4*S parameters):
   * <ol>
   *   <li>keys[0], ..., keys[S-1]  — IN clause for the reads CTE
   *   <li>for i in 0..S-1: keys[(i+1)%S], keys[i]  — CASE WHEN target = ? THEN (SELECT ... WHERE source = ?)
   *   <li>keys[0], ..., keys[S-1]  — IN clause for the UPDATE WHERE
   * </ol>
   */
  @Override
  public String createSwapStatement(String tableName, String[] keys, String field) {
    int s = keys.length;
    StringBuilder sql = new StringBuilder();

    // CTE 1: read all values
    sql.append("WITH vals AS (");
    sql.append("SELECT ").append(JdbcDBClient.PRIMARY_KEY).append(", ").append(field);
    sql.append(" FROM ").append(tableName);
    sql.append(" WHERE ").append(JdbcDBClient.PRIMARY_KEY).append(" IN (");
    for (int i = 0; i < s; i++) {
      if (i > 0) {
        sql.append(", ");
      }
      sql.append("?");
    }
    sql.append(")), ");

    // CTE 2: update rows with cyclic rotation
    sql.append("update_rows AS (");
    sql.append("UPDATE ").append(tableName);
    sql.append(" SET ").append(field).append(" = CASE");
    for (int i = 0; i < s; i++) {
      // keys[(i+1)%s] gets keys[i]'s value
      sql.append(" WHEN ").append(JdbcDBClient.PRIMARY_KEY).append(" = ?");
      sql.append(" THEN (SELECT ").append(field).append(" FROM vals WHERE ");
      sql.append(JdbcDBClient.PRIMARY_KEY).append(" = ?)");
    }
    sql.append(" ELSE ").append(field);
    sql.append(" END");
    sql.append(" WHERE ").append(JdbcDBClient.PRIMARY_KEY).append(" IN (");
    for (int i = 0; i < s; i++) {
      if (i > 0) {
        sql.append(", ");
      }
      sql.append("?");
    }
    sql.append(") RETURNING 1) ");
    sql.append("SELECT COUNT(*) AS affected_rows FROM update_rows");

    return sql.toString();
  }

  /**
   * Creates a single CTE-based SQL statement for the check-and-increment operation
   * (Calvin micro-benchmark): the counters are read, their sum is checked, and they are all
   * incremented if it is non-negative. The fields are stored as text, hence the casts.
   *
   * <p>Parameter binding order (total = 2*S parameters): keys[0], ..., keys[S-1] for the reads,
   * then keys[0], ..., keys[S-1] for the UPDATE.
   */
  @Override
  public String createCheckAndIncrementStatement(String tableName, String[] keys, String field) {
    String inClause = inClause(keys.length);
    StringBuilder sql = new StringBuilder();

    // CTE 1: read all counters
    sql.append("WITH vals AS (");
    sql.append("SELECT ").append(field).append(" FROM ").append(tableName);
    sql.append(" WHERE ").append(JdbcDBClient.PRIMARY_KEY).append(" IN ").append(inClause);
    sql.append("), ");

    // CTE 2: check the constraint
    sql.append("chk AS (");
    sql.append("SELECT COALESCE(SUM(CAST(").append(field).append(" AS INT8)), 0) >= 0 AS ok,");
    sql.append(" COUNT(*) AS n FROM vals");
    sql.append("), ");

    // CTE 3: increment all counters if it holds
    sql.append("update_rows AS (");
    sql.append("UPDATE ").append(tableName);
    sql.append(" SET ").append(field).append(" = CAST(CAST(").append(field).append(" AS INT8) + 1 AS TEXT)");
    sql.append(" WHERE ").append(JdbcDBClient.PRIMARY_KEY).append(" IN ").append(inClause);
    sql.append(" AND (SELECT ok FROM chk)");
    sql.append(" RETURNING 1) ");

    sql.append("SELECT (SELECT ok FROM chk) AS ok, (SELECT n FROM chk) AS read_rows,");
    sql.append(" (SELECT COUNT(*) FROM update_rows) AS affected_rows");

    return sql.toString();
  }

  private static String inClause(int n) {
    StringBuilder sb = new StringBuilder("(");
    for (int i = 0; i < n; i++) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append("?");
    }
    return sb.append(")").toString();
  }

  @Override
  public void activateTracing(Connection conn) throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("SET tracing = 'kv'");
    }
  }

  @Override
  public void outputTraceResult(Connection conn) throws SQLException {
    try (Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery("SHOW TRACE FOR SESSION")) {
      printToStdout(rs);
    }
    // Reset trace buffer for the next operation
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("SET tracing = off");
      stmt.execute("SET tracing = 'kv'");
    }
  }

  @Override
  public void outputAggregatedStats(Connection conn) throws SQLException {
    try (Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery("SELECT * FROM crdb_internal.cluster_statement_statistics")) {
      ResultSetMetaData meta = rs.getMetaData();
      int cols = meta.getColumnCount();
      System.out.println("=== CockroachDB Aggregated Statement Statistics ===");
      while (rs.next()) {
        StringBuilder row = new StringBuilder();
        for (int i = 1; i <= cols; i++) {
          if (i > 1) {
            row.append(", ");
          }
          row.append(meta.getColumnName(i)).append("=").append(rs.getString(i));
        }
        System.out.println(row.toString());
      }
    }
  }

  public static void printToStdout(ResultSet rs) throws SQLException {
    ResultSetMetaData md = rs.getMetaData();
    int cols = md.getColumnCount();

    // header
    for (int i = 1; i <= cols; i++) {
      if (i > 1) {
        System.out.print("\t");
      }
      System.out.print(md.getColumnLabel(i));
    }
    System.out.println();

    // rows
    while (rs.next()) {
      for (int i = 1; i <= cols; i++) {
        if (i > 1) {
          System.out.print("\t");
        }
        Object v = rs.getObject(i);
        System.out.print(v == null ? "NULL" : v.toString());
      }
      System.out.println();
    }
  }
}
