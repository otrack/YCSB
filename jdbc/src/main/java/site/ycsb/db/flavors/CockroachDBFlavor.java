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

import site.ycsb.db.JdbcDBClient;

/**
 * A flavor for CockroachDB that provides optimized two-phase transaction support.
 * Uses CockroachDB's CTE (Common Table Expression) syntax to perform atomic transfers
 * in a single SQL statement.
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

    sql.append("WITH ");
    
    sql.append("balance1 AS (");
    sql.append("SELECT ").append(field).append(" FROM ").append(tableName);
    sql.append(" WHERE ").append(JdbcDBClient.PRIMARY_KEY).append(" = ? FOR UPDATE");
    sql.append("), ");
    
    sql.append("balance2 AS (");
    sql.append("SELECT ").append(field).append(" FROM ").append(tableName);
    sql.append(" WHERE ").append(JdbcDBClient.PRIMARY_KEY).append(" = ? FOR UPDATE");
    sql.append("), ");
    
    sql.append("update1 AS (");
    sql.append("UPDATE ").append(tableName);
    sql.append(" SET ").append(field).append(" = (SELECT ").append(field).append(" FROM balance2)");
    sql.append(" WHERE ").append(JdbcDBClient.PRIMARY_KEY).append(" = ?");
    sql.append(" RETURNING 1");
    sql.append("), ");
    
    sql.append("update2 AS (");
    sql.append("UPDATE ").append(tableName);
    sql.append(" SET ").append(field).append(" = (SELECT ").append(field).append(" FROM balance1)");
    sql.append(" WHERE ").append(JdbcDBClient.PRIMARY_KEY).append(" = ?");
    sql.append(" RETURNING 1");
    sql.append(") ");
    
    // Final SELECT to complete the statement
    sql.append("SELECT (SELECT COUNT(*) FROM update1) + (SELECT COUNT(*) FROM update2) AS affected_rows");
    
    return sql.toString();
  }
}
