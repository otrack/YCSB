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

  /**
   * Creates a two-phase transaction statement for transferring values between two records.
   * Uses CockroachDB's CTE (WITH clause) syntax with SELECT FOR UPDATE for proper row locking.
   * 
   * The generated SQL statement performs an atomic transfer operation:
   * 1. Read both account balances with row locks (SELECT FOR UPDATE)
   * 2. Update first account (decrement by 1)
   * 3. Update second account (increment by 1)
   * 
   * All operations are performed in a single SQL statement using CTEs.
   * 
   * @param tableName the name of the table
   * @param key1 the first record key (source account)
   * @param key2 the second record key (destination account)
   * @param field the field name to transfer
   * @return A complete SQL statement using CTEs for atomic transfer
   */
  @Override
  public String createTransferStatement(String tableName, String key1, String key2, String field) {
    StringBuilder sql = new StringBuilder();
    
    // Use CockroachDB's CTE syntax to perform the entire transfer in a single statement
    // This ensures atomicity and uses SELECT FOR UPDATE for proper row locking
    sql.append("WITH ");
    
    // Read first account balance with row lock
    sql.append("balance1 AS (");
    sql.append("SELECT ").append(field).append(" FROM ").append(tableName);
    sql.append(" WHERE ").append(JdbcDBClient.PRIMARY_KEY).append(" = ? FOR UPDATE");
    sql.append("), ");
    
    // Read second account balance with row lock
    sql.append("balance2 AS (");
    sql.append("SELECT ").append(field).append(" FROM ").append(tableName);
    sql.append(" WHERE ").append(JdbcDBClient.PRIMARY_KEY).append(" = ? FOR UPDATE");
    sql.append("), ");
    
    // Update first account (decrement by 1)
    sql.append("update1 AS (");
    sql.append("UPDATE ").append(tableName);
    sql.append(" SET ").append(field).append(" = (SELECT ").append(field).append(" FROM balance1) - 1");
    sql.append(" WHERE ").append(JdbcDBClient.PRIMARY_KEY).append(" = ?");
    sql.append(" RETURNING 1");
    sql.append("), ");
    
    // Update second account (increment by 1)
    sql.append("update2 AS (");
    sql.append("UPDATE ").append(tableName);
    sql.append(" SET ").append(field).append(" = (SELECT ").append(field).append(" FROM balance2) + 1");
    sql.append(" WHERE ").append(JdbcDBClient.PRIMARY_KEY).append(" = ?");
    sql.append(" RETURNING 1");
    sql.append(") ");
    
    // Final SELECT to complete the statement
    sql.append("SELECT (SELECT COUNT(*) FROM update1) + (SELECT COUNT(*) FROM update2) AS affected_rows");
    
    return sql.toString();
  }
}
