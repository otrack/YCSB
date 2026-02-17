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

import site.ycsb.db.StatementType;

/**
 * DBFlavor captures minor differences in syntax and behavior among JDBC implementations and SQL
 * dialects. This class also acts as a factory to instantiate concrete flavors based on the JDBC URL.
 */
public abstract class DBFlavor {

  enum DBName {
    DEFAULT,
    PHOENIX,
    COCKROACHDB
  }

  private final DBName dbName;

  public DBFlavor(DBName dbName) {
    this.dbName = dbName;
  }

  public static DBFlavor fromJdbcUrl(String url) {
    if (url.startsWith("jdbc:phoenix")) {
      return new PhoenixDBFlavor();
    }
    if (url.startsWith("jdbc:postgresql") && url.contains("cockroach")) {
      return new CockroachDBFlavor();
    }
    // CockroachDB can also use jdbc:cockroachdb URL format
    if (url.startsWith("jdbc:cockroachdb")) {
      return new CockroachDBFlavor();
    }
    return new DefaultDBFlavor();
  }

  /**
   * Create and return a SQL statement for inserting data.
   */
  public abstract String createInsertStatement(StatementType insertType, String key);

  /**
   * Create and return a SQL statement for reading data.
   */
  public abstract String createReadStatement(StatementType readType, String key);

  /**
   * Create and return a SQL statement for deleting data.
   */
  public abstract String createDeleteStatement(StatementType deleteType, String key);

  /**
   * Create and return a SQL statement for updating data.
   */
  public abstract String createUpdateStatement(StatementType updateType, String key);

  /**
   * Create and return a SQL statement for scanning data.
   */
  public abstract String createScanStatement(StatementType scanType, String key,
                                             boolean sqlserverScans, boolean sqlansiScans);

  /**
   * Create and return a SQL statement for transferring data between two records.
   * This allows database-specific optimizations for two-phase transactions.
   * 
   * @param tableName the name of the table
   * @param key1 the first record key (source)
   * @param key2 the second record key (destination)
   * @param field the field name to transfer
   * @return SQL statement for transfer operation, or null to use default implementation
   */
  public String createTransferStatement(String tableName, String key1, String key2, String field) {
    // By default, return null to indicate no special transfer statement
    // Subclasses can override to provide database-specific implementations
    return null;
  }
  
  /**
   * Indicates whether this database flavor supports optimized two-phase transfer transactions.
   * If true, the JDBC client will use its built-in two-phase transaction logic.
   * If false, it will fall back to the default implementation in DB class.
   * 
   * @return true if the flavor supports optimized transfers, false otherwise
   */
  public boolean supportsOptimizedTransfer() {
    return false;
  }
}
