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
 */
public class CockroachDBFlavor extends DefaultDBFlavor {
  
  public CockroachDBFlavor() {
    super(DBName.DEFAULT);
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
   * Uses CockroachDB's SELECT ... FOR UPDATE syntax for proper row locking.
   * 
   * Returns a marker string indicating CockroachDB-specific handling is needed.
   * The actual implementation is done in JdbcDBClient with proper transaction management.
   * 
   * @param tableName the name of the table
   * @param key1 the first record key (source account)
   * @param key2 the second record key (destination account)
   * @param field the field name to transfer
   * @return A marker string indicating custom transfer handling
   */
  @Override
  public String createTransferStatement(String tableName, String key1, String key2, String field) {
    // Return a non-null marker to indicate this flavor handles transfers specially
    return "COCKROACHDB_TRANSFER";
  }
}
