/**
 * Copyright (c) 2026 YCSB contributors. All rights reserved.
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

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CockroachDBFlavorTest {

  @Test
  public void createTransferStatementUsesSingleUpdateCte() {
    CockroachDBFlavor flavor = new CockroachDBFlavor();
    String sql = flavor.createTransferStatement("usertable", "user1", "user2", "field0");

    assertTrue(sql.contains("WITH update_rows AS (UPDATE usertable"));
    assertTrue(sql.contains("SET field0 = CASE"));
    assertTrue(sql.contains("WHERE YCSB_KEY IN (?, ?)"));
    assertTrue(sql.contains("SELECT COUNT(*) AS affected_rows FROM update_rows"));
    assertFalse(sql.contains("FOR UPDATE"));
  }
}
