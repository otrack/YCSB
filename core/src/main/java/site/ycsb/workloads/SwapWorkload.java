/**
 * Copyright (c) 2010-2016 Yahoo! Inc., 2017 YCSB contributors All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package site.ycsb.workloads;

import site.ycsb.DB;
import site.ycsb.WorkloadException;

import java.util.Properties;

/**
 * A workload that generalizes the closed economy workload using a cyclic swap operation.
 *
 * <p>The workload introduces a {@code swap} operation that selects S users at random and
 * cyclically rotates their {@code field0} values:
 * for each i in 0..S-1, {@code user[keys[(i+1) % S]].field0} is set to {@code user[keys[i]].field0}.
 *
 * <p>This preserves the closed-economy property (all values are conserved; only rearranged).
 *
 * <p>Additional property:
 * <ul>
 *   <li><b>swapsize</b>: the number of users (S) involved in each swap operation (default: 3)
 * </ul>
 */
public class SwapWorkload extends ClosedEconomyWorkload {

  /**
   * The name of the property for the number of users involved in each swap.
   */
  public static final String SWAP_SIZE_PROPERTY = "swap.s";

  /**
   * The default number of users involved in each swap.
   */
  public static final String SWAP_SIZE_PROPERTY_DEFAULT = "3";

  private int swapSize;

  @Override
  public void init(Properties p) throws WorkloadException {
    super.init(p);
    swapSize = Integer.parseInt(p.getProperty(SWAP_SIZE_PROPERTY, SWAP_SIZE_PROPERTY_DEFAULT));
    if (swapSize < 2) {
      throw new WorkloadException("swapsize must be at least 2");
    }
    System.out.println("[CONFIG] swap size: " + swapSize);
  }

  /**
   * Do a transactional swap: cyclically rotate field0 values among S randomly chosen users.
   */
  @Override
  protected boolean doTransactionReadModifyWrite(DB db) {
    // Choose S distinct keys
    String[] keys = new String[swapSize];
    long[] keyNums = new long[swapSize];
    for (int i = 0; i < swapSize; i++) {
      long keyNum;
      boolean duplicate;
      do {
        keyNum = nextKeyNum();
        duplicate = false;
        for (int j = 0; j < i; j++) {
          if (keyNums[j] == keyNum) {
            duplicate = true;
            break;
          }
        }
      } while (duplicate);
      keyNums[i] = keyNum;
      keys[i] = buildKeyName(keyNum);
    }

    return db.swap(table, keys, ClosedEconomyWorkload.DEFAULT_FIELD_NAME).isOk();
  }
}
