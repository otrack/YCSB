/**
 * Copyright (c) 2026 YCSB contributors All rights reserved.
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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/**
 * The micro-benchmark of the Calvin paper (Thomson et al., SIGMOD 2012, Section 6.2).
 *
 * <p>The records are split into a small pool of "hot" records (key numbers {@code [0, H)})
 * and a large pool of "cold" records (key numbers {@code [H, recordcount)}).
 * Each record holds a counter in {@code field0}, initially 0.
 *
 * <p>Each transaction reads {@code k} distinct records, among which a tunable fraction is hot,
 * checks that the sum of their counters is non-negative, and if so increments each counter
 * (see {@link DB#checkAndIncrement}).
 *
 * <p>Additional properties:
 * <ul>
 *   <li><b>calvin.txnsize</b>: the number of records per transaction (k, default: 10)</li>
 *   <li><b>calvin.hotfraction</b>: the fraction of hot records per transaction (default: 0.1, i.e.,
 *   1 hot record out of 10). When {@code hotfraction * k} is not an integer, the number of hot
 *   records is randomly rounded so that its mean is {@code hotfraction * k}.</li>
 *   <li><b>calvin.hotrecords</b>: the size of the hot pool (H, default: 1000)</li>
 *   <li><b>calvin.contentionindex</b>: if set, overrides {@code calvin.hotrecords} with
 *   {@code H = round(1 / contentionindex)}</li>
 * </ul>
 */
public class CalvinWorkload extends ClosedEconomyWorkload {

  public static final String TXN_SIZE_PROPERTY = "calvin.txnsize";
  public static final String TXN_SIZE_PROPERTY_DEFAULT = "10";

  public static final String HOT_FRACTION_PROPERTY = "calvin.hotfraction";
  public static final String HOT_FRACTION_PROPERTY_DEFAULT = "0.1";

  public static final String HOT_RECORDS_PROPERTY = "calvin.hotrecords";
  public static final String HOT_RECORDS_PROPERTY_DEFAULT = "1000";

  public static final String CONTENTION_INDEX_PROPERTY = "calvin.contentionindex";

  private int txnSize;
  private double hotFraction;
  private long hotRecords;

  @Override
  public void init(Properties p) throws WorkloadException {
    super.init(p);

    try {
      txnSize = Integer.parseInt(p.getProperty(TXN_SIZE_PROPERTY, TXN_SIZE_PROPERTY_DEFAULT));
      hotFraction = Double.parseDouble(p.getProperty(HOT_FRACTION_PROPERTY, HOT_FRACTION_PROPERTY_DEFAULT));
      String ci = p.getProperty(CONTENTION_INDEX_PROPERTY);
      if (ci != null) {
        double contentionIndex = Double.parseDouble(ci);
        if (contentionIndex <= 0 || contentionIndex > 1) {
          throw new WorkloadException(CONTENTION_INDEX_PROPERTY + " must be in (0, 1]");
        }
        hotRecords = Math.round(1.0 / contentionIndex);
      } else {
        hotRecords = Long.parseLong(p.getProperty(HOT_RECORDS_PROPERTY, HOT_RECORDS_PROPERTY_DEFAULT));
      }
    } catch (NumberFormatException e) {
      throw new WorkloadException(e);
    }

    if (txnSize < 1) {
      throw new WorkloadException(TXN_SIZE_PROPERTY + " must be at least 1");
    }
    if (hotFraction < 0 || hotFraction > 1) {
      throw new WorkloadException(HOT_FRACTION_PROPERTY + " must be in [0, 1]");
    }
    long maxHot = (long) Math.ceil(hotFraction * txnSize);
    long maxCold = txnSize - (long) Math.floor(hotFraction * txnSize);
    if (hotRecords < maxHot) {
      throw new WorkloadException("hot pool (" + hotRecords + ") smaller than hot records per transaction ("
          + maxHot + ")");
    }
    if (recordCount - hotRecords < maxCold) {
      throw new WorkloadException("cold pool (" + (recordCount - hotRecords)
          + ") smaller than cold records per transaction (" + maxCold + ")");
    }

    System.out.println("[CONFIG] calvin txn size: " + txnSize);
    System.out.println("[CONFIG] calvin hot fraction: " + hotFraction);
    System.out.println("[CONFIG] calvin hot records: " + hotRecords);
  }

  public int getTxnSize() {
    return txnSize;
  }

  public long getHotRecords() {
    return hotRecords;
  }

  /**
   * Number of hot records for the next transaction: {@code hotfraction * k}, randomly rounded.
   */
  protected int nextHotCount(Random random) {
    double h = hotFraction * txnSize;
    int base = (int) Math.floor(h);
    return random.nextDouble() < (h - base) ? base + 1 : base;
  }

  /**
   * Choose the (sorted, distinct) key numbers accessed by the next transaction.
   */
  public long[] nextTransactionKeyNums(Random random) {
    int hot = nextHotCount(random);
    long[] keyNums = new long[txnSize];
    Set<Long> chosen = new HashSet<>();
    int i = 0;
    while (i < hot) {
      long keyNum = (long) (random.nextDouble() * hotRecords);
      if (chosen.add(keyNum)) {
        keyNums[i++] = keyNum;
      }
    }
    while (i < txnSize) {
      long keyNum = hotRecords + (long) (random.nextDouble() * (recordCount - hotRecords));
      if (chosen.add(keyNum)) {
        keyNums[i++] = keyNum;
      }
    }
    // Order keys to prevent deadlocks (in lock-based database systems)
    Arrays.sort(keyNums);
    return keyNums;
  }

  /**
   * Do a Calvin micro-benchmark transaction: read k records, check their sum, increment each counter.
   */
  @Override
  protected boolean doTransactionReadModifyWrite(DB db) {
    long[] keyNums = nextTransactionKeyNums(ThreadLocalRandom.current());
    String[] keys = new String[keyNums.length];
    for (int i = 0; i < keyNums.length; i++) {
      keys[i] = buildKeyName(keyNums[i]);
    }
    return db.checkAndIncrement(table, keys, DEFAULT_FIELD_NAME).isOk();
  }

  /**
   * Every committed transaction adds exactly k to the sum of all counters.
   * The expected sum is an upper bound, as some of the {@code count} attempted transactions may abort.
   */
  @Override
  protected long expectedSum(long count) {
    return txnSize * count;
  }

  @Override
  protected boolean isConsistent(long countedSum, long count) {
    return countedSum >= 0 && countedSum % txnSize == 0 && countedSum <= expectedSum(count);
  }
}
