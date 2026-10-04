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
 * <p>The records are split into {@code N} partitions (the machines of the paper, see {@link CalvinPartitioner}).
 * Each partition has a small pool of "hot" records (its first {@code H} records) and a large pool of "cold"
 * records (the others). Each record holds a counter in {@code field0}, initially 0.
 *
 * <p>Each transaction reads {@code k} distinct records, checks that the sum of their counters is non-negative,
 * and if so increments each counter (see {@link DB#checkAndIncrement}). A single-partition transaction takes all its
 * records from its home partition, among which a tunable fraction is hot. A multipartition transaction spans
 * {@code span} distinct partitions (the home partition and randomly chosen others), and takes {@code k / span}
 * records from each of them, among which as many hot records as a single-partition transaction does.
 * With the default parameters, a single-partition transaction accesses 1 hot and 9 cold records, and a
 * multipartition transaction 1 hot and 4 cold records on each of the 2 partitions, as in the paper.
 *
 * <p>Additional properties:
 * <ul>
 *   <li><b>calvin.txnsize</b>: the number of records per transaction (k, default: 10)</li>
 *   <li><b>calvin.hotfraction</b>: the fraction of hot records of a single-partition transaction (default: 0.1,
 *   i.e., 1 hot record out of 10); a multipartition transaction accesses as many hot records on each of its
 *   partitions. When {@code hotfraction * k} is not an integer, the number of hot records is randomly rounded
 *   so that its mean is {@code hotfraction * k}.</li>
 *   <li><b>calvin.hotrecords</b>: the size of the hot pool of each partition (H, default: 1000)</li>
 *   <li><b>calvin.contentionindex</b>: if set, overrides {@code calvin.hotrecords} with
 *   {@code H = round(1 / contentionindex)}, i.e., the fraction of the hot records of a partition that a
 *   transaction accesses there</li>
 *   <li><b>calvin.partitions</b>: the number of partitions (N, default: 1)</li>
 *   <li><b>calvin.partitioner</b>: how the records are split into partitions, which should match how the
 *   database places them on its nodes (default: mod); see {@link CalvinPartitioner}:
 *   <ul>
 *     <li>mod: key number {@code i} is on partition {@code i mod N} (Calvin, Tiga)</li>
 *     <li>range: contiguous blocks of key numbers (range-sharded stores); the keys are zero padded so that
 *     their string order is the numerical one</li>
 *     <li>murmur3: equal slices of Cassandra's Murmur3 token ring (one token per node)</li>
 *     <li>tiga: the shards of Tiga's YCSB client</li>
 *   </ul></li>
 *   <li><b>calvin.mpproportion</b>: the proportion of multipartition transactions (default: 0, ignored when
 *   {@code N = 1})</li>
 *   <li><b>calvin.mpspan</b>: the number of partitions of a multipartition transaction (default: 2)</li>
 *   <li><b>calvin.homepartition</b>: the home partition of the transactions, either a partition number or
 *   "random" for a partition chosen uniformly at random for each transaction (default: random)</li>
 *   <li><b>zeropadding</b>: as in {@link CoreWorkload}</li>
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

  public static final String PARTITIONS_PROPERTY = "calvin.partitions";
  public static final String PARTITIONS_PROPERTY_DEFAULT = "1";

  public static final String PARTITIONER_PROPERTY = "calvin.partitioner";
  public static final String PARTITIONER_PROPERTY_DEFAULT = CalvinPartitioner.MOD;

  public static final String MP_PROPORTION_PROPERTY = "calvin.mpproportion";
  public static final String MP_PROPORTION_PROPERTY_DEFAULT = "0";

  public static final String MP_SPAN_PROPERTY = "calvin.mpspan";
  public static final String MP_SPAN_PROPERTY_DEFAULT = "2";

  public static final String HOME_PARTITION_PROPERTY = "calvin.homepartition";
  public static final String HOME_PARTITION_PROPERTY_DEFAULT = "random";

  private int txnSize;
  private double hotFraction;
  private long hotRecords;
  private int partitions;
  private CalvinPartitioner partitioner;
  private double mpProportion;
  private int mpSpan;
  private int homePartition; // -1 when random
  private int zeroPadding;

  @Override
  public void init(Properties p) throws WorkloadException {
    super.init(p);

    String partitionerName;
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
      partitions = Integer.parseInt(p.getProperty(PARTITIONS_PROPERTY, PARTITIONS_PROPERTY_DEFAULT));
      partitionerName = p.getProperty(PARTITIONER_PROPERTY, PARTITIONER_PROPERTY_DEFAULT);
      mpProportion = Double.parseDouble(p.getProperty(MP_PROPORTION_PROPERTY, MP_PROPORTION_PROPERTY_DEFAULT));
      mpSpan = Integer.parseInt(p.getProperty(MP_SPAN_PROPERTY, MP_SPAN_PROPERTY_DEFAULT));
      String home = p.getProperty(HOME_PARTITION_PROPERTY, HOME_PARTITION_PROPERTY_DEFAULT);
      homePartition = HOME_PARTITION_PROPERTY_DEFAULT.equals(home) ? -1 : Integer.parseInt(home);
      zeroPadding = Integer.parseInt(p.getProperty(CoreWorkload.ZERO_PADDING_PROPERTY,
          CoreWorkload.ZERO_PADDING_PROPERTY_DEFAULT));
    } catch (NumberFormatException e) {
      throw new WorkloadException(e);
    }

    if (txnSize < 1) {
      throw new WorkloadException(TXN_SIZE_PROPERTY + " must be at least 1");
    }
    if (hotFraction < 0 || hotFraction > 1) {
      throw new WorkloadException(HOT_FRACTION_PROPERTY + " must be in [0, 1]");
    }
    if (partitions < 1) {
      throw new WorkloadException(PARTITIONS_PROPERTY + " must be at least 1");
    }
    if (mpProportion < 0 || mpProportion > 1) {
      throw new WorkloadException(MP_PROPORTION_PROPERTY + " must be in [0, 1]");
    }
    if (homePartition < -1 || homePartition >= partitions) {
      throw new WorkloadException(HOME_PARTITION_PROPERTY + " must be \"random\" or in [0, " + partitions + ")");
    }
    // As in Calvin, there is no multipartition transaction with a single partition
    if (partitions == 1) {
      mpProportion = 0;
    }
    long maxHot = (long) Math.ceil(hotFraction * txnSize);
    long maxCold = txnSize - (long) Math.floor(hotFraction * txnSize);
    if (mpProportion > 0) {
      if (mpSpan < 2 || mpSpan > partitions) {
        throw new WorkloadException(MP_SPAN_PROPERTY + " must be in [2, " + partitions + "]");
      }
      if (txnSize % mpSpan != 0) {
        throw new WorkloadException(TXN_SIZE_PROPERTY + " (" + txnSize + ") must be a multiple of "
            + MP_SPAN_PROPERTY + " (" + mpSpan + ")");
      }
      if (maxHot > txnSize / mpSpan) {
        throw new WorkloadException("more hot records per partition (" + maxHot + ") than records per partition ("
            + (txnSize / mpSpan) + ") in a multipartition transaction");
      }
    }

    // Range partitioning requires the string order of the keys to be the numerical one
    if (CalvinPartitioner.RANGE.equals(partitionerName)) {
      zeroPadding = Math.max(zeroPadding, String.valueOf(Math.max(0, recordCount - 1)).length());
    }
    try {
      partitioner = CalvinPartitioner.create(partitionerName, partitions, recordCount, this::buildKeyName);
    } catch (IllegalArgumentException e) {
      throw new WorkloadException(e.getMessage());
    }

    for (int q = 0; q < partitions; q++) {
      if (hotRecords < maxHot) {
        throw new WorkloadException("hot pool (" + hotRecords + ") smaller than hot records per transaction ("
            + maxHot + ")");
      }
      if (partitioner.size(q) - hotRecords < maxCold) {
        throw new WorkloadException("cold pool of partition " + q + " (" + (partitioner.size(q) - hotRecords)
            + ") smaller than cold records per transaction (" + maxCold + ")");
      }
    }

    System.out.println("[CONFIG] calvin txn size: " + txnSize);
    System.out.println("[CONFIG] calvin hot fraction: " + hotFraction);
    System.out.println("[CONFIG] calvin hot records per partition: " + hotRecords);
    System.out.println("[CONFIG] calvin partitions: " + partitions + " (" + partitionerName + ")");
    System.out.println("[CONFIG] calvin multipartition proportion: " + mpProportion + " (span " + mpSpan + ")");
    System.out.println("[CONFIG] calvin home partition: "
        + (homePartition < 0 ? HOME_PARTITION_PROPERTY_DEFAULT : homePartition));
  }

  public int getTxnSize() {
    return txnSize;
  }

  public long getHotRecords() {
    return hotRecords;
  }

  public CalvinPartitioner getPartitioner() {
    return partitioner;
  }

  @Override
  protected String buildKeyName(long keyNum) {
    if (zeroPadding <= 1) {
      return super.buildKeyName(keyNum);
    }
    String value = Long.toString(keyNum);
    StringBuilder sb = new StringBuilder("user");
    for (int i = value.length(); i < zeroPadding; i++) {
      sb.append('0');
    }
    return sb.append(value).toString();
  }

  /**
   * Number of hot records for the next transaction on one of its partitions: {@code hotfraction * k},
   * randomly rounded.
   */
  protected int nextHotCount(Random random) {
    double h = hotFraction * txnSize;
    int base = (int) Math.floor(h);
    return random.nextDouble() < (h - base) ? base + 1 : base;
  }

  /**
   * Choose the partitions accessed by the next transaction, starting with its home partition.
   */
  public int[] nextTransactionPartitions(Random random) {
    int home = homePartition >= 0 ? homePartition : random.nextInt(partitions);
    if (mpProportion == 0 || random.nextDouble() >= mpProportion) {
      return new int[]{home};
    }
    int[] parts = new int[mpSpan];
    parts[0] = home;
    int i = 1;
    while (i < mpSpan) {
      int q = random.nextInt(partitions);
      boolean fresh = true;
      for (int j = 0; j < i; j++) {
        fresh &= parts[j] != q;
      }
      if (fresh) {
        parts[i++] = q;
      }
    }
    return parts;
  }

  /**
   * Choose the (sorted, distinct) key numbers accessed by the next transaction.
   */
  public long[] nextTransactionKeyNums(Random random) {
    int[] parts = nextTransactionPartitions(random);
    int perPartition = txnSize / parts.length;
    long[] keyNums = new long[txnSize];
    Set<Long> chosen = new HashSet<>();
    int i = 0;
    for (int q : parts) {
      int hot = nextHotCount(random);
      long cold = partitioner.size(q) - hotRecords;
      int end = i + perPartition;
      while (i < end) {
        long index = i < end - perPartition + hot
            ? (long) (random.nextDouble() * hotRecords)
            : hotRecords + (long) (random.nextDouble() * cold);
        long keyNum = partitioner.keyNum(q, index);
        if (chosen.add(keyNum)) {
          keyNums[i++] = keyNum;
        }
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
