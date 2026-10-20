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

import org.HdrHistogram.ConcurrentHistogram;
import org.HdrHistogram.Histogram;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * The micro-benchmark of the Calvin paper (Thomson et al., SIGMOD 2012, Section 6.2).
 *
 * <p>The records are split into {@code N} partitions (the machines of the paper, see {@link CalvinPartitioner}).
 * Each partition has a small pool of "hot" records (its first {@code H} records) and a large pool of "cold"
 * records (the others). Each record holds a counter in {@code field0}, initially 0.
 *
 * <p>Each transaction accesses {@code k} distinct records and increments each counter. When
 * {@code calvin.checkinvariant} holds, it first reads them and checks that the sum of their counters is
 * non-negative, writing only if so (see {@link DB#checkAndIncrement}); otherwise it increments them without
 * checking anything (see {@link DB#increment}). A single-partition transaction takes all its
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
 *     <li>swiftpaxos: the shards of SwiftPaxos (Janus), as placed by its YCSB driver</li>
 *   </ul></li>
 *   <li><b>calvin.mpproportion</b>: the proportion of multipartition transactions (default: 0, ignored when
 *   {@code N = 1})</li>
 *   <li><b>calvin.mpspan</b>: the number of partitions of a multipartition transaction (default: 2)</li>
 *   <li><b>calvin.homepartition</b>: the home partition of the transactions, either a partition number or
 *   "random" for a partition chosen uniformly at random for each transaction (default: random)</li>
 *   <li><b>calvin.checkinvariant</b>: whether a transaction reads its records and checks that the sum of their
 *   counters is non-negative before incrementing them (default: false). The check always holds, as the counters
 *   start at 0 and only grow, but it makes the writes of a transaction depend on its reads, which not every
 *   system can execute (e.g., the one-shot transactions of Janus); leave it off to compare such systems
 *   fairly.</li>
 *   <li><b>zeropadding</b>: as in {@link CoreWorkload}</li>
 *   <li><b>calvin.ramp</b>: a comma-separated, increasing list of numbers of active client threads, e.g.
 *   {@code 16,24,36}. Instead of a fixed number of threads, the run goes through one stage per entry, each
 *   lasting {@code calvin.rampstep} seconds, in which only the first {@code n} threads issue transactions
 *   (the others wait), so that a single run yields a whole latency/throughput curve. {@code -threads} must be
 *   at least the last entry, and the run must last long enough ({@code maxexecutiontime}, with an operation
 *   count that is not reached first); the last stage goes on until the end of the run but only its first
 *   step is reported. The warm-up phase, if any, is not ramped. At the end, the per-stage results are
 *   printed as {@code [RAMP], <stage>, <threads>, <seconds>, <ops>, <failed>, <ops/s>, <avg us>, <p50 us>,
 *   <p95 us>, <p99 us>}, where {@code ops} counts the committed transactions.</li>
 *   <li><b>calvin.rampstep</b>: the duration of a stage, in seconds (default: 30)</li>
 *   <li><b>calvin.rampstart</b>: the time (epoch seconds) at which the first stage starts. The clients of a
 *   run, which share it, then go through the stages at the same moments, whatever time each takes to get ready
 *   (their clocks are assumed to be synchronized). It must be later than the end of the warm-up and of the
 *   set-up of the threads. Until it, the threads of the first stage run, unmeasured. Default: the first
 *   transaction after the warm-up, on each client.</li>
 *   <li><b>calvin.rampskip</b>: the seconds at the start of each stage that are left out of its results, as
 *   the closed loop settles (default: 5)</li>
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

  public static final String CHECK_INVARIANT_PROPERTY = "calvin.checkinvariant";
  public static final String CHECK_INVARIANT_PROPERTY_DEFAULT = "false";

  public static final String RAMP_PROPERTY = "calvin.ramp";
  public static final String RAMP_STEP_PROPERTY = "calvin.rampstep";
  public static final String RAMP_STEP_PROPERTY_DEFAULT = "30";
  public static final String RAMP_START_PROPERTY = "calvin.rampstart";
  public static final String RAMP_SKIP_PROPERTY = "calvin.rampskip";
  public static final String RAMP_SKIP_PROPERTY_DEFAULT = "5";

  private int txnSize;
  private double hotFraction;
  private long hotRecords;
  private int partitions;
  private CalvinPartitioner partitioner;
  private double mpProportion;
  private int mpSpan;
  private int homePartition; // -1 when random
  private boolean checkInvariant;
  private int zeroPadding;

  // Ramp of the number of active threads (empty when there is none)
  private int[] ramp = new int[0];
  private long rampStepNs;
  private long rampSkipNs;
  // Whether the stages are being run: with a warm-up, from its end (see resetStopRequested)
  private volatile boolean rampActive;
  // Start of the first stage (System.nanoTime), 0 until the first transaction
  private final AtomicLong rampStart = new AtomicLong(0);
  private final ThreadLocal<Integer> threadId = new ThreadLocal<>();
  private Histogram[] stageLatency = new Histogram[0];
  private LongAdder[] stageFailed = new LongAdder[0];

  @Override
  public void init(Properties p) throws WorkloadException {
    super.init(p);
    initRamp(p);

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
      checkInvariant = Boolean.parseBoolean(
          p.getProperty(CHECK_INVARIANT_PROPERTY, CHECK_INVARIANT_PROPERTY_DEFAULT).trim());
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
    System.out.println("[CONFIG] calvin check invariant: " + checkInvariant);
  }

  private void initRamp(Properties p) throws WorkloadException {
    String spec = p.getProperty(RAMP_PROPERTY, "").trim();
    if (spec.isEmpty()) {
      return;
    }
    try {
      String[] parts = spec.split(",");
      ramp = new int[parts.length];
      for (int i = 0; i < parts.length; i++) {
        ramp[i] = Integer.parseInt(parts[i].trim());
        if (ramp[i] < 1 || (i > 0 && ramp[i] <= ramp[i - 1])) {
          throw new WorkloadException(RAMP_PROPERTY + " must be an increasing list of positive numbers: " + spec);
        }
      }
      rampStepNs = TimeUnit.SECONDS.toNanos(Long.parseLong(p.getProperty(RAMP_STEP_PROPERTY,
          RAMP_STEP_PROPERTY_DEFAULT)));
      rampSkipNs = TimeUnit.SECONDS.toNanos(Long.parseLong(p.getProperty(RAMP_SKIP_PROPERTY,
          RAMP_SKIP_PROPERTY_DEFAULT)));
    } catch (NumberFormatException e) {
      throw new WorkloadException("invalid " + RAMP_PROPERTY + ", " + RAMP_STEP_PROPERTY + " or "
          + RAMP_SKIP_PROPERTY + ": " + e.getMessage());
    }
    if (rampStepNs <= 0 || rampSkipNs < 0 || rampSkipNs >= rampStepNs) {
      throw new WorkloadException(RAMP_SKIP_PROPERTY + " must be in [0, " + RAMP_STEP_PROPERTY + ")");
    }
    String start = p.getProperty(RAMP_START_PROPERTY, "").trim();
    if (!start.isEmpty()) {
      try {
        long untilStartMs = 1000 * Long.parseLong(start) - System.currentTimeMillis();
        rampStart.set(System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(untilStartMs));
      } catch (NumberFormatException e) {
        throw new WorkloadException("invalid " + RAMP_START_PROPERTY + ": " + e.getMessage());
      }
    }
    int threads = Integer.parseInt(p.getProperty("threadcount", "1"));
    if (threads < ramp[ramp.length - 1]) {
      throw new WorkloadException("-threads (" + threads + ") is below the last stage of " + RAMP_PROPERTY
          + " (" + ramp[ramp.length - 1] + ")");
    }
    stageLatency = new Histogram[ramp.length];
    stageFailed = new LongAdder[ramp.length];
    for (int i = 0; i < ramp.length; i++) {
      stageLatency[i] = new ConcurrentHistogram(TimeUnit.MINUTES.toMicros(10), 3);
      stageFailed[i] = new LongAdder();
    }
    // With a warm-up, the stages start once it is over (see resetStopRequested)
    rampActive = Long.parseLong(p.getProperty(WARMUP_EXECUTION_TIME, WARMUP_EXECUTION_TIME_DEFAULT)) <= 0;
    System.out.println("[CONFIG] calvin ramp: " + spec + " threads, " + (rampStepNs / 1000000000L)
        + "s per stage, first " + (rampSkipNs / 1000000000L) + "s of each left out");
  }

  @Override
  public Object initThread(Properties p, int mythreadid, int threadcount) throws WorkloadException {
    threadId.set(mythreadid);
    return super.initThread(p, mythreadid, threadcount);
  }

  @Override
  public void resetStopRequested() {
    super.resetStopRequested();
    // Called at the end of the warm-up phase
    rampActive = ramp.length > 0;
  }

  /** The nanoseconds since the start of the ramp, which the first caller starts unless it is fixed. */
  private long rampOffset(long now) {
    rampStart.compareAndSet(0, now);
    return now - rampStart.get();
  }

  /** The stage of the ramp at time {@code now}: the first one until the ramp starts, the last one after it. */
  private int stageAt(long now) {
    return (int) Math.max(0, Math.min(ramp.length - 1, rampOffset(now) / rampStepNs));
  }

  @Override
  public boolean doTransaction(DB db, Object threadState) {
    if (ramp.length > 0) {
      Integer id = threadId.get();
      // Wait for the stage in which this thread becomes active; during the warm-up, that of the first stage
      while (id != null && id >= ramp[rampActive ? stageAt(System.nanoTime()) : 0]) {
        if (isStopRequested()) {
          return true;
        }
        try {
          Thread.sleep(10);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return true;
        }
      }
    }
    return super.doTransaction(db, threadState);
  }

  /**
   * The stage of the ramp, if any: {@code ramp stage 2/6 (36 threads, 14s in)}, or {@code ramp over} once the
   * last stage has gone on for a whole step (it then runs until the end of the run, unreported).
   */
  @Override
  public String getStatus() {
    long start = rampStart.get();
    if (ramp.length == 0 || !rampActive) {
      return "";
    }
    if (start == 0) {
      return "ramp not started";
    }
    long offset = System.nanoTime() - start;
    if (offset < 0) {
      return "ramp starts in " + (-offset) / 1000000000L + "s";
    }
    int stage = (int) (offset / rampStepNs);
    if (stage >= ramp.length) {
      return "ramp over";
    }
    return "ramp stage " + (stage + 1) + "/" + ramp.length + " (" + ramp[stage] + " threads, "
        + (offset % rampStepNs) / 1000000000L + "s in)";
  }

  @Override
  public void cleanup() throws WorkloadException {
    super.cleanup();
    if (rampStart.get() == 0) {
      return;
    }
    double seconds = (rampStepNs - rampSkipNs) / 1e9;
    for (int i = 0; i < ramp.length; i++) {
      Histogram h = stageLatency[i];
      System.out.println("[RAMP], " + i + ", " + ramp[i] + ", " + seconds + ", " + h.getTotalCount() + ", "
          + stageFailed[i].sum() + ", " + String.format("%.2f", h.getTotalCount() / seconds) + ", "
          + String.format("%.2f", h.getMean()) + ", " + h.getValueAtPercentile(50) + ", "
          + h.getValueAtPercentile(95) + ", " + h.getValueAtPercentile(99));
    }
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

  public boolean isCheckInvariant() {
    return checkInvariant;
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
   * Do a Calvin micro-benchmark transaction: increment the counters of k records, after checking that their sum
   * is non-negative when {@code calvin.checkinvariant} holds.
   */
  @Override
  protected boolean doTransactionReadModifyWrite(DB db) {
    long[] keyNums = nextTransactionKeyNums(ThreadLocalRandom.current());
    String[] keys = new String[keyNums.length];
    for (int i = 0; i < keyNums.length; i++) {
      keys[i] = buildKeyName(keyNums[i]);
    }
    long begin = System.nanoTime();
    boolean ok = (checkInvariant
        ? db.checkAndIncrement(table, keys, DEFAULT_FIELD_NAME)
        : db.increment(table, keys, DEFAULT_FIELD_NAME)).isOk();
    if (rampActive) {
      // Attribute the transaction to the stage in which it started, unless it is a settling one
      long offset = begin - rampStart.get();
      int stage = (int) (offset / rampStepNs);
      if (offset >= 0 && stage < ramp.length && offset % rampStepNs >= rampSkipNs) {
        if (ok) {
          stageLatency[stage].recordValue(Math.min((System.nanoTime() - begin) / 1000,
              stageLatency[stage].getHighestTrackableValue()));
        } else {
          stageFailed[stage].increment();
        }
      }
    }
    return ok;
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
