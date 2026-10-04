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
package site.ycsb.workloads;

import org.testng.annotations.Test;
import site.ycsb.*;
import site.ycsb.measurements.Measurements;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static org.testng.Assert.*;

public class TestCalvinWorkload {

  private static final String[] ALL_PARTITIONERS = {
      CalvinPartitioner.MOD, CalvinPartitioner.RANGE, CalvinPartitioner.MURMUR3, CalvinPartitioner.TIGA};

  private static Properties properties(int recordCount, int operationCount) {
    final Properties p = new Properties();
    p.setProperty(Client.RECORD_COUNT_PROPERTY, String.valueOf(recordCount));
    p.setProperty(Client.OPERATION_COUNT_PROPERTY, String.valueOf(operationCount));
    p.setProperty(ClosedEconomyWorkload.READ_PROPORTION_PROPERTY, "0.0");
    p.setProperty(ClosedEconomyWorkload.UPDATE_PROPORTION_PROPERTY, "0.0");
    p.setProperty(ClosedEconomyWorkload.READMODIFYWRITE_PROPORTION_PROPERTY, "1.0");
    Measurements.setProperties(p);
    return p;
  }

  @Test
  public void testKeyGeneration() throws WorkloadException {
    final Properties p = properties(10000, 1000);
    p.setProperty(CalvinWorkload.HOT_RECORDS_PROPERTY, "20");
    p.setProperty(CalvinWorkload.HOT_FRACTION_PROPERTY, "0.3");
    final CalvinWorkload workload = new CalvinWorkload();
    workload.init(p);

    Random random = new Random(42);
    for (int t = 0; t < 1000; t++) {
      long[] keys = workload.nextTransactionKeyNums(random);
      assertEquals(keys.length, 10);
      Set<Long> distinct = new HashSet<>();
      int hot = 0;
      for (int i = 0; i < keys.length; i++) {
        assertTrue(keys[i] >= 0 && keys[i] < 10000);
        assertTrue(i == 0 || keys[i - 1] < keys[i], "keys must be sorted and distinct");
        distinct.add(keys[i]);
        if (keys[i] < 20) {
          hot++;
        }
      }
      assertEquals(distinct.size(), 10);
      assertEquals(hot, 3);
    }
  }

  @Test
  public void testFractionalHotCount() throws WorkloadException {
    final Properties p = properties(10000, 1000);
    p.setProperty(CalvinWorkload.HOT_FRACTION_PROPERTY, "0.25");
    final CalvinWorkload workload = new CalvinWorkload();
    workload.init(p);

    Random random = new Random(7);
    int total = 0;
    int n = 10000;
    for (int t = 0; t < n; t++) {
      int hot = 0;
      for (long k : workload.nextTransactionKeyNums(random)) {
        if (k < workload.getHotRecords()) {
          hot++;
        }
      }
      assertTrue(hot == 2 || hot == 3);
      total += hot;
    }
    assertEquals(total / (double) n, 2.5, 0.05);
  }

  @Test
  public void testContentionIndex() throws WorkloadException {
    final Properties p = properties(10000, 1000);
    p.setProperty(CalvinWorkload.CONTENTION_INDEX_PROPERTY, "0.01");
    final CalvinWorkload workload = new CalvinWorkload();
    workload.init(p);
    assertEquals(workload.getHotRecords(), 100L);
  }

  @Test(expectedExceptions = WorkloadException.class)
  public void testHotPoolTooSmall() throws WorkloadException {
    final Properties p = properties(10000, 1000);
    p.setProperty(CalvinWorkload.HOT_RECORDS_PROPERTY, "2");
    p.setProperty(CalvinWorkload.HOT_FRACTION_PROPERTY, "0.5");
    new CalvinWorkload().init(p);
  }

  /**
   * The hot records of each partition.
   */
  private static Set<Long> hotKeys(CalvinWorkload workload) {
    Set<Long> hot = new HashSet<>();
    CalvinPartitioner partitioner = workload.getPartitioner();
    for (int q = 0; q < partitioner.getPartitions(); q++) {
      for (long i = 0; i < workload.getHotRecords(); i++) {
        hot.add(partitioner.keyNum(q, i));
      }
    }
    return hot;
  }

  private static void checkPartitioner(CalvinPartitioner partitioner, long recordCount) {
    boolean[] seen = new boolean[(int) recordCount];
    long total = 0;
    for (int q = 0; q < partitioner.getPartitions(); q++) {
      long previous = -1;
      for (long i = 0; i < partitioner.size(q); i++) {
        long k = partitioner.keyNum(q, i);
        assertTrue(k > previous, "records of a partition must be in increasing key order");
        previous = k;
        assertFalse(seen[(int) k]);
        seen[(int) k] = true;
        assertEquals(partitioner.partitionOf(k), q);
      }
      total += partitioner.size(q);
    }
    assertEquals(total, recordCount);
  }

  @Test
  public void testPartitioners() {
    for (String name : ALL_PARTITIONERS) {
      for (int n : new int[]{1, 3, 4}) {
        checkPartitioner(CalvinPartitioner.create(name, n, 10007, k -> "user" + k), 10007);
      }
    }
    CalvinPartitioner mod = CalvinPartitioner.create(CalvinPartitioner.MOD, 4, 100, k -> "user" + k);
    assertEquals(mod.keyNum(1, 3), 13L);
    CalvinPartitioner range = CalvinPartitioner.create(CalvinPartitioner.RANGE, 4, 100, k -> "user" + k);
    assertEquals(range.keyNum(1, 3), 28L);
  }

  @Test
  public void testMurmur3Token() {
    // Reference values from org.apache.cassandra.utils.MurmurHash (Murmur3Partitioner)
    assertEquals(CalvinPartitioner.token("user0"), -2208156872511035598L);
    assertEquals(CalvinPartitioner.token("user1"), -1727604350198519072L);
    assertEquals(CalvinPartitioner.token("user42"), -7655210805937031185L);
    assertEquals(CalvinPartitioner.token("user999999"), -2660273607411015980L);
    assertEquals(CalvinPartitioner.token("user0000123456"), -8638059774879058191L);
    assertEquals(CalvinPartitioner.token("a"), -8839064797231613815L);
    assertEquals(CalvinPartitioner.token(""), 0L);
    assertEquals(CalvinPartitioner.token("user1234567890123"), -3214745186126657514L);
    assertEquals(CalvinPartitioner.token("été-key-longer-than-16"), 2497290201445556141L);
  }

  @Test
  public void testTigaKey() {
    // Reference values from (std::hash<std::string>(key) & 0x7FFFFFFF) % 2000005 with g++/libstdc++ (x86_64)
    assertEquals(CalvinPartitioner.stdHash("user0".getBytes(StandardCharsets.UTF_8)), 524465452692400574L);
    assertEquals(CalvinPartitioner.stdHash("user1".getBytes(StandardCharsets.UTF_8)), -6201104696919348498L);
    assertEquals(CalvinPartitioner.tigaKey("user0"), 1594483);
    assertEquals(CalvinPartitioner.tigaKey("user1"), 47560);
    assertEquals(CalvinPartitioner.tigaKey("user42"), 69971);
    assertEquals(CalvinPartitioner.tigaKey("user999999"), 725948);
    assertEquals(CalvinPartitioner.tigaKey("user1234567"), 869881);
    assertEquals(CalvinPartitioner.tigaKey("a"), 926590);
    assertEquals(CalvinPartitioner.tigaKey(""), 1913597);
    assertEquals(CalvinPartitioner.tigaKey("user12345678901234"), 928094);
    assertEquals(CalvinPartitioner.tigaKey("\u00e9t\u00e9"), 62222);
  }

  @Test
  public void testTokenSlices() {
    for (int n : new int[]{1, 2, 3, 4, 7, 8}) {
      // Each node owns (previous token, its token]
      long previous = CalvinPartitioner.initialToken(n - 1, n);
      for (int q = 0; q < n; q++) {
        long token = CalvinPartitioner.initialToken(q, n);
        assertEquals(CalvinPartitioner.slice(token, n), q);
        if (n > 1) {
          assertEquals(CalvinPartitioner.slice(previous + 1, n), q);
        }
        previous = token;
      }
      assertEquals(CalvinPartitioner.slice(Long.MIN_VALUE, n), 0);
      assertEquals(CalvinPartitioner.slice(Long.MAX_VALUE, n), n - 1);
    }
  }

  @Test
  public void testRangeKeysArePadded() throws WorkloadException {
    final Properties p = properties(20000, 1000);
    p.setProperty(CalvinWorkload.PARTITIONS_PROPERTY, "4");
    p.setProperty(CalvinWorkload.PARTITIONER_PROPERTY, CalvinPartitioner.RANGE);
    final CalvinWorkload workload = new CalvinWorkload();
    workload.init(p);
    assertEquals(workload.buildKeyName(42), "user00042");
    assertEquals(workload.buildKeyName(19999), "user19999");
    assertTrue(workload.buildKeyName(4999).compareTo(workload.buildKeyName(5000)) < 0);
  }

  @Test
  public void testSinglePartitionTransactions() throws WorkloadException {
    for (String name : ALL_PARTITIONERS) {
      final Properties p = properties(40000, 1000);
      p.setProperty(CalvinWorkload.PARTITIONS_PROPERTY, "4");
      p.setProperty(CalvinWorkload.PARTITIONER_PROPERTY, name);
      p.setProperty(CalvinWorkload.HOT_RECORDS_PROPERTY, "50");
      final CalvinWorkload workload = new CalvinWorkload();
      workload.init(p);
      Set<Long> hotKeys = hotKeys(workload);
      assertEquals(hotKeys.size(), 200);

      Random random = new Random(3);
      int[] homes = new int[4];
      for (int t = 0; t < 4000; t++) {
        long[] keys = workload.nextTransactionKeyNums(random);
        assertEquals(keys.length, 10);
        Set<Integer> parts = new HashSet<>();
        int hot = 0;
        for (int i = 0; i < keys.length; i++) {
          assertTrue(i == 0 || keys[i - 1] < keys[i], "keys must be sorted and distinct");
          parts.add(workload.getPartitioner().partitionOf(keys[i]));
          if (hotKeys.contains(keys[i])) {
            hot++;
          }
        }
        assertEquals(parts.size(), 1, name);
        assertEquals(hot, 1, name);
        homes[parts.iterator().next()]++;
      }
      for (int q = 0; q < 4; q++) {
        assertEquals(homes[q], 1000, 150);
      }
    }
  }

  @Test
  public void testMultiPartitionTransactions() throws WorkloadException {
    for (String name : ALL_PARTITIONERS) {
      final Properties p = properties(40000, 1000);
      p.setProperty(CalvinWorkload.PARTITIONS_PROPERTY, "4");
      p.setProperty(CalvinWorkload.PARTITIONER_PROPERTY, name);
      p.setProperty(CalvinWorkload.HOT_RECORDS_PROPERTY, "50");
      p.setProperty(CalvinWorkload.MP_PROPORTION_PROPERTY, "0.3");
      final CalvinWorkload workload = new CalvinWorkload();
      workload.init(p);
      Set<Long> hotKeys = hotKeys(workload);

      Random random = new Random(5);
      int n = 10000;
      int mp = 0;
      for (int t = 0; t < n; t++) {
        long[] keys = workload.nextTransactionKeyNums(random);
        assertEquals(keys.length, 10);
        int[] perPartition = new int[4];
        int[] hotPerPartition = new int[4];
        for (long k : keys) {
          int q = workload.getPartitioner().partitionOf(k);
          perPartition[q]++;
          if (hotKeys.contains(k)) {
            hotPerPartition[q]++;
          }
        }
        int span = 0;
        for (int q = 0; q < 4; q++) {
          if (perPartition[q] > 0) {
            span++;
            // One hot record on each partition (1 hot and 4 cold, or 1 hot and 9 cold)
            assertEquals(hotPerPartition[q], 1, name);
          }
        }
        assertTrue(span == 1 || span == 2, name);
        if (span == 2) {
          mp++;
          for (int q = 0; q < 4; q++) {
            assertTrue(perPartition[q] == 0 || perPartition[q] == 5, name);
          }
        }
      }
      assertEquals(mp / (double) n, 0.3, 0.02, name);
    }
  }

  @Test
  public void testHomePartition() throws WorkloadException {
    final Properties p = properties(40000, 1000);
    p.setProperty(CalvinWorkload.PARTITIONS_PROPERTY, "4");
    p.setProperty(CalvinWorkload.MP_PROPORTION_PROPERTY, "1.0");
    p.setProperty(CalvinWorkload.MP_SPAN_PROPERTY, "2");
    p.setProperty(CalvinWorkload.HOME_PARTITION_PROPERTY, "2");
    final CalvinWorkload workload = new CalvinWorkload();
    workload.init(p);
    Random random = new Random(11);
    for (int t = 0; t < 1000; t++) {
      int[] parts = workload.nextTransactionPartitions(random);
      assertEquals(parts.length, 2);
      assertEquals(parts[0], 2);
      assertNotEquals(parts[1], 2);
    }
  }

  @Test
  public void testSinglePartitionIgnoresMultiPartition() throws WorkloadException {
    final Properties p = properties(10000, 1000);
    p.setProperty(CalvinWorkload.MP_PROPORTION_PROPERTY, "1.0");
    final CalvinWorkload workload = new CalvinWorkload();
    workload.init(p);
    assertEquals(workload.nextTransactionPartitions(new Random(1)).length, 1);
  }

  @Test(expectedExceptions = WorkloadException.class)
  public void testSpanLargerThanPartitions() throws WorkloadException {
    final Properties p = properties(10000, 1000);
    p.setProperty(CalvinWorkload.PARTITIONS_PROPERTY, "2");
    p.setProperty(CalvinWorkload.MP_PROPORTION_PROPERTY, "0.5");
    p.setProperty(CalvinWorkload.MP_SPAN_PROPERTY, "3");
    new CalvinWorkload().init(p);
  }

  @Test(expectedExceptions = WorkloadException.class)
  public void testColdPoolTooSmall() throws WorkloadException {
    final Properties p = properties(1000, 1000);
    p.setProperty(CalvinWorkload.PARTITIONS_PROPERTY, "4");
    p.setProperty(CalvinWorkload.HOT_RECORDS_PROPERTY, "245");
    new CalvinWorkload().init(p);
  }

  @Test
  public void testWorkload() throws Exception {
    runWorkload(new Properties());
  }

  @Test
  public void testPartitionedWorkload() throws Exception {
    Properties extra = new Properties();
    extra.setProperty(CalvinWorkload.PARTITIONS_PROPERTY, "4");
    extra.setProperty(CalvinWorkload.PARTITIONER_PROPERTY, CalvinPartitioner.RANGE);
    extra.setProperty(CalvinWorkload.MP_PROPORTION_PROPERTY, "0.5");
    runWorkload(extra);
  }

  private void runWorkload(Properties extra) throws Exception {
    BasicTransactionalDB.clearData();

    final int recordCount = 1000;
    final int opsPerClient = 10000;
    final int numClients = 4;
    final Properties p = properties(recordCount, opsPerClient * numClients);
    p.setProperty(CalvinWorkload.HOT_RECORDS_PROPERTY, "10");
    p.putAll(extra);

    final CalvinWorkload workload = new CalvinWorkload();
    workload.init(p);

    BasicTransactionalDB loadDB = new BasicTransactionalDB();
    loadDB.setProperties(p);
    loadDB.init();
    for (int i = 0; i < recordCount; i++) {
      workload.doInsert(loadDB, null);
    }
    assertEquals(loadDB.validate(), 0L);
    loadDB.cleanup();

    CountDownLatch latch = new CountDownLatch(numClients);
    List<Thread> threads = new ArrayList<>();
    for (int i = 0; i < numClients; i++) {
      BasicTransactionalDB db = new BasicTransactionalDB();
      db.setProperties(p);
      ClientThread clientThread = new ClientThread(db, true, workload, p, opsPerClient, 0, latch);
      clientThread.setThreadId(i);
      clientThread.setThreadCount(numClients);
      Thread thread = new Thread(clientThread);
      threads.add(thread);
      thread.start();
    }
    latch.await();
    for (Thread thread : threads) {
      thread.join();
    }

    BasicTransactionalDB validateDB = new BasicTransactionalDB();
    validateDB.setProperties(p);
    validateDB.init();
    long finalSum = validateDB.validate();
    validateDB.cleanup();
    // Every transaction commits and increments exactly txnsize counters
    assertEquals(finalSum, (long) workload.getTxnSize() * opsPerClient * numClients);

    BasicTransactionalDB.clearData();
  }
}
