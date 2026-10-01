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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static org.testng.Assert.*;

public class TestCalvinWorkload {

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

  @Test
  public void testWorkload() throws Exception {
    BasicTransactionalDB.clearData();

    final int recordCount = 1000;
    final int opsPerClient = 10000;
    final int numClients = 4;
    final Properties p = properties(recordCount, opsPerClient * numClients);
    p.setProperty(CalvinWorkload.HOT_RECORDS_PROPERTY, "10");

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
