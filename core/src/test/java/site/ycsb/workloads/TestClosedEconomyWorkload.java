/**
 * Copyright (c) 2016 YCSB contributors. All rights reserved.
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

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.ArrayList;
import java.util.List;

import static org.testng.Assert.*;

public class TestClosedEconomyWorkload {

  @Test
  public void testInit() throws WorkloadException {
    final Properties p = new Properties();
    p.setProperty(Client.RECORD_COUNT_PROPERTY, "1000");
    p.setProperty(Client.OPERATION_COUNT_PROPERTY, "1000");
    p.setProperty(ClosedEconomyWorkload.READ_PROPORTION_PROPERTY, "0.5");
    p.setProperty(ClosedEconomyWorkload.READMODIFYWRITE_PROPORTION_PROPERTY, "0.5");

    Measurements.setProperties(p);
    final ClosedEconomyWorkload workload = new ClosedEconomyWorkload();
    workload.init(p);

    assertNotNull(workload);
  }

  @Test
  public void testBuildKeyName() throws WorkloadException {
    final Properties p = new Properties();
    p.setProperty(Client.RECORD_COUNT_PROPERTY, "1000");
    p.setProperty(Client.OPERATION_COUNT_PROPERTY, "1000");

    Measurements.setProperties(p);
    final ClosedEconomyWorkload workload = new ClosedEconomyWorkload();
    workload.init(p);

    String keyName = workload.buildKeyName(5);
    assertEquals(keyName, "user5");
  }

  @Test
  public void testBuildValues() throws WorkloadException {
    final Properties p = new Properties();
    p.setProperty(Client.RECORD_COUNT_PROPERTY, "1000");
    p.setProperty(Client.OPERATION_COUNT_PROPERTY, "1000");

    Measurements.setProperties(p);
    final ClosedEconomyWorkload workload = new ClosedEconomyWorkload();
    workload.init(p);

    java.util.HashMap<String, site.ycsb.ByteIterator> values = workload.buildValues();
    assertNotNull(values);
    assertTrue(values.containsKey(ClosedEconomyWorkload.DEFAULT_FIELD_NAME));
    assertEquals(values.get(ClosedEconomyWorkload.DEFAULT_FIELD_NAME).toString(), "0");
  }

  @Test
  public void testWorkload() throws Exception {
    // Clear any existing data
    BasicTransactionalDB.clearData();
    
    // Setup properties for the test
    final Properties p = new Properties();
    final int recordCount = 1000;
    final int opsPerClient = 100000;
    final int numClients = 4;
    
    p.setProperty(Client.RECORD_COUNT_PROPERTY, String.valueOf(recordCount));
    p.setProperty(Client.OPERATION_COUNT_PROPERTY, String.valueOf(opsPerClient * numClients));
    p.setProperty(ClosedEconomyWorkload.READ_PROPORTION_PROPERTY, "0.0");
    p.setProperty(ClosedEconomyWorkload.UPDATE_PROPORTION_PROPERTY, "0.0");
    p.setProperty(ClosedEconomyWorkload.INSERT_PROPORTION_PROPERTY, "0.0");
    p.setProperty(ClosedEconomyWorkload.READMODIFYWRITE_PROPORTION_PROPERTY, "1.0");
    p.setProperty(ClosedEconomyWorkload.REQUEST_DISTRIBUTION_PROPERTY, "uniform");
    
    Measurements.setProperties(p);
    
    // Initialize workload
    final ClosedEconomyWorkload workload = new ClosedEconomyWorkload();
    workload.init(p);
    
    // Load initial data
    System.out.println("Loading initial data...");
    BasicTransactionalDB loadDB = new BasicTransactionalDB();
    loadDB.setProperties(p);
    loadDB.init();
    
    for (int i = 0; i < recordCount; i++) {
      workload.doInsert(loadDB, null);
    }
    loadDB.cleanup();
    
    // Verify initial sum is 0 (all accounts start with 0 balance)
    BasicTransactionalDB validateDB = new BasicTransactionalDB();
    validateDB.setProperties(p);
    validateDB.init();
    long initialSum = validateDB.validate();
    validateDB.cleanup();
    
    System.out.println("Initial sum: " + initialSum);
    assertEquals(initialSum, 0L, "Initial sum should be 0 (all accounts start with 0 balance)");
    
    // Run workload with multiple clients
    System.out.println("Running workload with " + numClients + " clients...");
    CountDownLatch latch = new CountDownLatch(numClients);
    List<Thread> threads = new ArrayList<>();
    
    for (int i = 0; i < numClients; i++) {
      BasicTransactionalDB db = new BasicTransactionalDB();
      db.setProperties(p);
      
      ClientThread clientThread = new ClientThread(
          db,
          true, // dotransactions
          workload,
          p,
          opsPerClient,
          0, // no target rate
          latch
      );
      clientThread.setThreadId(i);
      clientThread.setThreadCount(numClients);
      
      Thread thread = new Thread(clientThread);
      threads.add(thread);
      thread.start();
    }
    
    // Wait for all clients to complete
    latch.await();
    
    for (Thread thread : threads) {
      thread.join();
    }
    
    System.out.println("Workload complete. Validating final sum...");
    
    // Validate final sum is still 0 (closed economy property)
    BasicTransactionalDB finalValidateDB = new BasicTransactionalDB();
    finalValidateDB.setProperties(p);
    finalValidateDB.init();
    long finalSum = finalValidateDB.validate();
    finalValidateDB.cleanup();
    
    System.out.println("Final sum: " + finalSum);
    assertEquals(finalSum, 0L, "Final sum should be 0 (closed economy property maintains sum)");
    
    // Clear data for next test
    BasicTransactionalDB.clearData();
  }
}
