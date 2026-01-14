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
import site.ycsb.Client;
import site.ycsb.WorkloadException;
import site.ycsb.measurements.Measurements;

import java.util.Properties;

import static org.testng.Assert.*;

public class TestClosedEconomyWorkload {

  @Test
  public void testInit() throws WorkloadException {
    final Properties p = new Properties();
    p.setProperty(Client.RECORD_COUNT_PROPERTY, "1000");
    p.setProperty(Client.OPERATION_COUNT_PROPERTY, "1000");
    p.setProperty(ClosedEconomyWorkload.TOTAL_CASH_PROPERTY, "1000000");
    p.setProperty(ClosedEconomyWorkload.READ_PROPORTION_PROPERTY, "0.5");
    p.setProperty(ClosedEconomyWorkload.READMODIFYWRITE_PROPORTION_PROPERTY, "0.5");

    Measurements.setProperties(p);
    final ClosedEconomyWorkload workload = new ClosedEconomyWorkload();
    workload.init(p);

    assertNotNull(workload);
  }

  @Test
  public void testInitWithIncompatibleTotalCash() throws WorkloadException {
    final Properties p = new Properties();
    p.setProperty(Client.RECORD_COUNT_PROPERTY, "1000");
    p.setProperty(Client.OPERATION_COUNT_PROPERTY, "1000");
    p.setProperty(ClosedEconomyWorkload.TOTAL_CASH_PROPERTY, "999"); // Not divisible by 1000
    p.setProperty(ClosedEconomyWorkload.READ_PROPORTION_PROPERTY, "0.5");
    p.setProperty(ClosedEconomyWorkload.READMODIFYWRITE_PROPORTION_PROPERTY, "0.5");

    Measurements.setProperties(p);
    final ClosedEconomyWorkload workload = new ClosedEconomyWorkload();
    workload.init(p);

    // Should still initialize, but with adjusted values
    assertNotNull(workload);
  }

  @Test
  public void testBuildKeyName() throws WorkloadException {
    final Properties p = new Properties();
    p.setProperty(Client.RECORD_COUNT_PROPERTY, "1000");
    p.setProperty(Client.OPERATION_COUNT_PROPERTY, "1000");
    p.setProperty(ClosedEconomyWorkload.TOTAL_CASH_PROPERTY, "1000000");

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
    p.setProperty(ClosedEconomyWorkload.TOTAL_CASH_PROPERTY, "1000000");

    Measurements.setProperties(p);
    final ClosedEconomyWorkload workload = new ClosedEconomyWorkload();
    workload.init(p);

    java.util.HashMap<String, site.ycsb.ByteIterator> values = workload.buildValues();
    assertNotNull(values);
    assertTrue(values.containsKey(ClosedEconomyWorkload.DEFAULT_FIELD_NAME));
    assertEquals(values.get(ClosedEconomyWorkload.DEFAULT_FIELD_NAME).toString(), "1000");
  }
}
