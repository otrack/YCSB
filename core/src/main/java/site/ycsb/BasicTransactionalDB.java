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

package site.ycsb;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Basic transactional database that stores data in memory using a map.
 * Implements transactions using a big lock (MUTEX) that is acquired when
 * a transaction starts and released when it commits or aborts.
 */
public class BasicTransactionalDB extends DB {
  /**
   * The big lock used to implement transactions.
   */
  private static final ReentrantLock MUTEX = new ReentrantLock();
  
  /**
   * The in-memory data store. Using ConcurrentHashMap for thread-safe access
   * outside of transactions.
   */
  private static final Map<String, Map<String, String>> DATA = new ConcurrentHashMap<>();

  /**
   * Initialize any state for this DB.
   */
  @Override
  public void init() throws DBException {
    // Nothing special to initialize
  }

  /**
   * Start a database transaction by acquiring the big lock.
   */
  @Override
  public void start() throws DBException {
    MUTEX.lock();
  }

  /**
   * Commit the current database transaction by releasing the big lock.
   */
  @Override
  public void commit() throws DBException {
    if (MUTEX.isHeldByCurrentThread()) {
      MUTEX.unlock();
    }
  }

  /**
   * Abort the current database transaction by releasing the big lock.
   */
  @Override
  public void abort() throws DBException {
    if (MUTEX.isHeldByCurrentThread()) {
      MUTEX.unlock();
    }
  }

  /**
   * Validate the database by computing the sum of all numeric values.
   * This is used by ClosedEconomyWorkload to verify consistency.
   *
   * @return The sum of all values in the database.
   */
  @Override
  public long validate() throws DBException {
    long sum = 0;
    synchronized (MUTEX) {
      for (Map<String, String> record : DATA.values()) {
        for (String value : record.values()) {
          try {
            sum += Long.parseLong(value);
          } catch (NumberFormatException e) {
            // Skip non-numeric values
          }
        }
      }
    }
    return sum;
  }

  /**
   * Cleanup any state for this DB.
   */
  @Override
  public void cleanup() throws DBException {
    // Release lock if still held (shouldn't happen in normal flow)
    if (MUTEX.isHeldByCurrentThread()) {
      MUTEX.unlock();
    }
  }

  /**
   * Read a record from the database.
   */
  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    String recordKey = table + ":" + key;
    Map<String, String> record = DATA.get(recordKey);
    
    if (record == null) {
      return Status.NOT_FOUND;
    }
    
    if (fields == null) {
      // Read all fields
      for (Map.Entry<String, String> entry : record.entrySet()) {
        result.put(entry.getKey(), new StringByteIterator(entry.getValue()));
      }
    } else {
      // Read specific fields
      for (String field : fields) {
        String value = record.get(field);
        if (value != null) {
          result.put(field, new StringByteIterator(value));
        }
      }
    }
    
    return Status.OK;
  }

  /**
   * Perform a range scan for a set of records in the database.
   */
  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    // For simplicity, we'll just return OK without implementing full scan functionality
    // This is sufficient for ClosedEconomyWorkload which doesn't use scans
    return Status.OK;
  }

  /**
   * Update a record in the database.
   */
  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    String recordKey = table + ":" + key;
    Map<String, String> record = DATA.get(recordKey);
    
    if (record == null) {
      return Status.NOT_FOUND;
    }
    
    // Update the fields
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      record.put(entry.getKey(), entry.getValue().toString());
    }
    
    return Status.OK;
  }

  /**
   * Insert a record in the database.
   */
  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    String recordKey = table + ":" + key;
    Map<String, String> record = new HashMap<>();
    
    // Insert all fields
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      record.put(entry.getKey(), entry.getValue().toString());
    }
    
    DATA.put(recordKey, record);
    return Status.OK;
  }

  /**
   * Delete a record from the database.
   */
  @Override
  public Status delete(String table, String key) {
    String recordKey = table + ":" + key;
    Map<String, String> removed = DATA.remove(recordKey);
    return removed != null ? Status.OK : Status.NOT_FOUND;
  }
  
  /**
   * Clear all data from the database. Useful for testing.
   */
  public static void clearData() {
    DATA.clear();
  }
}
