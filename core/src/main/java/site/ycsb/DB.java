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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

/**
 * A layer for accessing a database to be benchmarked. Each thread in the client
 * will be given its own instance of whatever DB class is to be used in the test.
 * This class should be constructed using a no-argument constructor, so we can
 * load it dynamically. Any argument-based initialization should be
 * done by init().
 *
 * Note that YCSB does not make any use of the return codes returned by this class.
 * Instead, it keeps a count of the return values and presents them to the user.
 *
 * The semantics of methods such as insert, update and delete vary from database
 * to database.  In particular, operations may or may not be durable once these
 * methods commit, and some systems may return 'success' regardless of whether
 * or not a tuple with a matching key existed before the call.  Rather than dictate
 * the exact semantics of these methods, we recommend you either implement them
 * to match the database's default semantics, or the semantics of your 
 * target application.  For the sake of comparison between experiments we also 
 * recommend you explain the semantics you chose when presenting performance results.
 */
public abstract class DB {
  /**
   * Properties for configuring this DB.
   */
  private Properties properties = new Properties();

  /**
   * Set the properties for this DB.
   */
  public void setProperties(Properties p) {
    properties = p;

  }

  /**
   * Get the set of properties for this DB.
   */
  public Properties getProperties() {
    return properties;
  }

  /**
   * Initialize any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  public void init() throws DBException {
  }

  /**
   * Start a database transaction.
   * This is called before a transaction begins.
   */
  public void start() throws DBException {
  }

  /**
   * Commit the current database transaction.
   */
  public void commit() throws DBException {
  }

  /**
   * Abort the current database transaction.
   */
  public void abort() throws DBException {
  }

  /**
   * Validate the current database transaction.
   * This method is used by workloads that need to validate consistency,
   * such as the ClosedEconomyWorkload which checks that the total sum of values remains constant.
   *
   * @return The sum of all values in the database, or -1 if validation is not supported.
   */
  public long validate() throws DBException {
    return -1;
  }

  /**
   * Transfer operation for the closed economy workload.
   * This method implements a transfer of value from one account to another.
   * The default implementation uses an interactive approach with separate read and update operations.
   * Database implementations that support transactions should override this method to use
   * a proper two-phase transaction (BEGIN, reads, updates, COMMIT).
   *
   * @param table The name of the table
   * @param key1 The key of the first account (source)
   * @param key2 The key of the second account (destination)
   * @param field The field name containing the balance
   * @return The result of the operation.
   */
  public Status transfer(String table, String key1, String key2, String field) {
    // Default interactive implementation
    HashMap<String, ByteIterator> result1 = new HashMap<>();
    HashMap<String, ByteIterator> result2 = new HashMap<>();
    
    Set<String> fields = new java.util.HashSet<>();
    fields.add(field);
    
    // Read both accounts
    Status status = read(table, key1, fields, result1);
    if (!status.isOk()) {
      return status;
    }
    
    status = read(table, key2, fields, result2);
    if (!status.isOk()) {
      return status;
    }
    
    try {
      // Parse balances
      long balance1 = Long.parseLong(result1.get(field).toString());
      long balance2 = Long.parseLong(result2.get(field).toString());
      
      // Transfer 1 unit from account 1 to account 2
      balance1--;
      balance2++;
      
      // Create update values
      HashMap<String, ByteIterator> update1 = new HashMap<>();
      update1.put(field, new StringByteIterator(Long.toString(balance1)));
      
      HashMap<String, ByteIterator> update2 = new HashMap<>();
      update2.put(field, new StringByteIterator(Long.toString(balance2)));
      
      // Update both accounts
      status = update(table, key1, update1);
      if (!status.isOk()) {
        return status;
      }
      
      status = update(table, key2, update2);
      return status;
      
    } catch (NumberFormatException e) {
      return Status.ERROR;
    }
  }

  /**
   * Swap operation for the swap workload.
   * This method implements a cyclic rotation of field values among S users (keys):
   * for each i in 0..S-1, user[keys[(i+1) % S]].field is set to user[keys[i]].field.
   * The default implementation uses an interactive approach with separate read and update operations.
   * Database implementations that support transactions should override this method.
   *
   * @param table The name of the table
   * @param keys  The keys of the S users to swap among
   * @param field The field name containing the value to rotate
   * @return The result of the operation.
   */
  public Status swap(String table, String[] keys, String field) {
    int s = keys.length;
    Set<String> fields = new java.util.HashSet<>();
    fields.add(field);

    // Read all values
    String[] values = new String[s];
    for (int i = 0; i < s; i++) {
      HashMap<String, ByteIterator> result = new HashMap<>();
      Status status = read(table, keys[i], fields, result);
      if (!status.isOk()) {
        return status;
      }
      ByteIterator val = result.get(field);
      values[i] = val != null ? val.toString() : "0";
    }

    // Write: keys[(i+1) % s].field = keys[i].field  (cyclic rotation)
    for (int i = 0; i < s; i++) {
      HashMap<String, ByteIterator> update = new HashMap<>();
      update.put(field, new StringByteIterator(values[i]));
      Status status = update(table, keys[(i + 1) % s], update);
      if (!status.isOk()) {
        return status;
      }
    }

    return Status.OK;
  }

  /**
   * Cleanup any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  public void cleanup() throws DBException {
  }

  /**
   * Read a record from the database. Each field/value pair from the result will be stored in a HashMap.
   *
   * @param table The name of the table
   * @param key The record key of the record to read.
   * @param fields The list of fields to read, or null for all of them
   * @param result A HashMap of field/value pairs for the result
   * @return The result of the operation.
   */
  public abstract Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result);

  /**
   * Perform a range scan for a set of records in the database. Each field/value pair from the result will be stored
   * in a HashMap.
   *
   * @param table The name of the table
   * @param startkey The record key of the first record to read.
   * @param recordcount The number of records to read
   * @param fields The list of fields to read, or null for all of them
   * @param result A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
   * @return The result of the operation.
   */
  public abstract Status scan(String table, String startkey, int recordcount, Set<String> fields,
                              Vector<HashMap<String, ByteIterator>> result);

  /**
   * Update a record in the database. Any field/value pairs in the specified values HashMap will be written into the
   * record with the specified record key, overwriting any existing values with the same field name.
   *
   * @param table The name of the table
   * @param key The record key of the record to write.
   * @param values A HashMap of field/value pairs to update in the record
   * @return The result of the operation.
   */
  public abstract Status update(String table, String key, Map<String, ByteIterator> values);

  /**
   * Insert a record in the database. Any field/value pairs in the specified values HashMap will be written into the
   * record with the specified record key.
   *
   * @param table The name of the table
   * @param key The record key of the record to insert.
   * @param values A HashMap of field/value pairs to insert in the record
   * @return The result of the operation.
   */
  public abstract Status insert(String table, String key, Map<String, ByteIterator> values);

  /**
   * Delete a record from the database.
   *
   * @param table The name of the table
   * @param key The record key of the record to delete.
   * @return The result of the operation.
   */
  public abstract Status delete(String table, String key);
}
