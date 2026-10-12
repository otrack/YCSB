/**
 * Copyright (c) 2012-2016 YCSB contributors. All rights reserved.
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

package site.ycsb.db.swiftpaxos;

import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.workloads.CalvinPartitioner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Vector;

import com.imdea.sw.SwiftPaxos;

/**
 * This is a client implementation for SwiftPaxos.
 * <p>
 * A record is stored as a row of the column model of the SwiftPaxos state machine, under the key {@link #hash(String)}
 * of its name. Single-record operations are single commands. {@link #transfer} and {@link #checkAndIncrement} are
 * one-shot transactions of increments: the SwiftPaxos client executes them atomically if the protocol supports
 * transactions (Janus), and their increments one after the other otherwise. {@link #swap} keeps the default,
 * interactive, implementation: its writes depend on its reads, which one-shot transactions cannot express.
 */
public class SwiftPaxosClient extends DB {

  private static final int BUFFER_SIZE = 1024 * 1024; // 1 MB

  public static final String LEADERLESS_PROPERTY = "leaderless";
  public static final String LEADERLESS_PROPERTY_DEFAULT = "false";

  public static final String FAST_PROPERTY = "fast";
  public static final String FAST_PROPERTY_DEFAULT = "false";

  public static final String VERBOSE_PROPERTY = "verbose";
  public static final String VERBOSE_PROPERTY_DEFAULT = "false";

  public static final String MADDR_PROPERTY = "maddr";
  public static final String MADDR_PROPERTY_DEFAULT = "127.0.0.1";

  public static final String MPORT_PROPERTY = "mport";
  public static final int MPORT_PROPERTY_DEFAULT = 7087;

  private SwiftPaxos handle;
  private boolean verbose;

  public SwiftPaxosClient() {
    SwiftPaxos.initNative();
  }

  @Override
  public void init() throws DBException {
    boolean leaderless = Boolean.parseBoolean(
        getProperties().getProperty(LEADERLESS_PROPERTY, LEADERLESS_PROPERTY_DEFAULT));
    boolean fast = Boolean.parseBoolean(getProperties().getProperty(FAST_PROPERTY, FAST_PROPERTY_DEFAULT));
    String master = getProperties().getProperty(MADDR_PROPERTY, MADDR_PROPERTY_DEFAULT);
    int port = getProperties().containsKey(MPORT_PROPERTY) ?
        Integer.parseInt(getProperties().getProperty(MPORT_PROPERTY)) : MPORT_PROPERTY_DEFAULT;

    this.verbose = Boolean.parseBoolean(getProperties().getProperty(VERBOSE_PROPERTY, VERBOSE_PROPERTY_DEFAULT));
    this.handle = new SwiftPaxos(BUFFER_SIZE, master, port, fast, leaderless, verbose);

    try {
      handle.connect();
    } catch (Exception e) {
      e.printStackTrace();
      throw new DBException("cannot connect to SwiftPaxos", e);
    }
  }

  @Override
  public void cleanup() {
    handle.disconnect();
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    try {
      handle.write(hash(key), SwiftPaxos.encodeRow(columns(values)));
      if (verbose) {
        System.out.println("INSERT: " + key + " -> " + values);
      }
      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return Status.ERROR;
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    try {
      final byte[] data = handle.read(hash(key));
      if (data.length == 0) {
        return Status.NOT_FOUND;
      }
      fill(SwiftPaxos.decodeRow(data), fields, result);
      if (verbose) {
        System.out.println("READ: " + key + " -> " + result);
      }
      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return Status.ERROR;
  }

  /**
   * Scans the keys in [hash(startkey), hash(startkey) + recordcount]. As keys are hashed, this range does not follow
   * the order of the record names.
   */
  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    try {
      final byte[] data = handle.scan(hash(startkey), recordcount);
      for (Map<String, byte[]> row : SwiftPaxos.decodeRows(data)) {
        HashMap<String, ByteIterator> item = new HashMap<>();
        fill(row, fields, item);
        result.add(item);
      }
      if (verbose) {
        System.out.println("SCAN: " + startkey + "[0-" + recordcount + "] -> " + result);
      }
      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return Status.ERROR;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    try {
      handle.execute(SwiftPaxos.Op.update(hash(key), columns(values)));
      if (verbose) {
        System.out.println("UPDATE: " + key + " -> " + values);
      }
      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return Status.ERROR;
  }

  @Override
  public Status delete(String table, String key) {
    try {
      handle.execute(SwiftPaxos.Op.delete(hash(key)));
      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return Status.ERROR;
  }

  /**
   * Moves one unit from the counter {@code field} of {@code key1} to that of {@code key2}, in a one-shot transaction.
   */
  @Override
  public Status transfer(String table, String key1, String key2, String field) {
    try {
      handle.execute(SwiftPaxos.Op.incr(hash(key1), field, -1), SwiftPaxos.Op.incr(hash(key2), field, 1));
      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return Status.ERROR;
  }

  /**
   * Increments the counter {@code field} of each record in a one-shot transaction. The counters of the Calvin
   * micro-benchmark start at 0 and are only incremented, so the constraint check of the benchmark (the sum of the
   * counters is non-negative) always holds: it is not evaluated, as the writes of a one-shot transaction cannot depend
   * on its reads.
   */
  @Override
  public Status checkAndIncrement(String table, String[] keys, String field) {
    try {
      List<SwiftPaxos.Op> ops = new ArrayList<>(keys.length);
      for (String key : keys) {
        ops.add(SwiftPaxos.Op.incr(hash(key), field, 1));
      }
      handle.execute(ops);
      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return Status.ERROR;
  }

  /** The SwiftPaxos key of a record (see {@link CalvinPartitioner#swiftPaxosKey(String)}). */
  public static long hash(String string) {
    return CalvinPartitioner.swiftPaxosKey(string);
  }

  private static Map<String, byte[]> columns(Map<String, ByteIterator> values) {
    Map<String, byte[]> cols = new TreeMap<>();
    for (Map.Entry<String, ByteIterator> e : values.entrySet()) {
      cols.put(e.getKey(), e.getValue().toArray());
    }
    return cols;
  }

  private static void fill(Map<String, byte[]> row, Set<String> fields, Map<String, ByteIterator> result) {
    for (Map.Entry<String, byte[]> e : row.entrySet()) {
      if (fields == null || fields.contains(e.getKey())) {
        result.put(e.getKey(), new ByteArrayByteIterator(e.getValue()));
      }
    }
  }

}
