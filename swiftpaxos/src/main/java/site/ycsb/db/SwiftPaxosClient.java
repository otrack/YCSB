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

package site.ycsb.db;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import com.imdea.sw.*;

/**
 * This is a client implementation for SwiftPaxos.
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
    boolean leaderless = LEADERLESS_PROPERTY_DEFAULT.equals(getProperties().getProperty(LEADERLESS_PROPERTY));
    boolean fast = FAST_PROPERTY_DEFAULT.equals(getProperties().getProperty(FAST_PROPERTY));
    String master = getProperties().containsKey(MADDR_PROPERTY) ?
        getProperties().getProperty(MADDR_PROPERTY) : MADDR_PROPERTY_DEFAULT;
    int port = getProperties().containsKey(MPORT_PROPERTY) ?
        Integer.parseInt(getProperties().getProperty(MPORT_PROPERTY)) : MPORT_PROPERTY_DEFAULT;

    this.verbose = VERBOSE_PROPERTY_DEFAULT.equals(getProperties().getProperty(VERBOSE_PROPERTY));
    this.handle = new SwiftPaxos(BUFFER_SIZE, master, port, fast, leaderless, verbose);

    try {
      handle.connect();
    } catch (Exception e) {
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
      byte[] data = marshal(StringByteIterator.getStringMap(values));
      handle.write(hash(key), data);
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
      StringByteIterator.putAllAsByteIterators(result, unmarshal(data));
      if (verbose) {
        System.out.println("READ: " + key + " -> " + result);
      }
      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return Status.ERROR;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    // FIXME
    result = new Vector<>();
    HashMap<String, ByteIterator> item = new HashMap<>();
    try {
      final byte[] data = handle.scan(hash(startkey), recordcount);
      StringByteIterator.putAllAsByteIterators(item, unmarshal(data));
      result.add(item);
      if (verbose) {
        System.out.println("SCAN: " + startkey + "[0-" + recordcount + "] -> " + result.toString());
      }
      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return Status.ERROR;
  }


  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    // FIXME
    return insert(table, key, values);
  }

  @Override
  public Status delete(String table, String key) {
    // FIXME
    return insert(table, key, null);
  }

  // adapted from String.hashCode()
  public static long hash(String string) {
    long h = 1125899906842597L; // prime
    int len = string.length();

    for (int i = 0; i < len; i++) {
      h = 31 * h + string.charAt(i);
    }
    return h;
  }

  private static byte[] marshal(Map<String, String> map) throws IOException {
    ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
    ObjectOutputStream out = new ObjectOutputStream(byteOut);
    out.writeObject(map);
    return byteOut.toByteArray();
  }

  private static Map<String, String> unmarshal(byte[] data) throws IOException, ClassNotFoundException {
    ByteArrayInputStream byteIn = new ByteArrayInputStream(data);
    ObjectInputStream in = new ObjectInputStream(byteIn);
    return (Map<String, String>) in.readObject();
  }

}
