package site.ycsb.db;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import com.tiga.ycsb.YcsbClient;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Tiga, Calvin, and Detock client binding for YCSB.
 */
public class TigaClient extends DB {

  private YcsbClient client;

  public static final String CONFIG_PROPERTY = "tiga.config";
  public static final String MODE_PROPERTY = "tiga.mode";

  public static final String OPENLOOP_RATE_PROPERTY = "tiga.openloop.rate";
  public static final String OPENLOOP_MAX_OUTSTANDING_PROPERTY = "tiga.openloop.maxOutstanding";
  public static final String OPENLOOP_SEC_PROPERTY = "tiga.openloop.sec";
  public static final String OPENLOOP_SWAPSIZE_PROPERTY = "tiga.openloop.swapSize";
  public static final String OPENLOOP_RECORDCOUNT_PROPERTY = "tiga.openloop.recordCount";
  public static final String OPENLOOP_ARRIVAL_PROPERTY = "tiga.openloop.arrival";

  private static final AtomicBoolean OPEN_LOOP_STARTED = new AtomicBoolean(false);

  @Override
  public void init() throws DBException {
    String configPath = getProperties().getProperty(CONFIG_PROPERTY);
    if (configPath == null) {
      throw new DBException("Required property '" + CONFIG_PROPERTY + "' not specified");
    }

    String mode = getProperties().getProperty(MODE_PROPERTY, "tiga");

    String rateStr = getProperties().getProperty(OPENLOOP_RATE_PROPERTY);
    if (rateStr != null) {
      if (OPEN_LOOP_STARTED.compareAndSet(false, true)) {
        try {
          long rate = Long.parseLong(rateStr.trim());
          long maxOutstanding = Long.parseLong(getProperties()
              .getProperty(OPENLOOP_MAX_OUTSTANDING_PROPERTY, Long.toString(Math.max(1, rate * 2))).trim());
          long runSec = Long.parseLong(getProperties().getProperty(OPENLOOP_SEC_PROPERTY, "60").trim());
          long swapSize = Long.parseLong(getProperties()
              .getProperty(OPENLOOP_SWAPSIZE_PROPERTY, getProperties().getProperty("swap.s", "3")).trim());
          long recordCount = Long.parseLong(getProperties()
              .getProperty(OPENLOOP_RECORDCOUNT_PROPERTY, "0").trim());
          String arrival = getProperties().getProperty(OPENLOOP_ARRIVAL_PROPERTY, "deterministic").trim();

          client = new YcsbClient(configPath, mode);
          client.setOpenLoopArrival("poisson".equalsIgnoreCase(arrival) ? 1 : 0);
          int ret = client.runSwapOpenLoop(rate, maxOutstanding, runSec, recordCount, swapSize);
          if (ret != 0) {
            throw new DBException("Open-loop pump failed to start (returned " + ret + ")");
          }
        } catch (NumberFormatException e) {
          throw new DBException("Invalid open-loop numeric property: " + e.getMessage(), e);
        }
      }
      return;
    }

    try {
      client = new YcsbClient(configPath, mode);
    } catch (Exception e) {
      throw new DBException("Failed to initialize Tiga native client wrapper: " + e.getMessage(), e);
    }
  }

  @Override
  public void cleanup() throws DBException {
    if (client != null) {
      client.close();
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    HashMap<String, String> rawResult = new HashMap<>();
    try {
      if (fields == null) {
        fields = new java.util.HashSet<>();
        int fieldCount = Integer.parseInt(getProperties().getProperty("fieldcount", "10"));
        for (int i = 0; i < fieldCount; i++) {
          fields.add("field" + i);
        }
      }
      int ret = client.read(key, fields, rawResult);
      if (ret == 0) {
        // Return YCSB expected format: Map<String, ByteIterator>
        StringByteIterator.putAllAsByteIterators(result, rawResult);
        return Status.OK;
      }
    } catch (Exception e) {
      System.err.println("Error executing native read: " + e.getMessage());
    }
    return Status.ERROR;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    try {
      int ret = client.update(key, StringByteIterator.getStringMap(values));
      if (ret == 0) {
        return Status.OK;
      }
    } catch (Exception e) {
      System.err.println("Error executing native update: " + e.getMessage());
    }
    return Status.ERROR;
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    try {
      int ret = client.insert(key, StringByteIterator.getStringMap(values));
      if (ret == 0) {
        return Status.OK;
      }
    } catch (Exception e) {
      System.err.println("Error executing native insert: " + e.getMessage());
    }
    return Status.ERROR;
  }

  @Override
  public Status delete(String table, String key) {
    // Core database implementation does not support key deletion
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
                    Vector<HashMap<String, ByteIterator>> result) {
    // Ordered range scans are not currently supported by coordinator APIs
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status transfer(String table, String key1, String key2, String field) {
    try {
      int ret = client.transfer(key1, key2, field);
      if (ret == 0) {
        return Status.OK;
      }
    } catch (Exception e) {
      System.err.println("Error executing native transfer: " + e.getMessage());
    }
    return Status.ERROR;
  }

  @Override
  public Status swap(String table, String[] keys, String field) {
    if (OPEN_LOOP_STARTED.get()) {
      return Status.OK;
    }
    try {
      int ret = client.swap(keys, field);
      if (ret == 0) {
        return Status.OK;
      }
    } catch (Exception e) {
      System.err.println("Error executing native swap: " + e.getMessage());
    }
    return Status.ERROR;
  }

  @Override
  public Status checkAndIncrement(String table, String[] keys, String field) {
    try {
      int ret = client.checkAndIncrement(keys, field);
      if (ret == 0) {
        return Status.OK;
      }
    } catch (Exception e) {
      System.err.println("Error executing native checkAndIncrement: " + e.getMessage());
    }
    return Status.ERROR;
  }
}
