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

/**
 * Tiga, Calvin, and Detock client binding for YCSB.
 */
public class TigaClient extends DB {

  private YcsbClient client;

  public static final String CONFIG_PROPERTY = "tiga.config";
  public static final String MODE_PROPERTY = "tiga.mode";

  @Override
  public void init() throws DBException {
    String configPath = getProperties().getProperty(CONFIG_PROPERTY);
    if (configPath == null) {
      throw new DBException("Required property '" + CONFIG_PROPERTY + "' not specified");
    }

    String mode = getProperties().getProperty(MODE_PROPERTY, "tiga");

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
}
