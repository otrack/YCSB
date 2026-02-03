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

package site.ycsb.workloads;

import site.ycsb.*;
import site.ycsb.generator.*;
import site.ycsb.measurements.Measurements;

import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The YCSB-T workload. This workload implements a closed economy model for testing transactional
 * workloads. It models scenarios where transactions transfer values between accounts while
 * maintaining a constant total sum (e.g., banking applications).
 * 
 * <p>The workload is based on the YCSB+T paper:
 * "YCSB+T: Benchmarking Web-scale Transactional Databases"
 * by Akon Dey, Alan Fekete, Raghunath Nambiar, and Uwe Rohm
 * 
 * <p>Properties to control the workload:
 * <ul>
 * <li><b>table</b>: the name of the database table (default: usertable)
 * <li><b>fieldcount</b>: the number of fields in a record (default: 10)
 * <li><b>fieldlength</b>: the size of each field (default: 100)
 * <li><b>readproportion</b>: proportion of read transactions (default: 0.95)
 * <li><b>updateproportion</b>: proportion of update transactions (default: 0.05)
 * <li><b>insertproportion</b>: proportion of insert transactions (default: 0)
 * <li><b>scanproportion</b>: proportion of scan transactions (default: 0)
 * <li><b>readmodifywriteproportion</b>: proportion of read-modify-write transactions (default: 0)
 * <li><b>requestdistribution</b>: distribution for selecting records - uniform, zipfian, hotspot,
 * exponential, or latest (default: uniform)
 * <li><b>maxscanlength</b>: maximum number of records to scan (default: 1000)
 * <li><b>scanlengthdistribution</b>: distribution for scan lengths - uniform or zipfian (default: uniform)
 * <li><b>insertorder</b>: order for inserting records - ordered or hashed (default: hashed)
 * <li><b>validatebyquery</b>: whether to validate by query or by read operations (default: true)
 * </ul>
 * 
 * <p>Note: All accounts start with an initial balance of 0. The closed economy property
 * ensures that the total sum across all accounts remains 0 after all transactions.
 */
public class ClosedEconomyWorkload extends Workload {

  /**
   * The name of the database table to run queries against.
   */
  public static final String TABLENAME_PROPERTY = "table";

  /**
   * The default name of the database table to run queries against.
   */
  public static final String TABLENAME_PROPERTY_DEFAULT = "usertable";

  /**
   * The name of the property for the number of fields in a record.
   */
  public static final String FIELD_COUNT_PROPERTY = "fieldcount";

  /**
   * Default number of fields in a record.
   */
  public static final String FIELD_COUNT_PROPERTY_DEFAULT = "10";

  /**
   * The name of the property for the field length distribution.
   */
  public static final String FIELD_LENGTH_DISTRIBUTION_PROPERTY = "fieldlengthdistribution";

  /**
   * The default field length distribution.
   */
  public static final String FIELD_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT = "constant";

  /**
   * The name of the property for the length of a field in bytes.
   */
  public static final String FIELD_LENGTH_PROPERTY = "fieldlength";

  /**
   * The default maximum length of a field in bytes.
   */
  public static final String FIELD_LENGTH_PROPERTY_DEFAULT = "100";

  /**
   * The name of a property that specifies the filename containing the field length histogram.
   */
  public static final String FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY = "fieldlengthhistogram";

  /**
   * The default filename containing a field length histogram.
   */
  public static final String FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY_DEFAULT = "hist.txt";

  /**
   * The name of the property for deciding whether to read one field or all fields.
   */
  public static final String READ_ALL_FIELDS_PROPERTY = "readallfields";

  /**
   * The default value for the readallfields property.
   */
  public static final String READ_ALL_FIELDS_PROPERTY_DEFAULT = "true";

  /**
   * The name of the property for deciding whether to write one field or all fields.
   */
  public static final String WRITE_ALL_FIELDS_PROPERTY = "writeallfields";

  /**
   * The default value for the writeallfields property.
   */
  public static final String WRITE_ALL_FIELDS_PROPERTY_DEFAULT = "false";

  /**
   * The name of the property for the proportion of transactions that are reads.
   */
  public static final String READ_PROPORTION_PROPERTY = "readproportion";

  /**
   * The default proportion of transactions that are reads.
   */
  public static final String READ_PROPORTION_PROPERTY_DEFAULT = "0.95";

  /**
   * The name of the property for the proportion of transactions that are updates.
   */
  public static final String UPDATE_PROPORTION_PROPERTY = "updateproportion";

  /**
   * The default proportion of transactions that are updates.
   */
  public static final String UPDATE_PROPORTION_PROPERTY_DEFAULT = "0.05";

  /**
   * The name of the property for the proportion of transactions that are inserts.
   */
  public static final String INSERT_PROPORTION_PROPERTY = "insertproportion";

  /**
   * The default proportion of transactions that are inserts.
   */
  public static final String INSERT_PROPORTION_PROPERTY_DEFAULT = "0.0";

  /**
   * The name of the property for the proportion of transactions that are scans.
   */
  public static final String SCAN_PROPORTION_PROPERTY = "scanproportion";

  /**
   * The default proportion of transactions that are scans.
   */
  public static final String SCAN_PROPORTION_PROPERTY_DEFAULT = "0.0";

  /**
   * The name of the property for the proportion of transactions that are read-modify-write.
   */
  public static final String READMODIFYWRITE_PROPORTION_PROPERTY = "readmodifywriteproportion";

  /**
   * The default proportion of transactions that are read-modify-write.
   */
  public static final String READMODIFYWRITE_PROPORTION_PROPERTY_DEFAULT = "0.0";

  /**
   * The name of the property for the distribution of requests across the keyspace.
   */
  public static final String REQUEST_DISTRIBUTION_PROPERTY = "requestdistribution";

  /**
   * The value of theta used if a zipfian distribution is used to generate the requests.
   */
  public static final String ZIPFIAN_THETA_PROPERTY = "zipfiantheta";

  /**
   * The default value of theta.
   */
  public static final String ZIPFIAN_THETA_PROPERTY_DEFAULT = "0.99";

  /**
   * The default distribution of requests across the keyspace.
   */
  public static final String REQUEST_DISTRIBUTION_PROPERTY_DEFAULT = "uniform";

  /**
   * The name of the property for the max scan length.
   */
  public static final String MAX_SCAN_LENGTH_PROPERTY = "maxscanlength";

  /**
   * The default max scan length.
   */
  public static final String MAX_SCAN_LENGTH_PROPERTY_DEFAULT = "1000";

  /**
   * The name of the property for the scan length distribution.
   */
  public static final String SCAN_LENGTH_DISTRIBUTION_PROPERTY = "scanlengthdistribution";

  /**
   * The default scan length distribution.
   */
  public static final String SCAN_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT = "uniform";

  /**
   * The name of the property for the order to insert records.
   */
  public static final String INSERT_ORDER_PROPERTY = "insertorder";

  /**
   * Default insert order.
   */
  public static final String INSERT_ORDER_PROPERTY_DEFAULT = "hashed";

  /**
   * Percentage data items that constitute the hot set.
   */
  public static final String HOTSPOT_DATA_FRACTION = "hotspotdatafraction";

  /**
   * Default value of the size of the hot set.
   */
  public static final String HOTSPOT_DATA_FRACTION_DEFAULT = "0.2";

  /**
   * Percentage operations that access the hot set.
   */
  public static final String HOTSPOT_OPN_FRACTION = "hotspotopnfraction";

  /**
   * Default value of the percentage operations accessing the hot set.
   */
  public static final String HOTSPOT_OPN_FRACTION_DEFAULT = "0.8";

  /**
   * The name of the property for validating by query.
   */
  public static final String VALIDATE_BY_QUERY_PROPERTY = "validatebyquery";

  /**
   * The default value for the validatebyquery property.
   */
  public static final String VALIDATE_BY_QUERY_PROPERTY_DEFAULT = "true";

  /**
   * Field name prefix.
   */
  public static final String FIELD_NAME = "field";

  /**
   * Default field name for storing the cash value.
   */
  public static final String DEFAULT_FIELD_NAME = "field0";

  protected String table;
  protected long fieldCount;
  protected NumberGenerator fieldLengthGenerator;
  protected boolean readAllFields;
  protected boolean writeAllFields;
  protected NumberGenerator keySequence;
  protected NumberGenerator validationKeySequence;
  protected DiscreteGenerator operationChooser;
  protected NumberGenerator keyChooser;
  protected Generator fieldChooser;
  protected CounterGenerator transactionInsertKeySequence;
  protected NumberGenerator scanLength;
  protected boolean orderedInserts;
  protected long recordCount;
  protected long opCount;
  protected AtomicInteger actualOpCount = new AtomicInteger(0);
  protected Measurements measurements;
  protected boolean validateByQuery;

  private final Hashtable<String, String> operations = new Hashtable<String, String>() {
    {
      put("READ", "TX-READ");
      put("UPDATE", "TX-UPDATE");
      put("INSERT", "TX-INSERT");
      put("SCAN", "TX-SCAN");
      put("READMODIFYWRITE", "TX-READMODIFYWRITE");
    }
  };

  protected static NumberGenerator getFieldLengthGenerator(Properties p) throws WorkloadException {
    NumberGenerator fieldLengthGenerator;
    String fieldLengthDistribution = p.getProperty(
        FIELD_LENGTH_DISTRIBUTION_PROPERTY, FIELD_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT);

    long fieldLength = Long.parseLong(p.getProperty(FIELD_LENGTH_PROPERTY, FIELD_LENGTH_PROPERTY_DEFAULT));
    String fieldLengthHistogram = p.getProperty(
        FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY, FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY_DEFAULT);

    if (fieldLengthDistribution.compareTo("constant") == 0) {
      fieldLengthGenerator = new ConstantIntegerGenerator((int) fieldLength);
    } else if (fieldLengthDistribution.compareTo("uniform") == 0) {
      fieldLengthGenerator = new UniformLongGenerator(1, fieldLength);
    } else if (fieldLengthDistribution.compareTo("zipfian") == 0) {
      fieldLengthGenerator = new ZipfianGenerator(1, fieldLength);
    } else if (fieldLengthDistribution.compareTo("histogram") == 0) {
      try {
        fieldLengthGenerator = new HistogramGenerator(fieldLengthHistogram);
      } catch (IOException e) {
        throw new WorkloadException(
            "Couldn't read field length histogram file: " + fieldLengthHistogram, e);
      }
    } else {
      throw new WorkloadException(
          "Unknown field length distribution \"" + fieldLengthDistribution + "\"");
    }
    return fieldLengthGenerator;
  }

  /**
   * Initialize the scenario. Called once, in the main client thread, before any operations are started.
   */
  @Override
  public void init(Properties p) throws WorkloadException {
    table = p.getProperty(TABLENAME_PROPERTY, TABLENAME_PROPERTY_DEFAULT);

    fieldCount = Long.parseLong(p.getProperty(FIELD_COUNT_PROPERTY, FIELD_COUNT_PROPERTY_DEFAULT));
    fieldLengthGenerator = getFieldLengthGenerator(p);

    double readProportion = Double.parseDouble(
        p.getProperty(READ_PROPORTION_PROPERTY, READ_PROPORTION_PROPERTY_DEFAULT));
    double updateProportion = Double.parseDouble(
        p.getProperty(UPDATE_PROPORTION_PROPERTY, UPDATE_PROPORTION_PROPERTY_DEFAULT));
    double insertProportion = Double.parseDouble(
        p.getProperty(INSERT_PROPORTION_PROPERTY, INSERT_PROPORTION_PROPERTY_DEFAULT));
    double scanProportion = Double.parseDouble(
        p.getProperty(SCAN_PROPORTION_PROPERTY, SCAN_PROPORTION_PROPERTY_DEFAULT));
    double readModifyWriteProportion = Double.parseDouble(
        p.getProperty(READMODIFYWRITE_PROPORTION_PROPERTY, READMODIFYWRITE_PROPORTION_PROPERTY_DEFAULT));

    recordCount = Long.parseLong(p.getProperty(Client.RECORD_COUNT_PROPERTY));

    String requestDistrib = p.getProperty(
        REQUEST_DISTRIBUTION_PROPERTY, REQUEST_DISTRIBUTION_PROPERTY_DEFAULT);
    long maxScanLength = Long.parseLong(
        p.getProperty(MAX_SCAN_LENGTH_PROPERTY, MAX_SCAN_LENGTH_PROPERTY_DEFAULT));
    String scanLengthDistrib = p.getProperty(
        SCAN_LENGTH_DISTRIBUTION_PROPERTY, SCAN_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT);

    long insertStart = Long.parseLong(p.getProperty(INSERT_START_PROPERTY, INSERT_START_PROPERTY_DEFAULT));

    readAllFields = Boolean.parseBoolean(
        p.getProperty(READ_ALL_FIELDS_PROPERTY, READ_ALL_FIELDS_PROPERTY_DEFAULT));
    writeAllFields = Boolean.parseBoolean(
        p.getProperty(WRITE_ALL_FIELDS_PROPERTY, WRITE_ALL_FIELDS_PROPERTY_DEFAULT));

    long insertCount = Integer.parseInt(
        p.getProperty(INSERT_COUNT_PROPERTY, String.valueOf(recordCount - insertStart)));

    if (recordCount < (insertStart + insertCount)) {
      System.err.println("Invalid combination of insertstart, insertcount and recordcount.");
      System.err.println("recordcount must be bigger than insertstart + insertcount.");
      System.exit(-1);
    }

    if (p.getProperty(INSERT_ORDER_PROPERTY, INSERT_ORDER_PROPERTY_DEFAULT).compareTo("hashed") == 0) {
      orderedInserts = false;
    } else if (requestDistrib.compareTo("exponential") == 0) {
      double percentile = Double.parseDouble(
          p.getProperty(ExponentialGenerator.EXPONENTIAL_PERCENTILE_PROPERTY,
              ExponentialGenerator.EXPONENTIAL_PERCENTILE_DEFAULT));
      double frac = Double.parseDouble(
          p.getProperty(ExponentialGenerator.EXPONENTIAL_FRAC_PROPERTY,
              ExponentialGenerator.EXPONENTIAL_FRAC_DEFAULT));
      keyChooser = new ExponentialGenerator(percentile, recordCount * frac);
    } else {
      orderedInserts = true;
    }

    keySequence = new CounterGenerator(insertStart);
    validationKeySequence = new CounterGenerator(insertStart);
    operationChooser = new DiscreteGenerator();

    if (readProportion > 0) {
      operationChooser.addValue(readProportion, "READ");
    }
    if (updateProportion > 0) {
      operationChooser.addValue(updateProportion, "UPDATE");
    }
    if (insertProportion > 0) {
      operationChooser.addValue(insertProportion, "INSERT");
    }
    if (readModifyWriteProportion > 0) {
      operationChooser.addValue(readModifyWriteProportion, "READMODIFYWRITE");
    }

    transactionInsertKeySequence = new CounterGenerator(recordCount);

    if (requestDistrib.compareTo("uniform") == 0) {
      keyChooser = new UniformLongGenerator(0, recordCount - 1);
    } else if (requestDistrib.compareTo("zipfian") == 0) {
      long operationCount = Long.parseLong(p.getProperty(Client.OPERATION_COUNT_PROPERTY));
      long expectedNewKeys = (long) (((double) operationCount) * insertProportion * 2.0);

      double theta = Double.parseDouble(
          p.getProperty(ZIPFIAN_THETA_PROPERTY, ZIPFIAN_THETA_PROPERTY_DEFAULT));

      keyChooser = new ScrambledZipfianGenerator(
          insertStart, insertStart + insertCount + expectedNewKeys, theta);
    } else if (requestDistrib.compareTo("latest") == 0) {
      keyChooser = new SkewedLatestGenerator(transactionInsertKeySequence);
    } else if (requestDistrib.equals("hotspot")) {
      double hotSetFraction = Double.parseDouble(
          p.getProperty(HOTSPOT_DATA_FRACTION, HOTSPOT_DATA_FRACTION_DEFAULT));
      double hotOpnFraction = Double.parseDouble(
          p.getProperty(HOTSPOT_OPN_FRACTION, HOTSPOT_OPN_FRACTION_DEFAULT));
      keyChooser = new HotspotIntegerGenerator(0, recordCount - 1, hotSetFraction, hotOpnFraction);
    } else {
      throw new WorkloadException("Unknown request distribution \"" + requestDistrib + "\"");
    }

    fieldChooser = new UniformLongGenerator(0, fieldCount - 1);

    if (scanLengthDistrib.compareTo("uniform") == 0) {
      scanLength = new UniformLongGenerator(1, maxScanLength);
    } else if (scanLengthDistrib.compareTo("zipfian") == 0) {
      scanLength = new ZipfianGenerator(1, maxScanLength);
    } else {
      throw new WorkloadException("Distribution \"" + scanLengthDistrib + "\" not allowed for scan length");
    }

    measurements = Measurements.getMeasurements();
    validateByQuery = Boolean.parseBoolean(
        p.getProperty(VALIDATE_BY_QUERY_PROPERTY, VALIDATE_BY_QUERY_PROPERTY_DEFAULT));

    System.out.println("[CONFIG] READ_Proportion: " + readProportion);
    System.out.println("[CONFIG] UPDATE_Proportion: " + updateProportion);
    System.out.println("[CONFIG] INSERT_Proportion: " + insertProportion);
    System.out.println("[CONFIG] SCAN_Proportion: " + scanProportion);
    System.out.println("[CONFIG] READMODIFYWRITE_Proportion: " + readModifyWriteProportion);
    System.out.println("[CONFIG] Request_Distribution: " + requestDistrib);
    System.out.println("[CONFIG] Record_Count: " + recordCount);
  }

  /**
   * Build a key name from the key number.
   */
  protected String buildKeyName(long keyNum) {
    return "user" + keyNum;
  }

  /**
   * Build the initial values for a record.
   * All accounts start with an initial balance of 0.
   */
  protected HashMap<String, ByteIterator> buildValues() {
    HashMap<String, ByteIterator> values = new HashMap<>();
    String fieldKey = DEFAULT_FIELD_NAME;
    ByteIterator data = new StringByteIterator("0");
    values.put(fieldKey, data);
    return values;
  }

  /**
   * Build update values for a record.
   */
  protected HashMap<String, ByteIterator> buildUpdate() {
    HashMap<String, ByteIterator> values = new HashMap<>();
    String fieldName = "field" + fieldChooser.nextString();
    ByteIterator data = new RandomByteIterator(fieldLengthGenerator.nextValue().longValue());
    values.put(fieldName, data);
    return values;
  }

  /**
   * Do one insert operation.
   */
  @Override
  public boolean doInsert(DB db, Object threadState) {
    long keyNum = keySequence.nextValue().longValue();
    String dbKey = buildKeyName(keyNum);
    HashMap<String, ByteIterator> values = buildValues();
    if (db.insert(table, dbKey, values).isOk()) {
      actualOpCount.addAndGet(1);
      return true;
    }
    return false;
  }

  /**
   * Do one transaction operation.
   */
  @Override
  public boolean doTransaction(DB db, Object threadState) {
    boolean ret;
    long st = System.nanoTime();

    String op = operationChooser.nextString();

    if (op.equals("READ")) {
      ret = doTransactionRead(db);
    } else if (op.equals("UPDATE")) {
      ret = doTransactionUpdate(db);
    } else if (op.equals("INSERT")) {
      ret = doTransactionInsert(db);
    } else if (op.equals("SCAN")) {
      ret = doTransactionScan(db);
    } else {
      ret = doTransactionReadModifyWrite(db);
    }

    long en = System.nanoTime();
    measurements.measure(operations.get(op), (int) ((en - st) / 1000));
    if (ret) {
      measurements.reportStatus(operations.get(op), Status.OK);
    } else {
      measurements.reportStatus(operations.get(op), Status.ERROR);
    }
    actualOpCount.addAndGet(1);

    return ret;
  }

  /**
   * Get the next key number, ensuring it's within valid range.
   */
  protected long nextKeyNum() {
    long keyNum;
    if (keyChooser instanceof ExponentialGenerator) {
      do {
        keyNum = transactionInsertKeySequence.lastValue() - keyChooser.nextValue().longValue();
      } while (keyNum < 0);
    } else {
      do {
        keyNum = keyChooser.nextValue().longValue();
      } while (keyNum > transactionInsertKeySequence.lastValue());
    }
    return keyNum;
  }

  /**
   * Do a transactional read.
   */
  protected boolean doTransactionRead(DB db) {
    long keyNum = nextKeyNum();
    String keyname = buildKeyName(keyNum);

    HashSet<String> fields = null;
    if (!readAllFields) {
      String fieldName = "field" + fieldChooser.nextString();
      fields = new HashSet<>();
      fields.add(fieldName);
    }

    HashMap<String, ByteIterator> result = new HashMap<>();
    return db.read(table, keyname, fields, result).isOk();
  }

  /**
   * Do a transactional read-modify-write (transfer money between two accounts).
   */
  protected boolean doTransactionReadModifyWrite(DB db) {
    long first = nextKeyNum();
    long second = first;
    while (second == first) {
      second = nextKeyNum();
    }
    // Order keys to prevent deadlocks
    if (first > second) {
      long temp = first;
      first = second;
      second = temp;
    }

    String firstKey = buildKeyName(first);
    String secondKey = buildKeyName(second);

    HashSet<String> fields = new HashSet<>();
    if (!readAllFields) {
      String fieldName = "field" + fieldChooser.nextString();
      fields.add(fieldName);
    } else {
      fields.add(DEFAULT_FIELD_NAME);
    }

    HashMap<String, ByteIterator> firstValues = new HashMap<>();
    HashMap<String, ByteIterator> secondValues = new HashMap<>();

    long st = System.nanoTime();
    if (db.read(table, firstKey, fields, firstValues).isOk() &&
        db.read(table, secondKey, fields, secondValues).isOk()) {
      try {
        long firstAmount = Long.parseLong(firstValues.get(DEFAULT_FIELD_NAME).toString());
        long secondAmount = Long.parseLong(secondValues.get(DEFAULT_FIELD_NAME).toString());

        if (firstAmount > 0) {
          firstAmount--;
          secondAmount++;
        }

        firstValues.put(DEFAULT_FIELD_NAME, new StringByteIterator(Long.toString(firstAmount)));
        secondValues.put(DEFAULT_FIELD_NAME, new StringByteIterator(Long.toString(secondAmount)));

        if (!db.update(table, firstKey, firstValues).isOk() ||
            !db.update(table, secondKey, secondValues).isOk()) {
          return false;
        }

        long en = System.nanoTime();
        measurements.measure(operations.get("READMODIFYWRITE"), (int) (en - st) / 1000);
      } catch (NumberFormatException e) {
        return false;
      }
      return true;
    }
    return false;
  }

  /**
   * Do a transactional scan.
   */
  protected boolean doTransactionScan(DB db) {
    long keyNum = nextKeyNum();
    String startKeyName = buildKeyName(keyNum);
    int len = scanLength.nextValue().intValue();

    HashSet<String> fields = null;
    if (!readAllFields) {
      String fieldName = "field" + fieldChooser.nextString();
      fields = new HashSet<>();
      fields.add(fieldName);
    }

    return db.scan(table, startKeyName, len, fields, new Vector<>()).isOk();
  }

  /**
   * Do a transactional update.
   */
  protected boolean doTransactionUpdate(DB db) {
    long keyNum = nextKeyNum();
    String keyName = buildKeyName(keyNum);

    HashMap<String, ByteIterator> values;
    if (writeAllFields) {
      values = buildValues();
    } else {
      values = buildUpdate();
    }

    return db.update(table, keyName, values).isOk();
  }

  /**
   * Do a transactional insert.
   */
  protected boolean doTransactionInsert(DB db) {
    long keyNum = transactionInsertKeySequence.nextValue();
    String dbKey = buildKeyName(keyNum);
    HashMap<String, ByteIterator> values = buildValues();
    return db.insert(table, dbKey, values).isOk();
  }

  /**
   * Validate by reading all records.
   */
  private long validateByRead(DB db) throws DBException {
    HashSet<String> fields = new HashSet<>();
    fields.add(DEFAULT_FIELD_NAME);
    HashMap<String, ByteIterator> values = new HashMap<>();
    long countedSum = 0;
    long st = System.nanoTime();

    for (long i = 0; i < recordCount; i++) {
      String keyname = buildKeyName(validationKeySequence.nextValue().longValue());
      db.start();
      db.read(table, keyname, fields, values);
      db.commit();
      countedSum += Long.parseLong(values.get(DEFAULT_FIELD_NAME).toString());
    }

    long en = System.nanoTime();
    System.out.println("[VALIDATE] AverageLatency(us): " + (en - st) / 1000);
    return countedSum;
  }

  /**
   * Perform validation of the database after the workload has executed.
   *
   * @return false if the workload left the database in an inconsistent state, true if consistent.
   */
  public boolean validate(DB db) throws WorkloadException {
    long countedSum;
    System.out.println("Validating data...");
    try {
      if (validateByQuery) {
        System.err.println("Validating by query execution...");
        countedSum = db.validate();
        System.out.println("[VALIDATE] METHOD: QUERY");
      } else {
        System.err.println("Validating by read operations...");
        countedSum = validateByRead(db);
        System.out.println("[VALIDATE] METHOD: READ-OPERATIONS");
      }
    } catch (Exception e) {
      throw new WorkloadException(e);
    }

    if (countedSum == -1) {
      System.err.println("No validation done due to no validate() implementation in this database client.");
      System.out.println("[VALIDATE] STATUS: FAILED-NO IMPLEMENTATION");
      return false;
    }

    long count = actualOpCount.intValue();
    // In a closed economy starting with all 0 balances, the sum should always be 0
    final long expectedSum = 0;
    double anomalyScore = Math.abs((expectedSum - countedSum) / (1.0 * Math.max(count, 1)));

    if (countedSum != expectedSum) {
      printValidationMessages(System.err, "FAILED", expectedSum, countedSum, count, anomalyScore);
      printValidationMessages(System.out, "FAILED", expectedSum, countedSum, count, anomalyScore);
      return false;
    } else {
      printValidationMessages(System.out, "SUCCESS", expectedSum, countedSum, count, anomalyScore);
      return true;
    }
  }

  /**
   * Print validation messages.
   */
  private static void printValidationMessages(PrintStream stream, String status, long expectedSum,
                                              long countedSum, long count, double anomalyScore) {
    stream.println("[VALIDATE] STATUS: " + status);
    stream.println("[VALIDATE] EXPECTED SUM: " + expectedSum);
    stream.println("[VALIDATE] COUNTED SUM: " + countedSum);
    stream.println("[VALIDATE] ACTUAL OPERATIONS: " + count);
    stream.println("[VALIDATE] ANOMALY SCORE: " + anomalyScore);
  }
}
