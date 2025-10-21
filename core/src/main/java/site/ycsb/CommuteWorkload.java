package site.ycsb;

import site.ycsb.workloads.CoreWorkload;

import java.util.HashMap;
import java.util.Properties;
import java.util.Random;

/**
 * For some parameter theta, each client thread chooses a common key with chance theta,
 * and a (thread-)private one otherwise.
 */
public class CommuteWorkload extends CoreWorkload {

  public static final String THETA_PROPERTY = "commute.theta";
  public static final String THETA_DEFAULT = "0.5";

  public static final String COMMON_KEY_PROPERTY = "commute.commonkey";
  public static final String COMMON_KEY_DEFAULT = "user_common";

  public static final String PRIVATE_KEY_PREFIX_PROPERTY = "commute.privatekeyprefix";
  public static final String PRIVATE_KEY_PREFIX_DEFAULT = "user_private_";

  private double theta;
  private String commonKey;
  private String privateKeyPrefix;

  private Random random;

  @Override
  public void init(Properties p) throws WorkloadException {
    // Keep CoreWorkload initialization so other properties (recordcount, fields, etc.) are set
    super.init(p);

    String thetaStr = p.getProperty(THETA_PROPERTY, THETA_DEFAULT);
    try {
      theta = Double.parseDouble(thetaStr);
    } catch (NumberFormatException e) {
      throw new WorkloadException("Invalid value for " + THETA_PROPERTY + ": " + thetaStr);
    }
    if (theta < 0.0 || theta > 1.0) {
      throw new WorkloadException("Invalid value for " + THETA_PROPERTY + ": " + theta);
    }

    commonKey = p.getProperty(COMMON_KEY_PROPERTY, COMMON_KEY_DEFAULT);
    privateKeyPrefix = p.getProperty(PRIVATE_KEY_PREFIX_PROPERTY, PRIVATE_KEY_PREFIX_DEFAULT);

    random = new Random(System.nanoTime());
  }

  @Override
  public boolean doInsert(DB db, Object threadstate) {
    return doTransaction(db, threadstate);
  }

  @Override
  public boolean doTransaction(DB db, Object threadstate) {
    HashMap<String, ByteIterator> value = new HashMap<String, ByteIterator>();
    value.put(chooseKey(), new RandomByteIterator(1));
    db.insert(null, chooseKey(), value);
    return true;
  }

  public String chooseKey() {
    if (random.nextDouble() < theta) {
      return commonKey;
    } else {
      return privateKeyPrefix + Thread.currentThread().getName();
    }
  }

}
