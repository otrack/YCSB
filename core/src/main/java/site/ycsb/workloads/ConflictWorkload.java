package site.ycsb.workloads;

import site.ycsb.WorkloadException;
import site.ycsb.generator.SequentialGenerator;

import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

/**
 * For some parameter theta, each client thread chooses a common key with chance theta,
 * and a (thread-)private one otherwise.
 */
public class ConflictWorkload extends CoreWorkload {

  public static final String THETA_PROPERTY = "conflict.theta";
  public static final String THETA_DEFAULT = "0.5";
  public static final String SHIFT_PROPERTY = "conflict.shift";
  public static final String SHIFT_DEFAULT = "0";

  private double theta;
  private long shift;

  private SequentialGenerator localKeyGenerator;
  private ThreadLocal<Long> clientLocalKey;
  private ThreadLocalRandom random;

  private Long commonKey;

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

    try {
      shift = Integer.parseInt(p.getProperty(SHIFT_PROPERTY, String.valueOf(SHIFT_DEFAULT)));
    } catch (NumberFormatException e) {
      throw new WorkloadException("Invalid value for " + SHIFT_PROPERTY + ": " + shift);
    }

    if (shift > recordcount || shift < 0) {
      throw new WorkloadException("Invalid value for " + SHIFT_PROPERTY + ": " + shift);
    }

    long start = Long.parseLong(p.getProperty(INSERT_START_PROPERTY, INSERT_START_PROPERTY_DEFAULT));
    long count = Integer.parseInt(p.getProperty(INSERT_COUNT_PROPERTY, String.valueOf(recordcount - start)));

    localKeyGenerator = new SequentialGenerator(start, start + count - 1);
    commonKey = localKeyGenerator.nextLong();
    clientLocalKey = new ThreadLocal<>();
  }

  @Override
  public Object initThread(Properties p, int mythreadid, int threadcount) throws WorkloadException {
    super.initThread(p, mythreadid, threadcount);
    if (threadcount >= recordcount) {
      throw new WorkloadException("Invalid thread count" + threadcount);
    }
    clientLocalKey.set(localKeyGenerator.nextLong() + shift);
    random = ThreadLocalRandom.current();
    return null;
  }

  @Override
  public long nextKeynum() {
    if (random.nextDouble() < theta) {
      return commonKey;
    }
    return clientLocalKey.get();
  }

  public long commonKeynum() {
    return commonKey;
  }

}
