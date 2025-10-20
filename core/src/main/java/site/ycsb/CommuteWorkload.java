package site.ycsb;

import java.util.Properties;
import java.util.Random;

/**
 * CommuteWorkload
 *
 * Workload semantics:
 * - For each client id (clientid property) there is one private key unique to that client.
 * - There is one common key shared by all clients.
 * - On each operation, with probability theta the client accesses the common key,
 *   otherwise it accesses its private key.
 *
 * Configuration properties (defaults in parentheses):
 * - commute.theta (0.5) : probability to access the common key on each operation.
 * - commute.commonkey (user_common) : name of the common key.
 * - commute.privatekeyprefix (userpriv_) : prefix for per-client private keys (full key = prefix + clientid).
 *
 * Notes:
 * - This class intentionally only implements the key-selection behavior (init + chooseKey).
 *   It does not override CoreWorkload.doTransaction here to avoid tying to a specific
 *   CoreWorkload internal API (which can vary across YCSB branches). If you prefer the
 *   workload to force the key used by CoreWorkload, copy CoreWorkload.doTransaction into
 *   this class and replace its key-selection with chooseKey().
 */
public class CommuteWorkload extends CoreWorkload {

  public static final String THETA_PROPERTY = "commute.theta";
  public static final String THETA_DEFAULT = "0.5";

  public static final String COMMON_KEY_PROPERTY = "commute.commonkey";
  public static final String COMMON_KEY_DEFAULT = "user_common";

  public static final String PRIVATE_KEY_PREFIX_PROPERTY = "commute.privatekeyprefix";
  public static final String PRIVATE_KEY_PREFIX_DEFAULT = "userpriv_";

  private double theta;
  private String commonKey;
  private String privateKeyPrefix;

  private Random random;
  private int clientId = 0;

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

    try {
      clientId = Integer.parseInt(p.getProperty("clientid", "0"));
    } catch (NumberFormatException e) {
      clientId = 0;
    }

    // seed randomness using current time and client id for variability across clients
    random = new Random(System.currentTimeMillis() + clientId);
  }

  /**
   * Choose the key for the next operation.
   *
   * With probability theta return the common key, otherwise return the client's private key.
   *
   * This method is public so unit tests can exercise it. If you prefer it protected,
   * adjust the tests accordingly.
   */
  public String chooseKey() {
    if (random.nextDouble() < theta) {
      return commonKey;
    } else {
      return privateKeyPrefix + clientId;
    }
  }

  /**
   * Convenience getter used by tests or other tools.
   */
  public double getTheta() {
    return theta;
  }

  public String getCommonKey() {
    return commonKey;
  }

  public String getPrivateKeyPrefix() {
    return privateKeyPrefix;
  }

  public int getClientId() {
    return clientId;
  }
}
