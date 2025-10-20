package site.ycsb;

import org.junit.Test;
import static org.junit.Assert.*;

import java.util.Properties;

/**
 * Unit tests for CommuteWorkload key-selection behavior.
 *
 * These tests exercise:
 *  - theta = 1.0 -> always common key
 *  - theta = 0.0 -> always private key (prefix + clientid)
 *  - theta ~ 0.25 -> approximate sampling frequency
 *  - invalid theta values -> WorkloadException
 *
 * Tests are in-package so they can access package-visible or public members.
 */
public class CommuteWorkloadTest {

  @Test
  public void testAlwaysCommonKey() throws Exception {
    CommuteWorkload w = new CommuteWorkload();
    Properties p = new Properties();
    p.setProperty(CommuteWorkload.THETA_PROPERTY, "1.0");
    p.setProperty(CommuteWorkload.COMMON_KEY_PROPERTY, "common_key");
    p.setProperty(CommuteWorkload.PRIVATE_KEY_PREFIX_PROPERTY, "priv_");
    p.setProperty("clientid", "7");
    w.init(p);

    for (int i = 0; i < 100; i++) {
      String k = w.chooseKey();
      assertEquals("Expected the common key when theta=1.0", "common_key", k);
    }
  }

  @Test
  public void testAlwaysPrivateKey() throws Exception {
    CommuteWorkload w = new CommuteWorkload();
    Properties p = new Properties();
    p.setProperty(CommuteWorkload.THETA_PROPERTY, "0.0");
    p.setProperty(CommuteWorkload.COMMON_KEY_PROPERTY, "common_key");
    p.setProperty(CommuteWorkload.PRIVATE_KEY_PREFIX_PROPERTY, "priv_");
    p.setProperty("clientid", "3");
    w.init(p);

    String expected = "priv_3";
    for (int i = 0; i < 100; i++) {
      String k = w.chooseKey();
      assertEquals("Expected the private key when theta=0.0", expected, k);
    }
  }

  @Test
  public void testThetaApproximation() throws Exception {
    CommuteWorkload w = new CommuteWorkload();
    Properties p = new Properties();
    final double theta = 0.25;
    p.setProperty(CommuteWorkload.THETA_PROPERTY, Double.toString(theta));
    p.setProperty(CommuteWorkload.COMMON_KEY_PROPERTY, "common_key");
    p.setProperty(CommuteWorkload.PRIVATE_KEY_PREFIX_PROPERTY, "priv_");
    p.setProperty("clientid", "12");
    w.init(p);

    final int N = 10000;
    int commonCount = 0;
    for (int i = 0; i < N; i++) {
      if ("common_key".equals(w.chooseKey())) {
        commonCount++;
      }
    }
    double frac = (double) commonCount / N;
    double tol = 0.03; // ~3% tolerance for N=10000
    assertTrue(
        String.format("Observed fraction %.4f not within [%f, %f] of theta %f", frac, theta - tol, theta + tol, theta),
        frac >= (theta - tol) && frac <= (theta + tol));
  }

  @Test(expected = WorkloadException.class)
  public void testInvalidThetaThrows() throws Exception {
    CommuteWorkload w = new CommuteWorkload();
    Properties p = new Properties();
    p.setProperty(CommuteWorkload.THETA_PROPERTY, "1.5"); // invalid >1.0
    w.init(p);
  }
}