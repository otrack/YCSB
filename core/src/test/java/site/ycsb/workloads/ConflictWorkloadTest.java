package site.ycsb.workloads;

import site.ycsb.Client;
import site.ycsb.WorkloadException;
import org.testng.annotations.Test;
import site.ycsb.measurements.Measurements;

import java.util.Properties;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.AssertJUnit.assertTrue;

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
public class ConflictWorkloadTest {

  @Test
  public void testAlwaysCommonKey() throws Exception {
    final Properties p = getUTProperties();
    Measurements.setProperties(p);
    p.setProperty(ConflictWorkload.THETA_PROPERTY, "1.0");
    ConflictWorkload w = new ConflictWorkload();
    w.init(p);
    w.initThread(p,1,1);
    for (int i = 0; i < 100; i++) {
      long k = w.nextKeynum();
      assertEquals(k, w.commonKeynum(), "Expected the common key when theta=1.0");
    }
  }

  @Test
  public void testAlwaysPrivateKey() throws Exception {
    final Properties p = getUTProperties();
    Measurements.setProperties(p);
    p.setProperty(ConflictWorkload.THETA_PROPERTY, "0.0");
    ConflictWorkload w = new ConflictWorkload();
    w.init(p);
    w.initThread(p,1,1);
    long commonKey = w.commonKeynum();
    for (int i = 0; i < 100; i++) {
      long k = w.nextKeynum();
      assertNotEquals(k, commonKey, "Expected the private key when theta=0.0; got " + k+", expected=" + commonKey);
    }
  }

  @Test
  public void testThetaApproximation() throws Exception {
    final Properties p = getUTProperties();
    Measurements.setProperties(p);
    final double theta = 0.25;
    p.setProperty(ConflictWorkload.THETA_PROPERTY, Double.toString(theta));
    ConflictWorkload w = new ConflictWorkload();
    w.init(p);
    w.initThread(p,1,1);

    final int N = 10000;
    int commonCount = 0;
    for (int i = 0; i < N; i++) {
      if (w.nextKeynum() == w.commonKeynum()) commonCount++;
    }
    double frac = (double) commonCount / N;
    double tol = 0.03; // ~3% tolerance for N=10000
    assertTrue(
        String.format("Observed fraction %.4f not within [%f, %f] of theta %f", frac, theta - tol, theta + tol, theta),
        frac >= (theta - tol) && frac <= (theta + tol));
  }

  @Test(expectedExceptions = WorkloadException.class)
  public void testInvalidThetaThrows() throws Exception {
    Properties p = getUTProperties();
    Measurements.setProperties(p);
    p.setProperty(ConflictWorkload.THETA_PROPERTY, "1.5"); // invalid >1.0
    ConflictWorkload w = new ConflictWorkload();
    w.init(p);
  }

  @Test(expectedExceptions = WorkloadException.class)
  public void testInvalidRerordCount() throws Exception {
    Properties p = getUTProperties();
    Measurements.setProperties(p);
    p.setProperty(ConflictWorkload.THETA_PROPERTY, "1.0"); // invalid >1.0
    ConflictWorkload w = new ConflictWorkload();
    w.init(p);
    w.initThread(p,1,2);
  }

  /** Helper method that generates unit testing defaults for the properties map */
  private Properties getUTProperties() {
    final Properties p = new Properties();
    p.put(Client.RECORD_COUNT_PROPERTY, "2");
    p.put(CoreWorkload.FIELD_COUNT_PROPERTY, "1");
    p.put(CoreWorkload.FIELD_LENGTH_PROPERTY, "1");
    return p;
  }
}