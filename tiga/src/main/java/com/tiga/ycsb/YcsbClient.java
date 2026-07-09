package com.tiga.ycsb;

import java.util.Map;
import java.util.Set;

/**
 * Native JNI wrapper client for Tiga/Calvin/Detock databases.
 */
public class YcsbClient {
  static {
    try {
      System.loadLibrary("tigaycsb");
    } catch (UnsatisfiedLinkError e) {
      try {
        loadFromJar();
      } catch (Exception ex) {
        System.err.println("Failed to load native library tigaycsb: " + e.getMessage());
        System.err.println("Failed to load native library from JAR resources: " + ex.getMessage());
        throw new RuntimeException(ex);
      }
    }
  }

  private static void loadFromJar() throws Exception {
    String libName = "libtigaycsb.so";
    java.io.InputStream in = YcsbClient.class.getClassLoader().getResourceAsStream(libName);
    if (in == null) {
      throw new java.io.FileNotFoundException("Library " + libName + " not found in JAR resources");
    }
    java.io.File tempFile = java.io.File.createTempFile("libtigaycsb", ".so");
    tempFile.deleteOnExit();
    try (java.io.FileOutputStream out = new java.io.FileOutputStream(tempFile)) {
      byte[] buffer = new byte[8192];
      int read;
      while ((read = in.read(buffer)) != -1) {
        out.write(buffer, 0, read);
      }
    }
    System.load(tempFile.getAbsolutePath());
  }

  // Pointer to the underlying C++ wrapper object (stored as a long)
  private final long clientHandle;

  public YcsbClient(String configPath, String mode) {
    this.clientHandle = initClient(configPath, mode);
    if (this.clientHandle == 0) {
      throw new RuntimeException("Failed to initialize native " + mode + " client");
    }
  }

  public void close() {
    if (clientHandle != 0) {
      closeClient(clientHandle);
    }
  }

  // Native API mappings
  private native long initClient(String configPath, String mode);
  private native void closeClient(long handle);

  public native int read(String key, Set<String> fields, Map<String, String> result);
  public native int update(String key, Map<String, String> values);
  public native int insert(String key, Map<String, String> values);
}
