// Ported from snowflake-common: ResourceBundleManager
package net.snowflake.ingest.streaming.internal.fileTransferAgent;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class ResourceBundleManagerTest {

  private static final String BUNDLE_NAME = ErrorCode.errorMessageResource;

  @Test
  public void testGetSingleton() {
    ResourceBundleManager mgr = ResourceBundleManager.getSingleton(BUNDLE_NAME);
    assertNotNull(mgr);
    // Same instance on second call
    ResourceBundleManager mgr2 = ResourceBundleManager.getSingleton(BUNDLE_NAME);
    assertTrue("getSingleton should return same instance", mgr == mgr2);
  }

  @Test
  public void testGetLocalizedMessage() {
    ResourceBundleManager mgr = ResourceBundleManager.getSingleton(BUNDLE_NAME);
    // Error code 200001 = "JDBC driver internal error: {0}."
    String msg = mgr.getLocalizedMessage("200001");
    assertNotNull(msg);
    assertTrue(msg.contains("internal error"));
  }

  @Test
  public void testGetLocalizedMessageWithParams() {
    ResourceBundleManager mgr = ResourceBundleManager.getSingleton(BUNDLE_NAME);
    // Error code 200001 = "JDBC driver internal error: {0}."
    String msg = mgr.getLocalizedMessage("200001", "test detail");
    assertTrue(msg.contains("test detail"));
  }

  @Test
  public void testMissingKey() {
    ResourceBundleManager mgr = ResourceBundleManager.getSingleton(BUNDLE_NAME);
    String msg = mgr.getLocalizedMessage("nonexistent_key");
    assertEquals("!nonexistent_key!", msg);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNullKey() {
    ResourceBundleManager mgr = ResourceBundleManager.getSingleton(BUNDLE_NAME);
    mgr.getLocalizedMessage(null);
  }

  @Test
  public void testGetKeySet() {
    ResourceBundleManager mgr = ResourceBundleManager.getSingleton(BUNDLE_NAME);
    assertNotNull(mgr.getKeySet());
    assertTrue("Should have error message keys", mgr.getKeySet().size() > 0);
    assertTrue("Should contain error code 200001", mgr.getKeySet().contains("200001"));
  }

  @Test
  public void testGetResourceBundle() {
    ResourceBundleManager mgr = ResourceBundleManager.getSingleton(BUNDLE_NAME);
    assertNotNull(mgr.getResourceBundle());
  }
}
