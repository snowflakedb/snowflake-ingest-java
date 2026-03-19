/*
 * Tests for StorageClientUtil — replicated from snowflake-jdbc SnowflakeUtil.
 */

package net.snowflake.ingest.streaming.internal.fileTransferAgent;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class StorageClientUtilTest {

  @Test
  public void testCreateCaseInsensitiveMap() {
    Map<String, String> source = new HashMap<>();
    source.put("Key", "value");
    Map<String, String> result = StorageClientUtil.createCaseInsensitiveMap(source);
    assertEquals("value", result.get("key"));
    assertEquals("value", result.get("KEY"));
    assertEquals("value", result.get("Key"));
  }

  @Test
  public void testCreateCaseInsensitiveMapNull() {
    Map<String, String> result = StorageClientUtil.createCaseInsensitiveMap(null);
    assertTrue(result.isEmpty());
  }

  @Test
  public void testGetRootCause() {
    Exception root = new Exception("root");
    Exception mid = new Exception("mid", root);
    Exception top = new Exception("top", mid);
    assertEquals(root, StorageClientUtil.getRootCause(top));
  }

  @Test
  public void testGetRootCauseNoCause() {
    Exception ex = new Exception("no cause");
    assertEquals(ex, StorageClientUtil.getRootCause(ex));
  }

  @Test
  public void testIsBlank() {
    assertTrue(StorageClientUtil.isBlank(null));
    assertTrue(StorageClientUtil.isBlank(""));
    assertTrue(StorageClientUtil.isBlank("   "));
    assertFalse(StorageClientUtil.isBlank("text"));
  }
}
