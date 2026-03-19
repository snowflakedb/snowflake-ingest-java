/*
 * Tests for HttpClientSettingsKey — replicated from snowflake-jdbc.
 */

package net.snowflake.ingest.streaming.internal.fileTransferAgent;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import net.snowflake.ingest.utils.OCSPMode;
import org.junit.Test;

public class HttpClientSettingsKeyTest {

  @Test
  public void testNoProxyConstructor() {
    HttpClientSettingsKey key = new HttpClientSettingsKey(OCSPMode.FAIL_OPEN);
    assertEquals(OCSPMode.FAIL_OPEN, key.getOcspMode());
    assertFalse(key.usesProxy());
  }

  @Test
  public void testProxyConstructor() {
    HttpClientSettingsKey key =
        new HttpClientSettingsKey(
            OCSPMode.FAIL_OPEN, "proxy.host", 8080, "localhost", "user", "pass", "https");
    assertTrue(key.usesProxy());
    assertEquals("proxy.host", key.getProxyHost());
    assertEquals(8080, key.getProxyPort());
    assertEquals("localhost", key.getNonProxyHosts());
    assertEquals("user", key.getProxyUser());
    assertEquals("pass", key.getProxyPassword());
    assertEquals("https", key.getProxyProtocol());
  }

  @Test
  public void testEquality() {
    HttpClientSettingsKey a = new HttpClientSettingsKey(OCSPMode.FAIL_OPEN);
    HttpClientSettingsKey b = new HttpClientSettingsKey(OCSPMode.FAIL_OPEN);
    assertTrue(a.equals(b));
    assertEquals(a.hashCode(), b.hashCode());
  }

  @Test
  public void testNullDefaults() {
    HttpClientSettingsKey key = new HttpClientSettingsKey(null);
    assertEquals(OCSPMode.FAIL_OPEN, key.getOcspMode());
  }
}
