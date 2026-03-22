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
  public void testFullConstructor() {
    HttpClientSettingsKey key =
        new HttpClientSettingsKey(
            OCSPMode.FAIL_OPEN, "host", 8080, "localhost", "user", "pass", "https", "suffix", true);
    assertTrue(key.usesProxy());
    assertEquals("host", key.getProxyHost());
    assertEquals(8080, key.getProxyPort());
    assertEquals("localhost", key.getNonProxyHosts());
    assertEquals("user", key.getProxyUser());
    assertEquals("pass", key.getProxyPassword());
    assertEquals("suffix", key.getUserAgentSuffix());
    assertTrue(key.getGzipDisabled());
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
