package net.snowflake.ingest.streaming.internal.fileTransferAgent;

import static org.junit.Assert.*;

import net.snowflake.ingest.utils.OCSPMode;
import org.junit.Test;

public class HttpClientSettingsKeyTest {
  @Test
  public void testNoProxy() {
    HttpClientSettingsKey key = new HttpClientSettingsKey(OCSPMode.FAIL_OPEN);
    assertEquals(OCSPMode.FAIL_OPEN, key.getOcspMode());
    assertFalse(key.usesProxy());
  }

  @Test
  public void testProxy() {
    HttpClientSettingsKey key =
        new HttpClientSettingsKey(
            OCSPMode.FAIL_OPEN, "host", 8080, "localhost", "user", "pass", "https");
    assertTrue(key.usesProxy());
    assertEquals("host", key.getProxyHost());
    assertEquals(8080, key.getProxyPort());
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
    assertEquals(OCSPMode.FAIL_OPEN, new HttpClientSettingsKey(null).getOcspMode());
  }
}
