/*
 * Tests for SFSessionProperty — replicated from snowflake-jdbc.
 */

package net.snowflake.ingest.utils;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class SFSessionPropertyTest {

  @Test
  public void testPropertyKeyValues() {
    assertEquals("privateKey", SFSessionProperty.PRIVATE_KEY.getPropertyKey());
    assertEquals("useProxy", SFSessionProperty.USE_PROXY.getPropertyKey());
    assertEquals("proxyHost", SFSessionProperty.PROXY_HOST.getPropertyKey());
    assertEquals("proxyPort", SFSessionProperty.PROXY_PORT.getPropertyKey());
    assertEquals("proxyUser", SFSessionProperty.PROXY_USER.getPropertyKey());
    assertEquals("proxyPassword", SFSessionProperty.PROXY_PASSWORD.getPropertyKey());
    assertEquals("nonProxyHosts", SFSessionProperty.NON_PROXY_HOSTS.getPropertyKey());
    assertEquals("proxyProtocol", SFSessionProperty.PROXY_PROTOCOL.getPropertyKey());
    assertEquals("allowUnderscoresInHost", SFSessionProperty.ALLOW_UNDERSCORES_IN_HOST.getPropertyKey());
  }
}
