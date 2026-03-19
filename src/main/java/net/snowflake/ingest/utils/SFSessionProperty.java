/*
 * Replicated from snowflake-jdbc: net.snowflake.client.core.SFSessionProperty
 * Tag: v3.25.1
 * Source: https://github.com/snowflakedb/snowflake-jdbc/blob/v3.25.1/src/main/java/net/snowflake/client/core/SFSessionProperty.java
 *
 * Only the property keys used by the ingest SDK are included.
 * The string values must match JDBC exactly since they are used as property-map
 * keys consumed by the JDBC file transfer agent (non-Iceberg upload path).
 */

package net.snowflake.ingest.utils;

public enum SFSessionProperty {
  PRIVATE_KEY("privateKey"),
  USE_PROXY("useProxy"),
  PROXY_HOST("proxyHost"),
  PROXY_PORT("proxyPort"),
  PROXY_USER("proxyUser"),
  PROXY_PASSWORD("proxyPassword"),
  NON_PROXY_HOSTS("nonProxyHosts"),
  PROXY_PROTOCOL("proxyProtocol"),
  ALLOW_UNDERSCORES_IN_HOST("allowUnderscoresInHost");

  private final String propertyKey;

  SFSessionProperty(String propertyKey) {
    this.propertyKey = propertyKey;
  }

  public String getPropertyKey() {
    return this.propertyKey;
  }
}
