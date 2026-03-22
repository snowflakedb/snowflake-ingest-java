/*
 * Replicated from snowflake-jdbc (v3.25.1)
 * Source: https://github.com/snowflakedb/snowflake-jdbc/blob/v3.25.1/src/main/java/net/snowflake/client/core/HttpProtocol.java
 */

package net.snowflake.ingest.streaming.internal.fileTransferAgent;

public enum HttpProtocol {
  HTTP("http"),

  HTTPS("https");

  private final String scheme;

  HttpProtocol(String scheme) {
    this.scheme = scheme;
  }

  public String getScheme() {
    return scheme;
  }
}
