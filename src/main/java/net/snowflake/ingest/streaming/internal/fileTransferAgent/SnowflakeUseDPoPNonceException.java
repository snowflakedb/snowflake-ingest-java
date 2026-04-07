/*
 * Replicated from snowflake-jdbc (v3.25.1)
 * Source: https://github.com/snowflakedb/snowflake-jdbc/blob/v3.25.1/src/main/java/net/snowflake/client/jdbc/SnowflakeUseDPoPNonceException.java
 *
 * Permitted differences: package declaration, @SnowflakeJdbcInternalApi annotation removed.
 */
package net.snowflake.ingest.streaming.internal.fileTransferAgent;

public class SnowflakeUseDPoPNonceException extends RuntimeException {

  private final String nonce;

  public SnowflakeUseDPoPNonceException(String nonce) {
    this.nonce = nonce;
  }

  public String getNonce() {
    return nonce;
  }
}
