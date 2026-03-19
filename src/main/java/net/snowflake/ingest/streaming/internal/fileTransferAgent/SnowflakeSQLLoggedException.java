/*
 * Replicated from snowflake-jdbc: net.snowflake.client.jdbc.SnowflakeSQLLoggedException
 * Tag: v3.25.1
 * Source: https://github.com/snowflakedb/snowflake-jdbc/blob/v3.25.1/src/main/java/net/snowflake/client/jdbc/SnowflakeSQLLoggedException.java
 * Source: https://github.com/snowflakedb/snowflake-jdbc/blob/v3.25.1/src/main/java/net/snowflake/client/jdbc/SnowflakeSQLException.java
 * Source: /jdbc/SnowflakeSQLException.java
 *
 * The JDBC original sends OOB/in-band telemetry on construction. That behaviour
 * is stripped because all callers in the ingest SDK pass null for the session
 * parameter, and the OOB telemetry path depends on JDBC-internal classes being
 * removed. The session parameter is dropped from all constructors.
 */

package net.snowflake.ingest.streaming.internal.fileTransferAgent;

/** Extends SnowflakeSQLException. Session-less variant for the ingest SDK. */
public class SnowflakeSQLLoggedException extends SnowflakeSQLException {

  /** (int vendorCode, String sqlState, Object... params) */
  public SnowflakeSQLLoggedException(int vendorCode, String sqlState, Object... params) {
    super(sqlState, vendorCode, params);
  }

  /** (String sqlState, int vendorCode, Throwable ex, Object... params) */
  public SnowflakeSQLLoggedException(
      String sqlState, int vendorCode, Throwable ex, Object... params) {
    super(null, ex, sqlState, vendorCode, params);
  }
}
