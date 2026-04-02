/*
 * Constants replicated from snowflake-jdbc (v3.25.1) and snowflake-common (5.1.4).
 *
 * LoginInfoDTO.SF_JDBC_APP_ID from snowflake-common:
 *   net.snowflake.common.core.LoginInfoDTO (decompiled from snowflake-common-5.1.4.jar)
 *
 * SnowflakeDriver.implementVersion from snowflake-jdbc:
 *   https://github.com/snowflakedb/snowflake-jdbc/blob/v3.25.1/src/main/java/net/snowflake/client/jdbc/SnowflakeDriver.java
 */
package net.snowflake.ingest.streaming.internal.fileTransferAgent;

/**
 * Driver-level constants replicated from JDBC and snowflake-common. These are used by OOB telemetry
 * classes (TelemetryEvent, SnowflakeSQLLoggedException) to identify the driver type and version in
 * telemetry payloads.
 */
public class SnowflakeDriverConstants {
  /** From net.snowflake.common.core.LoginInfoDTO.SF_JDBC_APP_ID */
  public static final String SF_JDBC_APP_ID = "JDBC";

  /** From net.snowflake.client.jdbc.SnowflakeDriver.implementVersion */
  public static String implementVersion = "3.25.1";
}
