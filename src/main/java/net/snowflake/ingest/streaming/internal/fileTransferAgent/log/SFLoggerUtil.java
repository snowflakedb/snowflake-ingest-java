/*
 * Replicated from snowflake-jdbc (v3.25.1)
 * Source: https://github.com/snowflakedb/snowflake-jdbc/blob/v3.25.1/src/main/java/net/snowflake/client/log/SFLoggerUtil.java
 *
 * Permitted differences: package. Only isVariableProvided replicated;
 * initializeSnowflakeLogger omitted (deep dependency on CommonsLoggingWrapper
 * infrastructure not used by ingest). isNullOrEmpty call inlined (original
 * imports from SnowflakeUtil which is in a different package).
 * @SnowflakeJdbcInternalApi removed.
 */
package net.snowflake.ingest.streaming.internal.fileTransferAgent.log;

public class SFLoggerUtil {
  private static final String NOT_PROVIDED_LOG = "not provided";
  private static final String PROVIDED_LOG = "provided";

  private static boolean isNullOrEmpty(String s) {
    return s == null || s.isEmpty();
  }

  public static <T> String isVariableProvided(T variable) {
    if (variable instanceof String) {
      return (isNullOrEmpty((String) variable)) ? NOT_PROVIDED_LOG : PROVIDED_LOG;
    }
    return variable == null ? NOT_PROVIDED_LOG : PROVIDED_LOG;
  }
}
