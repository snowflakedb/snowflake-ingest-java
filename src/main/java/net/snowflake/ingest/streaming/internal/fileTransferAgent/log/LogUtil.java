/*
 * Local utility for the log package. Replaces SnowflakeUtil.systemGetProperty
 * and SnowflakeUtil.isNullOrEmpty which are used by the replicated logger classes.
 */

package net.snowflake.ingest.streaming.internal.fileTransferAgent.log;

final class LogUtil {
  private LogUtil() {}

  /** Replicated from SnowflakeUtil.systemGetProperty */
  static String systemGetProperty(String property) {
    try {
      return System.getProperty(property);
    } catch (SecurityException ex) {
      return null;
    }
  }

  /** Replicated from SnowflakeUtil.isNullOrEmpty */
  static boolean isNullOrEmpty(String str) {
    return str == null || str.isEmpty();
  }
}
