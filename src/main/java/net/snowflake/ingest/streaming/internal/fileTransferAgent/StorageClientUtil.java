/*
 * Utility methods replicated from snowflake-jdbc: net.snowflake.client.jdbc.SnowflakeUtil
 * Tag: v3.25.1
 * Source: https://github.com/snowflakedb/snowflake-jdbc/blob/v3.25.1/src/main/java/net/snowflake/client/jdbc/SnowflakeUtil.java
 *
 * Only the methods used by the ingest storage clients are included.
 * Copied verbatim — see individual method Javadoc for JDBC source line references.
 */

package net.snowflake.ingest.streaming.internal.fileTransferAgent;

import java.util.Map;
import java.util.TreeMap;
import net.snowflake.ingest.streaming.internal.fileTransferAgent.log.SFLogger;
import net.snowflake.ingest.streaming.internal.fileTransferAgent.log.SFLoggerFactory;

final class StorageClientUtil {
  private static final SFLogger logger = SFLoggerFactory.getLogger(StorageClientUtil.class);

  private StorageClientUtil() {}

  static Map<String, String> createCaseInsensitiveMap(Map<String, String> input) {
    Map<String, String> caseInsensitiveMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    if (input != null) {
      caseInsensitiveMap.putAll(input);
    }
    return caseInsensitiveMap;
  }

  static Throwable getRootCause(Exception ex) {
    Throwable cause = ex;
    while (cause.getCause() != null) {
      cause = cause.getCause();
    }

    return cause;
  }

  static boolean isBlank(String input) {
    if ("".equals(input) || input == null) {
      return true;
    }

    for (char c : input.toCharArray()) {
      if (!Character.isWhitespace(c)) {
        return false;
      }
    }

    return true;
  }

  /** Replicated from SnowflakeUtil.systemGetProperty */
  static String systemGetProperty(String property) {
    try {
      return System.getProperty(property);
    } catch (SecurityException ex) {
      logger.debug("Security exception raised: {}", ex.getMessage());
      return null;
    }
  }

  /** Replicated from SnowflakeUtil.isNullOrEmpty */
  static boolean isNullOrEmpty(String str) {
    return str == null || str.isEmpty();
  }

  /**
   * Replicated from SnowflakeUtil.isWindows, which delegates to Constants.getOS(). The OS detection
   * logic from Constants is inlined here to avoid replicating the full Constants class.
   *
   * @see <a
   *     href="https://github.com/snowflakedb/snowflake-jdbc/blob/v3.25.1/src/main/java/net/snowflake/client/core/Constants.java">Constants.java</a>
   */
  private static volatile String detectedOS = null;

  static boolean isWindows() {
    if (detectedOS == null) {
      String operSys = systemGetProperty("os.name");
      detectedOS = operSys != null ? operSys.toLowerCase() : "";
    }
    return detectedOS.contains("win");
  }
}
