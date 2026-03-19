/*
 * Utility methods replicated from snowflake-jdbc: net.snowflake.client.jdbc.SnowflakeUtil
 * Tag: v3.25.1
 * Source: https://github.com/snowflakedb/snowflake-jdbc/blob/v3.25.1/src/main/java/net/snowflake/client/jdbc/SnowflakeUtil.java
 *
 * Only the methods used by the ingest storage clients are included.
 */

package net.snowflake.ingest.streaming.internal.fileTransferAgent;

import java.util.Map;
import java.util.TreeMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

final class StorageClientUtil {

  private StorageClientUtil() {}

  /** Replicated from SnowflakeUtil.createCaseInsensitiveMap */
  static <V> Map<String, V> createCaseInsensitiveMap(Map<String, V> map) {
    Map<String, V> caseInsensitiveMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    if (map != null) {
      caseInsensitiveMap.putAll(map);
    }
    return caseInsensitiveMap;
  }

  /** Replicated from SnowflakeUtil.getRootCause */
  static Throwable getRootCause(Throwable t) {
    Throwable root = ExceptionUtils.getRootCause(t);
    return root != null ? root : t;
  }

  /** Replicated from SnowflakeUtil.isBlank */
  static boolean isBlank(String str) {
    return StringUtils.isBlank(str);
  }
}
