/*
 * Utility methods replicated from snowflake-jdbc: net.snowflake.client.jdbc.SnowflakeUtil
 * Tag: v3.25.1
 * Source: https://github.com/snowflakedb/snowflake-jdbc/blob/v3.25.1/src/main/java/net/snowflake/client/jdbc/SnowflakeUtil.java
 *
 * Only the methods used by the ingest storage clients are included.
 */

package net.snowflake.ingest.streaming.internal.fileTransferAgent;

import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import net.snowflake.ingest.utils.OCSPMode;
import net.snowflake.ingest.utils.SFSessionProperty;
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

  /** Replicated from SnowflakeUtil.convertProxyPropertiesToHttpClientKey */
  static HttpClientSettingsKey convertProxyPropertiesToHttpClientKey(OCSPMode mode, Properties info)
      throws SnowflakeSQLException {
    if (info != null
        && info.size() > 0
        && info.getProperty(SFSessionProperty.USE_PROXY.getPropertyKey()) != null) {
      boolean useProxy =
          Boolean.parseBoolean(info.getProperty(SFSessionProperty.USE_PROXY.getPropertyKey()));
      if (useProxy) {
        String proxyHost = info.getProperty(SFSessionProperty.PROXY_HOST.getPropertyKey());
        int proxyPort;
        try {
          proxyPort =
              Integer.parseInt(info.getProperty(SFSessionProperty.PROXY_PORT.getPropertyKey()));
        } catch (NullPointerException | NumberFormatException e) {
          throw new SnowflakeSQLException(
              StorageErrorCode.INVALID_PROXY_PROPERTIES.getSqlState(),
              StorageErrorCode.INVALID_PROXY_PROPERTIES.getMessageCode(),
              "Could not parse port number");
        }
        String proxyUser = info.getProperty(SFSessionProperty.PROXY_USER.getPropertyKey());
        String proxyPassword = info.getProperty(SFSessionProperty.PROXY_PASSWORD.getPropertyKey());
        String nonProxyHosts = info.getProperty(SFSessionProperty.NON_PROXY_HOSTS.getPropertyKey());
        String proxyProtocol = info.getProperty(SFSessionProperty.PROXY_PROTOCOL.getPropertyKey());
        return new HttpClientSettingsKey(
            mode, proxyHost, proxyPort, nonProxyHosts, proxyUser, proxyPassword, proxyProtocol);
      }
    }
    return new HttpClientSettingsKey(mode);
  }

  /** Replicated from SnowflakeUtil.createDefaultExecutorService */
  static ThreadPoolExecutor createDefaultExecutorService(
      final String threadNamePrefix, int parallel) {
    ThreadFactory threadFactory =
        new ThreadFactory() {
          private int threadCount = 1;

          @Override
          public Thread newThread(Runnable r) {
            Thread thread = new Thread(r);
            thread.setName(threadNamePrefix + threadCount++);
            return thread;
          }
        };
    return (ThreadPoolExecutor) Executors.newFixedThreadPool(parallel, threadFactory);
  }
}
