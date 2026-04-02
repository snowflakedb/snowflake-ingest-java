/*
 * Utility methods replicated from snowflake-jdbc: net.snowflake.client.jdbc.SnowflakeUtil
 * Tag: v3.25.1
 * Source: https://github.com/snowflakedb/snowflake-jdbc/blob/v3.25.1/src/main/java/net/snowflake/client/jdbc/SnowflakeUtil.java
 *
 * Only the methods used by the ingest storage clients are included.
 * Copied verbatim — see individual method Javadoc for JDBC source line references.
 */

package net.snowflake.ingest.streaming.internal.fileTransferAgent;

import static java.util.Arrays.stream;

import com.microsoft.azure.storage.OperationContext;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import net.snowflake.ingest.streaming.internal.fileTransferAgent.log.SFLogger;
import net.snowflake.ingest.streaming.internal.fileTransferAgent.log.SFLoggerFactory;
import net.snowflake.ingest.utils.OCSPMode;
import net.snowflake.ingest.utils.SFSessionProperty;
import org.apache.http.Header;
import org.apache.http.NameValuePair;

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

  /** Replicated from SnowflakeUtil.createCaseInsensitiveMap(Header[]) */
  static Map<String, String> createCaseInsensitiveMap(Header[] headers) {
    if (headers != null) {
      return createCaseInsensitiveMap(
          stream(headers)
              .collect(Collectors.toMap(NameValuePair::getName, NameValuePair::getValue)));
    } else {
      return new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    }
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

  /**
   * Replicated from SnowflakeUtil.convertProxyPropertiesToHttpClientKey.
   *
   * <p>Note: JDBC version throws SnowflakeSQLException on bad proxy port. We use the same JDBC
   * SnowflakeSQLException here temporarily until Step 5 replaces it.
   */
  static HttpClientSettingsKey convertProxyPropertiesToHttpClientKey(OCSPMode mode, Properties info)
      throws SnowflakeSQLException {
    if (info != null
        && info.size() > 0
        && info.getProperty(SFSessionProperty.USE_PROXY.getPropertyKey()) != null) {
      Boolean useProxy =
          Boolean.valueOf(info.getProperty(SFSessionProperty.USE_PROXY.getPropertyKey()));
      if (useProxy) {
        String proxyHost = info.getProperty(SFSessionProperty.PROXY_HOST.getPropertyKey());
        int proxyPort;
        try {
          proxyPort =
              Integer.parseInt(info.getProperty(SFSessionProperty.PROXY_PORT.getPropertyKey()));
        } catch (NumberFormatException | NullPointerException e) {
          throw new SnowflakeSQLException(
              ErrorCode.INVALID_PROXY_PROPERTIES, "Could not parse port number");
        }
        String proxyUser = info.getProperty(SFSessionProperty.PROXY_USER.getPropertyKey());
        String proxyPassword = info.getProperty(SFSessionProperty.PROXY_PASSWORD.getPropertyKey());
        String nonProxyHosts = info.getProperty(SFSessionProperty.NON_PROXY_HOSTS.getPropertyKey());
        String proxyProtocol = info.getProperty(SFSessionProperty.PROXY_PROTOCOL.getPropertyKey());
        // userAgentSuffix and gzipDisabled are not set by the ingest SDK
        return new HttpClientSettingsKey(
            mode,
            proxyHost,
            proxyPort,
            nonProxyHosts,
            proxyUser,
            proxyPassword,
            proxyProtocol,
            null,
            false);
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

  /**
   * Replicated from SnowflakeUtil.convertSystemPropertyToBooleanValue (JDBC). Reads a system
   * property and returns it as a boolean.
   */
  static boolean convertSystemPropertyToBooleanValue(String systemProperty, boolean defaultValue) {
    String val = systemGetProperty(systemProperty);
    if (val != null) {
      return Boolean.parseBoolean(val);
    }
    return defaultValue;
  }

  private static final String NO_SPACE_LEFT_ON_DEVICE_ERR = "No space left on device";

  /**
   * Replicated from SnowflakeFileTransferAgent.throwJCEMissingError. Source:
   * https://github.com/snowflakedb/snowflake-jdbc/blob/v3.25.1/src/main/java/net/snowflake/client/jdbc/SnowflakeFileTransferAgent.java
   *
   * @deprecated use {@link #throwJCEMissingError(String, Exception, String)}
   */
  @Deprecated
  static void throwJCEMissingError(String operation, Exception ex) throws SnowflakeSQLException {
    throwJCEMissingError(operation, ex, null);
  }

  /**
   * Replicated from SnowflakeFileTransferAgent.throwJCEMissingError. Source:
   * https://github.com/snowflakedb/snowflake-jdbc/blob/v3.25.1/src/main/java/net/snowflake/client/jdbc/SnowflakeFileTransferAgent.java
   */
  static void throwJCEMissingError(String operation, Exception ex, String queryId)
      throws SnowflakeSQLException {
    // Most likely cause: Unlimited strength policy files not installed
    String msg =
        "Strong encryption with Java JRE requires JCE "
            + "Unlimited Strength Jurisdiction Policy files. "
            + "Follow JDBC client installation instructions "
            + "provided by Snowflake or contact Snowflake Support.";

    logger.error(
        "JCE Unlimited Strength policy files missing: {}. {}.",
        ex.getMessage(),
        ex.getCause().getMessage());

    String bootLib = systemGetProperty("sun.boot.library.path");
    if (bootLib != null) {
      msg +=
          " The target directory on your system is: " + Paths.get(bootLib, "security").toString();
      logger.error(msg);
    }
    throw new SnowflakeSQLException(
        queryId,
        ex,
        SqlState.SYSTEM_ERROR,
        ErrorCode.AWS_CLIENT_ERROR.getMessageCode(),
        operation,
        msg);
  }

  /**
   * Replicated from SnowflakeFileTransferAgent.throwNoSpaceLeftError. Source:
   * https://github.com/snowflakedb/snowflake-jdbc/blob/v3.25.1/src/main/java/net/snowflake/client/jdbc/SnowflakeFileTransferAgent.java
   *
   * @deprecated use {@link #throwNoSpaceLeftError(Object, String, Exception, String)}
   */
  @Deprecated
  static void throwNoSpaceLeftError(Object session, String operation, Exception ex)
      throws SnowflakeSQLLoggedException {
    throwNoSpaceLeftError(session, operation, ex, null);
  }

  /**
   * Replicated from SnowflakeFileTransferAgent.throwNoSpaceLeftError. Source:
   * https://github.com/snowflakedb/snowflake-jdbc/blob/v3.25.1/src/main/java/net/snowflake/client/jdbc/SnowflakeFileTransferAgent.java
   *
   * <p>Note: session parameter is always null from ingest callers. Kept for API shape.
   */
  static void throwNoSpaceLeftError(Object session, String operation, Exception ex, String queryId)
      throws SnowflakeSQLLoggedException {
    String exMessage = getRootCause(ex).getMessage();
    if (exMessage != null && exMessage.equals(NO_SPACE_LEFT_ON_DEVICE_ERR)) {
      throw new SnowflakeSQLLoggedException(
          queryId,
          SqlState.SYSTEM_ERROR,
          ErrorCode.IO_ERROR.getMessageCode(),
          ex,
          "Encountered exception during " + operation + ":" + ex.getMessage());
    }
  }

  /**
   * Replicated from SnowflakeUtil.assureOnlyUserAccessibleFilePermissions. Source:
   * https://github.com/snowflakedb/snowflake-jdbc/blob/v3.25.1/src/main/java/net/snowflake/client/jdbc/SnowflakeUtil.java
   */
  static void assureOnlyUserAccessibleFilePermissions(
      File file, boolean isOwnerOnlyStageFilePermissionsEnabled) throws IOException {
    if (isWindows()) {
      return;
    }
    if (!isOwnerOnlyStageFilePermissionsEnabled) {
      // If the owner only stage file permissions are not enabled, we do not need to set the file
      // permissions.
      return;
    }
    boolean disableUserPermissions =
        file.setReadable(false, false)
            && file.setWritable(false, false)
            && file.setExecutable(false, false);
    boolean setOwnerPermissionsOnly = file.setReadable(true, true) && file.setWritable(true, true);

    if (disableUserPermissions && setOwnerPermissionsOnly) {
      logger.info("Successfuly set OwnerOnly permission for {}. ", file.getAbsolutePath());
    } else {
      file.delete();
      logger.error(
          "Failed to set OwnerOnly permission for {}. Failed to download", file.getAbsolutePath());
      throw new IOException(
          String.format(
              "Failed to set OwnerOnly permission for %s. Failed to download",
              file.getAbsolutePath()));
    }
  }

  /**
   * Replicated from SnowflakeUtil.getEpochTimeInMicroSeconds. Source:
   * https://github.com/snowflakedb/snowflake-jdbc/blob/v3.25.1/src/main/java/net/snowflake/client/jdbc/SnowflakeUtil.java
   */
  static long getEpochTimeInMicroSeconds() {
    Instant timestamp = Instant.now();
    long micros =
        TimeUnit.SECONDS.toMicros(timestamp.getEpochSecond())
            + TimeUnit.NANOSECONDS.toMicros(timestamp.getNano());
    return micros;
  }

  /**
   * Replicated from HttpUtil.setSessionlessProxyForAzure. Source:
   * https://github.com/snowflakedb/snowflake-jdbc/blob/v3.25.1/src/main/java/net/snowflake/client/core/HttpUtil.java
   */
  static void setSessionlessProxyForAzure(Properties proxyProperties, OperationContext opContext)
      throws SnowflakeSQLException {
    if (proxyProperties != null
        && proxyProperties.size() > 0
        && proxyProperties.getProperty(SFSessionProperty.USE_PROXY.getPropertyKey()) != null) {
      Boolean useProxy =
          Boolean.valueOf(
              proxyProperties.getProperty(SFSessionProperty.USE_PROXY.getPropertyKey()));
      if (useProxy) {
        String proxyHost =
            proxyProperties.getProperty(SFSessionProperty.PROXY_HOST.getPropertyKey());
        int proxyPort;
        try {
          proxyPort =
              Integer.parseInt(
                  proxyProperties.getProperty(SFSessionProperty.PROXY_PORT.getPropertyKey()));
        } catch (NumberFormatException | NullPointerException e) {
          throw new SnowflakeSQLException(
              ErrorCode.INVALID_PROXY_PROPERTIES, "Could not parse port number");
        }
        Proxy azProxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyHost, proxyPort));
        logger.debug("Setting sessionless Azure proxy. Host: {}, port: {}", proxyHost, proxyPort);
        opContext.setProxy(azProxy);
      } else {
        logger.debug("Omitting sessionless Azure proxy setup as proxy is disabled");
      }
    } else {
      logger.debug("Omitting sessionless Azure proxy setup");
    }
  }
}
