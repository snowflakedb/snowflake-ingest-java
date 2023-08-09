/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;


import net.snowflake.client.core.HttpClientSettingsKey;
import net.snowflake.client.core.OCSPMode;
import net.snowflake.client.core.SFSessionProperty;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HttpResponse;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.*;

/**
 * @author jhuang
 */
public class SnowflakeUtil {
  static final SFLogger logger = SFLoggerFactory.getLogger(SnowflakeUtil.class);

  /**
   * Setup JDBC proxy properties if necessary.
   *
   * @param mode OCSP mode
   * @param info proxy server properties.
   */
  public static HttpClientSettingsKey convertProxyPropertiesToHttpClientKey(
          OCSPMode mode, Properties info) throws SnowflakeSQLException {
    // Setup proxy properties.
    if (info != null
            && info.size() > 0
            && info.getProperty(SFSessionProperty.USE_PROXY.getPropertyKey()) != null) {
      Boolean useProxy =
              Boolean.valueOf(info.getProperty(SFSessionProperty.USE_PROXY.getPropertyKey()));
      if (useProxy) {
        // set up other proxy related values.
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
        Boolean gzipDisabled =
                (info.getProperty(SFSessionProperty.GZIP_DISABLED.getPropertyKey()).isEmpty()
                        ? false
                        : Boolean.valueOf(
                        info.getProperty(SFSessionProperty.GZIP_DISABLED.getPropertyKey())));
        // Check for any user agent suffix
        String userAgentSuffix = "";
        if (info.containsKey(SFSessionProperty.USER_AGENT_SUFFIX)) {
          userAgentSuffix = (String) info.get(SFSessionProperty.USER_AGENT_SUFFIX);
        }
        // create key for proxy properties
        return new HttpClientSettingsKey(
                mode,
                proxyHost,
                proxyPort,
                nonProxyHosts,
                proxyUser,
                proxyPassword,
                proxyProtocol,
                userAgentSuffix,
                gzipDisabled);
      }
    }
    // if no proxy properties, return key with only OCSP mode
    return new HttpClientSettingsKey(mode);
  }

  /**
   * System.getProperty wrapper. If System.getProperty raises an SecurityException, it is ignored
   * and returns null.
   *
   * @param property the property name
   * @return the property value if set, otherwise null.
   */
  public static String systemGetProperty(String property) {
    try {
      return System.getProperty(property);
    } catch (SecurityException ex) {
      logger.debug("Security exception raised: {}", ex.getMessage());
      return null;
    }
  }

  public static boolean isBlank(String input) {
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

  public static Throwable getRootCause(Exception ex) {
    Throwable cause = ex;
    while (cause.getCause() != null) {
      cause = cause.getCause();
    }

    return cause;
  }

  public static long getEpochTimeInMicroSeconds() {
    Instant timestamp = Instant.now();
    long micros =
            TimeUnit.SECONDS.toMicros(timestamp.getEpochSecond())
                    + TimeUnit.NANOSECONDS.toMicros(timestamp.getNano());
    return micros;
  }

  /**
   * Returns a new thread pool configured with the default settings.
   *
   * @param threadNamePrefix prefix of the thread name
   * @param parallel the number of concurrency
   * @return A new thread pool configured with the default settings.
   */
  public static ThreadPoolExecutor createDefaultExecutorService(
          final String threadNamePrefix, final int parallel) {
    ThreadFactory threadFactory =
            new ThreadFactory() {
              private int threadCount = 1;

              public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName(threadNamePrefix + threadCount++);
                return thread;
              }
            };
    return (ThreadPoolExecutor) Executors.newFixedThreadPool(parallel, threadFactory);
  }

  /**
   * A utility to log response details.
   *
   * <p>Used when there is an error in http response
   *
   * @param response http response get from server
   * @param logger logger object
   */
  public static void logResponseDetails(HttpResponse response,  net.snowflake.client.log.SFLogger logger) {
    if (response == null) {
      logger.error("null response", false);
      return;
    }

    // log the response
    if (response.getStatusLine() != null) {
      logger.error("Response status line reason: {}", response.getStatusLine().getReasonPhrase());
    }

    // log each header from response
    Header[] headers = response.getAllHeaders();
    if (headers != null) {
      for (Header header : headers) {
        logger.debug("Header name: {}, value: {}", header.getName(), header.getValue());
      }
    }

    // log response
    if (response.getEntity() != null) {
      try {
        StringWriter writer = new StringWriter();
        BufferedReader bufferedReader =
                new BufferedReader(new InputStreamReader((response.getEntity().getContent())));
        IOUtils.copy(bufferedReader, writer);
        logger.error("Response content: {}", writer.toString());
      } catch (IOException ex) {
        logger.error("Failed to read content due to exception: " + "{}", ex.getMessage());
      }
    }
  }

  /**
   * System.getenv wrapper. If System.getenv raises an SecurityException, it is ignored and returns
   * null.
   *
   * @param env the environment variable name.
   * @return the environment variable value if set, otherwise null.
   */
  public static String systemGetEnv(String env) {
    try {
      return System.getenv(env);
    } catch (SecurityException ex) {
      logger.debug(
              "Failed to get environment variable {}. Security exception raised: {}",
              env,
              ex.getMessage());
    }
    return null;
  }

}
