/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.utils;

import static net.snowflake.ingest.utils.Utils.isNullOrEmpty;

import java.security.Security;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;
import javax.net.ssl.SSLContext;
import net.snowflake.client.core.SFSessionProperty;
import net.snowflake.client.jdbc.internal.apache.http.HttpHost;
import net.snowflake.client.jdbc.internal.apache.http.HttpRequest;
import net.snowflake.client.jdbc.internal.apache.http.HttpResponse;
import net.snowflake.client.jdbc.internal.apache.http.NoHttpResponseException;
import net.snowflake.client.jdbc.internal.apache.http.auth.AuthScope;
import net.snowflake.client.jdbc.internal.apache.http.auth.Credentials;
import net.snowflake.client.jdbc.internal.apache.http.auth.UsernamePasswordCredentials;
import net.snowflake.client.jdbc.internal.apache.http.client.CredentialsProvider;
import net.snowflake.client.jdbc.internal.apache.http.client.HttpRequestRetryHandler;
import net.snowflake.client.jdbc.internal.apache.http.client.ServiceUnavailableRetryStrategy;
import net.snowflake.client.jdbc.internal.apache.http.client.config.RequestConfig;
import net.snowflake.client.jdbc.internal.apache.http.client.protocol.HttpClientContext;
import net.snowflake.client.jdbc.internal.apache.http.conn.routing.HttpRoute;
import net.snowflake.client.jdbc.internal.apache.http.conn.ssl.DefaultHostnameVerifier;
import net.snowflake.client.jdbc.internal.apache.http.conn.ssl.SSLConnectionSocketFactory;
import net.snowflake.client.jdbc.internal.apache.http.impl.client.BasicCredentialsProvider;
import net.snowflake.client.jdbc.internal.apache.http.impl.client.CloseableHttpClient;
import net.snowflake.client.jdbc.internal.apache.http.impl.client.HttpClientBuilder;
import net.snowflake.client.jdbc.internal.apache.http.impl.conn.DefaultProxyRoutePlanner;
import net.snowflake.client.jdbc.internal.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import net.snowflake.client.jdbc.internal.apache.http.pool.PoolStats;
import net.snowflake.client.jdbc.internal.apache.http.protocol.HttpContext;
import net.snowflake.client.jdbc.internal.apache.http.ssl.SSLContexts;
import net.snowflake.ingest.streaming.internal.StreamingIngestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Created by hyu on 8/10/17. */
public class HttpUtil {
  public static final String USE_PROXY = "http.useProxy";
  public static final String PROXY_HOST = "http.proxyHost";
  public static final String PROXY_PORT = "http.proxyPort";
  public static final String NON_PROXY_HOSTS = "http.nonProxyHosts";
  public static final String HTTP_PROXY_USER = "http.proxyUser";
  public static final String HTTP_PROXY_PASSWORD = "http.proxyPassword";
  private static final String SNOWFLAKE_DOMAIN_NAME = ".snowflakecomputing.com";

  private static final String PROXY_SCHEME = "http";
  private static final String FIRST_FAULT_TIMESTAMP = "FIRST_FAULT_TIMESTAMP";
  private static final Duration TOTAL_RETRY_DURATION = Duration.of(120, ChronoUnit.SECONDS);
  private static final Duration RETRY_INTERVAL = Duration.of(3, ChronoUnit.SECONDS);

  /**
   * How many times to retry when an IO exception is thrown. Value here is chosen to match the total
   * value of {@link HttpUtil.TOTAL_RETRY_DURATION} when exponential backoff of up to 4 seconds per
   * retry is used.
   */
  private static final int MAX_RETRIES = 10;

  private static volatile CloseableHttpClient httpClient;

  private static PoolingHttpClientConnectionManager connectionManager;

  private static IdleConnectionMonitorThread idleConnectionMonitorThread;

  /**
   * This lock is to synchronize on idleConnectionMonitorThread to avoid setting starting a thread
   * which was already started. (To avoid {@link IllegalThreadStateException})
   */
  private static final ReentrantLock idleConnectionMonitorThreadLock = new ReentrantLock(true);

  private static final int DEFAULT_CONNECTION_TIMEOUT_MINUTES = 1;
  private static final int DEFAULT_HTTP_CLIENT_SOCKET_TIMEOUT_MINUTES = 1;

  /**
   * After how many seconds of inactivity should be idle connections evicted from the connection
   * pool.
   */
  private static final int DEFAULT_EVICT_IDLE_AFTER_SECONDS = 60;

  // Default is 2, but scaling it up to 100 to match with default_max_connections
  private static final int DEFAULT_MAX_CONNECTIONS_PER_ROUTE = 100;

  // 100 is close to max partition number we have seen for a kafka topic ingesting into snowflake.
  private static final int DEFAULT_MAX_CONNECTIONS = 100;

  // Interval in which we check if there are connections which needs to be closed.
  private static final long IDLE_HTTP_CONNECTION_MONITOR_THREAD_INTERVAL_MS =
      TimeUnit.SECONDS.toMillis(5);

  // Only connections that are currently owned, not checked out, are subject to idle timeouts.
  private static final int DEFAULT_IDLE_CONNECTION_TIMEOUT_SECONDS = 30;

  /**
   * @param {@code String} account name to connect to (excluding snowflakecomputing.com domain)
   * @return Instance of CloseableHttpClient
   */
  public static CloseableHttpClient getHttpClient(String accountName) {
    if (httpClient == null) {
      synchronized (HttpUtil.class) {
        if (httpClient == null) {
          initHttpClient(accountName);
        }
      }
    }

    initIdleConnectionMonitoringThread();

    return httpClient;
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(HttpUtil.class);

  private static void initHttpClient(String accountName) {

    Security.setProperty("ocsp.enable", "true");

    SSLContext sslContext = SSLContexts.createDefault();

    SSLConnectionSocketFactory f =
        new SSLConnectionSocketFactory(
            sslContext, new String[] {"TLSv1.2"}, null, new DefaultHostnameVerifier());
    // Set connectionTimeout which is the timeout until a connection with the server is established
    // Set connectionRequestTimeout which is the time to wait for getting a connection from the
    // connection pool
    // Set socketTimeout which is the max time gap between two consecutive data packets
    RequestConfig requestConfig =
        RequestConfig.custom()
            .setConnectTimeout(
                (int)
                    TimeUnit.MILLISECONDS.convert(
                        DEFAULT_CONNECTION_TIMEOUT_MINUTES, TimeUnit.MINUTES))
            .setConnectionRequestTimeout(
                (int)
                    TimeUnit.MILLISECONDS.convert(
                        DEFAULT_CONNECTION_TIMEOUT_MINUTES, TimeUnit.MINUTES))
            .setSocketTimeout(
                (int)
                    TimeUnit.MILLISECONDS.convert(
                        DEFAULT_HTTP_CLIENT_SOCKET_TIMEOUT_MINUTES, TimeUnit.MINUTES))
            .build();

    // Below pooling client connection manager uses time_to_live value as -1 which means it will not
    // refresh a persisted connection
    connectionManager = new PoolingHttpClientConnectionManager();
    connectionManager.setDefaultMaxPerRoute(DEFAULT_MAX_CONNECTIONS_PER_ROUTE);
    connectionManager.setMaxTotal(DEFAULT_MAX_CONNECTIONS);

    // Use an anonymous class to implement the interface ServiceUnavailableRetryStrategy() The max
    // retry time is 3. The interval time is backoff.
    HttpClientBuilder clientBuilder =
        HttpClientBuilder.create()
            .setConnectionManager(connectionManager)
            .evictIdleConnections(DEFAULT_EVICT_IDLE_AFTER_SECONDS, TimeUnit.SECONDS)
            .setSSLSocketFactory(f)
            .setServiceUnavailableRetryStrategy(getServiceUnavailableRetryStrategy())
            .setRetryHandler(getHttpRequestRetryHandler())
            .setDefaultRequestConfig(requestConfig);

    // proxy settings
    if ("true".equalsIgnoreCase(System.getProperty(USE_PROXY)) && !shouldBypassProxy(accountName)) {
      if (System.getProperty(PROXY_PORT) == null) {
        throw new IllegalArgumentException(
            "proxy port number is not provided, please assign proxy port to http.proxyPort option");
      }
      if (System.getProperty(PROXY_HOST) == null) {
        throw new IllegalArgumentException(
            "proxy host IP is not provided, please assign proxy host IP to http.proxyHost option");
      }
      String proxyHost = System.getProperty(PROXY_HOST);
      int proxyPort = Integer.parseInt(System.getProperty(PROXY_PORT));
      HttpHost proxy = new HttpHost(proxyHost, proxyPort, PROXY_SCHEME);
      DefaultProxyRoutePlanner routePlanner = new DefaultProxyRoutePlanner(proxy);
      clientBuilder = clientBuilder.setRoutePlanner(routePlanner);

      // Check if proxy username and password are set
      final String proxyUser = System.getProperty(HTTP_PROXY_USER);
      final String proxyPassword = System.getProperty(HTTP_PROXY_PASSWORD);
      if (!isNullOrEmpty(proxyUser) && !isNullOrEmpty(proxyPassword)) {
        Credentials credentials = new UsernamePasswordCredentials(proxyUser, proxyPassword);
        AuthScope authScope = new AuthScope(proxyHost, proxyPort);
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(authScope, credentials);
        clientBuilder = clientBuilder.setDefaultCredentialsProvider(credentialsProvider);
      }
    }

    httpClient = clientBuilder.build();
  }

  /** Starts a daemon thread to monitor idle connections in http connection manager. */
  private static void initIdleConnectionMonitoringThread() {
    idleConnectionMonitorThreadLock.lock();
    try {
      // Start a new thread only if connectionManager was init before and
      // daemon thread was not started before or was closed because of SimpleIngestManager close
      if (connectionManager != null
          && (idleConnectionMonitorThread == null || idleConnectionMonitorThread.isShutdown())) {
        // Monitors in a separate thread where it closes any idle connections
        // https://hc.apache.org/httpcomponents-client-4.5.x/current/tutorial/html/connmgmt.html
        idleConnectionMonitorThread = new IdleConnectionMonitorThread(connectionManager);
        idleConnectionMonitorThread.setDaemon(true);
        idleConnectionMonitorThread.start();
      }
    } catch (Exception e) {
      LOGGER.warn("Unable to start Daemon thread for Http Idle Connection Monitoring", e);
    } finally {
      idleConnectionMonitorThreadLock.unlock();
    }
  }

  private static ServiceUnavailableRetryStrategy getServiceUnavailableRetryStrategy() {
    return new ServiceUnavailableRetryStrategy() {
      final int REQUEST_TIMEOUT = 408;
      final int TOO_MANY_REQUESTS = 429;
      final int SERVER_ERRORS = 500;

      @Override
      public boolean retryRequest(
          final HttpResponse response, final int executionCount, final HttpContext context) {
        Object firstFault = context.getAttribute(FIRST_FAULT_TIMESTAMP);
        long totalRetryDurationSoFarInSeconds = 0;
        if (firstFault == null) {
          context.setAttribute(FIRST_FAULT_TIMESTAMP, Instant.now());
        } else {
          Instant firstFaultInstant = (Instant) firstFault;
          Instant now = Instant.now();
          totalRetryDurationSoFarInSeconds = Duration.between(firstFaultInstant, now).getSeconds();

          if (totalRetryDurationSoFarInSeconds > TOTAL_RETRY_DURATION.getSeconds()) {
            LOGGER.info(
                String.format(
                    "Reached the max retry time of %d seconds, not retrying anymore",
                    TOTAL_RETRY_DURATION.getSeconds()));
            return false;
          }
        }

        int statusCode = response.getStatusLine().getStatusCode();
        boolean needNextRetry =
            (statusCode == REQUEST_TIMEOUT
                || statusCode == TOO_MANY_REQUESTS
                || statusCode >= SERVER_ERRORS);
        if (needNextRetry) {
          long interval = getRetryInterval();
          LOGGER.info(
              "In retryRequest for service unavailability with statusCode:{} and uri:{}",
              statusCode,
              getRequestUriFromContext(context));
          LOGGER.info(
              "Sleep time in millisecond: {}, retryCount: {}, total retry duration: {}s / {}s",
              interval,
              executionCount,
              totalRetryDurationSoFarInSeconds,
              TOTAL_RETRY_DURATION.getSeconds());
        }
        return needNextRetry;
      }

      @Override
      public long getRetryInterval() {
        return RETRY_INTERVAL.toMillis();
      }
    };
  }

  /**
   * Retry handler logic. Retry at most {@link HttpUtil.MAX_RETRIES} times if any of the following
   * exceptions is thrown by request execution:
   *
   * <ul>
   *   <li>No response from Service exception (NoHttpResponseException)
   *   <li>javax.net.ssl.SSLException: Connection reset.
   * </ul>
   *
   * @return retryHandler to add to http client.
   */
  static HttpRequestRetryHandler getHttpRequestRetryHandler() {
    return (exception, executionCount, httpContext) -> {
      final String requestURI = getRequestUriFromContext(httpContext);
      if (executionCount > MAX_RETRIES) {
        LOGGER.info("Max retry exceeded for requestURI:{}", requestURI);
        return false;
      }
      if (exception instanceof NoHttpResponseException
          || exception instanceof javax.net.ssl.SSLException
          || exception instanceof java.net.SocketException
          || exception instanceof java.net.UnknownHostException
          || exception instanceof java.net.SocketTimeoutException) {
        LOGGER.info(
            "Retrying request which caused {} with " + "URI:{}, retryCount:{} and maxRetryCount:{}",
            exception.getClass().getName(),
            requestURI,
            executionCount,
            MAX_RETRIES);
        StreamingIngestUtils.sleepForRetry(executionCount);
        return true;
      }
      LOGGER.info("No retry for URI:{} with exception {}", requestURI, exception.toString());
      return false;
    };
  }

  private static String getRequestUriFromContext(final HttpContext httpContext) {
    HttpClientContext clientContext = HttpClientContext.adapt(httpContext);
    HttpRequest httpRequest = clientContext.getRequest();
    return httpRequest.getRequestLine().getUri();
  }

  /**
   * Helper method to decide whether to add any properties related to proxy server. These properties
   * are passed on to snowflake JDBC while calling put API.
   *
   * @return proxy parameters that could be used by JDBC
   */
  public static Properties generateProxyPropertiesForJDBC() {
    Properties proxyProperties = new Properties();
    if (Boolean.parseBoolean(System.getProperty(USE_PROXY))) {
      if (isNullOrEmpty(System.getProperty(PROXY_PORT))) {
        throw new IllegalArgumentException(
            "proxy port number is not provided, please assign proxy port to http.proxyPort option");
      }
      if (isNullOrEmpty(System.getProperty(PROXY_HOST))) {
        throw new IllegalArgumentException(
            "proxy host IP is not provided, please assign proxy host IP to http.proxyHost option");
      }

      // Set proxy host and port
      proxyProperties.put(SFSessionProperty.USE_PROXY.getPropertyKey(), "true");
      proxyProperties.put(
          SFSessionProperty.PROXY_HOST.getPropertyKey(), System.getProperty(PROXY_HOST));
      proxyProperties.put(
          SFSessionProperty.PROXY_PORT.getPropertyKey(), System.getProperty(PROXY_PORT));

      // Check if proxy username and password are set
      final String proxyUser = System.getProperty(HTTP_PROXY_USER);
      final String proxyPassword = System.getProperty(HTTP_PROXY_PASSWORD);
      if (!isNullOrEmpty(proxyUser) && !isNullOrEmpty(proxyPassword)) {
        proxyProperties.put(SFSessionProperty.PROXY_USER.getPropertyKey(), proxyUser);
        proxyProperties.put(SFSessionProperty.PROXY_PASSWORD.getPropertyKey(), proxyPassword);
      }

      // Check if http.nonProxyHosts was set
      final String nonProxyHosts = System.getProperty(NON_PROXY_HOSTS);
      if (!isNullOrEmpty(nonProxyHosts)) {
        proxyProperties.put(SFSessionProperty.NON_PROXY_HOSTS.getPropertyKey(), nonProxyHosts);
      }
    }
    return proxyProperties;
  }

  /** Thread to monitor expired and idle connection, if found clear it and return it back to pool */
  private static class IdleConnectionMonitorThread extends Thread {

    private final PoolingHttpClientConnectionManager connectionManager;
    private volatile boolean shutdown;

    public IdleConnectionMonitorThread(PoolingHttpClientConnectionManager connectionManager) {
      super();
      this.connectionManager = connectionManager;
    }

    @Override
    public void run() {
      try {
        LOGGER.debug("Starting Idle Connection Monitor Thread ");
        synchronized (this) {
          while (!shutdown) {
            wait(IDLE_HTTP_CONNECTION_MONITOR_THREAD_INTERVAL_MS);

            StringBuilder sb = new StringBuilder();

            sb.append(
                createPoolStatsInfo("Total Pool Stats = ", connectionManager.getTotalStats()));
            Set<HttpRoute> routes = connectionManager.getRoutes();

            if (routes != null) {
              for (HttpRoute route : routes) {
                sb.append(createPoolStatsForRoute(connectionManager, route));
              }
            }

            LOGGER.debug("[IdleConnectionMonitorThread] Pool Stats:\n" + sb);

            // Close expired connections
            connectionManager.closeExpiredConnections();
            // Optionally, close connections
            // that have been idle longer than 30 sec
            connectionManager.closeIdleConnections(
                DEFAULT_IDLE_CONNECTION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
          }
        }
      } catch (InterruptedException ex) {
        LOGGER.warn("Terminating Idle Connection Monitor Thread ");
      }
    }

    private void shutdown() {
      if (!shutdown) {
        LOGGER.debug("Shutdown Idle Connection Monitor Thread ");
        shutdown = true;
        synchronized (this) {
          notifyAll();
        }
      }
    }

    private boolean isShutdown() {
      return this.shutdown;
    }
  }

  /** Shuts down the daemon thread. */
  public static void shutdownHttpConnectionManagerDaemonThread() {
    idleConnectionMonitorThread.shutdown();
  }

  /** Create Pool stats for a route */
  private static String createPoolStatsForRoute(
      PoolingHttpClientConnectionManager connectionManager, HttpRoute route) {
    PoolStats routeStats = connectionManager.getStats(route);
    return createPoolStatsInfo(
        String.format("Pool Stats for route %s = ", route.getTargetHost().toURI()), routeStats);
  }

  /** Returns a string with a title and pool stats. */
  private static String createPoolStatsInfo(String title, PoolStats poolStats) {
    if (poolStats != null) {
      return title + poolStats + "\n";
    }
    return title;
  }

  /**
   * Changes the account name to the format accountName.snowflakecomputing.com then returns a
   * boolean to indicate if we should go through a proxy or not.
   */
  public static Boolean shouldBypassProxy(String accountName) {
    String targetHost = accountName + SNOWFLAKE_DOMAIN_NAME;
    return System.getProperty(NON_PROXY_HOSTS) != null && isInNonProxyHosts(targetHost);
  }

  /**
   * The target hostname input is compared with the hosts in the '|' separated list provided by the
   * http.nonProxyHosts parameter using regex. The nonProxyHosts will be used as our Patterns, so we
   * need to replace the '.' and '*' characters since those are special regex constructs that mean
   * 'any character,' and 'repeat 0 or more times.'
   */
  private static Boolean isInNonProxyHosts(String targetHost) {
    String nonProxyHosts =
        System.getProperty(NON_PROXY_HOSTS).replace(".", "\\.").replace("*", ".*");
    String[] nonProxyHostsArray = nonProxyHosts.split("\\|");
    for (String i : nonProxyHostsArray) {
      if (Pattern.compile(i).matcher(targetHost).matches()) {
        return true;
      }
    }
    return false;
  }
}
