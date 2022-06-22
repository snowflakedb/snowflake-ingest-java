/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.utils;

import static net.snowflake.ingest.utils.StringsUtils.isNullOrEmpty;

import java.security.Security;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import javax.net.ssl.SSLContext;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.NoHttpResponseException;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.ServiceUnavailableRetryStrategy;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.conn.ssl.DefaultHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.DefaultProxyRoutePlanner;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.pool.PoolStats;
import org.apache.http.protocol.HttpContext;
import org.apache.http.ssl.SSLContexts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Created by hyu on 8/10/17. */
public class HttpUtil {
  private static final String USE_PROXY = "http.useProxy";
  private static final String PROXY_HOST = "http.proxyHost";
  private static final String PROXY_PORT = "http.proxyPort";

  private static final String HTTP_PROXY_USER = "http.proxyUser";
  private static final String HTTP_PROXY_PASSWORD = "http.proxyPassword";

  private static final String PROXY_SCHEME = "http";
  private static final int MAX_RETRIES = 3;
  static final int DEFAULT_CONNECTION_TIMEOUT = 1; // minute
  static final int DEFAULT_HTTP_CLIENT_SOCKET_TIMEOUT = 5; // minutes

  private static CloseableHttpClient httpClient;

  private static PoolingHttpClientConnectionManager connectionManager;

  private static IdleConnectionMonitorThread idleConnectionMonitorThread;

  /**
   * This lock is to synchronize on idleConnectionMonitorThread to avoid setting starting a thread
   * which was already started. (To avoid {@link IllegalThreadStateException})
   */
  private static ReentrantLock idleConnectionMonitorThreadLock = new ReentrantLock(true);

  private static final int DEFAULT_CONNECTION_TIMEOUT_MINUTES = 1;
  private static final int DEFAULT_HTTP_CLIENT_SOCKET_TIMEOUT_MINUTES = 5;

  // Default is 2, but scaling it up to 100 to match with default_max_connections
  private static final int DEFAULT_MAX_CONNECTIONS_PER_ROUTE = 100;

  // 100 is close to max partition number we have seen for a kafka topic ingesting into snowflake.
  private static final int DEFAULT_MAX_CONNECTIONS = 100;

  // Interval in which we check if there are connections which needs to be closed.
  private static final long IDLE_HTTP_CONNECTION_MONITOR_THREAD_INTERVAL_MS =
      TimeUnit.SECONDS.toMillis(5);

  // Only connections that are currently owned, not checked out, are subject to idle timeouts.
  private static final int DEFAULT_IDLE_CONNECTION_TIMEOUT_SECONDS = 30;

  public static CloseableHttpClient getHttpClient() {
    if (httpClient == null) {
      initHttpClient();
    }

    initIdleConnectionMonitoringThread();

    return httpClient;
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(HttpUtil.class);

  private static void initHttpClient() {

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

    /**
     * Use a anonymous class to implement the interface ServiceUnavailableRetryStrategy() The max
     * retry time is 3. The interval time is backoff.
     */
    HttpClientBuilder clientBuilder =
        HttpClientBuilder.create()
            .setConnectionManager(connectionManager)
            .setSSLSocketFactory(f)
            .setServiceUnavailableRetryStrategy(getServiceUnavailableRetryStrategy())
            .setRetryHandler(getHttpRequestRetryHandler())
            .setDefaultRequestConfig(requestConfig);

    // proxy settings
    if ("true".equalsIgnoreCase(System.getProperty(USE_PROXY))) {
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
      private int executionCount = 0;
      final int REQUEST_TIMEOUT = 408;

      @Override
      public boolean retryRequest(
          final HttpResponse response, final int executionCount, final HttpContext context) {
        this.executionCount = executionCount;
        int statusCode = response.getStatusLine().getStatusCode();
        if (executionCount == MAX_RETRIES + 1) {
          LOGGER.info("Reached the max retry time, not retrying anymore");
          return false;
        }
        boolean needNextRetry =
            (statusCode == REQUEST_TIMEOUT || statusCode >= 500)
                && executionCount < MAX_RETRIES + 1;
        if (needNextRetry) {
          long interval = (1 << executionCount) * 1000;
          LOGGER.warn(
              "In retryRequest for service unavailability with statusCode:{} and uri:{}",
              statusCode,
              getRequestUriFromContext(context));
          LOGGER.info("Sleep time in millisecond: {}, retryCount: {}", interval, executionCount);
        }
        return needNextRetry;
      }

      @Override
      // The waiting time is backoff, and is
      // the exponential of the executionCount.
      public long getRetryInterval() {
        long interval = (1 << executionCount) * 1000; // milliseconds
        return interval;
      }
    };
  }

  /**
   * Retry handler logic. Retry if No response from Service exception. (NoHttpResponseException)
   *
   * @return retryHandler to add to http client.
   */
  private static HttpRequestRetryHandler getHttpRequestRetryHandler() {
    return (exception, executionCount, httpContext) -> {
      final String requestURI = getRequestUriFromContext(httpContext);
      if (executionCount > MAX_RETRIES) {
        LOGGER.info("Max retry exceeded for requestURI:{}", requestURI);
        return false;
      }
      if (exception instanceof NoHttpResponseException) {
        LOGGER.info(
            "Retrying request which caused No HttpResponse Exception with "
                + "URI:{}, retryCount:{} and maxRetryCount:{}",
            requestURI,
            executionCount,
            MAX_RETRIES);
        return true;
      }
      LOGGER.info("No retry for URI:{} with exception", requestURI, exception);
      return false;
    };
  }

  private static String getRequestUriFromContext(final HttpContext httpContext) {
    HttpClientContext clientContext = HttpClientContext.adapt(httpContext);
    HttpRequest httpRequest = clientContext.getRequest();
    return httpRequest.getRequestLine().getUri();
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
        LOGGER.debug("Terminating Idle Connection Monitor Thread ");
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
}
