/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.utils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import java.security.Security;
import java.util.concurrent.TimeUnit;
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
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.conn.ssl.DefaultHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.DefaultProxyRoutePlanner;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.protocol.HttpContext;
import org.apache.http.ssl.SSLContexts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Created by hyu on 8/10/17. */
public class HttpUtil {
  private static String USE_PROXY = "http.useProxy";
  private static String PROXY_HOST = "http.proxyHost";
  private static String PROXY_PORT = "http.proxyPort";

  private static final String HTTP_PROXY_USER = "http.proxyUser";
  private static final String HTTP_PROXY_PASSWORD = "http.proxyPassword";

  private static String PROXY_SCHEME = "http";
  private static int MAX_RETRIES = 3;

  private static final int DEFAULT_MAX_CONNECTIONS_PER_ROUTE = 100;
  private static final int DEFAULT_MAX_CONNECTIONS = 100;

  private static final long MONITOR_THREAD_INTERVAL_MS = TimeUnit.SECONDS.toMillis(5);

  private static CloseableHttpClient httpClient;

  private static PoolingHttpClientConnectionManager connectionManager;

  public static CloseableHttpClient getHttpClient() {
    if (httpClient == null) {
      initHttpClient();
    }

    return httpClient;
  }

  /**
   * Get the connection manager used for the HttpClient
   * @return PoolingHttpClientConnectionManager instance
   */
  @VisibleForTesting
  public static PoolingHttpClientConnectionManager getConnectionManager() {
    return connectionManager;
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(HttpUtil.class);

  private static void initHttpClient() {

    Security.setProperty("ocsp.enable", "true");

    SSLContext sslContext = SSLContexts.createDefault();

    SSLConnectionSocketFactory f =
        new SSLConnectionSocketFactory(
            sslContext, new String[] {"TLSv1.2"}, null, new DefaultHostnameVerifier());

    connectionManager = new PoolingHttpClientConnectionManager(120, TimeUnit.SECONDS);
    connectionManager.setDefaultMaxPerRoute(DEFAULT_MAX_CONNECTIONS_PER_ROUTE);
    connectionManager.setMaxTotal(DEFAULT_MAX_CONNECTIONS);

    /**
     * Use a anonymous class to implement the interface ServiceUnavailableRetryStrategy() The max
     * retry time is 3. The interval time is backoff.
     */
    HttpClientBuilder clientBuilder =
        HttpClientBuilder.create()
            .setConnectionManager(connectionManager)
            .setConnectionTimeToLive(120, TimeUnit.SECONDS)
            .setSSLSocketFactory(f)
            .setServiceUnavailableRetryStrategy(getServiceUnavailableRetryStrategy())
            .setRetryHandler(getHttpRequestRetryHandler());

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
      if (!Strings.isNullOrEmpty(proxyUser) && !Strings.isNullOrEmpty(proxyPassword)) {
        Credentials credentials = new UsernamePasswordCredentials(proxyUser, proxyPassword);
        AuthScope authScope = new AuthScope(proxyHost, proxyPort);
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(authScope, credentials);
        clientBuilder = clientBuilder.setDefaultCredentialsProvider(credentialsProvider);
      }
    }

    httpClient = clientBuilder.build();

    // Monitors in a separate thread where it closes any idle connections
    // https://hc.apache.org/httpcomponents-client-4.5.x/current/tutorial/html/connmgmt.html
    IdleConnectionMonitorThread idleConnectionMonitorThread =
        new IdleConnectionMonitorThread(connectionManager);
    idleConnectionMonitorThread.start();
  }

  private static ServiceUnavailableRetryStrategy getServiceUnavailableRetryStrategy() {
    return new ServiceUnavailableRetryStrategy() {
      private int executionCount = 0;
      int REQUEST_TIMEOUT = 408;

      @Override
      public boolean retryRequest(
          final HttpResponse response, final int executionCount, final HttpContext context) {
        this.executionCount = executionCount;
        int statusCode = response.getStatusLine().getStatusCode();
        LOGGER.info(
            "In retryRequest for service unavailability with statusCode:{} and uri:{}",
            statusCode,
            getRequestUriFromContext(context));
        if (executionCount == MAX_RETRIES + 1) {
          LOGGER.info("Reached the max retry time, not retrying anymore");
          return false;
        }
        boolean needNextRetry =
            (statusCode == REQUEST_TIMEOUT || statusCode >= 500)
                && executionCount < MAX_RETRIES + 1;
        if (needNextRetry) {
          long interval = (1 << executionCount) * 1000;
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
            wait(MONITOR_THREAD_INTERVAL_MS);

            // Close expired connections
            connectionManager.closeExpiredConnections();
            // Optionally, close connections
            // that have been idle longer than 30 sec
            connectionManager.closeIdleConnections(30, TimeUnit.SECONDS);
          }
        }
      } catch (InterruptedException ex) {
        LOGGER.debug("Terminating Idle Connection Monitor Thread ");
      }
    }

    public void shutdown() {
      LOGGER.debug("Shutdown Idle Connection Monitor Thread ");
      shutdown = true;
      synchronized (this) {
        notifyAll();
      }
    }
  }
}
