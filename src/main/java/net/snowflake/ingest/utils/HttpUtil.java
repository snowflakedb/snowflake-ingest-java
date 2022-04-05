/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.utils;

import static net.snowflake.ingest.utils.Utils.isNullOrEmpty;

import java.security.Security;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLContext;
import net.snowflake.client.core.SFSessionProperty;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.NoHttpResponseException;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.ServiceUnavailableRetryStrategy;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.conn.ssl.DefaultHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.DefaultProxyRoutePlanner;
import org.apache.http.protocol.HttpContext;
import org.apache.http.ssl.SSLContexts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Created by hyu on 8/10/17. */
public class HttpUtil {
  public static final String USE_PROXY = "http.useProxy";
  public static final String PROXY_HOST = "http.proxyHost";
  public static final String PROXY_PORT = "http.proxyPort";

  public static final String HTTP_PROXY_USER = "http.proxyUser";
  public static final String HTTP_PROXY_PASSWORD = "http.proxyPassword";

  private static final String PROXY_SCHEME = "http";
  private static final int MAX_RETRIES = 3;
  static final int DEFAULT_CONNECTION_TIMEOUT = 1; // minute
  static final int DEFAULT_HTTP_CLIENT_SOCKET_TIMEOUT = 5; // minutes
  private static HttpClient httpClient;

  public static HttpClient getHttpClient() {
    if (httpClient == null) {
      initHttpClient();
    }

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
                (int) TimeUnit.MILLISECONDS.convert(DEFAULT_CONNECTION_TIMEOUT, TimeUnit.MINUTES))
            .setConnectionRequestTimeout(
                (int) TimeUnit.MILLISECONDS.convert(DEFAULT_CONNECTION_TIMEOUT, TimeUnit.MINUTES))
            .setSocketTimeout(
                (int)
                    TimeUnit.MILLISECONDS.convert(
                        DEFAULT_HTTP_CLIENT_SOCKET_TIMEOUT, TimeUnit.MINUTES))
            .build();
    /**
     * Use a anonymous class to implement the interface ServiceUnavailableRetryStrategy() The max
     * retry time is 3. The interval time is backoff.
     */
    HttpClientBuilder clientBuilder =
        HttpClients.custom()
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
    }
    return proxyProperties;
  }
}
