/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.utils;

import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.NoHttpResponseException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.conn.ssl.DefaultHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.DefaultProxyRoutePlanner;
import org.apache.http.protocol.HttpContext;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.client.ServiceUnavailableRetryStrategy;

import javax.net.ssl.SSLContext;
import java.security.Security;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by hyu on 8/10/17.
 */
public class HttpUtil
{

  private static String USE_PROXY = "http.useProxy";
  private static String PROXY_HOST = "http.proxyHost";
  private static String PROXY_PORT = "http.proxyPort";
  private static String PROXY_SCHEME = "http";
  private static int MAX_RETRIES = 3;


  static private HttpClient httpClient;

  static public HttpClient getHttpClient()
  {
    if (httpClient == null)
    {
      initHttpClient();
    }

    return httpClient;
  }
  private static final Logger LOGGER =
      LoggerFactory.getLogger(HttpUtil.class);
  private static void initHttpClient()
  {

    Security.setProperty("ocsp.enable", "true");

    SSLContext sslContext = SSLContexts.createDefault();

    SSLConnectionSocketFactory f = new SSLConnectionSocketFactory(
        sslContext,
        new String[]{"TLSv1.2"},
        null,
        new DefaultHostnameVerifier());


    /**
     * Use a anonymous class to implement
     * the interface ServiceUnavailableRetryStrategy()
     * The max retry time is 3.
     * The interval time is backoff.
     */

    HttpClientBuilder clientBuilder = HttpClients.custom()
        .setSSLSocketFactory(f)
        .setServiceUnavailableRetryStrategy(getServiceUnavailableRetryStrategy())
        .setRetryHandler(getHttpRequestRetryHandler());

    //proxy settings
    if("true".equalsIgnoreCase(System.getProperty(USE_PROXY)))
    {
      if(System.getProperty(PROXY_PORT) == null)
      {
        throw new IllegalArgumentException(
          "proxy port number is not provided, please assign proxy port to http.proxyPort option"
        );
      }
      if(System.getProperty(PROXY_HOST) == null)
      {
        throw new IllegalArgumentException(
          "proxy host IP is not provided, please assign proxy host IP to http.proxyHost option"
        );
      }
      String proxyHost = System.getProperty(PROXY_HOST);
      int proxyPort = Integer.parseInt(System.getProperty(PROXY_PORT));
      HttpHost proxy =  new HttpHost(proxyHost, proxyPort, PROXY_SCHEME);
      DefaultProxyRoutePlanner routePlanner = new DefaultProxyRoutePlanner(proxy);
      clientBuilder = clientBuilder.setRoutePlanner(routePlanner);
    }

    httpClient = clientBuilder.build();

  }

  private static ServiceUnavailableRetryStrategy getServiceUnavailableRetryStrategy()
  {
    return new ServiceUnavailableRetryStrategy() {
      private int executionCount = 0;
      int REQUEST_TIMEOUT = 408;

      @Override
      public boolean retryRequest(
              final HttpResponse response, final int executionCount,
              final HttpContext context)
      {
        this.executionCount = executionCount;
        int statusCode = response.getStatusLine().getStatusCode();
        LOGGER.info("In retryRequest for service unavailability with statusCode:{} and uri:{}",
                    statusCode, getRequestUriFromContext(context));
        if (executionCount == MAX_RETRIES + 1)
        {
          LOGGER.info("Reached the max retry time, not retrying anymore");
          return false;
        }
        boolean needNextRetry = (statusCode == REQUEST_TIMEOUT || statusCode >= 500)
                && executionCount < MAX_RETRIES + 1;
        if (needNextRetry)
        {
          long interval = (1 << executionCount) * 1000;
          LOGGER.info("Sleep time in millisecond: {}, retryCount: {}", interval, executionCount);
        }
        return needNextRetry;
      }

      @Override
      // The waiting time is backoff, and is
      // the exponential of the executionCount.
      public long getRetryInterval()
      {
        long interval = (1 << executionCount) * 1000;    // milliseconds
        return interval;
      }
    };
  }

  /**
   * Retry handler logic. Retry if No response from Service exception. (NoHttpResponseException)
   * @return retryHandler to add to http client.
   */
  private static HttpRequestRetryHandler getHttpRequestRetryHandler()
  {
    return (exception, executionCount, httpContext) -> {
      final String requestURI = getRequestUriFromContext(httpContext);
      if (executionCount > MAX_RETRIES)
      {
        LOGGER.info("Max retry exceeded for requestURI:{}", requestURI);
        return false;
      }
      if (exception instanceof NoHttpResponseException)
      {
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
}
