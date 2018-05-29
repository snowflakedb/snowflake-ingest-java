/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.utils;

import net.snowflake.ingest.SimpleIngestManager;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.conn.ssl.DefaultHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.HttpClients;
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
      LoggerFactory.getLogger(SimpleIngestManager.class);
  private static void initHttpClient()
  {
    Security.setProperty("ocsp.enable", "true");

    SSLContext sslContext = SSLContexts.createDefault();

    SSLConnectionSocketFactory f = new SSLConnectionSocketFactory(
        sslContext,
        new String[]{"TLSv1.2"},
        null,
        new DefaultHostnameVerifier());

    httpClient = HttpClients.custom()
        .setSSLSocketFactory(f)
        .setServiceUnavailableRetryStrategy(new ServiceUnavailableRetryStrategy()
        {
          private int executionCount = 0;
          @Override
          public boolean retryRequest(
              final HttpResponse response, final int executionCount,
              final HttpContext context) {
            this.executionCount = executionCount;
            int statusCode = response.getStatusLine().getStatusCode();

            boolean needNextRetry = (statusCode == 401 || statusCode >= 500 )
                && executionCount < 4;
            if(executionCount == 4)
            {
              LOGGER.warn("Because we already retry 3 times, " +
                  "so it will not retry send request any more.");
            }
            if(needNextRetry && executionCount < 4)
            {
              long interval = (1 << executionCount)  * 1000;
              LOGGER.warn("Will retry send request in " +
                  String.valueOf(interval) + " millisecond, "
                  + " and the excution count is " + executionCount);
            }
            return needNextRetry;
          }

          @Override
          public long getRetryInterval() {
            long interval = (1 << executionCount)  * 1000;
            return interval;
          }
        })
        .build();
  }

}
