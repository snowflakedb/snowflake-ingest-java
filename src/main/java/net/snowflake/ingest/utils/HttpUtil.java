/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.utils;

import org.apache.http.client.HttpClient;
import org.apache.http.conn.ssl.DefaultHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContexts;

import javax.net.ssl.SSLContext;
import java.security.Security;

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
        .build();
  }

}
