/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.connection;

import static net.snowflake.ingest.utils.Constants.*;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import net.snowflake.client.jdbc.internal.apache.http.HttpHeaders;
import net.snowflake.client.jdbc.internal.apache.http.client.methods.CloseableHttpResponse;
import net.snowflake.client.jdbc.internal.apache.http.client.methods.HttpPost;
import net.snowflake.client.jdbc.internal.apache.http.entity.ContentType;
import net.snowflake.client.jdbc.internal.apache.http.entity.StringEntity;
import net.snowflake.client.jdbc.internal.apache.http.impl.client.CloseableHttpClient;
import net.snowflake.client.jdbc.internal.apache.http.util.EntityUtils;
import net.snowflake.client.jdbc.internal.google.api.client.http.HttpStatusCodes;
import net.snowflake.client.jdbc.internal.google.gson.JsonObject;
import net.snowflake.client.jdbc.internal.google.gson.JsonParser;
import net.snowflake.ingest.utils.*;

/**
 * This class manages creating and automatically renewing the JWT token
 *
 * @author obabarinsa
 * @since 1.8
 */
final class OAuthManager extends SecurityManager {

  // the token lifetime is 59 minutes
  private static final float LIFETIME = 59;

  // the renewal time is 54 minutes
  private static final int RENEWAL_INTERVAL = 54;

  private static final String TOKEN_TYPE = "OAUTH";
  private static final String OAUTH_CONTENT_TYPE_HEADER = "application/x-www-form-urlencoded";

  private static final String GRANT_TYPE_PARAM = "grant_type";

  private static final String ACCESS_TOKEN = "access_token";
  private static final String REFRESH_TOKEN = "refresh_token";

  private final AtomicReference<OAuthCredential> oAuthCredential;

  private final URI tokenRequestURI;

  private final CloseableHttpClient httpClient;

  // Thread factory for daemon threads so that application can shutdown
  final ThreadFactory tf = ThreadFactoryUtil.poolThreadFactory(getClass().getSimpleName(), true);

  // the thread we use for renewing all tokens
  private final ScheduledExecutorService keyRenewer = Executors.newScheduledThreadPool(1, tf);

  // Reference to the Telemetry Service in the client
  private final TelemetryService telemetryService;

  /**
   * Creates a SecurityManager entity for a given account, user and KeyPair with a specified time to
   * renew the token
   *
   * @param accountName - the snowflake account name of this user
   * @param username - the snowflake username of the current user
   * @param oAuthCredential - the OAuth credential we're using to connect
   * @param timeTillRenewal - the time measure until we renew the token
   * @param unit the unit by which timeTillRenewal is measured
   * @param telemetryService reference to the telemetry service
   */
  OAuthManager(
      String accountName,
      String username,
      OAuthCredential oAuthCredential,
      URI tokenRequestURI,
      int timeTillRenewal,
      TimeUnit unit,
      TelemetryService telemetryService) {
    super(accountName, username);
    // if any of our arguments are null, throw an exception
    if (oAuthCredential == null) {
      throw new IllegalArgumentException();
    }

    this.oAuthCredential = new AtomicReference<>(oAuthCredential);

    this.tokenRequestURI = tokenRequestURI;

    this.httpClient = HttpUtil.getHttpClient(accountName);

    this.telemetryService = telemetryService;

    // generate our first token
    refreshToken();
  }

  /**
   * Creates a SecurityManager entity for a given account, user and KeyPair with the default time to
   * renew (RENEWAL_INTERVAL minutes)
   *
   * @param accountName - the snowflake account name of this user
   * @param username - the snowflake username of the current user
   * @param oAuthCredential - the OAuth credential we're using to connect
   * @param telemetryService reference to the telemetry service
   */
  OAuthManager(
      String accountName,
      String username,
      OAuthCredential oAuthCredential,
      URI tokenRequestURI,
      TelemetryService telemetryService) {
    this(
        accountName,
        username,
        oAuthCredential,
        tokenRequestURI,
        RENEWAL_INTERVAL,
        TimeUnit.MINUTES,
        telemetryService);
  }

  @Override
  String getToken() {
    return oAuthCredential.get().getAccessToken();
  }

  @Override
  String getTokenType() {
    return TOKEN_TYPE;
  }

  /** refreshToken - Get new access token using refresh_token, client_id, client_secret */
  @Override
  void refreshToken() {
    for (int retries = 0; retries < MAX_REFRESH_TOKEN_RETRY; retries++) {
      String respBodyString = null;
      try (CloseableHttpResponse httpResponse = httpClient.execute(makeRefreshTokenRequest())) {
        respBodyString = EntityUtils.toString(httpResponse.getEntity());

        if (httpResponse.getStatusLine().getStatusCode() == HttpStatusCodes.STATUS_CODE_OK) {
          JsonObject respBody = JsonParser.parseString(respBodyString).getAsJsonObject();

          if (respBody.has(ACCESS_TOKEN)) {
            oAuthCredential
                .get()
                .setAccessToken(respBody.get(ACCESS_TOKEN).toString().replaceAll("^\"|\"$", ""));
            LOGGER.debug("Refresh access token {}", oAuthCredential.get().getAccessToken());
            return;
          }
        }
      } catch (IOException e) {
        throw new SFException(ErrorCode.OAUTH_REFRESH_TOKEN_ERROR, e.getMessage());
      }

      LOGGER.debug(
          "Refresh access token fail with response {}, retry count={}", respBodyString, retries);

      try {
        Thread.sleep((1L << retries) * 1000L);
      } catch (InterruptedException e) {
        throw new SFException(ErrorCode.OAUTH_REFRESH_TOKEN_ERROR, e.getMessage());
      }
    }

    LOGGER.error("Fail to refresh access token");
    throw new SecurityException();
  }

  private HttpPost makeRefreshTokenRequest() {
    HttpPost post = new HttpPost(tokenRequestURI);
    post.addHeader(HttpHeaders.CONTENT_TYPE, OAUTH_CONTENT_TYPE_HEADER);
    post.addHeader(HttpHeaders.AUTHORIZATION, oAuthCredential.get().getAuthHeader());

    Map<Object, Object> payload = new HashMap<>();
    try {
      payload.put(GRANT_TYPE_PARAM, URLEncoder.encode(REFRESH_TOKEN, "UTF-8"));
      payload.put(
          REFRESH_TOKEN, URLEncoder.encode(oAuthCredential.get().getRefreshToken(), "UTF-8"));
    } catch (UnsupportedEncodingException e) {
      throw new SFException(e, ErrorCode.OAUTH_REFRESH_TOKEN_ERROR, e.getMessage());
    }

    String payloadString =
        payload.entrySet().stream()
            .map(e -> e.getKey() + "=" + e.getValue())
            .collect(Collectors.joining("&"));

    final StringEntity entity =
        new StringEntity(payloadString, ContentType.APPLICATION_FORM_URLENCODED);
    post.setEntity(entity);

    return post;
  }

  /** Currently, it only shuts down the instance of ExecutorService. */
  @Override
  public void close() {
    keyRenewer.shutdown();
  }
}
