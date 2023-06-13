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
import java.util.concurrent.atomic.AtomicBoolean;
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

  // Default update threshold, a floating-point value representing the ratio between the expiration
  // time of an access
  // token and the time needed to update it. It must be a value greater than 0 and less than or
  // equal to 1.
  private static final double DEFAULT_UPDATE_THRESHOLD_RATIO = 0.8;
  private static final String TOKEN_TYPE = "OAUTH";
  private static final String OAUTH_CONTENT_TYPE_HEADER = "application/x-www-form-urlencoded";

  private static final String GRANT_TYPE_PARAM = "grant_type";

  private static final String ACCESS_TOKEN = "access_token";
  private static final String REFRESH_TOKEN = "refresh_token";
  private static final String EXPIRES_IN = "expires_in";
  private final double updateThresholdRatio;

  private final AtomicReference<OAuthCredential> oAuthCredential;

  // Did we fail to regenerate our token at some point?
  private final AtomicBoolean refreshFailed;

  private final URI tokenRequestURI;

  private final CloseableHttpClient httpClient;

  // Thread factory for daemon threads so that application can shutdown
  final ThreadFactory tf = ThreadFactoryUtil.poolThreadFactory(getClass().getSimpleName(), true);

  // the thread we use for refreshing access token
  private final ScheduledExecutorService tokenRefresher = Executors.newScheduledThreadPool(1, tf);

  // Reference to the Telemetry Service in the client
  private final TelemetryService telemetryService;

  /**
   * Creates a OAuthManager entity for a given account, user and OAuthCredential with default time
   * to refresh the access token
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
        DEFAULT_UPDATE_THRESHOLD_RATIO,
        telemetryService);
  }

  /**
   * Creates a OAuthManager entity for a given account, user and OAuthCredential with a specified
   * time to renew the token
   *
   * @param accountName - the snowflake account name of this user
   * @param username - the snowflake username of the current user
   * @param oAuthCredential - the OAuth credential we're using to connect
   * @param updateThresholdRatio - the ratio between the expiration time of a token and the time
   *     needed to refresh it.
   * @param telemetryService reference to the telemetry service
   */
  OAuthManager(
      String accountName,
      String username,
      OAuthCredential oAuthCredential,
      URI tokenRequestURI,
      double updateThresholdRatio,
      TelemetryService telemetryService) {
    super(accountName, username);
    // if any of our arguments are null, throw an exception
    if (oAuthCredential == null) {
      throw new IllegalArgumentException();
    }

    if (updateThresholdRatio <= 0 || updateThresholdRatio >= 1) {
      throw new IllegalArgumentException("updateThresholdRation should fall in (0, 1)");
    }

    this.oAuthCredential = new AtomicReference<>(oAuthCredential);

    this.tokenRequestURI = tokenRequestURI;

    this.httpClient = HttpUtil.getHttpClient(accountName);

    this.telemetryService = telemetryService;

    this.updateThresholdRatio = updateThresholdRatio;

    refreshFailed = new AtomicBoolean();

    // generate our first token
    refreshToken();
  }

  @Override
  String getToken() {
    if (refreshFailed.get()) {
      LOGGER.error("getToken request failed due to token refresh failure");
      throw new SecurityException();
    }
    return oAuthCredential.get().getAccessToken();
  }

  @Override
  String getTokenType() {
    return TOKEN_TYPE;
  }

  /** refreshToken - Get new access token using refresh_token, client_id, client_secret */
  void refreshToken() {
    for (int retries = 0; retries < MAX_REFRESH_TOKEN_RETRY; retries++) {
      String respBodyString = null;
      try (CloseableHttpResponse httpResponse = httpClient.execute(makeRefreshTokenRequest())) {
        respBodyString = EntityUtils.toString(httpResponse.getEntity());

        if (httpResponse.getStatusLine().getStatusCode() == HttpStatusCodes.STATUS_CODE_OK) {
          JsonObject respBody = JsonParser.parseString(respBodyString).getAsJsonObject();

          if (respBody.has(ACCESS_TOKEN) && respBody.has(EXPIRES_IN)) {
            String newAccessToken = respBody.get(ACCESS_TOKEN).toString().replaceAll("^\"|\"$", "");
            oAuthCredential.get().setAccessToken(newAccessToken);

            // Refresh the token used in the telemetry service as well
            if (telemetryService != null) {
              telemetryService.refreshToken(newAccessToken);
            }

            // Schedule next refresh
            long nextRefreshDelay =
                (long) (respBody.get(EXPIRES_IN).getAsInt() * this.updateThresholdRatio);
            tokenRefresher.schedule(this::refreshToken, nextRefreshDelay, TimeUnit.SECONDS);

            LOGGER.info(
                "Refresh access token, next refresh is scheduled after {} seconds",
                nextRefreshDelay);
            return;
          }
          LOGGER.error("A response with status ok does not contain access_token and expires_in");
        }
      } catch (IOException e) {
        refreshFailed.set(true);
        throw new SFException(ErrorCode.OAUTH_REFRESH_TOKEN_ERROR, e.getMessage());
      }

      LOGGER.debug(
          "Refresh access token fail with response {}, retry count={}", respBodyString, retries);

      // Exponential backoff retries
      try {
        Thread.sleep((1L << retries) * 1000L);
      } catch (InterruptedException e) {
        throw new SFException(ErrorCode.OAUTH_REFRESH_TOKEN_ERROR, e.getMessage());
      }
    }

    refreshFailed.set(true);
    LOGGER.error("Fail to refresh access token");
    throw new SecurityException();
  }

  /** makeRefreshTokenRequest - make the request for refresh an access token */
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
    tokenRefresher.shutdown();
  }
}
