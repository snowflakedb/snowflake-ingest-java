/*
 * Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.connection;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import net.snowflake.client.jdbc.internal.apache.http.HttpHeaders;
import net.snowflake.client.jdbc.internal.apache.http.client.methods.CloseableHttpResponse;
import net.snowflake.client.jdbc.internal.apache.http.client.methods.HttpPost;
import net.snowflake.client.jdbc.internal.apache.http.client.utils.URIBuilder;
import net.snowflake.client.jdbc.internal.apache.http.entity.ContentType;
import net.snowflake.client.jdbc.internal.apache.http.entity.StringEntity;
import net.snowflake.client.jdbc.internal.apache.http.impl.client.CloseableHttpClient;
import net.snowflake.client.jdbc.internal.apache.http.util.EntityUtils;
import net.snowflake.client.jdbc.internal.google.api.client.http.HttpStatusCodes;
import net.snowflake.client.jdbc.internal.google.gson.JsonObject;
import net.snowflake.client.jdbc.internal.google.gson.JsonParser;
import net.snowflake.ingest.utils.Constants;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.HttpUtil;
import net.snowflake.ingest.utils.SFException;

/** This class manages creating and automatically refresh the OAuth token */
final class OAuthManager extends SecurityManager {
  private static final double DEFAULT_UPDATE_THRESHOLD_RATIO = 0.8;

  // the endpoint for token request
  private static final String TOKEN_REQUEST_ENDPOINT = "/oauth/token-request";

  // Content type header to specify the encoding
  private static final String OAUTH_CONTENT_TYPE_HEADER = "application/x-www-form-urlencoded";

  // Properties for token refresh request
  private static final String TOKEN_TYPE = "OAUTH";
  private static final String GRANT_TYPE_PARAM = "grant_type";
  private static final String ACCESS_TOKEN = "access_token";
  private static final String REFRESH_TOKEN = "refresh_token";
  private static final String EXPIRES_IN = "expires_in";

  // Update threshold, a floating-point value representing the ratio between the expiration time of
  // an access token and the time needed to update it. It must be a value greater than 0 and less
  // than 1. E.g. An access token with expires_in=600 and update_threshold_ratio=0.8 would be
  // updated after 600*0.8 = 480.
  private final double updateThresholdRatio;

  private final AtomicReference<OAuthCredential> oAuthCredential;

  private final URI tokenRequestURI;

  // Http client for submitting token refresh request
  private final CloseableHttpClient httpClient;

  /**
   * Creates a OAuthManager entity for a given account, user and OAuthCredential with default time
   * to refresh the access token
   *
   * @param accountName - the snowflake account name of this user
   * @param username - the snowflake username of the current user
   * @param oAuthCredential - the OAuth credential we're using to connect
   * @param baseURIBuilder - the uri builder with common scheme, host and port
   * @param telemetryService reference to the telemetry service
   */
  OAuthManager(
      String accountName,
      String username,
      OAuthCredential oAuthCredential,
      URIBuilder baseURIBuilder,
      TelemetryService telemetryService) {
    this(
        accountName,
        username,
        oAuthCredential,
        baseURIBuilder,
        DEFAULT_UPDATE_THRESHOLD_RATIO,
        telemetryService);
  }

  /**
   * Creates a OAuthManager entity for a given account, user and OAuthCredential with a specified
   * time to refresh the token
   *
   * @param accountName - the snowflake account name of this user
   * @param username - the snowflake username of the current user
   * @param oAuthCredential - the OAuth credential we're using to connect
   * @param baseURIBuilder - the uri builder with common scheme, host and port
   * @param updateThresholdRatio - the ratio between the expiration time of a token and the time
   *     needed to refresh it.
   * @param telemetryService reference to the telemetry service
   */
  OAuthManager(
      String accountName,
      String username,
      OAuthCredential oAuthCredential,
      URIBuilder baseURIBuilder,
      double updateThresholdRatio,
      TelemetryService telemetryService) {
    // disable telemetry service until jdbc v3.13.34 is released
    super(accountName, username, null);

    // if any of our arguments are null, throw an exception
    if (oAuthCredential == null || baseURIBuilder == null) {
      throw new IllegalArgumentException();
    }

    // check if update threshold is within (0, 1)
    if (updateThresholdRatio <= 0 || updateThresholdRatio >= 1) {
      throw new IllegalArgumentException("updateThresholdRation should fall in (0, 1)");
    }

    // build token request uri
    baseURIBuilder.setPath(TOKEN_REQUEST_ENDPOINT);
    try {
      this.tokenRequestURI = baseURIBuilder.build();
    } catch (URISyntaxException e) {
      throw new SFException(e, ErrorCode.MAKE_URI_FAILURE, e.getMessage());
    }

    this.oAuthCredential = new AtomicReference<>(oAuthCredential);
    this.httpClient = HttpUtil.getHttpClient(accountName);
    this.updateThresholdRatio = updateThresholdRatio;

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

  /**
   * Set refresh token, this method is for refresh token renewal without requiring to restart
   * client. This method only works when the authorization type is OAuth.
   *
   * @param refreshToken the new refresh token
   */
  public void setRefreshToken(String refreshToken) {
    oAuthCredential.get().setRefreshToken(refreshToken);
  }

  /** refreshToken - Get new access token using refresh_token, client_id, client_secret */
  void refreshToken() {
    for (int retries = 0; retries < Constants.MAX_REFRESH_TOKEN_RETRY; retries++) {
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
