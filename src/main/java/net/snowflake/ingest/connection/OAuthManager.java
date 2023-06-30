/*
 * Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.connection;

import java.util.concurrent.TimeUnit;
import net.snowflake.client.jdbc.internal.apache.http.client.utils.URIBuilder;
import net.snowflake.ingest.utils.Constants;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;

/** This class manages creating and automatically refresh the OAuth token */
public final class OAuthManager extends SecurityManager {
  private static final double DEFAULT_UPDATE_THRESHOLD_RATIO = 0.8;

  // the endpoint for token request
  private static final String TOKEN_REQUEST_ENDPOINT = "/oauth/token-request";

  // Content type header to specify the encoding
  private static final String OAUTH_CONTENT_TYPE_HEADER = "application/x-www-form-urlencoded";

  // Properties for token refresh request
  private static final String TOKEN_TYPE = "OAUTH";

  // Update threshold, a floating-point value representing the ratio between the expiration time of
  // an access token and the time needed to update it. It must be a value greater than 0 and less
  // than 1. E.g. An access token with expires_in=600 and update_threshold_ratio=0.8 would be
  // updated after 600*0.8 = 480.
  private final double updateThresholdRatio;

  private OAuthClient oAuthClient;

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
      throw new IllegalArgumentException("updateThresholdRatio should fall in (0, 1)");
    }
    this.updateThresholdRatio = updateThresholdRatio;
    this.oAuthClient = new SnowflakeOAuthClient(accountName, oAuthCredential, baseURIBuilder);

    // generate our first token
    refreshToken();
  }

  /**
   * Creates a OAuthManager entity for a given account, user and OAuthClient with a specified time
   * to refresh the token. Use for testing only.
   *
   * @param accountName - the snowflake account name of this user
   * @param username - the snowflake username of the current user
   * @param oAuthClient - the OAuth client to perform token refresh
   * @param updateThresholdRatio - the ratio between the expiration time of a token and the time
   *     needed to refresh it.
   */
  public OAuthManager(
      String accountName, String username, OAuthClient oAuthClient, double updateThresholdRatio) {
    super(accountName, username, null);

    this.updateThresholdRatio = updateThresholdRatio;
    this.oAuthClient = oAuthClient;

    refreshToken();
  }

  @Override
  String getToken() {
    if (refreshFailed.get()) {
      throw new SecurityException("getToken request failed due to token refresh failure");
    }
    return oAuthClient.getoAuthCredentialRef().get().getAccessToken();
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
  void setRefreshToken(String refreshToken) {
    oAuthClient.getoAuthCredentialRef().get().setRefreshToken(refreshToken);
  }

  /** refreshToken - Get new access token using refresh_token, client_id, client_secret */
  private void refreshToken() {
    for (int retries = 0; retries < Constants.MAX_OAUTH_REFRESH_TOKEN_RETRY; retries++) {
      try {
        oAuthClient.refreshToken();

        // Schedule next refresh
        long nextRefreshDelay =
            (long)
                (oAuthClient.getoAuthCredentialRef().get().getExpiresIn()
                    * this.updateThresholdRatio);
        tokenRefresher.schedule(this::refreshToken, nextRefreshDelay, TimeUnit.SECONDS);
        LOGGER.debug(
            "Refresh access token, next refresh is scheduled after {} seconds", nextRefreshDelay);

        return;
      } catch (SFException e1) {
        // Exponential backoff retries
        try {
          Thread.sleep((1L << retries) * 1000L);
        } catch (InterruptedException e2) {
          throw new SFException(ErrorCode.OAUTH_REFRESH_TOKEN_ERROR, e2.getMessage());
        }
      }
    }

    refreshFailed.set(true);
    throw new SecurityException("Fail to refresh access token");
  }

  /** Currently, it only shuts down the instance of ExecutorService. */
  @Override
  public void close() {
    tokenRefresher.shutdown();
  }
}
