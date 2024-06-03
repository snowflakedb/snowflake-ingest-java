/*
 * Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.connection;

import java.net.URI;
import java.util.Base64;

/** This class hold credentials for OAuth authentication */
public class OAuthCredential {
  private static final String BASIC_AUTH_HEADER_PREFIX = "Basic ";
  private final String authHeader;
  private final URI oAuthTokenEndpoint;
  private transient volatile String accessToken;
  private transient String refreshToken;
  private int expiresIn;
  private final boolean autoRefresh;

  /* Constructor for manually setting access token without auto refresh */
  public OAuthCredential(String accessToken) {
    this.authHeader = null;
    this.oAuthTokenEndpoint = null;
    this.accessToken = accessToken;
    this.autoRefresh = false;
  }

  /* Constructor for Snowflake OAuth with auto refresh */
  public OAuthCredential(String clientId, String clientSecret, String refreshToken) {
    this(clientId, clientSecret, refreshToken, null);
  }

  /* Constructor for OAuth with auto refresh */
  public OAuthCredential(
      String clientId, String clientSecret, String refreshToken, URI oAuthTokenEndpoint) {
    this.authHeader =
        BASIC_AUTH_HEADER_PREFIX
            + Base64.getEncoder().encodeToString((clientId + ":" + clientSecret).getBytes());
    this.refreshToken = refreshToken;
    this.oAuthTokenEndpoint = oAuthTokenEndpoint;
    this.autoRefresh = true;
  }

  public String getAuthHeader() {
    return authHeader;
  }

  public String getAccessToken() {
    return accessToken;
  }

  public void setAccessToken(String accessToken) {
    this.accessToken = accessToken;
  }

  public String getRefreshToken() {
    return refreshToken;
  }

  public void setRefreshToken(String refreshToken) {
    this.refreshToken = refreshToken;
  }

  public URI getOAuthTokenEndpoint() {
    return oAuthTokenEndpoint;
  }

  public void setExpiresIn(int expiresIn) {
    this.expiresIn = expiresIn;
  }

  public int getExpiresIn() {
    return expiresIn;
  }

  public boolean getIsAutoRefresh() {
    return autoRefresh;
  }
}
