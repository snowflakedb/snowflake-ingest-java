/*
 * Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.connection;

import java.util.Base64;

/** This class hold credentials for OAuth authentication */
public class OAuthCredential {
  private static final String BASIC_AUTH_HEADER_PREFIX = "Basic ";
  private final String authHeader;
  private final String clientId;
  private final String clientSecret;
  private String accessToken;
  private String refreshToken;
  private int expires_in;

  public OAuthCredential(String clientId, String clientSecret, String refreshToken) {
    this.authHeader =
        BASIC_AUTH_HEADER_PREFIX
            + Base64.getEncoder().encodeToString((clientId + ":" + clientSecret).getBytes());
    this.clientId = clientId;
    this.clientSecret = clientSecret;
    this.refreshToken = refreshToken;
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

  public void setExpires_in(int expires_in) {
    this.expires_in = expires_in;
  }

  public int getExpires_in() {
    return expires_in;
  }
}
