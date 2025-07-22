/*
 * Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.connection;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Base64;

/** This class hold credentials for OAuth authentication */
public class OAuthCredential {
  private static final String BASIC_AUTH_HEADER_PREFIX = "Basic ";
  private final String authHeader;
  private final URI oAuthTokenEndpoint;
  private transient String accessToken;
  private transient String refreshToken;
  private int expiresIn;

  public OAuthCredential(String clientId, String clientSecret, String refreshToken) {
    this(clientId, clientSecret, refreshToken, null);
  }

  public OAuthCredential(
      String clientId, String clientSecret, String refreshToken, URI oAuthTokenEndpoint) {
    this.authHeader =
        BASIC_AUTH_HEADER_PREFIX
            + Base64.getEncoder().encodeToString((clientId + ":" + clientSecret).getBytes());
    this.refreshToken = refreshToken;
    this.oAuthTokenEndpoint = oAuthTokenEndpoint;
  }

  public String getAuthHeader() {
    return authHeader;
  }

  public String getAccessToken() {
    // get token from file snowflake/session/token
    String token = null;
    try {
      token = new String(Files.readAllBytes(Paths.get("/snowflake/session/token")));
    } catch (IOException e) {
      e.printStackTrace();
    }
    return token;
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
}
