/*
 * Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.connection;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import com.google.api.client.http.HttpStatusCodes;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.HttpUtil;
import net.snowflake.ingest.utils.SFException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Implementation of Snowflake OAuth Client, used for refreshing an OAuth access token.
 */
public class SnowflakeOAuthClient implements OAuthClient {

  static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeOAuthClient.class);
  private static final String TOKEN_REQUEST_ENDPOINT = "/oauth/token-request";

  // Content type header to specify the encoding
  private static final String OAUTH_CONTENT_TYPE_HEADER = "application/x-www-form-urlencoded";
  private static final String GRANT_TYPE_PARAM = "grant_type";
  private static final String ACCESS_TOKEN = "access_token";
  private static final String REFRESH_TOKEN = "refresh_token";
  private static final String EXPIRES_IN = "expires_in";

  // OAuth credential
  private final AtomicReference<OAuthCredential> oAuthCredential;

  // exact uri for token request
  private final URI tokenRequestURI;

  // Http client for submitting token refresh request
  private final CloseableHttpClient httpClient;

  /**
   * Creates an SnowflakeOAuthClient given account, credential and base uri
   *
   * @param accountName - the snowflake account name of this user
   * @param oAuthCredential - the OAuth credential we're using to connect
   * @param baseURIBuilder - the uri builder with common scheme, host and port
   */
  SnowflakeOAuthClient(
      String accountName, OAuthCredential oAuthCredential, URIBuilder baseURIBuilder) {
    this.oAuthCredential = new AtomicReference<>(oAuthCredential);

    // build token request uri
    baseURIBuilder.setPath(TOKEN_REQUEST_ENDPOINT);
    try {
      this.tokenRequestURI = baseURIBuilder.build();
    } catch (URISyntaxException e) {
      throw new SFException(e, ErrorCode.MAKE_URI_FAILURE, e.getMessage());
    }

    this.httpClient = HttpUtil.getHttpClient(accountName);
  }

  /** Get access token */
  @Override
  public AtomicReference<OAuthCredential> getoAuthCredentialRef() {
    return oAuthCredential;
  }

  /** Refresh access token using a valid refresh token */
  @Override
  public void refreshToken() {
    String respBodyString = null;
    try (CloseableHttpResponse httpResponse = httpClient.execute(makeRefreshTokenRequest())) {
      respBodyString = EntityUtils.toString(httpResponse.getEntity());

      if (httpResponse.getStatusLine().getStatusCode() == HttpStatusCodes.STATUS_CODE_OK) {
        JsonObject respBody = JsonParser.parseString(respBodyString).getAsJsonObject();

        if (respBody.has(ACCESS_TOKEN) && respBody.has(EXPIRES_IN)) {
          // Trim surrounding quotation marks
          String newAccessToken = respBody.get(ACCESS_TOKEN).toString().replaceAll("^\"|\"$", "");

          oAuthCredential.get().setAccessToken(newAccessToken);
          oAuthCredential.get().setExpiresIn(respBody.get(EXPIRES_IN).getAsInt());
          return;
        }
      }
      throw new SFException(
          ErrorCode.OAUTH_REFRESH_TOKEN_ERROR,
          "Refresh access token fail with response: " + respBodyString);
    } catch (Exception e) {
      throw new SFException(ErrorCode.OAUTH_REFRESH_TOKEN_ERROR, e.getMessage());
    }
  }

  /** Helper method for making refresh request */
  private HttpUriRequest makeRefreshTokenRequest() {
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
}
