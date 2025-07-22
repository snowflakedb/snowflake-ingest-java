/*
 * Copyright (c) 2023-2025 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.connection;

import static net.snowflake.ingest.utils.HttpUtil.NON_PROXY_HOSTS;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.HttpUtil;
import net.snowflake.ingest.utils.SFException;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Implementation of OAuth Client, used for refreshing an OAuth access token.
 */
public class OAuthClient {

  static final Logger LOGGER = LoggerFactory.getLogger(OAuthClient.class);
  private static final String TOKEN_REQUEST_ENDPOINT = "/oauth/token-request";

  // Content type header to specify the encoding
  private static final String OAUTH_CONTENT_TYPE_HEADER = "application/x-www-form-urlencoded";
  private static final String GRANT_TYPE_PARAM = "grant_type";
  private static final String ACCESS_TOKEN = "access_token";
  private static final String REFRESH_TOKEN = "refresh_token";
  private static final String EXPIRES_IN = "expires_in";

  private static final ObjectMapper objectMapper = new ObjectMapper();

  // OAuth credential
  private final AtomicReference<OAuthCredential> oAuthCredential;

  // Http client for submitting token refresh request
  private final CloseableHttpClient httpClient;

  /**
   * Creates an AuthClient for Snowflake OAuth given account, credential and base uri
   *
   * @param oAuthCredential - the OAuth credential we're using to connect
   * @param baseURIBuilder - the uri builder with common scheme, host and port
   */
  OAuthClient(OAuthCredential oAuthCredential, URIBuilder baseURIBuilder) {
    this.oAuthCredential = new AtomicReference<>(oAuthCredential);

    // build token request uri
    baseURIBuilder.setPath(TOKEN_REQUEST_ENDPOINT);
    URI oAuthTokenEndpoint = oAuthCredential.getOAuthTokenEndpoint();
    this.httpClient =
        HttpUtil.initHttpClient(
            System.getProperty(NON_PROXY_HOSTS) != null
                && HttpUtil.isInNonProxyHosts(
                    oAuthTokenEndpoint == null ? "" : oAuthTokenEndpoint.getHost()));
  }

  /** Get access token */
  public AtomicReference<OAuthCredential> getOAuthCredentialRef() {
    return oAuthCredential;
  }

  /** Refresh access token using a valid refresh token */
  public void refreshToken() throws IOException {
    return;
//    CloseableHttpResponse httpResponse = httpClient.execute(makeRefreshTokenRequest());
//    String respBodyString = EntityUtils.toString(httpResponse.getEntity());
//
//    if (httpResponse.getStatusLine().getStatusCode() == HttpURLConnection.HTTP_OK) {
//      JsonNode respBody = objectMapper.readTree(respBodyString);
//
//      if (respBody.has(ACCESS_TOKEN) && respBody.has(EXPIRES_IN)) {
//        // Trim surrounding quotation marks
//        String newAccessToken = respBody.get(ACCESS_TOKEN).toString().replaceAll("^\"|\"$", "");
//        oAuthCredential.get().setAccessToken(newAccessToken);
//        oAuthCredential.get().setExpiresIn(respBody.get(EXPIRES_IN).asInt());
//        return;
//      }
//    }
//    throw new SFException(
//        ErrorCode.OAUTH_REFRESH_TOKEN_ERROR,
//        "Refresh access token fail with response: " + respBodyString);
  }

  /** Helper method for making refresh request */
  private HttpUriRequest makeRefreshTokenRequest() {
    // TODO SNOW-1538108 Use SnowflakeServiceClient to make the request
    HttpPost post = new HttpPost(oAuthCredential.get().getOAuthTokenEndpoint());
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
    post.setEntity(new StringEntity(payloadString, ContentType.APPLICATION_FORM_URLENCODED));

    return post;
  }
}
