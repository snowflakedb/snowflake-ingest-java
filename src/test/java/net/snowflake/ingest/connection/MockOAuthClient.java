/*
 * Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.connection;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import net.snowflake.client.jdbc.internal.apache.http.client.utils.URIBuilder;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;

/** Mock implementation of {@link OAuthClient}, only use for test */
public class MockOAuthClient extends OAuthClient {
  private final AtomicReference<OAuthCredential> oAuthCredential;

  private int futureRefreshFailCount = 0;

  public MockOAuthClient() {
    super(new OAuthCredential("CLIENT_ID", "CLIENT_SECRET", "REFRESH_TOKEN"), new URIBuilder());
    oAuthCredential =
        new AtomicReference<>(new OAuthCredential("CLIENT_ID", "CLIENT_SECRET", "REFRESH_TOKEN"));
    oAuthCredential.get().setExpiresIn(600);
  }

  @Override
  public AtomicReference<OAuthCredential> getOAuthCredentialRef() {
    return oAuthCredential;
  }

  @Override
  public void refreshToken() {
    if (futureRefreshFailCount == 0) {
      oAuthCredential.get().setAccessToken(UUID.randomUUID().toString());
      return;
    }
    futureRefreshFailCount--;
    throw new SFException(ErrorCode.OAUTH_REFRESH_TOKEN_ERROR);
  }

  public void setFutureRefreshFailCount(int futureRefreshFailCount) {
    this.futureRefreshFailCount = futureRefreshFailCount;
  }
}
