package net.snowflake.ingest.connection;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;

/** Mock implementation of {@link OAuthClient}, only use for test */
public class MockOAuthClient implements OAuthClient {
  private final AtomicReference<OAuthCredential> oAuthCredential;

  private int futureRefreshFailCount = 0;

  public MockOAuthClient() {
    OAuthCredential mockOAuthCredential =
        new OAuthCredential("CLIENT_ID", "CLIENT_SECRET", "REFRESH_TOKEN");
    oAuthCredential = new AtomicReference<>(mockOAuthCredential);
    oAuthCredential.get().setExpires_in(600);
  }

  @Override
  public AtomicReference<OAuthCredential> getoAuthCredentialRef() {
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
