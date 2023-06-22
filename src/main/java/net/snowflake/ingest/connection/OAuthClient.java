package net.snowflake.ingest.connection;

import java.util.concurrent.atomic.AtomicReference;

/** Interface to perform token refresh request from {@link OAuthManager} */
public interface OAuthClient {
  AtomicReference<OAuthCredential> getoAuthCredentialRef();

  void refreshToken();
}
