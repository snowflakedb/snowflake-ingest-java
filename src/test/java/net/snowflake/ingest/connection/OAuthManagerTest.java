package net.snowflake.ingest.connection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.sql.Connection;
import net.snowflake.ingest.TestUtils;
import org.junit.Before;
import org.junit.Test;

public class OAuthManagerTest {
  private Connection jdbcConnection;

  @Before
  public void beforeAll() throws Exception {
    jdbcConnection = TestUtils.getConnection(true);
  }

  /** Check if access token do refresh */
  @Test
  public void testTokenRefresh() throws Exception {
    OAuthCredential oAuthCredential = TestUtils.generateOAuthCredential(jdbcConnection);

    // Set update threshold ratio to 5e-3, access token should be refreshed after 3 seconds
    SecurityManager securityManager =
        new OAuthManager(
            TestUtils.getAccount(),
            TestUtils.getUser(),
            oAuthCredential,
            TestUtils.getBaseURIBuilder(),
            5e-3,
            null);

    String token = securityManager.getToken();

    // if we immediately request the same token, it should be exactly the same
    assertEquals(
        "Two token requests prior to renewals should be the same",
        token,
        securityManager.getToken());

    // sleep for enough time for the thread renewal thread to run
    Thread.sleep(6000);

    // ascertain that we have overwritten the token
    assertNotEquals(
        "The renewal thread should have reset the token", token, securityManager.getToken());
  }
}
