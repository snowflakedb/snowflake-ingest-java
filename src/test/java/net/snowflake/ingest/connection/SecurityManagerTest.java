package net.snowflake.ingest.connection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.concurrent.TimeUnit;
import net.snowflake.ingest.TestUtils;
import net.snowflake.ingest.utils.Constants;
import org.junit.Before;
import org.junit.Test;

/** SecurityManager - tests functionally of security manager */
public class SecurityManagerTest {
  // generate our keys using RSA
  private static final String ALGORITHM = "RSA";
  private KeyPair keyPair;

  @Before
  public void setup() throws Exception {
    // first we need to create a keypair
    KeyPairGenerator keyGen = KeyPairGenerator.getInstance(ALGORITHM);
    // we need a 2048 bit RSA key for this test
    keyGen.initialize(2048);
    // generate the actual keys
    keyPair = keyGen.generateKeyPair();
  }

  /** Test instantiate jwt manager with invalid params */
  @Test(expected = IllegalArgumentException.class)
  public void testJWTManagerInstantiate() {
    SecurityManager jwtManager = new JWTManager(null, null, null, 3, TimeUnit.SECONDS, null);
  }

  /** Test instantiate OAuth manager with invalid params */
  @Test(expected = IllegalArgumentException.class)
  public void testOAuthManagerInstantiate() {
    SecurityManager oAuthManager = new OAuthManager("account", "user", null, null, null);
  }

  /** Test get token type, should exactly match the request */
  @Test
  public void testGetTokenType() throws Exception {
    SecurityManager jwtManager =
        new JWTManager("account", "user", keyPair, 3, TimeUnit.SECONDS, null);
    assertEquals(jwtManager.getTokenType(), "KEYPAIR_JWT");

    SecurityManager oAuthManager =
        new OAuthManager(TestUtils.getAccount(), TestUtils.getUser(), new MockOAuthClient(), 0.8);
    assertEquals(oAuthManager.getTokenType(), "OAUTH");
  }

  /** Evaluates whether or not we are actually renewing jwt tokens */
  @Test
  public void testRegenerateJWTToken() throws InterruptedException {
    // create the security manager;
    SecurityManager manager = new JWTManager("account", "user", keyPair, 3, TimeUnit.SECONDS, null);

    // grab a token
    String token = manager.getToken();

    // if we immediately request the same token, it should be exactly the same
    assertEquals(
        "Two token requests prior to renewals should be the same", token, manager.getToken());

    // sleep for enough time for the thread renewal thread to run
    Thread.sleep(6000);

    // ascertain that we have overwritten the token
    assertNotEquals("The renewal thread should have reset the token", token, manager.getToken());
  }

  /** Check if OAuth token do refresh */
  @Test
  public void testRefreshOAuthToken() throws Exception {

    // Set update threshold ratio to 5e-3, access token should be refreshed after 3 seconds
    SecurityManager securityManager =
        new OAuthManager(TestUtils.getAccount(), TestUtils.getUser(), new MockOAuthClient(), 5e-3);

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

  /** Test behavior of getting token after refresh failed */
  @Test(expected = SecurityException.class)
  public void testGetJWTTokenFail() {
    SecurityManager manager = new JWTManager("account", "user", keyPair, 3, TimeUnit.SECONDS, null);
    manager.setRefreshFailed(true);
    String token = manager.getToken();
  }

  /** Test behavior of getting token after refresh failed */
  @Test(expected = SecurityException.class)
  public void testGetOAuthTokenFail() throws Exception {
    MockOAuthClient mockOAuthClient = new MockOAuthClient();

    SecurityManager manager =
        new OAuthManager(TestUtils.getAccount(), TestUtils.getUser(), mockOAuthClient, 0.8);
    manager.setRefreshFailed(true);
    String token = manager.getToken();
  }

  /** Test refresh oauth token fail */
  @Test(expected = SecurityException.class)
  public void testOAuthRefreshFail() throws Exception {
    MockOAuthClient mockOAuthClient = new MockOAuthClient();
    mockOAuthClient.setFutureRefreshFailCount(Constants.MAX_OAUTH_REFRESH_TOKEN_RETRY);

    SecurityManager securityManager =
        new OAuthManager(TestUtils.getAccount(), TestUtils.getUser(), mockOAuthClient, 0.8);
    securityManager.close();
  }

  /** Test retry, should success */
  @Test
  public void testOAuthRefreshRetry() throws Exception {
    MockOAuthClient mockOAuthClient = new MockOAuthClient();
    mockOAuthClient.setFutureRefreshFailCount(Constants.MAX_OAUTH_REFRESH_TOKEN_RETRY - 1);

    SecurityManager securityManager =
        new OAuthManager(TestUtils.getAccount(), TestUtils.getUser(), mockOAuthClient, 0.8);
    securityManager.close();
  }
}
