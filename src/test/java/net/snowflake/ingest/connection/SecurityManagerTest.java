package net.snowflake.ingest.connection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.sql.Connection;
import java.util.concurrent.TimeUnit;
import net.snowflake.ingest.TestUtils;
import org.junit.Before;
import org.junit.Test;

/** SecurityManager - tests functionally of security manager */
public class SecurityManagerTest {
  // generate our keys using RSA
  private static final String ALGORITHM = "RSA";
  private KeyPair keyPair;
  private OAuthCredential oAuthCredential;

  @Before
  public void setup() throws Exception {
    // first we need to create a keypair
    KeyPairGenerator keyGen = KeyPairGenerator.getInstance(ALGORITHM);
    // we need a 2048 bit RSA key for this test
    keyGen.initialize(2048);
    // generate the actual keys
    keyPair = keyGen.generateKeyPair();

    // get connection for generating OAuth credential
    Connection jdbcConnection = TestUtils.getConnection(true);
    oAuthCredential = TestUtils.generateOAuthCredential(jdbcConnection);
  }

  /** Test instantiate jwt manager with invalid params */
  @Test
  public void testJWTManagerInstantiate() {
    // Try to create jwt manager without required args
    try {
      SecurityManager jwtManager = new JWTManager(null, null, null, 3, TimeUnit.SECONDS, null);
    } catch (IllegalArgumentException e1) {
      try {
        SecurityManager jwtManager =
            new JWTManager("account", "user", null, 3, TimeUnit.SECONDS, null);
      } catch (IllegalArgumentException e2) {
        return;
      }
    }
    assertFalse("testJWTManagerInstantiate failed", true);
  }

  /** Test instantiate OAuth manager with invalid params */
  @Test
  public void testOAuthManagerInstantiate() {
    // Try to create OAuth manager without required args
    try {
      SecurityManager oAuthManager = new OAuthManager("account", "user", null, null, null);
    } catch (IllegalArgumentException e1) {
      try {
        SecurityManager oAuthManager = new OAuthManager("account", "user", null, null, -1, null);
      } catch (IllegalArgumentException e2) {
        return;
      }
    }
    assertFalse("testOAuthManagerInstantiate failed", true);
  }

  /** Test get token type, should exactly match the request */
  @Test
  public void testGetTokenType() throws Exception {
    SecurityManager jwtManager =
        new JWTManager("account", "user", keyPair, 3, TimeUnit.SECONDS, null);
    assertEquals(jwtManager.getTokenType(), "KEYPAIR_JWT");

    SecurityManager oAuthManager =
        new OAuthManager(
            TestUtils.getAccount(),
            TestUtils.getUser(),
            oAuthCredential,
            TestUtils.getBaseURIBuilder(),
            null);
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

  /** Test behavior of getting token after refresh failed */
  @Test
  public void testGetTokenFail() {
    SecurityManager manager = new JWTManager("account", "user", keyPair, 3, TimeUnit.SECONDS, null);
    manager.setRefreshFailed(true);
    try {
      String token = manager.getToken();
    } catch (SecurityException e) {
      return;
    }
    assertFalse("testGetTokenFail failed", true);
  }
}
