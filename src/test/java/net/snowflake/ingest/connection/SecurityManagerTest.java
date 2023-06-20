package net.snowflake.ingest.connection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Test;

/** SecurityManager - tests functionally of security manager */
public class SecurityManagerTest {
  // generate our keys using RSA
  private static final String ALGORITHM = "RSA";
  private KeyPair keyPair;

  @Before
  public void setup() throws NoSuchAlgorithmException {
    // first we need to create a keypair
    KeyPairGenerator keyGen = KeyPairGenerator.getInstance(ALGORITHM);
    // we need a 2048 bit RSA key for this test
    keyGen.initialize(2048);
    // generate the actual keys
    keyPair = keyGen.generateKeyPair();
  }

  /** Test instantiate security manager with invalid params */
  @Test
  public void testSecurityManagerInstantiate() {
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
    assertFalse("testSecurityManagerInstantiate failed", true);
  }

  /** Test get token type, should exactly match the request */
  @Test
  public void testGetTokenType() {
    SecurityManager manager = new JWTManager("account", "user", keyPair, 3, TimeUnit.SECONDS, null);
    assertEquals(manager.getTokenType(), "KEYPAIR_JWT");
  }

  /** Evaluates whether or not we are actually renewing jwt tokens */
  @Test
  public void doesRegenerateJWTToken() throws InterruptedException {
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
