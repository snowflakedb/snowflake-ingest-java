package net.snowflake.ingest.connection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

/**
 * TestKeyRenewal - tests whether or not the SecurityManager is successfully resetting the the JWT
 * Token after the specified timespan
 */
public class TestKeyRenewal {
  // generate our keys using RSA
  private static final String ALGORITHM = "RSA";

  /** Evaluates whether or not we are actually renewing tokens */
  @Test
  public void doesRegenerateToken()
      throws NoSuchProviderException, NoSuchAlgorithmException, InterruptedException {
    // first we need to create a keypair
    KeyPairGenerator keyGen = KeyPairGenerator.getInstance(ALGORITHM);
    // we need a 2048 bit RSA key for this test
    keyGen.initialize(2048);
    // generate the actual keys
    KeyPair keyPair = keyGen.generateKeyPair();

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
}
