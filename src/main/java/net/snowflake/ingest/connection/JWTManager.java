/*
 * Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.connection;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.JWSSigner;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import java.security.KeyPair;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import net.snowflake.ingest.utils.Cryptor;

/**
 * This class manages creating and automatically renewing the JWT token
 *
 * @author obabarinsa
 * @since 1.8
 */
final class JWTManager extends SecurityManager {
  // the token lifetime is 59 minutes
  private static final float LIFETIME_IN_MINUTES = 59;

  // the renewal time is 54 minutes
  private static final int RENEWAL_INTERVAL_IN_MINUTES = 54;

  private static final String TOKEN_TYPE = "KEYPAIR_JWT";

  // The public - private key pair we're using to connect to the service
  private final transient KeyPair keyPair;

  // the token itself
  protected final AtomicReference<String> token;

  /**
   * Creates a JWTManager entity for a given account, user and KeyPair with a specified time to
   * renew the token
   *
   * @param accountName - the snowflake account name of this user
   * @param username - the snowflake username of the current user
   * @param keyPair - the public/private key pair we're using to connect
   * @param timeTillRenewal - the time measure until we renew the token
   * @param unit the unit by which timeTillRenewal is measured
   * @param telemetryService reference to the telemetry service
   */
  JWTManager(
      String accountName,
      String username,
      KeyPair keyPair,
      int timeTillRenewal,
      TimeUnit unit,
      TelemetryService telemetryService) {
    super(accountName, username, telemetryService);
    // if any of our arguments are null, throw an exception
    if (keyPair == null) {
      throw new IllegalArgumentException();
    }
    token = new AtomicReference<>();

    // we have to keep around the keys
    this.keyPair = keyPair;

    // generate our first token
    regenerateToken();

    // schedule all future renewals
    tokenRefresher.scheduleAtFixedRate(
        this::regenerateToken, timeTillRenewal, timeTillRenewal, unit);
  }

  /**
   * Creates a JWTManager entity for a given account, user and KeyPair with the default time to
   * renew (RENEWAL_INTERVAL_IN_MINUTES minutes)
   *
   * @param accountName - the snowflake account name of this user
   * @param username - the snowflake username of the current user
   * @param keyPair - the public/private key pair we're using to connect
   * @param telemetryService reference to the telemetry service
   */
  JWTManager(
      String accountName, String username, KeyPair keyPair, TelemetryService telemetryService) {
    this(
        accountName,
        username,
        keyPair,
        RENEWAL_INTERVAL_IN_MINUTES,
        TimeUnit.MINUTES,
        telemetryService);
  }

  @Override
  String getToken() {
    // if we failed to regenerate the token at some point, throw
    if (refreshFailed.get()) {
      LOGGER.error("getToken request failed due to token regeneration failure");
      throw new SecurityException();
    }

    return token.get();
  }

  @Override
  String getTokenType() {
    return TOKEN_TYPE;
  }

  /** regenerateToken - Regenerates our Token given our current user, account and keypair */
  private void regenerateToken() {
    // create our JWT claim builder object
    JWTClaimsSet.Builder builder = new JWTClaimsSet.Builder();

    // get the subject to the fully qualified username
    String subject = String.format("%s.%s", account, user);

    // get the issuer
    String publicKeyFPInJwt = calculatePublicKeyFp(keyPair);
    String issuer = String.format("%s.%s.%s", account, user, publicKeyFPInJwt);

    // iat set to now
    Date iat = new Date(System.currentTimeMillis());

    // expiration in 59 minutes
    Date exp = new Date(iat.getTime() + 59 * 60 * 1000);

    // build claim set
    JWTClaimsSet claimsSet =
        builder.issuer(issuer).subject(subject).issueTime(iat).expirationTime(exp).build();
    LOGGER.debug("Creating new JWT with subject {} and issuer {}...", subject, issuer);

    SignedJWT signedJWT = new SignedJWT(new JWSHeader(JWSAlgorithm.RS256), claimsSet);

    JWSSigner signer = new RSASSASigner(this.keyPair.getPrivate());

    String newToken;
    try {
      signedJWT.sign(signer);
      newToken = signedJWT.serialize();
    } catch (JOSEException e) {
      refreshFailed.set(true);
      LOGGER.error("Failed to regenerate token! Exception is as follows : {}", e.getMessage());
      throw new SecurityException();
    }

    // atomically update the string
    LOGGER.info("Successfully created new JWT");
    token.set(newToken);

    // Refresh the token used in the telemetry service as well
    if (telemetryService != null) {
      telemetryService.refreshToken(newToken);
    }
  }

  /**
   * Given a keypair
   *
   * @return the fingerprint of public key
   *     <p>The idea is to hash public key's raw bytes using SHA-256 and converts hash into a string
   *     using Base64 encoding.
   */
  private String calculatePublicKeyFp(KeyPair keyPair) {
    // get the raw bytes of public key
    byte[] publicKeyRawBytes = keyPair.getPublic().getEncoded();

    // take sha256 on raw bytes and do base64 encode
    publicKeyFingerPrint = String.format("SHA256:%s", Cryptor.sha256HashBase64(publicKeyRawBytes));
    return publicKeyFingerPrint;
  }

  /** Currently, it only shuts down the instance of ExecutorService. */
  @Override
  public void close() {
    tokenRefresher.shutdown();
  }
}
