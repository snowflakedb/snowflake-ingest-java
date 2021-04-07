/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import net.snowflake.ingest.utils.Cryptor;
import net.snowflake.ingest.utils.ThreadFactoryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class manages creating and automatically renewing the JWT token
 *
 * @author obabarinsa
 * @since 1.8
 */
final class SecurityManager implements AutoCloseable {
  // the logger for SecurityManager
  private static final Logger LOGGER = LoggerFactory.getLogger(SecurityManager.class);

  // the token lifetime is 59 minutes
  private static final float LIFETIME = 59;

  // the renewal time is 54 minutes
  private static final int RENEWAL_INTERVAL = 54;

  // The public - private key pair we're using to connect to the service
  private transient KeyPair keyPair;

  // the name of the account on behalf of which we're connecting
  private String account;

  // Fingerprint of public key sent from client in jwt payload
  private String publicKeyFingerPrint;

  // the name of the user who will be loading the files
  private String user;

  // the token itself
  private AtomicReference<String> token;

  // Did we fail to regenerate our token at some point?
  private AtomicBoolean regenFailed;

  // Thread factory for daemon threads so that application can shutdown
  final ThreadFactory tf = ThreadFactoryUtil.poolThreadFactory(getClass().getSimpleName(), true);

  // the thread we use for renewing all tokens
  private final ScheduledExecutorService keyRenewer = Executors.newScheduledThreadPool(1, tf);

  /**
   * Creates a SecurityManager entity for a given account, user and KeyPair with a specified time to
   * renew the token
   *
   * @param accountname - the snowflake account name of this user
   * @param username - the snowflake username of the current user
   * @param keyPair - the public/private key pair we're using to connect
   * @param timeTillRenewal - the time measure until we renew the token
   * @param unit the unit by which timeTillRenewal is measured
   */
  SecurityManager(
      String accountname, String username, KeyPair keyPair, int timeTillRenewal, TimeUnit unit) {
    // if any of our arguments are null, throw an exception
    if (accountname == null || username == null || keyPair == null) {
      throw new IllegalArgumentException();
    }
    account = parseAccount(accountname);
    user = username.toUpperCase();

    // create our automatic reference to a string (our token)
    token = new AtomicReference<>();

    // we haven't yet failed to regenerate our token
    regenFailed = new AtomicBoolean();

    // we have to keep around the keys
    this.keyPair = keyPair;

    // generate our first token
    regenerateToken();

    // schedule all future renewals
    keyRenewer.scheduleAtFixedRate(this::regenerateToken, timeTillRenewal, timeTillRenewal, unit);
  }

  /**
   * Creates a SecurityManager entity for a given account, user and KeyPair with the default time to
   * renew (54 minutes)
   *
   * @param accountname - the snowflake account name of this user
   * @param username - the snowflake username of the current user
   * @param keyPair - the public/private key pair we're using to connect
   */
  SecurityManager(String accountname, String username, KeyPair keyPair) {
    this(accountname, username, keyPair, RENEWAL_INTERVAL, TimeUnit.MINUTES);
  }

  /**
   * Trims an account name if it contains a <b>"."</b>
   *
   * <p>Snowflake's python connector trims an accountname if it contains a "."
   *
   * @param accountName given accountName in SimpleIngestManager's constructor
   * @return initial part of account name if originally it contained a ".", otherwise return the
   *     same accountName.
   * @see <a
   *     href="https://github.com/snowflakedb/snowflake-connector-python/blob/master/src/snowflake/connector/util_text.py#L227">Python
   *     Connector</a>
   */
  private String parseAccount(final String accountName) {
    String parseAccount = null;
    if (accountName.contains(".")) {
      final String[] accountParts = accountName.split("[.]");
      if (accountParts.length > 1) {
        parseAccount = accountParts[0];
      }
    } else {
      parseAccount = accountName;
    }
    return parseAccount.toUpperCase();
  }

  /** regenerateToken - Regenerates our Token given our current user, account and keypair */
  private void regenerateToken() {
    // create our JWT claim builder object
    JWTClaimsSet.Builder builder = new JWTClaimsSet.Builder();

    // set the subject to the fully qualified username
    String subject = String.format("%s.%s", account, user);
    LOGGER.info("Creating Token with subject {}", subject);

    // set the issuer
    String publicKeyFPInJwt = calculatePublicKeyFp(keyPair);
    String issuer = String.format("%s.%s.%s", account, user, publicKeyFPInJwt);
    LOGGER.info("Creating Token with issuer {}", issuer);

    // iat set to now
    Date iat = new Date(System.currentTimeMillis());

    // expiration in 59 minutes
    Date exp = new Date(iat.getTime() + 59 * 60 * 1000);

    // build claim set
    JWTClaimsSet claimsSet =
        builder.issuer(issuer).subject(subject).issueTime(iat).expirationTime(exp).build();

    SignedJWT signedJWT = new SignedJWT(new JWSHeader(JWSAlgorithm.RS256), claimsSet);

    JWSSigner signer = new RSASSASigner(this.keyPair.getPrivate());

    String newToken;
    try {
      signedJWT.sign(signer);
      newToken = signedJWT.serialize();
    } catch (JOSEException e) {
      regenFailed.set(true);
      LOGGER.error("Failed to regenerate token! Exception is as follows : {}", e.getMessage());
      throw new SecurityException();
    }

    // atomically update the string
    LOGGER.info("Created new JWT");
    token.set(newToken);
  }

  /**
   * getToken - returns we've most recently generated
   *
   * @return the string version of a valid JWT token
   * @throws SecurityException if we failed to regenerate a token since the last call
   */
  String getToken() {
    // if we failed to regenerate the token at some point, throw
    if (regenFailed.get()) {
      LOGGER.error("getToken request failed due to token regeneration failure");
      throw new SecurityException();
    }

    return token.get();
  }

  /* Only used in testing at the moment */
  String getAccount() {
    return this.account;
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

  /** Only called by SecurityManagerTest */
  String getPublicKeyFingerPrint() {
    return publicKeyFingerPrint;
  }

  /** Currently, it only shuts down the instance of ExecutorService. */
  @Override
  public void close() {
    keyRenewer.shutdown();
  }
}
