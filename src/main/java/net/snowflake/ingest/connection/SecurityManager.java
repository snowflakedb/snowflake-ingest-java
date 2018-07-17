/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.connection;

import net.snowflake.ingest.utils.ThreadFactoryUtil;
import org.jose4j.jws.AlgorithmIdentifiers;
import org.jose4j.jws.JsonWebSignature;
import org.jose4j.jwt.JwtClaims;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.KeyPair;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This class manages creating and automatically renewing the JWT token
 * @author obabarinsa
 * @since 1.8
 */
final class SecurityManager
{
  //the logger for SecurityManager
  private static final Logger LOGGER =
                    LoggerFactory.getLogger(SecurityManager.class);

  //the token lifetime is 59 minutes
  private static final float LIFETIME = 59;

  //the renewal time is 54 minutes
  private static final int RENEWAL_INTERVAL = 54;

  //The public - private key pair we're using to connect to the service
  private transient KeyPair keyPair;

  //the name of the account on behalf of which we're connecting
  private String account;

  //the name of the user who will be loading the files
  private String user;

  //the token itself
  private AtomicReference<String> token;

  //Did we fail to regenerate our token at some point?
  private AtomicBoolean regenFailed;

  // Thread factory for daemon threads so that application can shutdown
  final ThreadFactory tf =
      ThreadFactoryUtil.poolThreadFactory(getClass().getSimpleName(), true);

  //the thread we use for renewing all tokens
  private final ScheduledExecutorService keyRenewer =
                                  Executors.newScheduledThreadPool(1, tf);

  /**
   * Creates a SecurityManager entity for a given account, user and KeyPair
   * with a specified time to renew the token
   *
   * @param accountname - the snowflake account name of this user
   * @param username - the snowflake username of the current user
   * @param keyPair - the public/private key pair we're using to connect
   * @param timeTillRenewal - the time measure until we renew the token
   * @param unit the unit by which timeTillRenewal is measured
   */
  SecurityManager(String accountname, String username, KeyPair keyPair,
                  int timeTillRenewal, TimeUnit unit)
  {
    //if any of our arguments are null, throw an exception
    if (accountname == null || username == null || keyPair == null)
    {
      throw new IllegalArgumentException();
    }
    account = accountname.toUpperCase();
    user = username.toUpperCase();

    //create our automatic reference to a string (our token)
    token = new AtomicReference<>();

    //we haven't yet failed to regenerate our token
    regenFailed = new AtomicBoolean();

    //we have to keep around the keys
    this.keyPair = keyPair;

    //generate our first token
    regenerateToken();

    //schedule all future renewals
    keyRenewer.scheduleAtFixedRate(this::regenerateToken,
        timeTillRenewal,
        timeTillRenewal, unit);
  }

  /**
   * Creates a SecurityManager entity for a given account, user and KeyPair
   * with the default time to renew (54 minutes)
   *
   * @param accountname - the snowflake account name of this user
   * @param username - the snowflake username of the current user
   * @param keyPair - the public/private key pair we're using to connect
   */
  SecurityManager(String accountname, String username, KeyPair keyPair)
  {
    this(accountname, username, keyPair, RENEWAL_INTERVAL, TimeUnit.MINUTES);
  }

  /**
   * regenerateToken - Regenerates our Token given our current user,
   *                    account and keypair
   */
  private void regenerateToken()
  {
    //create our JWT claim object
    JwtClaims claims = new JwtClaims();

    //set the issuer to the fully qualified username
    claims.setIssuer(account + "." + user);
    LOGGER.info("Creating Token with Issuer {}", account + "." + user);

    //the lifetime of the token is 59
    claims.setExpirationTimeMinutesInTheFuture(LIFETIME);

    //the token was issued as of this moment in time
    claims.setIssuedAtToNow();

    //now we need to create the JWS that will contain these claims
    JsonWebSignature websig = new JsonWebSignature();

    //set the payload of the web signature to a json version of our claims
    websig.setPayload(claims.toJson());
    LOGGER.info("Claims JSON is {}", claims.toJson());

    //sign the signature with our private key
    websig.setKey(keyPair.getPrivate());

    //sign using RSA-SHA256
    websig.setAlgorithmHeaderValue(AlgorithmIdentifiers.RSA_USING_SHA256);

    //the new token we want to use
    String newToken;
    //Extract our serialization
    try
    {
      newToken = websig.getCompactSerialization();
    }
    catch (Exception e)
    {
      regenFailed.set(true);
      LOGGER.error("Failed to regenerate token! Exception is as follows : {}",
                   e.getMessage());
      throw new SecurityException();
    }

    //atomically update the string
    LOGGER.info("Created new JWT  - {}", newToken);
    token.set(newToken);
  }


  /**
   * getToken - returns we've most recently generated
   *
   * @return the string version of a valid JWT token
   * @throws SecurityException if we failed to regenerate a token since the last call
   */
  String getToken()
  {
    //if we failed to regenerate the token at some point, throw
    if (regenFailed.get())
    {
      LOGGER.error("getToken request failed due to token regeneration failure");
      throw new SecurityException();
    }

    return token.get();
  }

}
