package net.snowflake.ingest.connection;

import org.jose4j.jws.AlgorithmIdentifiers;
import org.jose4j.jws.JsonWebSignature;
import org.jose4j.jwt.JwtClaims;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.KeyPair;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
/**
 * @author obabarinsa
 * This class manages creating and automatically renewing the JWT token
 * @since 1.8
 */
final class SecurityManager
{

  //the logger for SecurityManager
  private static final Logger LOGGER =  LoggerFactory.getLogger(SecurityManager.class.getName());

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

  //this will create a task responsible for automatically regenerating our key
  private ScheduledExecutorService keyRenewer;

  //the token itself
  private AtomicReference<String> token;

  //Did we fail to regenerate our token at some point?
  private AtomicBoolean regenFailed;

  /**
   * Creates a SecurityManager entity for a given account, user and KeyPair
   * @param accountname - the snowflake account name of this user
   * @param username - the snowflake username of the current user
   * @param keypair - the public/private key pair we're using to connect
   */
  public SecurityManager(String accountname, String username, KeyPair keypair)
  {
    //if any of our arguments are null, throw an exception
    if(accountname == null || username == null || keypair == null)
    {
      throw new IllegalArgumentException();
    }
    account = accountname.toUpperCase();
    user = username.toUpperCase();

    //create our automatic reference to a string (our token)
    token = new AtomicReference<>();

    //generate our key renewal thread
    keyRenewer = Executors.newScheduledThreadPool(1);

    //we haven't yet failed to regenerate our token
    regenFailed = new AtomicBoolean();

    //we have to keep around the keys
    this.keyPair = keypair;

    //generate our first token
    regenerateToken();

    //schedule all future renewals
    keyRenewer.scheduleAtFixedRate(this::regenerateToken,
        RENEWAL_INTERVAL,
        RENEWAL_INTERVAL, TimeUnit.MINUTES);
  }

  /**
   * Regenerates our Token given our current user, account and keypair
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
    catch(Exception e)
    {
      regenFailed.set(true);
      LOGGER.error("Failed to regenerate token! Exception is as follows : {}", e.getMessage());
      throw new SecurityException();
    }

    //atomically update the string
    token.set(newToken);
  }


  /**
   * getToken - returns we've most recently generated
   * @return the string version of a valid JWT token
   * @throws SecurityException if we failed to regenerate a token since the last call
   */
  public String getToken()
  {
    //if we failed to regenerate the token at some point, throw
    if(regenFailed.get())
    {
      LOGGER.error("getToken request failed due to token regeneration failure");
      throw new SecurityException();
    }

    return token.get();
  }

}
