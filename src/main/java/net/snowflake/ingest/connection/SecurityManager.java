package net.snowflake.ingest.connection;

import com.sun.istack.internal.logging.Logger;
import org.jose4j.jws.AlgorithmIdentifiers;
import org.jose4j.jws.JsonWebSignature;
import org.jose4j.jwt.JwtClaims;

import java.security.KeyPair;
import java.text.MessageFormat;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

/**
 * @author obabarinsa
 * This class manages creating and automatically renewing the JWT token
 * @since 1.6
 */
final class SecurityManager implements AutoCloseable
{

  //the logger for SecurityManager
  private static final Logger LOGGER =  Logger.getLogger(SecurityManager.class);

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
    LOGGER.log(Level.INFO, "Creating Token with Issuer {0}", account + "." + user);

    //the lifetime of the token is 59
    claims.setExpirationTimeMinutesInTheFuture(LIFETIME);

    //the token was issued as of this moment in time
    claims.setIssuedAtToNow();

    //now we need to create the JWS that will contain these claims
    JsonWebSignature websig = new JsonWebSignature();

    //set the payload of the web signature to a json version of our claims
    websig.setPayload(claims.toJson());
    LOGGER.log(Level.INFO, "Claims JSON is {0}", claims.toJson());

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
      LOGGER.severe("Failed to regenerate token! Exception is as follows", e);
      throw new SecurityException();
    }

    //atomically update the string
    token.set(newToken);
  }


  /**
   * getToken - returns we've most recently generated
   * @throws SecurityException if we failed to regenerate a token since the last call
   */
  public String getToken()
  {
    //if we failed to regenerate the token at some point, throw
    if(regenFailed.get())
    {
      LOGGER.severe("getToken request failed due to token regeneration failure");
      throw new SecurityException();
    }

    return token.get();
  }

  /**
   * This is an empty close method for the sake of making
   * this class autocloseable
   */
  public void close()
  {

  }
}
