/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.utils;

import static net.snowflake.ingest.utils.Constants.JDBC_PRIVATE_KEY;
import static net.snowflake.ingest.utils.Constants.JDBC_SSL;
import static net.snowflake.ingest.utils.Constants.JDBC_USER;
import static net.snowflake.ingest.utils.Constants.ROLE_NAME;

import com.codahale.metrics.Timer;
import com.google.common.base.Strings;
import java.io.StringReader;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Security;
import java.security.interfaces.RSAPrivateCrtKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.RSAPublicKeySpec;
import java.util.Map;
import java.util.Properties;
import net.snowflake.client.jdbc.internal.org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.apache.commons.codec.binary.Base64;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.openssl.jcajce.JceOpenSSLPKCS8DecryptorProviderBuilder;
import org.bouncycastle.operator.InputDecryptorProvider;
import org.bouncycastle.pkcs.PKCS8EncryptedPrivateKeyInfo;

/** Contains Ingest related utility functions */
public class Utils {

  private static final Logging logger = new Logging(Utils.class);

  /**
   * Assert when the String is null or Empty
   *
   * @param name
   * @param value
   * @throws SFException
   */
  public static void assertStringNotNullOrEmpty(String name, String value) throws SFException {
    if (Strings.isNullOrEmpty(value)) {
      throw new SFException(ErrorCode.NULL_OR_EMPTY_STRING, name);
    }
  }

  /**
   * Assert when the value is null
   *
   * @param name
   * @param value
   * @throws SFException
   */
  public static void assertNotNull(String name, Object value) throws SFException {
    if (value == null) {
      throw new SFException(ErrorCode.NULL_VALUE, name);
    }
  }

  /**
   * Create a Properties for snowflake connection
   *
   * @param inputProp input property map
   * @param sslEnabled if ssl is enabled
   * @return a Properties instance
   */
  public static Properties createProperties(Properties inputProp, boolean sslEnabled) {
    Properties properties = new Properties();

    // decrypt rsa key
    String privateKey = "";
    String privateKeyPassphrase = "";

    for (Map.Entry<Object, Object> entry : inputProp.entrySet()) {
      String key = entry.getKey().toString();
      String val = entry.getValue().toString();

      switch (key.toLowerCase()) {
        case Constants.PRIVATE_KEY:
          privateKey = val;
          break;
        case Constants.USER_NAME:
          properties.put(JDBC_USER, val);
          break;
        case Constants.ROLE_NAME:
          properties.put(ROLE_NAME, val);
          break;
        case Constants.PRIVATE_KEY_PASSPHRASE:
          privateKeyPassphrase = val;
          break;
        case Constants.ACCOUNT_URL:
          // do nothing as we already read it
          break;
        default:
          // ignore other keys
          logger.logDebug("Ignored property in input properties={}", key.toLowerCase());
      }
    }

    if (!privateKeyPassphrase.isEmpty()) {
      properties.put(JDBC_PRIVATE_KEY, parseEncryptedPrivateKey(privateKey, privateKeyPassphrase));
    } else if (!privateKey.isEmpty()) {
      properties.put(JDBC_PRIVATE_KEY, parsePrivateKey(privateKey));
    }

    // set ssl
    if (sslEnabled) {
      properties.put(JDBC_SSL, "on");
    } else {
      properties.put(JDBC_SSL, "off");
    }

    if (!properties.containsKey(JDBC_PRIVATE_KEY)) {
      throw new SFException(ErrorCode.MISSING_CONFIG, "private key");
    }

    if (!properties.containsKey(JDBC_USER)) {
      throw new SFException(ErrorCode.MISSING_CONFIG, "user name");
    }

    return properties;
  }

  /**
   * Parse a unencrypted private key
   *
   * @param key unencrypted private key
   * @return the unencrypted private key
   */
  public static PrivateKey parsePrivateKey(String key) {
    // remove header, footer, and line breaks
    key = key.replaceAll("-+[A-Za-z ]+-+", "");
    key = key.replaceAll("\\s", "");

    java.security.Security.addProvider(new BouncyCastleProvider());
    byte[] encoded = Base64.decodeBase64(key);
    try {
      KeyFactory kf = KeyFactory.getInstance("RSA");
      PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(encoded);
      return kf.generatePrivate(keySpec);
    } catch (Exception e) {
      throw new SFException(e, ErrorCode.INVALID_PRIVATE_KEY);
    }
  }

  /**
   * Parse a encrypted private key
   *
   * @param key encrypted private key
   * @param passphrase private key phrase
   * @return the decrypted private key
   */
  public static PrivateKey parseEncryptedPrivateKey(String key, String passphrase) {
    // remove header, footer, and line breaks
    key = key.replaceAll("-+[A-Za-z ]+-+", "");
    key = key.replaceAll("\\s", "");

    StringBuilder builder = new StringBuilder();
    builder.append("-----BEGIN ENCRYPTED PRIVATE KEY-----");
    for (int i = 0; i < key.length(); i++) {
      if (i % 64 == 0) {
        builder.append("\n");
      }
      builder.append(key.charAt(i));
    }
    builder.append("\n-----END ENCRYPTED PRIVATE KEY-----");
    key = builder.toString();
    Security.addProvider(new BouncyCastleFipsProvider());
    try {
      PEMParser pemParser = new PEMParser(new StringReader(key));
      PKCS8EncryptedPrivateKeyInfo encryptedPrivateKeyInfo =
          (PKCS8EncryptedPrivateKeyInfo) pemParser.readObject();
      pemParser.close();
      InputDecryptorProvider pkcs8Prov =
          new JceOpenSSLPKCS8DecryptorProviderBuilder().build(passphrase.toCharArray());
      JcaPEMKeyConverter converter =
          new JcaPEMKeyConverter().setProvider(BouncyCastleFipsProvider.PROVIDER_NAME);
      PrivateKeyInfo decryptedPrivateKeyInfo =
          encryptedPrivateKeyInfo.decryptPrivateKeyInfo(pkcs8Prov);
      return converter.getPrivateKey(decryptedPrivateKeyInfo);
    } catch (Exception e) {
      throw new SFException(e, ErrorCode.INVALID_ENCRYPTED_KEY);
    }
  }

  /**
   * Generate key pair object from private key
   *
   * @param privateKey private key
   * @return a key pair object
   * @throws NoSuchAlgorithmException if can't create key factory by using RSA algorithm
   * @throws InvalidKeySpecException if private key or public key is invalid
   */
  public static KeyPair createKeyPairFromPrivateKey(PrivateKey privateKey)
      throws NoSuchAlgorithmException, InvalidKeySpecException {
    if (!(privateKey instanceof RSAPrivateCrtKey)) {
      throw new IllegalArgumentException("Input private key is not a RSA private key");
    }

    KeyFactory kf = KeyFactory.getInstance("RSA");

    // generate public key from private key
    RSAPrivateCrtKey privk = (RSAPrivateCrtKey) privateKey;
    RSAPublicKeySpec publicKeySpec =
        new RSAPublicKeySpec(privk.getModulus(), privk.getPublicExponent());
    PublicKey publicK = kf.generatePublic(publicKeySpec);

    // create key pairs
    return new KeyPair(publicK, privateKey);
  }

  /** Create a new timer context if input is not null */
  public static Timer.Context createTimerContext(Timer timer) {
    return timer == null ? null : timer.time();
  }
}
