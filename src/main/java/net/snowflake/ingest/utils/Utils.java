/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.utils;

import static net.snowflake.ingest.utils.Constants.USER;

import com.codahale.metrics.Timer;
import io.netty.util.internal.PlatformDependent;
import java.io.StringReader;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
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
import java.util.List;
import java.util.Map;
import java.util.Properties;
import net.snowflake.client.core.SFSessionProperty;
import net.snowflake.client.jdbc.internal.org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import net.snowflake.client.jdbc.internal.org.bouncycastle.jce.provider.BouncyCastleProvider;
import net.snowflake.client.jdbc.internal.org.bouncycastle.openssl.PEMParser;
import net.snowflake.client.jdbc.internal.org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import net.snowflake.client.jdbc.internal.org.bouncycastle.openssl.jcajce.JceOpenSSLPKCS8DecryptorProviderBuilder;
import net.snowflake.client.jdbc.internal.org.bouncycastle.operator.InputDecryptorProvider;
import net.snowflake.client.jdbc.internal.org.bouncycastle.pkcs.PKCS8EncryptedPrivateKeyInfo;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.commons.codec.binary.Base64;

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
    if (isNullOrEmpty(value)) {
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
   * @return a Properties instance
   */
  public static Properties createProperties(Properties inputProp) {
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
        case Constants.PRIVATE_KEY_PASSPHRASE:
          privateKeyPassphrase = val;
          break;
        default:
          properties.put(key.toLowerCase(), val);
      }
    }

    if (!privateKeyPassphrase.isEmpty()) {
      properties.put(
          SFSessionProperty.PRIVATE_KEY.getPropertyKey(),
          parseEncryptedPrivateKey(privateKey, privateKeyPassphrase));
    } else if (!privateKey.isEmpty()) {
      properties.put(SFSessionProperty.PRIVATE_KEY.getPropertyKey(), parsePrivateKey(privateKey));
    }

    if (!properties.containsKey(SFSessionProperty.PRIVATE_KEY.getPropertyKey())) {
      throw new SFException(ErrorCode.MISSING_CONFIG, "private_key");
    }

    if (!properties.containsKey(USER)) {
      throw new SFException(ErrorCode.MISSING_CONFIG, "user");
    }

    if (!properties.containsKey(Constants.ACCOUNT_URL)) {
      if (!properties.containsKey(Constants.HOST)) {
        throw new SFException(ErrorCode.MISSING_CONFIG, "host");
      }
      if (!properties.containsKey(Constants.SCHEME)) {
        throw new SFException(ErrorCode.MISSING_CONFIG, "scheme");
      }
      if (!properties.containsKey(Constants.PORT)) {
        throw new SFException(ErrorCode.MISSING_CONFIG, "port");
      }

      properties.put(
          Constants.ACCOUNT_URL,
          Utils.constructAccountUrl(
              properties.get(Constants.SCHEME).toString(),
              properties.get(Constants.HOST).toString(),
              Integer.parseInt(properties.get(Constants.PORT).toString())));
    }

    if (!properties.containsKey(Constants.ROLE)) {
      throw new SFException(ErrorCode.MISSING_CONFIG, "role");
    }

    /**
     * Behavior change in JDBC release 3.13.25
     *
     * @see <a href="https://community.snowflake.com/s/article/JDBC-Driver-Release-Notes">Snowflake
     *     Documentation Release Notes </a>
     */
    properties.put(SFSessionProperty.ALLOW_UNDERSCORES_IN_HOST.getPropertyKey(), "true");

    return properties;
  }

  /** Construct account url from input schema, host and port */
  public static String constructAccountUrl(String scheme, String host, int port) {
    return String.format("%s://%s:%d", scheme, host, port);
  }

  /**
   * Parse an unencrypted private key
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
   * Parse an encrypted private key
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
    Security.addProvider(new BouncyCastleProvider());
    try {
      PEMParser pemParser = new PEMParser(new StringReader(key));
      PKCS8EncryptedPrivateKeyInfo encryptedPrivateKeyInfo =
          (PKCS8EncryptedPrivateKeyInfo) pemParser.readObject();
      pemParser.close();
      InputDecryptorProvider pkcs8Prov =
          new JceOpenSSLPKCS8DecryptorProviderBuilder().build(passphrase.toCharArray());
      JcaPEMKeyConverter converter =
          new JcaPEMKeyConverter().setProvider(BouncyCastleProvider.PROVIDER_NAME);
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

  /** Convert long value to a byte array */
  public static byte[] toByteArray(long value) {
    byte[] result = new byte[8];

    for (int i = 7; i >= 0; --i) {
      result[i] = (byte) ((int) (value & 255L));
      value >>= 8;
    }

    return result;
  }

  /** Convert int value to a byte array */
  public static byte[] toByteArray(int value) {
    return new byte[] {
      (byte) (value >> 24), (byte) (value >> 16), (byte) (value >> 8), (byte) value
    };
  }

  /** Utility function to check whether a string is null or empty */
  public static boolean isNullOrEmpty(String string) {
    return string == null || string.isEmpty();
  }

  /** Release any outstanding memory and then close the buffer allocator */
  public static void closeAllocator(BufferAllocator alloc) {
    for (BufferAllocator childAlloc : alloc.getChildAllocators()) {
      childAlloc.close();
    }
    alloc.close();
  }

  /** Util function to show memory usage info and debug memory issue in the SDK */
  public static void showMemory() {
    List<BufferPoolMXBean> pools = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class);
    for (BufferPoolMXBean pool : pools) {
      logger.logInfo(
          "Pool name={}, pool count={}, memory used={}, total capacity={}",
          pool.getName(),
          pool.getCount(),
          pool.getMemoryUsed(),
          pool.getTotalCapacity());
    }

    Runtime runtime = Runtime.getRuntime();
    logger.logInfo(
        "Max direct memory={}, max runtime memory={}, total runtime memory={}, free runtime"
            + " memory={}",
        PlatformDependent.maxDirectMemory(),
        runtime.maxMemory(),
        runtime.totalMemory(),
        runtime.freeMemory());

    MemoryUsage nonHeapMem = ManagementFactory.getMemoryMXBean().getNonHeapMemoryUsage();
    logger.logInfo(
        "Non-heap memory usage max={}, used={}, committed={}",
        nonHeapMem.getMax(),
        nonHeapMem.getUsed(),
        nonHeapMem.getCommitted());

    MemoryUsage heapMem = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
    logger.logInfo(
        "Heap memory usage max={}, used={}, committed={}",
        heapMem.getMax(),
        heapMem.getUsed(),
        heapMem.getCommitted());
  }

  /** Return the stack trace for a given exception */
  public static String getStackTrace(Throwable e) {
    if (e == null) {
      return null;
    }

    StringBuilder stackTrace = new StringBuilder();
    for (StackTraceElement element : e.getStackTrace()) {
      stackTrace.append(System.lineSeparator()).append(element.toString());
    }
    return stackTrace.toString();
  }
}
