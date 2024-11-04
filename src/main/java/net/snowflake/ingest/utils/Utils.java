/*
 * Copyright (c) 2021-2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.utils;

import static net.snowflake.ingest.utils.Constants.USER;

import com.codahale.metrics.Timer;
import io.netty.util.internal.PlatformDependent;
import java.io.IOException;
import java.io.StringReader;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.lang.reflect.InvocationTargetException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.Provider;
import java.security.PublicKey;
import java.security.Security;
import java.security.interfaces.RSAPrivateCrtKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.RSAPublicKeySpec;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import net.snowflake.client.core.SFSessionProperty;
import org.apache.commons.codec.binary.Base64;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.openssl.jcajce.JceOpenSSLPKCS8DecryptorProviderBuilder;
import org.bouncycastle.operator.InputDecryptorProvider;
import org.bouncycastle.pkcs.PKCS8EncryptedPrivateKeyInfo;

/** Contains Ingest related utility functions */
public class Utils {

  private static final Logging logger = new Logging(Utils.class);

  private static final String DEFAULT_SECURITY_PROVIDER_NAME =
      "org.bouncycastle.jce.provider.BouncyCastleProvider";

  /** provider name */
  private static final String BOUNCY_CASTLE_PROVIDER = "BC";

  /** provider name for FIPS */
  private static final String BOUNCY_CASTLE_FIPS_PROVIDER = "BCFIPS";

  static {
    // Add Bouncy Castle to the security provider. This is required to
    // verify the signature on OCSP response and attached certificates.
    if (Security.getProvider(BOUNCY_CASTLE_PROVIDER) == null
        && Security.getProvider(BOUNCY_CASTLE_FIPS_PROVIDER) == null) {
      Security.addProvider(instantiateSecurityProvider());
    }
  }

  public static Provider getProvider() {
    final Provider bcProvider = Security.getProvider(BOUNCY_CASTLE_PROVIDER);
    if (bcProvider != null) {
      return bcProvider;
    }
    final Provider bcFipsProvider = Security.getProvider(BOUNCY_CASTLE_FIPS_PROVIDER);
    if (bcFipsProvider != null) {
      return bcFipsProvider;
    }
    throw new SFException(ErrorCode.INTERNAL_ERROR, "No security provider found");
  }

  private static Provider instantiateSecurityProvider() {
    try {
      logger.logInfo("Adding security provider {}", DEFAULT_SECURITY_PROVIDER_NAME);
      Class klass = Class.forName(DEFAULT_SECURITY_PROVIDER_NAME);
      return (Provider) klass.getDeclaredConstructor().newInstance();
    } catch (ExceptionInInitializerError
        | ClassNotFoundException
        | NoSuchMethodException
        | InstantiationException
        | IllegalAccessException
        | IllegalArgumentException
        | InvocationTargetException
        | SecurityException ex) {
      throw new SFException(
          ErrorCode.CRYPTO_PROVIDER_ERROR, DEFAULT_SECURITY_PROVIDER_NAME, ex.getMessage());
    }
  }

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

    // Use JWT if authorization type not specified
    if (!properties.containsKey(Constants.AUTHORIZATION_TYPE)) {
      properties.put(Constants.AUTHORIZATION_TYPE, Constants.JWT);
    }

    String authType = properties.get(Constants.AUTHORIZATION_TYPE).toString();
    if (authType.equals(Constants.JWT)) {
      if (!properties.containsKey(SFSessionProperty.PRIVATE_KEY.getPropertyKey())) {
        throw new SFException(ErrorCode.MISSING_CONFIG, Constants.PRIVATE_KEY);
      }
    } else if (authType.equals(Constants.OAUTH)) {
      if (!properties.containsKey(Constants.OAUTH_CLIENT_ID)) {
        throw new SFException(ErrorCode.MISSING_CONFIG, Constants.OAUTH_CLIENT_ID);
      }
      if (!properties.containsKey(Constants.OAUTH_CLIENT_SECRET)) {
        throw new SFException(ErrorCode.MISSING_CONFIG, Constants.OAUTH_CLIENT_SECRET);
      }
      if (!properties.containsKey(Constants.OAUTH_REFRESH_TOKEN)) {
        throw new SFException(ErrorCode.MISSING_CONFIG, Constants.OAUTH_REFRESH_TOKEN);
      }
    } else {
      throw new SFException(
          ErrorCode.INVALID_CONFIG_PARAMETER,
          String.format("authorization_type, should be %s or %s", Constants.JWT, Constants.OAUTH));
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
      logger.logInfo("Snowflake role is not provided, the default user role will be applied.");
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
    try {
      PEMParser pemParser = new PEMParser(new StringReader(key));
      PKCS8EncryptedPrivateKeyInfo encryptedPrivateKeyInfo =
          (PKCS8EncryptedPrivateKeyInfo) pemParser.readObject();
      pemParser.close();
      InputDecryptorProvider pkcs8Prov =
          new JceOpenSSLPKCS8DecryptorProviderBuilder().build(passphrase.toCharArray());
      JcaPEMKeyConverter converter =
          new JcaPEMKeyConverter().setProvider(Utils.getProvider().getName());
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

  /**
   * Get the fully qualified table name
   *
   * @param dbName the database name
   * @param schemaName the schema name
   * @param tableName the table name
   * @return the fully qualified table name
   */
  public static String getFullyQualifiedTableName(
      String dbName, String schemaName, String tableName) {
    return String.format("%s.%s.%s", dbName, schemaName, tableName);
  }

  /**
   * Get the fully qualified channel name
   *
   * @param dbName the database name
   * @param schemaName the schema name
   * @param tableName the table name
   * @param channelName the channel name
   * @return the fully qualified channel name
   */
  public static String getFullyQualifiedChannelName(
      String dbName, String schemaName, String tableName, String channelName) {
    return String.format("%s.%s.%s.%s", dbName, schemaName, tableName, channelName);
  }

  /**
   * Get concat dot path, check if any path is empty or null. Escape the dot field name to avoid
   * column name collision.
   *
   * @param path the path
   */
  public static String concatDotPath(String... path) {
    StringBuilder sb = new StringBuilder();
    for (String p : path) {
      if (p == null) {
        throw new IllegalArgumentException("Path cannot be null");
      }
      if (sb.length() > 0) {
        sb.append(".");
      }
      sb.append(p.replace("\\", "\\\\").replace(".", "\\."));
    }
    return sb.toString();
  }

  /**
   * Get the footer size (metadata size) of a parquet file
   *
   * @param bytes the serialized parquet file
   * @return the footer size
   */
  public static long getParquetFooterSize(byte[] bytes) throws IOException {
    final int magicOffset = bytes.length - ParquetFileWriter.MAGIC.length;
    final int footerSizeOffset = magicOffset - Integer.BYTES;

    if (footerSizeOffset < 0) {
      throw new IllegalArgumentException(
          String.format("Invalid parquet file. File too small, file length=%s.", bytes.length));
    }

    String fileMagic = new String(bytes, magicOffset, ParquetFileWriter.MAGIC.length);
    if (!ParquetFileWriter.MAGIC_STR.equals(fileMagic)
        && !ParquetFileWriter.EF_MAGIC_STR.equals(fileMagic)) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid parquet file. Bad parquet magic, expected=[%s | %s], actual=%s.",
              ParquetFileWriter.MAGIC_STR, ParquetFileWriter.EF_MAGIC_STR, fileMagic));
    }

    return BytesUtils.readIntLittleEndian(bytes, footerSizeOffset);
  }

  public static String getTwoHexChars() {
    String twoHexChars =
        Integer.toHexString((ThreadLocalRandom.current().nextInt() & 0x7FFFFFFF) % 0x100);
    if (twoHexChars.length() == 1) {
      twoHexChars = "0" + twoHexChars;
    }

    return twoHexChars;
  }
}
