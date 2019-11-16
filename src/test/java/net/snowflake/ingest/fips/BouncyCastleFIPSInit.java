package net.snowflake.ingest.fips;

import org.bouncycastle.crypto.CryptoServicesRegistrar;
import org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider;

import java.security.Security;

public class BouncyCastleFIPSInit
{
  private final static String JCE_PROVIDER_BOUNCY_CASTLE_FIPS = "BCFIPS";
  private final static String JCE_PROVIDER_SUN_JCE = "SunJCE";
  private final static String JCE_PROVIDER_SUN_RSA_SIGN = "SunRsaSign";
  private final static String JCE_KEYSTORE_BOUNCY_CASTLE = "BCFKS";
  private final static String JCE_KEYSTORE_JKS = "JKS";
  private final static String BOUNCY_CASTLE_RNG_HYBRID_MODE = "C:HYBRID;ENABLE{All};";

  private final static String SSL_ENABLED_PROTOCOLS = "TLSv1.2,TLSv1.1,TLSv1";
  private final static String SSL_ENABLED_CIPHERSUITES = "TLS_AES_128_GCM_SHA256,"
      + "TLS_AES_256_GCM_SHA384,"
      + "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,"
      + "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,"
      + "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,"
      + "TLS_RSA_WITH_AES_256_GCM_SHA384,"
      + "TLS_ECDH_ECDSA_WITH_AES_256_GCM_SHA384,"
      + "TLS_ECDH_RSA_WITH_AES_256_GCM_SHA384,"
      + "TLS_DHE_RSA_WITH_AES_256_GCM_SHA384,"
      + "TLS_DHE_DSS_WITH_AES_256_GCM_SHA384(,"
      + "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,"
      + "TLS_RSA_WITH_AES_128_GCM_SHA256,"
      + "TLS_ECDH_ECDSA_WITH_AES_128_GCM_SHA256,"
      + "TLS_ECDH_RSA_WITH_AES_128_GCM_SHA256,"
      + "TLS_DHE_RSA_WITH_AES_128_GCM_SHA256,"
      + "TLS_DHE_DSS_WITH_AES_128_GCM_SHA256,"
      + "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384,"
      + "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384,"
      + "TLS_RSA_WITH_AES_256_CBC_SHA256,"
      + "TLS_ECDH_ECDSA_WITH_AES_256_CBC_SHA384,"
      + "TLS_ECDH_RSA_WITH_AES_256_CBC_SHA384,"
      + "TLS_DHE_RSA_WITH_AES_256_CBC_SHA256,"
      + "TLS_DHE_DSS_WITH_AES_256_CBC_SHA256,"
      + "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA,"
      + "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,"
      + "TLS_RSA_WITH_AES_256_CBC_SHA,"
      + "TLS_ECDH_ECDSA_WITH_AES_256_CBC_SHA,"
      + "TLS_ECDH_RSA_WITH_AES_256_CBC_SHA,"
      + "TLS_DHE_RSA_WITH_AES_256_CBC_SHA,"
      + "TLS_DHE_DSS_WITH_AES_256_CBC_SHA,"
      + "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256,"
      + "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256,"
      + "TLS_RSA_WITH_AES_128_CBC_SHA256,"
      + "TLS_ECDH_ECDSA_WITH_AES_128_CBC_SHA256,"
      + "TLS_ECDH_RSA_WITH_AES_128_CBC_SHA256,"
      + "TLS_DHE_RSA_WITH_AES_128_CBC_SHA256,"
      + "TLS_DHE_DSS_WITH_AES_128_CBC_SHA256,"
      + "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA,"
      + "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA,"
      + "TLS_RSA_WITH_AES_128_CBC_SHA,"
      + "TLS_ECDH_ECDSA_WITH_AES_128_CBC_SHA,"
      + "TLS_ECDH_RSA_WITH_AES_128_CBC_SHA,"
      + "TLS_DHE_RSA_WITH_AES_128_CBC_SHA,"
      + "TLS_DHE_DSS_WITH_AES_128_CBC_SHA";

  private final static  String JAVA_SYSTEM_PROPERTY_SSL_KEYSTORE_TYPE = "javax.net.ssl.keyStoreType";
  private final static  String JAVA_SYSTEM_PROPERTY_SSL_TRUSTSTORE_TYPE = "javax.net.ssl.trustStoreType";
  private final static  String JAVA_SYSTEM_PROPERTY_SSL_PROTOCOLS = "jdk.tls.client.protocols";
  private final static  String JAVA_SYSTEM_PROPERTY_SSL_CIPHERSUITES = "jdk.tls.client.cipherSuites";

  public static void init()
  {
    // set keystore types for BouncyCastle libraries
    System.setProperty(JAVA_SYSTEM_PROPERTY_SSL_KEYSTORE_TYPE,
                       JCE_KEYSTORE_BOUNCY_CASTLE);
    System.setProperty(JAVA_SYSTEM_PROPERTY_SSL_TRUSTSTORE_TYPE,
                       JCE_KEYSTORE_JKS);
    // remove Java's standard encryption and SSL providers
    Security.removeProvider(JCE_PROVIDER_SUN_JCE);
    Security.removeProvider(JCE_PROVIDER_SUN_RSA_SIGN);

    // workaround to connect to accounts.google.com over HTTPS, which consists
    // of disabling TLS 1.3 and disabling default SSL cipher suites that are
    // using CHACHA20_POLY1305 algorithms
    System.setProperty(JAVA_SYSTEM_PROPERTY_SSL_PROTOCOLS,
                       SSL_ENABLED_PROTOCOLS);
    System.setProperty(JAVA_SYSTEM_PROPERTY_SSL_CIPHERSUITES,
                       SSL_ENABLED_CIPHERSUITES);
    /*
     * Insert BouncyCastle's FIPS-compliant encryption and SSL providers.
     */
    BouncyCastleFipsProvider bcFipsProvider = new BouncyCastleFipsProvider(
        BOUNCY_CASTLE_RNG_HYBRID_MODE);
    /*
     * We remove BCFIPS provider pessimistically. This is a no-op if provider
     * does not exist. This is necessary to always add it to the first
     * position when calling insertProviderAt.
     *
     * JavaDoc for insertProviderAt states:
     *   "A provider cannot be added if it is already installed."
     */
    Security.removeProvider(JCE_PROVIDER_BOUNCY_CASTLE_FIPS);
    Security.insertProviderAt(bcFipsProvider, 1);
    CryptoServicesRegistrar.setApprovedOnlyMode(true);
  }
}
