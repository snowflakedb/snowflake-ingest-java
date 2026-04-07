/*
 * Replicated from snowflake-jdbc: net.snowflake.client.core.SFSSLConnectionSocketFactory
 * Tag: v3.25.1
 * Source: https://github.com/snowflakedb/snowflake-jdbc/blob/v3.25.1/src/main/java/net/snowflake/client/core/SFSSLConnectionSocketFactory.java
 *
 * Permitted differences: package, class renamed (SFSSLConnectionSocketFactory ->
 * IngestSSLConnectionSocketFactory to avoid confusion with JDBC version during transition).
 */

package net.snowflake.ingest.streaming.internal.fileTransferAgent;

import java.io.IOException;
import java.net.Proxy;
import java.net.Socket;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.TrustManager;
import net.snowflake.ingest.streaming.internal.fileTransferAgent.log.SFLogger;
import net.snowflake.ingest.streaming.internal.fileTransferAgent.log.SFLoggerFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.protocol.HttpContext;

/** Snowflake custom SSLConnectionSocketFactory */
class IngestSSLConnectionSocketFactory extends SSLConnectionSocketFactory {
  private static final SFLogger logger =
      SFLoggerFactory.getLogger(IngestSSLConnectionSocketFactory.class);

  private static final String SSL_VERSION = "TLSv1.2";

  private final boolean socksProxyDisabled;

  public IngestSSLConnectionSocketFactory(TrustManager[] trustManagers, boolean socksProxyDisabled)
      throws NoSuchAlgorithmException, KeyManagementException {
    super(
        initSSLContext(trustManagers),
        new String[] {SSL_VERSION},
        decideCipherSuites(),
        SSLConnectionSocketFactory.getDefaultHostnameVerifier());
    this.socksProxyDisabled = socksProxyDisabled;
  }

  private static SSLContext initSSLContext(TrustManager[] trustManagers)
      throws NoSuchAlgorithmException, KeyManagementException {
    // enforce using SSL_VERSION
    SSLContext sslContext = SSLContext.getInstance(SSL_VERSION);
    sslContext.init(
        null, // key manager
        trustManagers, // trust manager
        null); // secure random
    return sslContext;
  }

  @Override
  public Socket createSocket(HttpContext ctx) throws IOException {
    return socksProxyDisabled ? new Socket(Proxy.NO_PROXY) : super.createSocket(ctx);
  }

  /**
   * Decide cipher suites that will be passed into the SSLConnectionSocketFactory
   *
   * @return List of cipher suites.
   */
  private static String[] decideCipherSuites() {
    String sysCipherSuites = StorageClientUtil.systemGetProperty("https.cipherSuites");

    String[] cipherSuites =
        sysCipherSuites != null
            ? sysCipherSuites.split(",")
            :
            // use jdk default cipher suites
            ((SSLServerSocketFactory) SSLServerSocketFactory.getDefault()).getDefaultCipherSuites();

    // cipher suites need to be picked up in code explicitly for jdk 1.7
    // https://stackoverflow.com/questions/44378970/
    logger.trace(
        "Cipher suites used: {}",
        (net.snowflake.ingest.streaming.internal.fileTransferAgent.log.ArgSupplier)
            () -> Arrays.toString(cipherSuites));

    return cipherSuites;
  }
}
