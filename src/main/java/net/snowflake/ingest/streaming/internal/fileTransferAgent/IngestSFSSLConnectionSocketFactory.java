/*
 * Replicated from snowflake-jdbc: net.snowflake.client.core.SFSSLConnectionSocketFactory
 * Tag: v3.25.1
 * Source: https://github.com/snowflakedb/snowflake-jdbc/blob/v3.25.1/src/main/java/net/snowflake/client/core/SFSSLConnectionSocketFactory.java
 */

package net.snowflake.ingest.streaming.internal.fileTransferAgent;

import java.io.IOException;
import java.net.Proxy;
import java.net.Socket;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.TrustManager;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.protocol.HttpContext;

class IngestSFSSLConnectionSocketFactory extends SSLConnectionSocketFactory {
  private static final String SSL_VERSION = "TLSv1.2";
  private final boolean socksProxyDisabled;

  IngestSFSSLConnectionSocketFactory(TrustManager[] trustManagers, boolean socksProxyDisabled)
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
    SSLContext sslContext = SSLContext.getInstance(SSL_VERSION);
    sslContext.init(null, trustManagers, null);
    return sslContext;
  }

  @Override
  public Socket createSocket(HttpContext ctx) throws IOException {
    return this.socksProxyDisabled ? new Socket(Proxy.NO_PROXY) : super.createSocket(ctx);
  }

  private static String[] decideCipherSuites() {
    String sysCipherSuites = System.getProperty("https.cipherSuites");
    return sysCipherSuites != null
        ? sysCipherSuites.split(",")
        : ((SSLServerSocketFactory) SSLServerSocketFactory.getDefault()).getDefaultCipherSuites();
  }
}
