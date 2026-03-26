/*
 * Replicated from snowflake-jdbc (v3.25.1)
 * Source: https://github.com/snowflakedb/snowflake-jdbc/blob/v3.25.1/src/main/java/net/snowflake/client/jdbc/cloud/storage/S3HttpUtil.java
 *
 * Permitted differences: package, SFLogger uses ingest version,
 * SnowflakeUtil.isNullOrEmpty replaced with StorageClientUtil.isNullOrEmpty,
 * HttpClientSettingsKey/HttpProtocol/ErrorCode/SnowflakeSQLException/SFSessionProperty
 * use ingest versions (same package or ingest utils).
 * @SnowflakeJdbcInternalApi removed. SFLoggerUtil kept from JDBC temporarily.
 */
package net.snowflake.ingest.streaming.internal.fileTransferAgent;

import static net.snowflake.ingest.streaming.internal.fileTransferAgent.StorageClientUtil.isNullOrEmpty;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import java.util.Properties;
import net.snowflake.client.log.SFLoggerUtil;
import net.snowflake.ingest.streaming.internal.fileTransferAgent.log.SFLogger;
import net.snowflake.ingest.streaming.internal.fileTransferAgent.log.SFLoggerFactory;
import net.snowflake.ingest.utils.SFSessionProperty;

public class S3HttpUtil {
  private static final SFLogger logger =
      SFLoggerFactory.getLogger(net.snowflake.ingest.utils.HttpUtil.class);

  /**
   * A static function to set S3 proxy params when there is a valid session
   *
   * @param key key to HttpClient map containing OCSP and proxy info
   * @param clientConfig the configuration needed by S3 to set the proxy
   */
  // Parameter uses JDBC's HttpClientSettingsKey because session.getHttpClientKey() returns it.
  // This path is only used when session != null (never from streaming ingest).
  public static void setProxyForS3(
      net.snowflake.client.core.HttpClientSettingsKey key, ClientConfiguration clientConfig) {
    if (key != null && key.usesProxy()) {
      clientConfig.setProxyProtocol(
          key.getProxyHttpProtocol() == net.snowflake.client.core.HttpProtocol.HTTPS
              ? Protocol.HTTPS
              : Protocol.HTTP);
      clientConfig.setProxyHost(key.getProxyHost());
      clientConfig.setProxyPort(key.getProxyPort());
      clientConfig.setNonProxyHosts(key.getNonProxyHosts());
      String logMessage =
          "Setting S3 proxy. Host: "
              + key.getProxyHost()
              + ", port: "
              + key.getProxyPort()
              + ", protocol: "
              + key.getProxyHttpProtocol()
              + ", non-proxy hosts: "
              + key.getNonProxyHosts();
      if (!isNullOrEmpty(key.getProxyUser()) && !isNullOrEmpty(key.getProxyPassword())) {
        logMessage +=
            ", user: "
                + key.getProxyUser()
                + ", password is "
                + SFLoggerUtil.isVariableProvided(key.getProxyPassword());
        clientConfig.setProxyUsername(key.getProxyUser());
        clientConfig.setProxyPassword(key.getProxyPassword());
      }
      logger.debug(logMessage);
    } else {
      logger.debug("Omitting S3 proxy setup");
    }
  }

  /**
   * A static function to set S3 proxy params for sessionless connections using the proxy params
   * from the StageInfo
   *
   * @param proxyProperties proxy properties
   * @param clientConfig the configuration needed by S3 to set the proxy
   * @throws SnowflakeSQLException when an error is encountered
   */
  public static void setSessionlessProxyForS3(
      Properties proxyProperties, ClientConfiguration clientConfig) throws SnowflakeSQLException {
    // do nothing yet
    if (proxyProperties != null
        && proxyProperties.size() > 0
        && proxyProperties.getProperty(SFSessionProperty.USE_PROXY.getPropertyKey()) != null) {
      Boolean useProxy =
          Boolean.valueOf(
              proxyProperties.getProperty(SFSessionProperty.USE_PROXY.getPropertyKey()));
      if (useProxy) {
        // set up other proxy related values.
        String proxyHost =
            proxyProperties.getProperty(SFSessionProperty.PROXY_HOST.getPropertyKey());
        int proxyPort;
        try {
          proxyPort =
              Integer.parseInt(
                  proxyProperties.getProperty(SFSessionProperty.PROXY_PORT.getPropertyKey()));
        } catch (NumberFormatException | NullPointerException e) {
          throw new SnowflakeSQLException(
              ErrorCode.INVALID_PROXY_PROPERTIES, "Could not parse port number");
        }
        String proxyUser =
            proxyProperties.getProperty(SFSessionProperty.PROXY_USER.getPropertyKey());
        String proxyPassword =
            proxyProperties.getProperty(SFSessionProperty.PROXY_PASSWORD.getPropertyKey());
        String nonProxyHosts =
            proxyProperties.getProperty(SFSessionProperty.NON_PROXY_HOSTS.getPropertyKey());
        String proxyProtocol =
            proxyProperties.getProperty(SFSessionProperty.PROXY_PROTOCOL.getPropertyKey());
        Protocol protocolEnum =
            (!isNullOrEmpty(proxyProtocol) && proxyProtocol.equalsIgnoreCase("https"))
                ? Protocol.HTTPS
                : Protocol.HTTP;
        clientConfig.setProxyHost(proxyHost);
        clientConfig.setProxyPort(proxyPort);
        clientConfig.setNonProxyHosts(nonProxyHosts);
        clientConfig.setProxyProtocol(protocolEnum);
        String logMessage =
            "Setting sessionless S3 proxy. Host: "
                + proxyHost
                + ", port: "
                + proxyPort
                + ", non-proxy hosts: "
                + nonProxyHosts
                + ", protocol: "
                + proxyProtocol;
        if (!isNullOrEmpty(proxyUser) && !isNullOrEmpty(proxyPassword)) {
          logMessage += ", user: " + proxyUser + " with password provided";
          clientConfig.setProxyUsername(proxyUser);
          clientConfig.setProxyPassword(proxyPassword);
        }
        logger.debug(logMessage);
      } else {
        logger.debug("Omitting sessionless S3 proxy setup as proxy is disabled");
      }
    } else {
      logger.debug("Omitting sessionless S3 proxy setup");
    }
  }
}
