/*
 * Replicated from snowflake-jdbc: net.snowflake.client.core.HttpClientSettingsKey
 * Tag: v3.25.1
 * Source: https://github.com/snowflakedb/snowflake-jdbc/blob/v3.25.1/src/main/java/net/snowflake/client/core/HttpClientSettingsKey.java
 *
 * Value class wrapping OCSPMode and proxy properties.
 */

package net.snowflake.ingest.streaming.internal.fileTransferAgent;

import java.io.Serializable;
import net.snowflake.ingest.utils.OCSPMode;

public class HttpClientSettingsKey implements Serializable {
  private final OCSPMode ocspMode;
  private final boolean useProxy;
  private final String proxyHost;
  private final int proxyPort;
  private final String nonProxyHosts;
  private final String proxyUser;
  private final String proxyPassword;
  private final String proxyProtocol;

  public HttpClientSettingsKey(
      OCSPMode mode,
      String host,
      int port,
      String nonProxyHosts,
      String user,
      String password,
      String scheme) {
    this.useProxy = true;
    this.ocspMode = mode != null ? mode : OCSPMode.FAIL_OPEN;
    this.proxyHost = host != null ? host.trim() : "";
    this.proxyPort = port;
    this.nonProxyHosts = nonProxyHosts != null ? nonProxyHosts.trim() : "";
    this.proxyUser = user != null ? user.trim() : "";
    this.proxyPassword = password != null ? password.trim() : "";
    this.proxyProtocol = scheme != null ? scheme.trim() : "http";
  }

  public HttpClientSettingsKey(OCSPMode mode) {
    this.useProxy = false;
    this.ocspMode = mode != null ? mode : OCSPMode.FAIL_OPEN;
    this.proxyHost = "";
    this.proxyPort = 0;
    this.nonProxyHosts = "";
    this.proxyUser = "";
    this.proxyPassword = "";
    this.proxyProtocol = "http";
  }

  public OCSPMode getOcspMode() {
    return this.ocspMode;
  }

  public boolean usesProxy() {
    return this.useProxy;
  }

  public String getProxyHost() {
    return this.proxyHost;
  }

  public int getProxyPort() {
    return this.proxyPort;
  }

  public String getNonProxyHosts() {
    return this.nonProxyHosts;
  }

  public String getProxyUser() {
    return this.proxyUser;
  }

  public String getProxyPassword() {
    return this.proxyPassword;
  }

  public String getProxyProtocol() {
    return this.proxyProtocol;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof HttpClientSettingsKey)) return false;
    HttpClientSettingsKey other = (HttpClientSettingsKey) obj;
    if (other.ocspMode.getValue() != this.ocspMode.getValue()) return false;
    if (!other.useProxy && !this.useProxy) return true;
    return other.proxyHost.equalsIgnoreCase(this.proxyHost)
        && other.proxyPort == this.proxyPort
        && other.proxyUser.equalsIgnoreCase(this.proxyUser)
        && other.proxyPassword.equalsIgnoreCase(this.proxyPassword)
        && other.proxyProtocol.equalsIgnoreCase(this.proxyProtocol);
  }

  @Override
  public int hashCode() {
    return this.ocspMode.getValue()
        + (this.proxyHost
                + this.proxyPort
                + this.proxyUser
                + this.proxyPassword
                + this.proxyProtocol)
            .hashCode();
  }
}
