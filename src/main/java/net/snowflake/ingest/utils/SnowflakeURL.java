/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Snowflake URL Object https://account.region.snowflakecomputing.com:443 */
public class SnowflakeURL {

  private static final Logging logger = new Logging(SnowflakeURL.class);

  public static class SnowflakeURLBuilder {
    private String url;
    private boolean ssl;
    private String account;
    private int port;

    public SnowflakeURLBuilder setUrl(String url) {
      this.url = url;
      return this;
    }

    public SnowflakeURLBuilder setSsl(boolean ssl) {
      this.ssl = ssl;
      return this;
    }

    public SnowflakeURLBuilder setAccount(String account) {
      this.account = account;
      return this;
    }

    public SnowflakeURLBuilder setPort(int port) {
      this.port = port;
      return this;
    }

    public SnowflakeURL build() {
      return new SnowflakeURL(this);
    }
  }

  private String jdbcUrl;
  private String url;
  private boolean ssl;
  private String account;
  private int port;

  private SnowflakeURL(SnowflakeURLBuilder builder) {
    this.url = builder.url;
    this.ssl = builder.ssl;
    this.account = builder.account;
    this.port = builder.port;
    this.jdbcUrl = "jdbc:snowflake://" + builder.url + ":" + builder.port;
  }

  /**
   * Construct a SnowflakeURL object from a String
   *
   * @param urlStr
   */
  public SnowflakeURL(String urlStr) {
    Pattern pattern =
        Pattern.compile("^(https?://)?((([\\w\\d-]+)(\\.[\\w\\d-]+){2,})(:(\\d+))?)/?$");

    Matcher matcher = pattern.matcher(urlStr.trim().toLowerCase());

    if (!matcher.find()) {
      throw new SFException(ErrorCode.INVALID_URL);
    }

    ssl = !"http://".equals(matcher.group(1));
    url = matcher.group(3);
    account = matcher.group(4);

    if (matcher.group(7) != null) {
      port = Integer.parseInt(matcher.group(7));
    } else {
      port = ssl ? 443 : 80;
    }

    jdbcUrl = "jdbc:snowflake://" + url + ":" + port;
    logger.logDebug("parsed Snowflake URL={}", urlStr);
  }

  public String getJdbcUrl() {
    return jdbcUrl;
  }

  public String getAccount() {
    return account;
  }

  public boolean sslEnabled() {
    return ssl;
  }

  public String getScheme() {
    if (ssl) {
      return "https";
    } else {
      return "http";
    }
  }

  public String getFullUrl() {
    return url + ":" + port;
  }

  public String getUrlWithoutPort() {
    return url;
  }

  public int getPort() {
    return port;
  }

  @Override
  public String toString() {
    return getFullUrl();
  }
}
