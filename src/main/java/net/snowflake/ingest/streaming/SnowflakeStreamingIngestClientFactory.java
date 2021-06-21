/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming;

import java.util.Properties;
import net.snowflake.ingest.streaming.internal.SnowflakeStreamingIngestClientInternal;
import net.snowflake.ingest.utils.Constants;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;
import net.snowflake.ingest.utils.SnowflakeURL;
import net.snowflake.ingest.utils.Utils;

/** Builds a Streaming Ingest client for a specific account */
public class SnowflakeStreamingIngestClientFactory {
  public static Builder builder(String name) {
    return new Builder(name);
  }

  /** Build class to build a SnowflakeStreamingIngestClient */
  public static class Builder {
    private final String name;
    private Properties prop;

    private Builder(String name) {
      this.name = name;
    }

    public Builder setProperties(Properties prop) {
      this.prop = prop;
      return this;
    }

    public SnowflakeStreamingIngestClient build() {
      Utils.assertStringNotNullOrEmpty("client name", this.name);
      Utils.assertNotNull("connection properties", this.prop);

      if (!this.prop.containsKey(Constants.ACCOUNT_URL)) {
        throw new SFException(ErrorCode.MISSING_CONFIG, "Account URL");
      }

      if (!this.prop.containsKey(Constants.ROLE_NAME)) {
        throw new SFException(ErrorCode.MISSING_CONFIG, "Role Name");
      }

      SnowflakeURL accountURL = new SnowflakeURL(this.prop.getProperty(Constants.ACCOUNT_URL));
      Properties prop = Utils.createProperties(this.prop, accountURL.sslEnabled());
      return new SnowflakeStreamingIngestClientInternal(this.name, accountURL, prop);
    }
  }
}
