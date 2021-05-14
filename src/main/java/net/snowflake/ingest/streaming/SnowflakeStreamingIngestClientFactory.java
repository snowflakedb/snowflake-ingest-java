/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming;

import java.util.Properties;
import net.snowflake.ingest.streaming.internal.Constants;
import net.snowflake.ingest.streaming.internal.SnowflakeStreamingIngestClientInternal;
import net.snowflake.ingest.streaming.internal.SnowflakeURL;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;
import net.snowflake.ingest.utils.StreamingUtils;

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
      StreamingUtils.assertStringNotNullOrEmpty("client name", this.name);
      StreamingUtils.assertNotNull("connection properties", this.prop);

      if (!this.prop.containsKey(Constants.ACCOUNT_URL)) {
        throw new SFException(ErrorCode.MISSING_CONFIG, "Account URL");
      }

      SnowflakeURL accountURL = new SnowflakeURL(this.prop.getProperty(Constants.ACCOUNT_URL));
      Properties prop = StreamingUtils.createProperties(this.prop, accountURL.sslEnabled());
      return new SnowflakeStreamingIngestClientInternal(this.name, accountURL, prop);
    }
  }
}
