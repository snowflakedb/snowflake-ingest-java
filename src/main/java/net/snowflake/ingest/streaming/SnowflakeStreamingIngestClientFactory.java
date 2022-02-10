/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming;

import java.util.Map;
import java.util.Properties;
import net.snowflake.ingest.streaming.internal.SnowflakeStreamingIngestClientInternal;
import net.snowflake.ingest.utils.Constants;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;
import net.snowflake.ingest.utils.SnowflakeURL;
import net.snowflake.ingest.utils.Utils;

/** Factory class used to build a {@link SnowflakeStreamingIngestClient} for a specific account */
public class SnowflakeStreamingIngestClientFactory {
  public static Builder builder(String name) {
    return new Builder(name);
  }

  /** Builder class to build a {@link SnowflakeStreamingIngestClient} */
  public static class Builder {
    // Name of the client
    private final String name;

    // Properties that contains info used for Snowflake authentication and authorization
    private Properties prop;

    // Allows client to override some default parameter values
    private Map<String, Object> parameterOverrides;

    private Builder(String name) {
      this.name = name;
    }

    public Builder setProperties(Properties prop) {
      this.prop = prop;
      return this;
    }

    public Builder setParameterOverrides(Map<String, Object> parameterOverrides) {
      this.parameterOverrides = parameterOverrides;
      return this;
    }

    public SnowflakeStreamingIngestClient build() {
      Utils.assertStringNotNullOrEmpty("client name", this.name);
      Utils.assertNotNull("connection properties", this.prop);

      if (!this.prop.containsKey(Constants.ACCOUNT_URL)) {
        if (!this.prop.containsKey(Constants.HOST)) {
          throw new SFException(ErrorCode.MISSING_CONFIG, "host");
        }
        if (!this.prop.containsKey(Constants.SCHEME)) {
          throw new SFException(ErrorCode.MISSING_CONFIG, "scheme");
        }
        if (!this.prop.containsKey(Constants.PORT)) {
          throw new SFException(ErrorCode.MISSING_CONFIG, "port");
        }

        this.prop.put(
            Constants.ACCOUNT_URL,
            Utils.constructAccountUrl(
                this.prop.get(Constants.SCHEME).toString(),
                this.prop.get(Constants.HOST).toString(),
                Integer.parseInt(prop.get(Constants.PORT).toString())));
      }

      if (!this.prop.containsKey(Constants.ROLE)) {
        throw new SFException(ErrorCode.MISSING_CONFIG, "role");
      }

      SnowflakeURL accountURL = new SnowflakeURL(this.prop.getProperty(Constants.ACCOUNT_URL));
      Properties prop = Utils.createProperties(this.prop, accountURL.sslEnabled());
      return new SnowflakeStreamingIngestClientInternal(
          this.name, accountURL, prop, parameterOverrides);
    }
  }
}
