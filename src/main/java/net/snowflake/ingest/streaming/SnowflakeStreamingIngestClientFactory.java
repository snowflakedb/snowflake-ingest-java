/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming;

import java.util.Map;
import java.util.Properties;

import jdk.internal.joptsimple.internal.Strings;
import net.snowflake.ingest.streaming.internal.SnowflakeStreamingIngestClientInternal;
import net.snowflake.ingest.utils.Constants;
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

    // preset SnowflakeURL instance
    private SnowflakeURL snowflakeURL;

    // flag to specify if we need to add account name in the request header
    private boolean addAccountNameInRequest;

    private Builder(String name) {
      this.name = name;
    }

    public Builder setProperties(Properties prop) {
      this.prop = prop;
      return this;
    }

    public Builder setSnowflakeURL(SnowflakeURL snowflakeURL) {
      this.snowflakeURL = snowflakeURL;
      return this;
    }

    public Builder setAddAccountNameInRequest(boolean addAccountNameInRequest) {
      this.addAccountNameInRequest = addAccountNameInRequest;
      return this;
    }

    public Builder setParameterOverrides(Map<String, Object> parameterOverrides) {
      this.parameterOverrides = parameterOverrides;
      return this;
    }

    public SnowflakeStreamingIngestClient build() {
      Utils.assertStringNotNullOrEmpty("client name", this.name);
      Utils.assertNotNull("connection properties", this.prop);

      Properties prop = Utils.createProperties(this.prop);
      SnowflakeURL accountURL = this.snowflakeURL;
      if (accountURL == null) {
        accountURL = new SnowflakeURL(prop.getProperty(Constants.ACCOUNT_URL));
      }

      if (addAccountNameInRequest) {
        return new SnowflakeStreamingIngestClientInternal<>(
          this.name, accountURL, prop, this.parameterOverrides, addAccountNameInRequest);
      }
      return new SnowflakeStreamingIngestClientInternal<>(
        this.name, accountURL, prop, this.parameterOverrides);
    }
  }
}
