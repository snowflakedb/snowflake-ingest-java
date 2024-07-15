/*
 * Copyright (c) 2021-2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming;

import java.time.ZoneId;
import net.snowflake.ingest.utils.Utils;

/** A class that is used to open/create a {@link SnowflakeStreamingIngestChannel} */
public class OpenChannelRequest {
  public enum OnErrorOption {
    CONTINUE, // CONTINUE loading the rows, and return all the errors in the response
    ABORT, // ABORT the entire batch, and throw an exception when we hit the first error
    SKIP_BATCH, // If an error in the batch is detected return a response containing all error row
    // indexes. No data is ingested
  }

  /**
   * Default value of the timezone, which will be used for TIMESTAMP_LTZ and TIMESTAMP_TZ column
   * types when the user input does not have any timezone information.
   */
  private static final ZoneId DEFAULT_DEFAULT_TIMEZONE = ZoneId.of("America/Los_Angeles");

  // Name of the channel
  private final String channelName;

  // Name of the database that the channel belongs to
  private final String dbName;

  // Name of the schema that the channel belongs to
  private final String schemaName;

  // Name of the table that the channel belongs to
  private final String tableName;

  // On_error option on this channel
  private final OnErrorOption onErrorOption;

  // Default timezone for TIMESTAMP_LTZ and TIMESTAMP_TZ columns
  private final ZoneId defaultTimezone;

  private final String offsetToken;
  private final boolean isOffsetTokenProvided;

  private final OffsetTokenVerificationFunction offsetTokenVerificationFunction;

  public static OpenChannelRequestBuilder builder(String channelName) {
    return new OpenChannelRequestBuilder(channelName);
  }

  /** A builder class to build a OpenChannelRequest */
  public static class OpenChannelRequestBuilder {
    private final String channelName;
    private String dbName;
    private String schemaName;
    private String tableName;
    private OnErrorOption onErrorOption;
    private ZoneId defaultTimezone;

    private String offsetToken;
    private boolean isOffsetTokenProvided = false;

    private OffsetTokenVerificationFunction offsetTokenVerificationFunction;

    public OpenChannelRequestBuilder(String channelName) {
      this.channelName = channelName;
      this.defaultTimezone = DEFAULT_DEFAULT_TIMEZONE;
    }

    public OpenChannelRequestBuilder setDBName(String dbName) {
      this.dbName = dbName;
      return this;
    }

    public OpenChannelRequestBuilder setSchemaName(String schemaName) {
      this.schemaName = schemaName;
      return this;
    }

    public OpenChannelRequestBuilder setTableName(String tableName) {
      this.tableName = tableName;
      return this;
    }

    public OpenChannelRequestBuilder setOnErrorOption(OnErrorOption onErrorOption) {
      this.onErrorOption = onErrorOption;
      return this;
    }

    public OpenChannelRequestBuilder setDefaultTimezone(ZoneId defaultTimezone) {
      this.defaultTimezone = defaultTimezone;
      return this;
    }

    public OpenChannelRequestBuilder setOffsetToken(String offsetToken) {
      this.offsetToken = offsetToken;
      this.isOffsetTokenProvided = true;
      return this;
    }

    public OpenChannelRequestBuilder setOffsetTokenVerificationFunction(
        OffsetTokenVerificationFunction function) {
      this.offsetTokenVerificationFunction = function;
      return this;
    }

    public OpenChannelRequest build() {
      return new OpenChannelRequest(this);
    }
  }

  private OpenChannelRequest(OpenChannelRequestBuilder builder) {
    Utils.assertStringNotNullOrEmpty("channel name", builder.channelName);
    Utils.assertStringNotNullOrEmpty("database name", builder.dbName);
    Utils.assertStringNotNullOrEmpty("schema name", builder.schemaName);
    Utils.assertStringNotNullOrEmpty("table name", builder.tableName);
    Utils.assertNotNull("on_error option", builder.onErrorOption);
    Utils.assertNotNull("default_timezone", builder.defaultTimezone);

    this.channelName = builder.channelName;
    this.dbName = builder.dbName;
    this.schemaName = builder.schemaName;
    this.tableName = builder.tableName;
    this.onErrorOption = builder.onErrorOption;
    this.defaultTimezone = builder.defaultTimezone;
    this.offsetToken = builder.offsetToken;
    this.isOffsetTokenProvided = builder.isOffsetTokenProvided;
    this.offsetTokenVerificationFunction = builder.offsetTokenVerificationFunction;
  }

  public String getDBName() {
    return this.dbName;
  }

  public String getSchemaName() {
    return this.schemaName;
  }

  public String getTableName() {
    return this.tableName;
  }

  public String getChannelName() {
    return this.channelName;
  }

  public ZoneId getDefaultTimezone() {
    return this.defaultTimezone;
  }

  public String getFullyQualifiedTableName() {
    return Utils.getFullyQualifiedTableName(this.dbName, this.schemaName, this.tableName);
  }

  public OnErrorOption getOnErrorOption() {
    return this.onErrorOption;
  }

  public String getOffsetToken() {
    return this.offsetToken;
  }

  public boolean isOffsetTokenProvided() {
    return this.isOffsetTokenProvided;
  }

  public OffsetTokenVerificationFunction getOffsetTokenVerificationFunction() {
    return this.offsetTokenVerificationFunction;
  }
}
