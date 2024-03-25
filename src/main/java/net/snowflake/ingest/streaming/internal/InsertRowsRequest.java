/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import java.time.ZoneId;
import net.snowflake.ingest.streaming.OffsetTokenVerificationFunction;
import net.snowflake.ingest.utils.Utils;

public class InsertRowsRequest {

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
  private final net.snowflake.ingest.streaming.OpenChannelRequest.OnErrorOption onErrorOption;

  // Default timezone for TIMESTAMP_LTZ and TIMESTAMP_TZ columns
  private final ZoneId defaultTimezone;

  private final String offsetToken;
  private final boolean isOffsetTokenProvided;

  private final OffsetTokenVerificationFunction offsetTokenVerificationFunction;

  private final net.snowflake.ingest.streaming.OpenChannelRequest.ChannelType channelType;

  public static net.snowflake.ingest.streaming.OpenChannelRequest.OpenChannelRequestBuilder builder(
      String channelName) {
    return new net.snowflake.ingest.streaming.OpenChannelRequest.OpenChannelRequestBuilder(
        channelName);
  }

  /** A builder class to build a OpenChannelRequest */
  public static class InsertRowsRequestBuilder {
    private final String channelName;
    private String dbName;
    private String schemaName;
    private String tableName;
    private net.snowflake.ingest.streaming.OpenChannelRequest.OnErrorOption onErrorOption;
    private ZoneId defaultTimezone;
    private String offsetToken;
    private boolean isOffsetTokenProvided = false;
    private OffsetTokenVerificationFunction offsetTokenVerificationFunction;
    private net.snowflake.ingest.streaming.OpenChannelRequest.ChannelType channelType =
        net.snowflake.ingest.streaming.OpenChannelRequest.ChannelType.ROWSET_API;

    public InsertRowsRequestBuilder(String channelName) {
      this.channelName = channelName;
      this.defaultTimezone = DEFAULT_DEFAULT_TIMEZONE;
    }

    public InsertRowsRequestBuilder setDBName(String dbName) {
      this.dbName = dbName;
      return this;
    }

    public InsertRowsRequestBuilder setSchemaName(String schemaName) {
      this.schemaName = schemaName;
      return this;
    }

    public InsertRowsRequestBuilder setTableName(String tableName) {
      this.tableName = tableName;
      return this;
    }

    public InsertRowsRequestBuilder setOnErrorOption(
        net.snowflake.ingest.streaming.OpenChannelRequest.OnErrorOption onErrorOption) {
      this.onErrorOption = onErrorOption;
      return this;
    }

    public InsertRowsRequestBuilder setDefaultTimezone(ZoneId defaultTimezone) {
      this.defaultTimezone = defaultTimezone;
      return this;
    }

    public InsertRowsRequestBuilder setOffsetToken(String offsetToken) {
      this.offsetToken = offsetToken;
      this.isOffsetTokenProvided = true;
      return this;
    }

    public InsertRowsRequestBuilder setOffsetTokenVerificationFunction(
        OffsetTokenVerificationFunction function) {
      this.offsetTokenVerificationFunction = function;
      return this;
    }

    public InsertRowsRequestBuilder setChannelType(
        net.snowflake.ingest.streaming.OpenChannelRequest.ChannelType type) {
      this.channelType = type;
      return this;
    }

    public InsertRowsRequest build() {
      return new InsertRowsRequest(this);
    }
  }

  private InsertRowsRequest(InsertRowsRequestBuilder builder) {
    Utils.assertStringNotNullOrEmpty("channel name", builder.channelName);
    Utils.assertStringNotNullOrEmpty("database name", builder.dbName);
    Utils.assertStringNotNullOrEmpty("schema name", builder.schemaName);
    Utils.assertStringNotNullOrEmpty("table name", builder.tableName);
    Utils.assertNotNull("on_error option", builder.onErrorOption);
    Utils.assertNotNull("default_timezone", builder.defaultTimezone);
    Utils.assertNotNull("channel_type", builder.channelType);

    this.channelName = builder.channelName;
    this.dbName = builder.dbName;
    this.schemaName = builder.schemaName;
    this.tableName = builder.tableName;
    this.onErrorOption = builder.onErrorOption;
    this.defaultTimezone = builder.defaultTimezone;
    this.offsetToken = builder.offsetToken;
    this.isOffsetTokenProvided = builder.isOffsetTokenProvided;
    this.offsetTokenVerificationFunction = builder.offsetTokenVerificationFunction;
    this.channelType = builder.channelType;
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
    return String.format("%s.%s.%s", this.dbName, this.schemaName, this.tableName);
  }

  public net.snowflake.ingest.streaming.OpenChannelRequest.OnErrorOption getOnErrorOption() {
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

  public net.snowflake.ingest.streaming.OpenChannelRequest.ChannelType getChannelType() {
    return this.channelType;
  }
}
