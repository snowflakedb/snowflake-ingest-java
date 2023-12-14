/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming;

import net.snowflake.ingest.utils.Utils;

/** A class that is used to drop a {@link SnowflakeStreamingIngestChannel} */
public class DropChannelRequest {
  // Name of the channel
  private final String channelName;

  // Name of the database that the channel belongs to
  private final String dbName;

  // Name of the schema that the channel belongs to
  private final String schemaName;

  // Name of the table that the channel belongs to
  private final String tableName;

  // Optional client sequencer to verify when dropping the channel.
  private final Long clientSequencer;

  public static DropChannelRequestBuilder builder(String channelName) {
    return new DropChannelRequestBuilder(channelName);
  }

  /** A builder class to build a DropChannelRequest */
  public static class DropChannelRequestBuilder {
    private final String channelName;
    private String dbName;
    private String schemaName;
    private String tableName;

    private Long clientSequencer = null;

    public DropChannelRequestBuilder(String channelName) {
      this.channelName = channelName;
    }

    public DropChannelRequestBuilder setDBName(String dbName) {
      this.dbName = dbName;
      return this;
    }

    public DropChannelRequestBuilder setSchemaName(String schemaName) {
      this.schemaName = schemaName;
      return this;
    }

    public DropChannelRequestBuilder setTableName(String tableName) {
      this.tableName = tableName;
      return this;
    }

    public DropChannelRequestBuilder setClientSequencer(Long clientSequencer) {
      this.clientSequencer = clientSequencer;
      return this;
    }

    public DropChannelRequest build() {
      return new DropChannelRequest(this);
    }
  }

  private DropChannelRequest(DropChannelRequestBuilder builder) {
    Utils.assertStringNotNullOrEmpty("channel name", builder.channelName);
    Utils.assertStringNotNullOrEmpty("database name", builder.dbName);
    Utils.assertStringNotNullOrEmpty("schema name", builder.schemaName);
    Utils.assertStringNotNullOrEmpty("table name", builder.tableName);

    this.channelName = builder.channelName;
    this.dbName = builder.dbName;
    this.schemaName = builder.schemaName;
    this.tableName = builder.tableName;
    this.clientSequencer = builder.clientSequencer;
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

  public String getFullyQualifiedTableName() {
    return String.format("%s.%s.%s", this.dbName, this.schemaName, this.tableName);
  }

  public Long getClientSequencer() {
    return this.clientSequencer;
  }
}
