/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import net.snowflake.ingest.utils.StreamingUtils;

/** Builds a Streaming Ingest channel for a specific Streaming Ingest client */
public class SnowflakeStreamingIngestChannelFactory {
  public static SnowflakeStreamingIngestChannelBuilder builder(String name) {
    return new SnowflakeStreamingIngestChannelBuilder(name);
  }

  // Builder class to build a SnowflakeStreamingIngestChannel
  public static class SnowflakeStreamingIngestChannelBuilder {
    private String name;
    private String dbName;
    private String schemaName;
    private String tableName;
    private String offsetToken;
    private Long channelSequencer;
    private Long rowSequencer;
    private SnowflakeStreamingIngestClientInternal owningClient;

    private SnowflakeStreamingIngestChannelBuilder(String name) {
      this.name = name;
    }

    public SnowflakeStreamingIngestChannelBuilder setDBName(String dbName) {
      this.dbName = dbName;
      return this;
    }

    public SnowflakeStreamingIngestChannelBuilder setSchemaName(String schemaName) {
      this.schemaName = schemaName;
      return this;
    }

    public SnowflakeStreamingIngestChannelBuilder setTableName(String tableName) {
      this.tableName = tableName;
      return this;
    }

    public SnowflakeStreamingIngestChannelBuilder setOffsetToken(String offsetToken) {
      this.offsetToken = offsetToken;
      return this;
    }

    public SnowflakeStreamingIngestChannelBuilder setChannelSequencer(Long sequencer) {
      this.channelSequencer = sequencer;
      return this;
    }

    public SnowflakeStreamingIngestChannelBuilder setRowSequencer(Long sequencer) {
      this.rowSequencer = sequencer;
      return this;
    }

    public SnowflakeStreamingIngestChannelBuilder setOwningClient(
        SnowflakeStreamingIngestClientInternal client) {
      this.owningClient = client;
      return this;
    }

    public SnowflakeStreamingIngestChannelInternal build() {
      StreamingUtils.assertStringNotNullOrEmpty("channel name", this.name);
      StreamingUtils.assertStringNotNullOrEmpty("table name", this.tableName);
      StreamingUtils.assertStringNotNullOrEmpty("schema name", this.schemaName);
      StreamingUtils.assertStringNotNullOrEmpty("database name", this.dbName);
      StreamingUtils.assertNotNull("channel sequencer", this.channelSequencer);
      StreamingUtils.assertNotNull("row sequencer", this.rowSequencer);
      StreamingUtils.assertNotNull("channel owning client", this.owningClient);
      return new SnowflakeStreamingIngestChannelInternal(
          this.name,
          this.dbName,
          this.schemaName,
          this.tableName,
          this.offsetToken,
          this.channelSequencer,
          this.rowSequencer,
          this.owningClient);
    }
  }
}
