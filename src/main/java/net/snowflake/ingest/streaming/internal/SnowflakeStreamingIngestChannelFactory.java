/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import net.snowflake.ingest.utils.StreamingUtils;

/** Builds a Streaming Ingest channel for a specific Streaming Ingest client */
class SnowflakeStreamingIngestChannelFactory {
  static SnowflakeStreamingIngestChannelBuilder builder(String name) {
    return new SnowflakeStreamingIngestChannelBuilder(name);
  }

  // Builder class to build a SnowflakeStreamingIngestChannel
  static class SnowflakeStreamingIngestChannelBuilder {
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

    SnowflakeStreamingIngestChannelBuilder setDBName(String dbName) {
      this.dbName = dbName;
      return this;
    }

    SnowflakeStreamingIngestChannelBuilder setSchemaName(String schemaName) {
      this.schemaName = schemaName;
      return this;
    }

    SnowflakeStreamingIngestChannelBuilder setTableName(String tableName) {
      this.tableName = tableName;
      return this;
    }

    SnowflakeStreamingIngestChannelBuilder setOffsetToken(String offsetToken) {
      this.offsetToken = offsetToken;
      return this;
    }

    SnowflakeStreamingIngestChannelBuilder setChannelSequencer(Long sequencer) {
      this.channelSequencer = sequencer;
      return this;
    }

    SnowflakeStreamingIngestChannelBuilder setRowSequencer(Long sequencer) {
      this.rowSequencer = sequencer;
      return this;
    }

    SnowflakeStreamingIngestChannelBuilder setOwningClient(
        SnowflakeStreamingIngestClientInternal client) {
      this.owningClient = client;
      return this;
    }

    SnowflakeStreamingIngestChannelInternal build() {
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
