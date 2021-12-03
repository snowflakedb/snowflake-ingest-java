/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.utils.Utils;

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
    private String encryptionKey;
    private Long encryptionKeyId;
    private OpenChannelRequest.OnErrorOption onErrorOption;

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

    SnowflakeStreamingIngestChannelBuilder setEncryptionKey(String encryptionKey) {
      this.encryptionKey = encryptionKey;
      return this;
    }

    SnowflakeStreamingIngestChannelBuilder setEncryptionKeyId(Long encryptionKeyId) {
      this.encryptionKeyId = encryptionKeyId;
      return this;
    }

    SnowflakeStreamingIngestChannelBuilder setOnErrorOption(
        OpenChannelRequest.OnErrorOption onErrorOption) {
      this.onErrorOption = onErrorOption;
      return this;
    }

    SnowflakeStreamingIngestChannelBuilder setOwningClient(
        SnowflakeStreamingIngestClientInternal client) {
      this.owningClient = client;
      return this;
    }

    SnowflakeStreamingIngestChannelInternal build() {
      Utils.assertStringNotNullOrEmpty("channel name", this.name);
      Utils.assertStringNotNullOrEmpty("table name", this.tableName);
      Utils.assertStringNotNullOrEmpty("schema name", this.schemaName);
      Utils.assertStringNotNullOrEmpty("database name", this.dbName);
      Utils.assertNotNull("channel sequencer", this.channelSequencer);
      Utils.assertNotNull("row sequencer", this.rowSequencer);
      Utils.assertNotNull("channel owning client", this.owningClient);
      Utils.assertStringNotNullOrEmpty("encryption key", this.encryptionKey);
      Utils.assertNotNull("encryption key_id", this.encryptionKeyId);
      Utils.assertNotNull("on_error option", this.onErrorOption);
      return new SnowflakeStreamingIngestChannelInternal(
          this.name,
          this.dbName,
          this.schemaName,
          this.tableName,
          this.offsetToken,
          this.channelSequencer,
          this.rowSequencer,
          this.owningClient,
          this.encryptionKey,
          this.encryptionKeyId,
          this.onErrorOption);
    }
  }
}
