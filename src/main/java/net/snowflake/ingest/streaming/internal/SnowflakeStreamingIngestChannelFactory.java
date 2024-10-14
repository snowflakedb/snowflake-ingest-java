/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import java.time.ZoneId;
import net.snowflake.ingest.streaming.OffsetTokenVerificationFunction;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.utils.Utils;
import org.apache.parquet.column.ParquetProperties;

/** Builds a Streaming Ingest channel for a specific Streaming Ingest client */
class SnowflakeStreamingIngestChannelFactory {
  static <T> SnowflakeStreamingIngestChannelBuilder<T> builder(String name) {
    return new SnowflakeStreamingIngestChannelBuilder<>(name);
  }

  // Builder class to build a SnowflakeStreamingIngestChannel
  static class SnowflakeStreamingIngestChannelBuilder<T> {
    private String name;
    private String dbName;
    private String schemaName;
    private String tableName;
    private String offsetToken;
    private Long channelSequencer;
    private Long rowSequencer;
    private SnowflakeStreamingIngestClientInternal<T> owningClient;
    private String encryptionKey;
    private Long encryptionKeyId;
    private OpenChannelRequest.OnErrorOption onErrorOption;
    private ZoneId defaultTimezone;
    private OffsetTokenVerificationFunction offsetTokenVerificationFunction;
    private ParquetProperties.WriterVersion parquetWriterVersion;

    private SnowflakeStreamingIngestChannelBuilder(String name) {
      this.name = name;
    }

    SnowflakeStreamingIngestChannelBuilder<T> setDBName(String dbName) {
      this.dbName = dbName;
      return this;
    }

    SnowflakeStreamingIngestChannelBuilder<T> setSchemaName(String schemaName) {
      this.schemaName = schemaName;
      return this;
    }

    SnowflakeStreamingIngestChannelBuilder<T> setTableName(String tableName) {
      this.tableName = tableName;
      return this;
    }

    SnowflakeStreamingIngestChannelBuilder<T> setOffsetToken(String offsetToken) {
      this.offsetToken = offsetToken;
      return this;
    }

    SnowflakeStreamingIngestChannelBuilder<T> setChannelSequencer(Long sequencer) {
      this.channelSequencer = sequencer;
      return this;
    }

    SnowflakeStreamingIngestChannelBuilder<T> setRowSequencer(Long sequencer) {
      this.rowSequencer = sequencer;
      return this;
    }

    SnowflakeStreamingIngestChannelBuilder<T> setEncryptionKey(String encryptionKey) {
      this.encryptionKey = encryptionKey;
      return this;
    }

    SnowflakeStreamingIngestChannelBuilder<T> setEncryptionKeyId(Long encryptionKeyId) {
      this.encryptionKeyId = encryptionKeyId;
      return this;
    }

    SnowflakeStreamingIngestChannelBuilder<T> setOnErrorOption(
        OpenChannelRequest.OnErrorOption onErrorOption) {
      this.onErrorOption = onErrorOption;
      return this;
    }

    SnowflakeStreamingIngestChannelBuilder<T> setDefaultTimezone(ZoneId zoneId) {
      this.defaultTimezone = zoneId;
      return this;
    }

    SnowflakeStreamingIngestChannelBuilder<T> setOwningClient(
        SnowflakeStreamingIngestClientInternal<T> client) {
      this.owningClient = client;
      return this;
    }

    SnowflakeStreamingIngestChannelBuilder<T> setOffsetTokenVerificationFunction(
        OffsetTokenVerificationFunction function) {
      this.offsetTokenVerificationFunction = function;
      return this;
    }

    SnowflakeStreamingIngestChannelBuilder<T> setParquetWriterVersion(
        ParquetProperties.WriterVersion parquetWriterVersion) {
      this.parquetWriterVersion = parquetWriterVersion;
      return this;
    }

    SnowflakeStreamingIngestChannelInternal<T> build() {
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
      Utils.assertNotNull("default timezone", this.defaultTimezone);
      return new SnowflakeStreamingIngestChannelInternal<>(
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
          this.onErrorOption,
          this.defaultTimezone,
          this.offsetTokenVerificationFunction,
          this.parquetWriterVersion);
    }
  }
}
