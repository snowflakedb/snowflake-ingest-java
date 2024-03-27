/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import java.time.ZoneId;
import java.util.Arrays;
import net.snowflake.ingest.streaming.OffsetTokenVerificationFunction;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.utils.Utils;

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
    private String pipeName;
    private String offsetToken;
    private Long channelSequencer;
    private Long rowSequencer;
    private String continuationToken;
    private SnowflakeStreamingIngestClientInternal<T> owningClient;
    private String encryptionKey;
    private Long encryptionKeyId;
    private OpenChannelRequest.OnErrorOption onErrorOption;
    private ZoneId defaultTimezone;
    private OffsetTokenVerificationFunction offsetTokenVerificationFunction;
    private OpenChannelRequest.ChannelType type;

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

    SnowflakeStreamingIngestChannelBuilder<T> setPipeName(String pipeName) {
      this.pipeName = pipeName;
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

    SnowflakeStreamingIngestChannelBuilder<T> setContinuationToken(String token) {
      this.continuationToken = token;
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

    SnowflakeStreamingIngestChannelBuilder<T> setChannelType(OpenChannelRequest.ChannelType type) {
      this.type = type;
      return this;
    }

    SnowflakeStreamingIngestChannel build() {
      Utils.assertStringNotNullOrEmpty("channel name", this.name);
      Utils.assertStringNotNullOrEmpty("schema name", this.schemaName);
      Utils.assertStringNotNullOrEmpty("database name", this.dbName);
      Utils.assertNotNull("channel owning client", this.owningClient);
      Utils.assertNotNull("channel type", this.type);
      Utils.assertStringsNotNullOrEmpty(
          "table or pipe name", Arrays.asList(this.tableName, this.pipeName));
      if (this.type == OpenChannelRequest.ChannelType.CLOUD_STORAGE) {
        Utils.assertNotNull("channel sequencer", this.channelSequencer);
        Utils.assertNotNull("row sequencer", this.rowSequencer);
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
            this.owningClient.getParameterProvider().getBlobFormatVersion(),
            this.offsetTokenVerificationFunction);
      } else {
        Utils.assertNotNull("continuation token", this.continuationToken);
        return new SnowflakeStreamingIngestChannelRowset(
            this.name,
            this.dbName,
            this.schemaName,
            this.pipeName,
            this.tableName,
            this.offsetToken,
            this.continuationToken);
      }
    }
  }
}
