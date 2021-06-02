/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import net.snowflake.ingest.utils.StreamingUtils;

/** Metadata for a chunk that sends to Snowflake as part of the register blob request */
class ChunkMetadata {
  private final String dbName;
  private final String schemaName;
  private final String tableName;
  private Long chunkStartOffset;
  private final Integer chunkLength;
  private final List<ChannelMetadata> channels;
  private final EpInfo epInfo;

  static Builder builder() {
    return new Builder();
  }

  /** Builder class to build a ChunkMetadata */
  static class Builder {
    private String dbName;
    private String schemaName;
    private String tableName;
    private Long chunkStartOffset;
    private Integer chunkLength;
    private List<ChannelMetadata> channels;
    private EpInfo epInfo;

    Builder setOwningTable(SnowflakeStreamingIngestChannelInternal channel) {
      this.dbName = channel.getDBName();
      this.schemaName = channel.getSchemaName();
      this.tableName = channel.getTableName();
      return this;
    }

    Builder setDBName(String dbName) {
      this.dbName = dbName;
      return this;
    }

    Builder setSchemaName(String schemaName) {
      this.schemaName = schemaName;
      return this;
    }

    Builder setTableName(String tableName) {
      this.tableName = tableName;
      return this;
    }

    Builder setEpInfo(EpInfo epInfo) {
      this.epInfo = epInfo;
      return this;
    }

    Builder setChunkStartOffset(Long startOffset) {
      this.chunkStartOffset = startOffset;
      return this;
    }

    Builder setChunkLength(Integer chunkLength) {
      this.chunkLength = chunkLength;
      return this;
    }

    Builder setChannelList(List<ChannelMetadata> channels) {
      this.channels = channels;
      return this;
    }

    ChunkMetadata build() {
      return new ChunkMetadata(this);
    }
  }

  private ChunkMetadata(Builder builder) {
    StreamingUtils.assertStringNotNullOrEmpty("chunk database name", builder.dbName);
    StreamingUtils.assertStringNotNullOrEmpty("chunk schema name", builder.schemaName);
    StreamingUtils.assertStringNotNullOrEmpty("chunk table name", builder.tableName);
    StreamingUtils.assertNotNull("chunk start offset", builder.chunkStartOffset);
    StreamingUtils.assertNotNull("chunk length", builder.chunkLength);
    StreamingUtils.assertNotNull("chunk channels", builder.channels);
    StreamingUtils.assertNotNull("chunk ep info", builder.epInfo);

    this.dbName = builder.dbName;
    this.schemaName = builder.schemaName;
    this.tableName = builder.tableName;
    this.chunkStartOffset = builder.chunkStartOffset;
    this.chunkLength = builder.chunkLength;
    this.channels = builder.channels;
    this.epInfo = builder.epInfo;
  }

  /**
   * Advance the start offset of a chunk
   *
   * @param offset
   */
  void advanceStartOffset(int offset) {
    this.chunkStartOffset += offset;
  }

  @JsonProperty("database")
  String getDBName() {
    return this.dbName;
  }

  @JsonProperty("schema")
  String getSchemaName() {
    return this.schemaName;
  }

  @JsonProperty("table")
  String getTableName() {
    return this.tableName;
  }

  @JsonProperty("chunk_start_offset")
  Long getChunkStartOffset() {
    return this.chunkStartOffset;
  }

  @JsonProperty("chunk_length")
  Integer getChunkLength() {
    return chunkLength;
  }

  @JsonProperty("channels")
  List<ChannelMetadata> getChannels() {
    return this.channels;
  }

  @JsonProperty("eps")
  EpInfo getEpInfo() {
    return this.epInfo;
  }
}
