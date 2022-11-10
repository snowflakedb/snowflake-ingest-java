/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import net.snowflake.ingest.streaming.internal.SnowflakeStreamingIngestChannelInternal.ChannelContext;
import net.snowflake.ingest.utils.Utils;

/** Metadata for a chunk that sends to Snowflake as part of the register blob request */
class ChunkMetadata {
  private final String dbName;
  private final String schemaName;
  private final String tableName;
  private Long chunkStartOffset;
  private final Integer chunkLength;
  private final List<ChannelMetadata> channels;
  private final String chunkMD5;
  private final EpInfo epInfo;
  private final Long encryptionKeyId;

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
    private String chunkMD5;
    private EpInfo epInfo;
    private Long encryptionKeyId;

    Builder setOwningTableFromChannelContext(ChannelContext channelContext) {
      this.dbName = channelContext.dbName;
      this.schemaName = channelContext.schemaName;
      this.tableName = channelContext.tableName;
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

    Builder setChunkMD5(String chunkMD5) {
      this.chunkMD5 = chunkMD5;
      return this;
    }

    Builder setEncryptionKeyId(Long encryptionKeyId) {
      this.encryptionKeyId = encryptionKeyId;
      return this;
    }

    ChunkMetadata build() {
      return new ChunkMetadata(this);
    }
  }

  private ChunkMetadata(Builder builder) {
    Utils.assertStringNotNullOrEmpty("chunk database name", builder.dbName);
    Utils.assertStringNotNullOrEmpty("chunk schema name", builder.schemaName);
    Utils.assertStringNotNullOrEmpty("chunk table name", builder.tableName);
    Utils.assertNotNull("chunk start offset", builder.chunkStartOffset);
    Utils.assertNotNull("chunk length", builder.chunkLength);
    Utils.assertNotNull("chunk channels", builder.channels);
    Utils.assertNotNull("chunk MD5", builder.chunkMD5);
    Utils.assertNotNull("chunk ep info", builder.epInfo);
    Utils.assertNotNull("encryption key id", builder.encryptionKeyId);

    this.dbName = builder.dbName;
    this.schemaName = builder.schemaName;
    this.tableName = builder.tableName;
    this.chunkStartOffset = builder.chunkStartOffset;
    this.chunkLength = builder.chunkLength;
    this.channels = builder.channels;
    this.chunkMD5 = builder.chunkMD5;
    this.epInfo = builder.epInfo;
    this.encryptionKeyId = builder.encryptionKeyId;
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

  @JsonProperty("chunk_md5")
  public String getChunkMD5() {
    return this.chunkMD5;
  }

  @JsonProperty("eps")
  EpInfo getEpInfo() {
    return this.epInfo;
  }

  @JsonProperty("encryption_key_id")
  Long getEncryptionKeyId() {
    return this.encryptionKeyId;
  }
}
