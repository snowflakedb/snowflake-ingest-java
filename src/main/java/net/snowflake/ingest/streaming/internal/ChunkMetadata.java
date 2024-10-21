/*
 * Copyright (c) 2021-2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import net.snowflake.ingest.utils.Utils;

/** Metadata for a chunk that sends to Snowflake as part of the register blob request */
class ChunkMetadata {
  private final String dbName;
  private final String schemaName;
  private final String tableName;
  private Long chunkStartOffset;
  private final Integer chunkLength;
  private final Integer uncompressedChunkLength;
  private final List<ChannelMetadata> channels;
  private final String chunkMD5;
  private final EpInfo epInfo;
  private final Long encryptionKeyId;
  private final Long firstInsertTimeInMs;
  private final Long lastInsertTimeInMs;
  private Integer majorVersion;
  private Integer minorVersion;
  private Long createdOn;
  private Long metadataSize;
  private Long extendedMetadataSize;

  static Builder builder() {
    return new Builder();
  }

  /** Builder class to build a ChunkMetadata */
  static class Builder {
    private String dbName;
    private String schemaName;
    private String tableName;
    private Long chunkStartOffset;
    private Integer chunkLength; // compressedChunkLength

    private Integer uncompressedChunkLength;

    private List<ChannelMetadata> channels;
    private String chunkMD5;
    private EpInfo epInfo;
    private Long encryptionKeyId;
    private Long firstInsertTimeInMs;
    private Long lastInsertTimeInMs;
    private Integer majorVersion;
    private Integer minorVersion;
    private Long createdOn;
    private Long metadataSize;
    private Long extendedMetadataSize;

    Builder setOwningTableFromChannelContext(ChannelFlushContext channelFlushContext) {
      this.dbName = channelFlushContext.getDbName();
      this.schemaName = channelFlushContext.getSchemaName();
      this.tableName = channelFlushContext.getTableName();
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

    /**
     * Currently we send estimated uncompressed size that is close to the actual parquet data size
     * and mostly about user data but parquet encoding overhead may be slightly different.
     */
    public Builder setUncompressedChunkLength(Integer uncompressedChunkLength) {
      this.uncompressedChunkLength = uncompressedChunkLength;
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

    Builder setFirstInsertTimeInMs(Long firstInsertTimeInMs) {
      this.firstInsertTimeInMs = firstInsertTimeInMs;
      return this;
    }

    Builder setLastInsertTimeInMs(Long lastInsertTimeInMs) {
      this.lastInsertTimeInMs = lastInsertTimeInMs;
      return this;
    }

    Builder setMajorVersion(Integer majorVersion) {
      this.majorVersion = majorVersion;
      return this;
    }

    Builder setMinorVersion(Integer minorVersion) {
      this.minorVersion = minorVersion;
      return this;
    }

    Builder setCreatedOn(Long createdOn) {
      this.createdOn = createdOn;
      return this;
    }

    Builder setMetadataSize(Long metadataSize) {
      this.metadataSize = metadataSize;
      return this;
    }

    Builder setExtendedMetadataSize(Long extendedMetadataSize) {
      this.extendedMetadataSize = extendedMetadataSize;
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
    Utils.assertNotNull("chunk first insert time in ms", builder.firstInsertTimeInMs);
    Utils.assertNotNull("chunk last insert time in ms", builder.lastInsertTimeInMs);

    this.dbName = builder.dbName;
    this.schemaName = builder.schemaName;
    this.tableName = builder.tableName;
    this.chunkStartOffset = builder.chunkStartOffset;
    this.chunkLength = builder.chunkLength;
    this.uncompressedChunkLength = builder.uncompressedChunkLength;
    this.channels = builder.channels;
    this.chunkMD5 = builder.chunkMD5;
    this.epInfo = builder.epInfo;
    this.encryptionKeyId = builder.encryptionKeyId;
    this.firstInsertTimeInMs = builder.firstInsertTimeInMs;
    this.lastInsertTimeInMs = builder.lastInsertTimeInMs;

    // iceberg-specific fields, no need for conditional since both sides are nullable and the
    // caller of ChunkMetadata.Builder only sets these fields when we're in iceberg mode
    this.majorVersion = builder.majorVersion;
    this.minorVersion = builder.minorVersion;
    this.createdOn = builder.createdOn;
    this.metadataSize = builder.metadataSize;
    this.extendedMetadataSize = builder.extendedMetadataSize;
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

  @JsonProperty("chunk_length_uncompressed")
  public Integer getUncompressedChunkLength() {
    return uncompressedChunkLength;
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

  @JsonProperty("first_insert_time_in_ms")
  Long getFirstInsertTimeInMs() {
    return this.firstInsertTimeInMs;
  }

  @JsonProperty("last_insert_time_in_ms")
  Long getLastInsertTimeInMs() {
    return this.lastInsertTimeInMs;
  }

  // Snowflake service had a bug that did not allow the client to add new json fields in some
  // contracts; thus these new fields have a NON_NULL attribute. NON_DEFAULT will ignore an explicit
  // zero value, thus NON_NULL is a better fit.
  @JsonProperty("major_vers")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  Integer getMajorVersion() {
    return this.majorVersion;
  }

  @JsonProperty("minor_vers")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  Integer getMinorVersion() {
    return this.minorVersion;
  }

  @JsonProperty("created")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  Long getCreatedOn() {
    return this.createdOn;
  }

  @JsonProperty("metadata_size")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  Long getMetadataSize() {
    return this.metadataSize;
  }

  @JsonProperty("ext_metadata_size")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  Long getExtendedMetadataSize() {
    return this.extendedMetadataSize;
  }
}
