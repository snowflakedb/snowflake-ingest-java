/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import net.snowflake.ingest.utils.Constants;
import net.snowflake.ingest.utils.ParameterProvider;
import org.apache.arrow.util.VisibleForTesting;

import java.util.List;

/** Metadata for a blob that sends to Snowflake as part of the register blob request */
class BlobMetadata {
  enum BlobCreationReason {
    NONE("NONE"),
    BUFFER_BYTE_SIZE("max buffer threshold hit"),
    ENCRYPTION_KEY_ID_CHANGE("encryption key id changed"),
    SCHEMA_CHANGE("schema changed"),
    ;

    private final String description;

    BlobCreationReason(final String description) {
      this.description = description;
    }
  }

  // data about the blob
  private final String path;
  private final String md5;
  private final Constants.BdecVersion bdecVersion;
  private final List<ChunkMetadata> chunks;
  private final BlobStats blobStats;
  private BlobCreationReason blobCreationReason;

  // used for testing only
  @VisibleForTesting
  BlobMetadata(String path, String md5, List<ChunkMetadata> chunks, BlobStats blobStats) {
    this(path, md5, ParameterProvider.BLOB_FORMAT_VERSION_DEFAULT, chunks, blobStats);
  }

  BlobMetadata(
      String path,
      String md5,
      Constants.BdecVersion bdecVersion,
      List<ChunkMetadata> chunks,
      BlobStats blobStats) {
    this.path = path;
    this.md5 = md5;
    this.bdecVersion = bdecVersion;
    this.chunks = chunks;
    this.blobStats = blobStats;
    this.blobCreationReason = BlobCreationReason.NONE;
  }

  @JsonIgnore
  Constants.BdecVersion getVersion() {
    return bdecVersion;
  }

  @JsonProperty("path")
  String getPath() {
    return this.path;
  }

  @JsonProperty("md5")
  String getMD5() {
    return this.md5;
  }

  @JsonProperty("chunks")
  List<ChunkMetadata> getChunks() {
    return this.chunks;
  }

  @JsonProperty("bdec_version")
  byte getVersionByte() {
    return bdecVersion.toByte();
  }

  @JsonProperty("blob_stats")
  BlobStats getBlobStats() {
    return this.blobStats;
  }

  void setBlobCreationReason(BlobCreationReason blobCreationReason) {
    this.blobCreationReason = blobCreationReason;
  }

  /** Create {@link BlobMetadata}. */
  static BlobMetadata createBlobMetadata(
      String path,
      String md5,
      Constants.BdecVersion bdecVersion,
      List<ChunkMetadata> chunks,
      BlobStats blobStats) {
    return new BlobMetadata(path, md5, bdecVersion, chunks, blobStats);
  }
}
