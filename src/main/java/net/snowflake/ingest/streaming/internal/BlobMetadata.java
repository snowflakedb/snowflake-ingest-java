/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import com.google.common.annotations.VisibleForTesting;
import java.util.List;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.annotation.JsonIgnore;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.annotation.JsonProperty;
import net.snowflake.ingest.utils.Constants;
import net.snowflake.ingest.utils.ParameterProvider;

/** Metadata for a blob that sends to Snowflake as part of the register blob request */
class BlobMetadata {
  private final String path;
  private final String md5;
  private final Constants.BdecVersion bdecVersion;
  private final List<ChunkMetadata> chunks;
  private final BlobStats blobStats;
  private final boolean spansMixedTables;

  // used for testing only
  @VisibleForTesting
  BlobMetadata(String path, String md5, List<ChunkMetadata> chunks, BlobStats blobStats) {
    this(
        path,
        md5,
        ParameterProvider.BLOB_FORMAT_VERSION_DEFAULT,
        chunks,
        blobStats,
        chunks == null ? false : chunks.size() > 1);
  }

  BlobMetadata(
      String path,
      String md5,
      Constants.BdecVersion bdecVersion,
      List<ChunkMetadata> chunks,
      BlobStats blobStats,
      boolean spansMixedTables) {
    this.path = path;
    this.md5 = md5;
    this.bdecVersion = bdecVersion;
    this.chunks = chunks;
    this.blobStats = blobStats;
    this.spansMixedTables = spansMixedTables;
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

  @JsonProperty("spans_mixed_tables")
  boolean getSpansMixedTables() {
    return this.spansMixedTables;
  }

  /** Create {@link BlobMetadata}. */
  static BlobMetadata createBlobMetadata(
      String path,
      String md5,
      Constants.BdecVersion bdecVersion,
      List<ChunkMetadata> chunks,
      BlobStats blobStats,
      boolean spansMixedTables) {
    return new BlobMetadata(path, md5, bdecVersion, chunks, blobStats, spansMixedTables);
  }
}
