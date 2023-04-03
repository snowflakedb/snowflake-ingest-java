/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import net.snowflake.ingest.utils.Constants;
import net.snowflake.ingest.utils.ParameterProvider;

import java.util.List;

/** Metadata for a blob that sends to Snowflake as part of the register blob request */
class BlobMetadata {
  public static final long DEFAULT_BLOB_LATENCY = -1;

  private final String path;
  private final String md5;
  private final Constants.BdecVersion bdecVersion;
  private final List<ChunkMetadata> chunks;

  // latencies
  private final long buildLatencyMs;
  private final long uploadLatencyMs;

  // used for testing only
  BlobMetadata(String path, String md5, List<ChunkMetadata> chunks) {
    this(
        path,
        md5,
        ParameterProvider.BLOB_FORMAT_VERSION_DEFAULT,
        chunks,
        DEFAULT_BLOB_LATENCY,
        DEFAULT_BLOB_LATENCY);
  }

  BlobMetadata(
      String path,
      String md5,
      Constants.BdecVersion bdecVersion,
      List<ChunkMetadata> chunks,
      long buildLatencyMs,
      long uploadLatencyMs) {
    this.path = path;
    this.md5 = md5;
    this.bdecVersion = bdecVersion;
    this.chunks = chunks;

    this.buildLatencyMs = buildLatencyMs;
    this.uploadLatencyMs = uploadLatencyMs;
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

  @JsonProperty("build_latency_ms")
  long getBuildLatencyMs() {
    return this.buildLatencyMs;
  }

  @JsonProperty("upload_latency_ms")
  long getUploadLatencyMs() {
    return this.uploadLatencyMs;
  }

  /** Create {@link BlobMetadata}. */
  static BlobMetadata createBlobMetadata(
      String path,
      String md5,
      Constants.BdecVersion bdecVersion,
      List<ChunkMetadata> chunks,
      long buildLatencyMs,
      long uploadLatencyMs) {
    return new BlobMetadata(path, md5, bdecVersion, chunks, buildLatencyMs, uploadLatencyMs);
  }
}
