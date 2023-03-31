/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import net.snowflake.ingest.utils.Constants;
import net.snowflake.ingest.utils.ParameterProvider;

/** Metadata for a blob that sends to Snowflake as part of the register blob request */
public class BlobMetadata {
  public static final long DEFAULT_LATENCY_MS = -1;

  private final String path;
  private final String md5;
  private final Constants.BdecVersion bdecVersion;
  private final List<ChunkMetadata> chunks;

  // latency stats, default to -1
  private long buildLatencyMs = DEFAULT_LATENCY_MS;
  private long uploadLatencyMs = DEFAULT_LATENCY_MS;
  private long registerLatencyMs = DEFAULT_LATENCY_MS;
  private long flushLatencyMs = DEFAULT_LATENCY_MS;

  BlobMetadata(String path, String md5, List<ChunkMetadata> chunks) {
    this(path, md5, ParameterProvider.BLOB_FORMAT_VERSION_DEFAULT, chunks);
  }

  BlobMetadata(
      String path, String md5, Constants.BdecVersion bdecVersion, List<ChunkMetadata> chunks) {
    this.path = path;
    this.md5 = md5;
    this.bdecVersion = bdecVersion;
    this.chunks = chunks;
  }

  @JsonIgnore
  Constants.BdecVersion getVersion() {
    return bdecVersion;
  }

  @JsonProperty("path")
  public String getPath() {
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

  // TODO: @rcheng question - do these need @jsonproperties?
  public long getBuildLatencyMs() {
    return this.buildLatencyMs;
  }

  public long getFlushLatencyMs() {
    return this.flushLatencyMs;
  }

  public long getRegisterLatencyMs() {
    return this.registerLatencyMs;
  }

  public long getUploadLatencyMs() {
    return this.uploadLatencyMs;
  }

  void setBuildLatencyMs(long buildLatencyMs) {
    this.buildLatencyMs = buildLatencyMs;
  }

  void setUploadLatencyMs(long uploadLatencyMs) {
    this.uploadLatencyMs = uploadLatencyMs;
  }

  void setRegisterLatencyMs(long registerLatencyMs) {
    this.registerLatencyMs = registerLatencyMs;
  }

  void setFlushLatencyMs(long flushLatencyMs) {
    this.flushLatencyMs = flushLatencyMs;
  }

  /** Create {@link BlobMetadata}. */
  static BlobMetadata createBlobMetadata(
      String path, String md5, Constants.BdecVersion bdecVersion, List<ChunkMetadata> chunks) {
    return new BlobMetadata(path, md5, bdecVersion, chunks);
  }
}
