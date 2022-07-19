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
class BlobMetadata {
  private final String path;
  private final String md5;
  private final Constants.BdecVerion bdecVersion;
  private final List<ChunkMetadata> chunks;

  BlobMetadata(String path, String md5, List<ChunkMetadata> chunks) {
    this(path, md5, ParameterProvider.BLOB_FORMAT_VERSION_DEFAULT, chunks);
  }

  BlobMetadata(
      String path, String md5, Constants.BdecVerion bdecVersion, List<ChunkMetadata> chunks) {
    this.path = path;
    this.md5 = md5;
    this.bdecVersion = bdecVersion;
    this.chunks = chunks;
  }

  @JsonIgnore
  Constants.BdecVerion getVersion() {
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
}
