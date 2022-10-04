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
  private final Constants.BdecVersion bdecVersion;
  private final List<ChunkMetadata> chunks;

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

  /**
   * Create {@link BlobMetadata} in case of {@link Constants.BdecVersion#ONE} and {@link
   * BlobMetadataWithBdecVersion} otherwise to send BDEC version to server side.
   */
  static BlobMetadata createBlobMetadata(
      String path, String md5, Constants.BdecVersion bdecVersion, List<ChunkMetadata> chunks) {
    // TODO SNOW-659721: Unify BlobMetadata with BlobMetadataWithBdecVersion once server side
    // BdecVersion in production
    return bdecVersion == Constants.BdecVersion.ONE
        ? new BlobMetadata(path, md5, bdecVersion, chunks)
        : new BlobMetadataWithBdecVersion(path, md5, bdecVersion, chunks);
  }
}
