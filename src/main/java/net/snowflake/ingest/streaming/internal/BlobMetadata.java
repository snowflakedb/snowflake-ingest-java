/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/** Metadata for a blob that sends to Snowflake as part of the register blob request */
class BlobMetadata {
  private final String name;
  private final String path;
  private final List<ChunkMetadata> chunks;

  BlobMetadata(String name, String path, List<ChunkMetadata> chunks) {
    this.name = name;
    this.path = path;
    this.chunks = chunks;
  }

  @JsonProperty("path")
  String getPath() {
    return this.path;
  }

  @JsonProperty("chunks")
  List<ChunkMetadata> getChunks() {
    return this.chunks;
  }

  @JsonIgnore
  String getName() {
    return this.name;
  }
}
