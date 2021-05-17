/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/** Metadata for a blob that sends to Snowflake as part of the register blob request */
public class BlobMetadata {
  private final String path;
  private final List<ChunkMetadata> chunks;

  public BlobMetadata(String path, List<ChunkMetadata> chunks) {
    this.path = path;
    this.chunks = chunks;
  }

  @JsonProperty("path")
  public String getPath() {
    return this.path;
  }

  @JsonProperty("chunks")
  public List<ChunkMetadata> getChunks() {
    return this.chunks;
  }
}
