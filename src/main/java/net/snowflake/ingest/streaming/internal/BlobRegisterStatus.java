/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/**
 * Indicates the status of a blob registration, return from server as part of the register blob
 * REST_API
 */
class BlobRegisterStatus {
  private List<ChunkRegisterStatus> chunksStatus;

  @JsonProperty("chunks")
  void setChunksStatus(List<ChunkRegisterStatus> chunksStatus) {
    this.chunksStatus = chunksStatus;
  }

  List<ChunkRegisterStatus> getChunksStatus() {
    return this.chunksStatus;
  }
}
