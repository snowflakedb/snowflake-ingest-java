/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.stream.Collectors;

/** Class used to serialize the blob register request */
class RegisterBlobRequest implements IStreamingIngestRequest {
  @JsonProperty("request_id")
  private String requestId;

  @JsonProperty("role")
  private String role;

  @JsonProperty("blobs")
  private List<BlobMetadata> blobs;

  RegisterBlobRequest(String requestId, String role, List<BlobMetadata> blobs) {
    this.requestId = requestId;
    this.role = role;
    this.blobs = blobs;
  }

  String getRequestId() {
    return requestId;
  }

  String getRole() {
    return role;
  }

  List<BlobMetadata> getBlobs() {
    return blobs;
  }

  @Override
  public String getStringForLogging() {
    return String.format(
        "RegisterBlobRequest(requestId=%s, role=%s, blobs=[%s])",
        requestId,
        role,
        blobs.stream().map(BlobMetadata::getPath).collect(Collectors.joining(", ")));
  }
}
