/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/** Class used to serialize the blob register request */
public class RegisterBlobRequest implements StreamingIngestRequest {
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
}
