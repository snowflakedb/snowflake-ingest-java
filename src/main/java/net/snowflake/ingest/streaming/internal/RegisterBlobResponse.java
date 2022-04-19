/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/** Response to the RegisterBlobRequest */
class RegisterBlobResponse {
  private Long statusCode;
  private String message;
  private List<BlobRegisterStatus> blobsStatus;

  @JsonProperty("status_code")
  void setStatusCode(Long statusCode) {
    this.statusCode = statusCode;
  }

  Long getStatusCode() {
    return this.statusCode;
  }

  @JsonProperty("message")
  void setMessage(String message) {
    this.message = message;
  }

  String getMessage() {
    return this.message;
  }

  @JsonProperty("blobs")
  void setBlobsStatus(List<BlobRegisterStatus> blobsStatus) {
    this.blobsStatus = blobsStatus;
  }

  List<BlobRegisterStatus> getBlobsStatus() {
    return this.blobsStatus;
  }
}
