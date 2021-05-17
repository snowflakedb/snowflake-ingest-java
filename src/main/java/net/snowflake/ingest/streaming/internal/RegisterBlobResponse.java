/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/** Response to the RegisterBlobRequest */
public class RegisterBlobResponse {
  private Long statusCode;
  private String message;
  private List<BlobRegisterStatus> blobsStatus;

  @JsonProperty("status_code")
  public void setStatusCode(Long statusCode) {
    this.statusCode = statusCode;
  }

  public Long getStatusCode() {
    return this.statusCode;
  }

  @JsonProperty("message")
  public void setMessage(String message) {
    this.message = message;
  }

  public String getMessage() {
    return this.message;
  }

  @JsonProperty("blobs")
  public void setBlobsStatus(List<BlobRegisterStatus> blobsStatus) {
    this.blobsStatus = blobsStatus;
  }

  public List<BlobRegisterStatus> getBlobsStatus() {
    return this.blobsStatus;
  }
}
