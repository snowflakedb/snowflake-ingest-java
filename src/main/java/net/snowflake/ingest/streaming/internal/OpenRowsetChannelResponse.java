/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Response to the open channel request to open a rowset api channel */
class OpenRowsetChannelResponse extends StreamingIngestResponse {
  private String message;
  private String continuationToken;
  private String offsetToken;
  private Long statusCode;

  @JsonProperty("message")
  void setMessage(String message) {
    this.message = message;
  }

  @Override
  String getMessage() {
    return this.message;
  }

  @JsonProperty("status_code")
  void setStatusCode(Long statusCode) {
    this.statusCode = statusCode;
  }

  @Override
  Long getStatusCode() {
    return this.statusCode;
  }

  @JsonProperty("offset_token")
  void setOffsetToken(String offsetToken) {
    this.offsetToken = offsetToken;
  }

  String getOffsetToken() {
    return this.offsetToken;
  }

  @JsonProperty("continuation_token")
  void setContinuationToken(String continuationToken) {
    this.continuationToken = continuationToken;
  }

  String getContinuationToken() {
    return this.continuationToken;
  }
}
