/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Indicates the status of a channel registration, return from server as part of the register blob
 * REST_API
 */
class ChannelRegisterStatus {
  private Long statusCode;

  @JsonProperty("status_code")
  void setStatusCode(Long statusCode) {
    this.statusCode = statusCode;
  }

  Long getStatusCode() {
    return this.statusCode;
  }
}
