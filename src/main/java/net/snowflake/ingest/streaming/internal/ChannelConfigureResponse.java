/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/** Class used to deserialize responses from channel configure endpoint */
@JsonIgnoreProperties(ignoreUnknown = true)
class ChannelConfigureResponse extends StreamingIngestResponse {
  @JsonProperty("status_code")
  private Long statusCode;

  @JsonProperty("message")
  private String message;

  @JsonProperty("stage_location")
  private FileLocationInfo stageLocation;

  @Override
  Long getStatusCode() {
    return statusCode;
  }

  void setStatusCode(Long statusCode) {
    this.statusCode = statusCode;
  }

  String getMessage() {
    return message;
  }

  void setMessage(String message) {
    this.message = message;
  }

  FileLocationInfo getStageLocation() {
    return stageLocation;
  }

  void setStageLocation(FileLocationInfo stageLocation) {
    this.stageLocation = stageLocation;
  }
}
