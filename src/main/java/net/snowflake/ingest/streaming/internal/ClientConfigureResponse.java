/*
 * Copyright (c) 2022 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;

abstract class ClientConfigureResponse extends StreamingIngestResponse {
  private String prefix;
  private Long statusCode;
  private Map<String, Object> stageLocationInfo;
  private String message;
  private List<String> srcLocations;

  ClientConfigureResponse() {}

  @JsonProperty("src_locations")
  List<String> getSrcLocations() {
    return srcLocations;
  }

  @JsonProperty("src_locations")
  void setSrcLocations(List<String> srcLocations) {
    this.srcLocations = srcLocations;
  }

  @JsonProperty("message")
  String getMessage() {
    return message;
  }

  @JsonProperty("message")
  void setMessage(String message) {
    this.message = message;
  }

  @JsonProperty("status_code")
  void setStatusCode(Long statusCode) {
    this.statusCode = statusCode;
  }

  @JsonProperty("status_code")
  Long getStatusCode() {
    return this.statusCode;
  }

  @JsonProperty("prefix")
  String getPrefix() {
    return prefix;
  }

  @JsonProperty("prefix")
  void setPrefix(String prefix) {
    this.prefix = prefix;
  }

  @JsonProperty("stage_location")
  Map<String, Object> getStageLocationInfo() {
    return stageLocationInfo;
  }

  @JsonProperty("stage_location")
  void setStageLocationInfo(Map<String, Object> stageLocationInfo) {
    this.stageLocationInfo = stageLocationInfo;
  }
}
