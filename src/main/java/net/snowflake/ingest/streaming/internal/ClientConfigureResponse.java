/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/** Class used to deserialize responses from configure endpoint */
@JsonIgnoreProperties(ignoreUnknown = true)
class ClientConfigureResponse extends StreamingIngestResponse {
  @JsonProperty("prefix")
  private String prefix;

  @JsonProperty("status_code")
  private Long statusCode;

  @JsonProperty("message")
  private String message;

  @JsonProperty("stage_location")
  private FileLocationInfo stageLocation;

  @JsonProperty("deployment_id")
  private Long deploymentId;

  String getPrefix() {
    return prefix;
  }

  void setPrefix(String prefix) {
    this.prefix = prefix;
  }

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

  Long getDeploymentId() {
    return deploymentId;
  }

  void setDeploymentId(Long deploymentId) {
    this.deploymentId = deploymentId;
  }

  String getClientPrefix() {
    if (this.deploymentId == null) {
      return this.prefix;
    }
    return this.prefix + "_" + this.deploymentId;
  }
}
