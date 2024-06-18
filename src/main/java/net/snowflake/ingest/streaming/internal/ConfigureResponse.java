package net.snowflake.ingest.streaming.internal;

import net.snowflake.client.jdbc.internal.fasterxml.jackson.annotation.JsonProperty;

class ConfigureResponse extends StreamingIngestResponse {
  @JsonProperty("prefix")
  private String prefix;

  @JsonProperty("status_code")
  private Long statusCode;

  @JsonProperty("message")
  private String message;

  @JsonProperty("stage_location")
  private StageMetadata stageMetadata;

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

  StageMetadata getStageMetadata() {
    return stageMetadata;
  }

  void setStageMetadata(StageMetadata stageMetadata) {
    this.stageMetadata = stageMetadata;
  }

  Long getDeploymentId() {
    return deploymentId;
  }

  void setDeploymentId(Long deploymentId) {
    this.deploymentId = deploymentId;
  }
}
