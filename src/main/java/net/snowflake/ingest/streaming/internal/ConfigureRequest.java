/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/** Abstract class for {@link ChannelConfigureRequest} and {@link ClientConfigureRequest} */
abstract class ConfigureRequest implements StreamingIngestRequest {
  @JsonProperty("role")
  private String role;

  // File name for the GCS signed url request
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonProperty("file_name")
  private String fileName;

  String getRole() {
    return role;
  }

  void setRole(String role) {
    this.role = role;
  }

  String getFileName() {
    return fileName;
  }

  void setFileName(String fileName) {
    this.fileName = fileName;
  }

  @Override
  public abstract String getStringForLogging();
}
