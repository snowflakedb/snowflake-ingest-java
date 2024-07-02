/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/** Class used to serialize client configure request */
class ClientConfigureRequest implements ConfigureRequest {
  @JsonProperty("role")
  private String role;

  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonProperty("file_name")
  private String fileName;

  /**
   * Constructor for client configure request
   *
   * @param role Role to be used for the request.
   */
  ClientConfigureRequest(String role) {
    this.role = role;
  }

  @Override
  public String getRole() {
    return role;
  }

  String getFileName() {
    return fileName;
  }

  /** Set the file name for the GCS signed url request. */
  @Override
  public void setFileName(String fileName) {
    this.fileName = fileName;
  }

  @Override
  public String getStringForLogging() {
    return String.format("ClientConfigureRequest(role=%s, file_name=%s)", role, fileName);
  }
}
