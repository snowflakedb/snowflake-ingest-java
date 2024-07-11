/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

/** Class used to serialize client configure request */
class ClientConfigureRequest extends ConfigureRequest {
  /**
   * Constructor for client configure request
   *
   * @param role Role to be used for the request.
   */
  ClientConfigureRequest(String role) {
    setRole(role);
  }

  @Override
  public String getStringForLogging() {
    return String.format("ClientConfigureRequest(role=%s, file_name=%s)", getRole(), getFileName());
  }
}
