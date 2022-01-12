/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.ingest.connection;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/** ClientStatusResponse - response from a get client status request */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ClientStatusResponse {
  private Long clientSequencer;

  private String offsetToken;

  @Override
  public String toString() {
    return "IngestResponse{"
        + "clientSequencer='"
        + clientSequencer
        + '\''
        + ", offsetToken='"
        + offsetToken
        + '\''
        + '}';
  }

  /**
   * unique identifier for the client
   *
   * @return clientSequencer
   */
  public Long getClientSequencer() {
    return clientSequencer;
  }

  /**
   * offset number for kafka connector
   *
   * @return offsetToken
   */
  public String getOffsetToken() {
    return offsetToken;
  }
}
