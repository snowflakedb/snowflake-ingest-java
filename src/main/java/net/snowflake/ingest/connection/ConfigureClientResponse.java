package net.snowflake.ingest.connection;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.UUID;

/** ConfigureClientResponse - response from a configure client request */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ConfigureClientResponse {
  private Long clientSequencer;

  @Override
  public String toString() {
    return "IngestResponse{" + "clientSequencer='" + clientSequencer + '\'' + '}';
  }

  public Long getClientSequencer() {
    return clientSequencer;
  }
}
