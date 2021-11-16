package net.snowflake.ingest.connection;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.UUID;

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

  public Long getClientSequencer() {
    return clientSequencer;
  }

  public String getOffsetToken() {
    return offsetToken;
  }
}
