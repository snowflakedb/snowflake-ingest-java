package net.snowflake.ingest.streaming.internal;

import com.fasterxml.jackson.annotation.JsonProperty;

public class RefreshTableInformationResponse extends StreamingIngestResponse {
  @JsonProperty("status_code")
  private Long statusCode;

  @JsonProperty("message")
  private String message;

  @JsonProperty("iceberg_location")
  private FileLocationInfo icebergLocationInfo;

  @Override
  Long getStatusCode() {
    return this.statusCode;
  }

  String getMessage() {
    return this.message;
  }

  FileLocationInfo getIcebergLocationInfo() {
    return this.icebergLocationInfo;
  }
}
