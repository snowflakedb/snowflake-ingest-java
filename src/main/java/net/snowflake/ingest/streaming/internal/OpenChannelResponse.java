/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/** Response to the OpenChannelRequest */
public class OpenChannelResponse {
  private Long statusCode;
  private String message;
  private Long clientSequencer;
  private Long rowSequencer;
  private String offsetToken;
  private List<ColumnMetadata> tableColumns;

  @JsonProperty("status_code")
  public void setStatusCode(Long statusCode) {
    this.statusCode = statusCode;
  }

  public Long getStatusCode() {
    return this.statusCode;
  }

  @JsonProperty("message")
  public void setMessage(String message) {
    this.message = message;
  }

  public String getMessage() {
    return this.message;
  }

  @JsonProperty("client_sequencer")
  public void setClientSequencer(Long clientSequencer) {
    this.clientSequencer = clientSequencer;
  }

  public Long getClientSequencer() {
    return this.clientSequencer;
  }

  @JsonProperty("row_sequencer")
  public void setRowSequencer(Long rowSequencer) {
    this.rowSequencer = rowSequencer;
  }

  public Long getRowSequencer() {
    return this.rowSequencer;
  }

  @JsonProperty("offset_token")
  public void setOffsetToken(String offsetToken) {
    this.offsetToken = offsetToken;
  }

  public String getOffsetToken() {
    return this.offsetToken;
  }

  @JsonProperty("table_columns")
  public void setTableColumns(List<ColumnMetadata> tableColumns) {
    this.tableColumns = tableColumns;
  }

  public List<ColumnMetadata> getTableColumns() {
    return this.tableColumns;
  }
}
