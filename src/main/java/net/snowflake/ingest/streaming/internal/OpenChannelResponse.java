/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/** Response to the OpenChannelRequest */
class OpenChannelResponse {
  private Long statusCode;
  private String message;
  private Long clientSequencer;
  private Long rowSequencer;
  private String offsetToken;
  private List<ColumnMetadata> tableColumns;

  @JsonProperty("status_code")
  void setStatusCode(Long statusCode) {
    this.statusCode = statusCode;
  }

  Long getStatusCode() {
    return this.statusCode;
  }

  @JsonProperty("message")
  void setMessage(String message) {
    this.message = message;
  }

  String getMessage() {
    return this.message;
  }

  @JsonProperty("client_sequencer")
  void setClientSequencer(Long clientSequencer) {
    this.clientSequencer = clientSequencer;
  }

  Long getClientSequencer() {
    return this.clientSequencer;
  }

  @JsonProperty("row_sequencer")
  void setRowSequencer(Long rowSequencer) {
    this.rowSequencer = rowSequencer;
  }

  Long getRowSequencer() {
    return this.rowSequencer;
  }

  @JsonProperty("offset_token")
  void setOffsetToken(String offsetToken) {
    this.offsetToken = offsetToken;
  }

  String getOffsetToken() {
    return this.offsetToken;
  }

  @JsonProperty("table_columns")
  void setTableColumns(List<ColumnMetadata> tableColumns) {
    this.tableColumns = tableColumns;
  }

  List<ColumnMetadata> getTableColumns() {
    return this.tableColumns;
  }
}
