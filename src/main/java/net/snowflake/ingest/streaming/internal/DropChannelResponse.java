/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Response for a {@link net.snowflake.ingest.streaming.DropChannelRequest}. */
class DropChannelResponse extends StreamingIngestResponse {
  private Long statusCode;
  private String message;
  private String dbName;
  private String schemaName;
  private String tableName;
  private String channelName;

  @JsonProperty("status_code")
  void setStatusCode(Long statusCode) {
    this.statusCode = statusCode;
  }

  @Override
  Long getStatusCode() {
    return this.statusCode;
  }

  @JsonProperty("message")
  void setMessage(String message) {
    this.message = message;
  }

  @Override
  String getMessage() {
    return this.message;
  }

  @JsonProperty("database")
  void setDBName(String dbName) {
    this.dbName = dbName;
  }

  String getDBName() {
    return this.dbName;
  }

  @JsonProperty("schema")
  void setSchemaName(String schemaName) {
    this.schemaName = schemaName;
  }

  String getSchemaName() {
    return this.schemaName;
  }

  @JsonProperty("table")
  void setTableName(String tableName) {
    this.tableName = tableName;
  }

  String getTableName() {
    return this.tableName;
  }

  @JsonProperty("channel")
  void setChannelName(String channelName) {
    this.channelName = channelName;
  }

  String getChannelName() {
    return this.channelName;
  }
}
