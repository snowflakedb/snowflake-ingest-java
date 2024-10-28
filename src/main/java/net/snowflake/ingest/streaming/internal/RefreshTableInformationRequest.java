/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import com.fasterxml.jackson.annotation.JsonProperty;

public class RefreshTableInformationRequest implements IStreamingIngestRequest {
  @JsonProperty("database")
  private String dbName;

  @JsonProperty("schema")
  private String schemaName;

  @JsonProperty("table")
  private String tableName;

  @JsonProperty("role")
  private String role;

  @JsonProperty("is_iceberg")
  private boolean enableIcebergStreaming;

  public RefreshTableInformationRequest(
      TableRef tableRef, String role, boolean enableIcebergStreaming) {
    this.dbName = tableRef.dbName;
    this.schemaName = tableRef.schemaName;
    this.tableName = tableRef.tableName;
    this.role = role;
    this.enableIcebergStreaming = enableIcebergStreaming;
  }

  String getDBName() {
    return this.dbName;
  }

  String getSchemaName() {
    return this.schemaName;
  }

  String getTableName() {
    return this.tableName;
  }

  String getRole() {
    return this.role;
  }

  boolean getEnableIcebergStreaming() {
    return this.enableIcebergStreaming;
  }

  @Override
  public String getStringForLogging() {
    return String.format(
        "RefreshTableInformation(db=%s, schema=%s, table=%s, role=%s, enableIcebergStreaming=%s)",
        dbName, schemaName, tableName, role, enableIcebergStreaming);
  }
}
