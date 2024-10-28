/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import com.fasterxml.jackson.annotation.JsonProperty;

class GeneratePresignedUrlsRequest implements IStreamingIngestRequest {
  @JsonProperty("database")
  private String dbName;

  @JsonProperty("schema")
  private String schemaName;

  @JsonProperty("table")
  private String tableName;

  @JsonProperty("role")
  private String role;

  @JsonProperty("count")
  private Integer count;

  @JsonProperty("timeout_in_seconds")
  private Integer timeoutInSeconds;

  @JsonProperty("deployment_global_id")
  private Long deploymentGlobalId;

  @JsonProperty("is_iceberg")
  private boolean enableIcebergStreaming;

  public GeneratePresignedUrlsRequest(
      TableRef tableRef,
      String role,
      int count,
      int timeoutInSeconds,
      Long deploymentGlobalId,
      boolean enableIcebergStreaming) {
    this.dbName = tableRef.dbName;
    this.schemaName = tableRef.schemaName;
    this.tableName = tableRef.tableName;
    this.count = count;
    this.role = role;
    this.timeoutInSeconds = timeoutInSeconds;
    this.deploymentGlobalId = deploymentGlobalId;
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

  Integer getCount() {
    return this.count;
  }

  Long getDeploymentGlobalId() {
    return this.deploymentGlobalId;
  }

  Integer getTimeoutInSeconds() {
    return this.timeoutInSeconds;
  }

  boolean getEnableIcebergStreaming() {
    return this.enableIcebergStreaming;
  }

  @Override
  public String getStringForLogging() {
    return String.format(
        "GetPresignedUrlsRequest(db=%s, schema=%s, table=%s, count=%s, timeoutInSeconds=%s"
            + " deploymentGlobalId=%s role=%s, enableIcebergStreaming=%s)",
        dbName,
        schemaName,
        tableName,
        count,
        timeoutInSeconds,
        deploymentGlobalId,
        role,
        enableIcebergStreaming);
  }
}
