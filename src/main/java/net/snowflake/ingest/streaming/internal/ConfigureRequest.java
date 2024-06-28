/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import net.snowflake.ingest.utils.Utils;

/** Class used to serialize the client / channel configure request. */
public class ConfigureRequest implements StreamingIngestRequest {
  @JsonProperty("role")
  private String role;

  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonProperty("database")
  private String database;

  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonProperty("schema")
  private String schema;

  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonProperty("table")
  private String table;

  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonProperty("file_name")
  private String fileName;

  /**
   * Constructor for client configure request
   *
   * @param role Role to be used for the request.
   */
  ConfigureRequest(String role) {
    this.role = role;
  }

  /**
   * Constructor for channel configure request
   *
   * @param role Role to be used for the request.
   * @param database Database name.
   * @param schema Schema name.
   * @param table Table name.
   */
  ConfigureRequest(String role, String database, String schema, String table) {
    this.role = role;
    this.database = database;
    this.schema = schema;
    this.table = table;
  }

  String getRole() {
    return role;
  }

  String getDatabase() {
    return database;
  }

  String getSchema() {
    return schema;
  }

  String getTable() {
    return table;
  }

  String getFileName() {
    return fileName;
  }

  /** Set the file name for the GCS signed url request. */
  void setFileName(String fileName) {
    this.fileName = fileName;
  }

  String getFullyQualifiedTableName() {
    return Utils.getFullyQualifiedTableName(database, schema, table);
  }
}
