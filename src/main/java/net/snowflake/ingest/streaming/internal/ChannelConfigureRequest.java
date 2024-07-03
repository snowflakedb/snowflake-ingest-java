/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import net.snowflake.ingest.utils.Utils;

/** Class used to serialize the client / channel configure request. */
class ChannelConfigureRequest implements ConfigureRequest {
  @JsonProperty("role")
  private String role;

  @JsonProperty("database")
  private String database;

  @JsonProperty("schema")
  private String schema;

  @JsonProperty("table")
  private String table;

  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonProperty("file_name")
  private String fileName;

  /**
   * Constructor for channel configure request
   *
   * @param role Role to be used for the request.
   * @param database Database name.
   * @param schema Schema name.
   * @param table Table name.
   */
  ChannelConfigureRequest(String role, String database, String schema, String table) {
    this.role = role;
    this.database = database;
    this.schema = schema;
    this.table = table;
  }

  @Override
  public String getRole() {
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
  @Override
  public void setFileName(String fileName) {
    this.fileName = fileName;
  }

  @Override
  public String getStringForLogging() {
    return String.format(
        "ChannelConfigureRequest(role=%s, db=%s, schema=%s, table=%s, file_name=%s)",
        role, database, schema, table, fileName);
  }
}
