/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Class used to serialize the channel configure request. */
class ChannelConfigureRequest extends ClientConfigureRequest {
  @JsonProperty("database")
  private String database;

  @JsonProperty("schema")
  private String schema;

  @JsonProperty("table")
  private String table;

  /**
   * Constructor for channel configure request
   *
   * @param role Role to be used for the request.
   * @param database Database name.
   * @param schema Schema name.
   * @param table Table name.
   */
  ChannelConfigureRequest(String role, String database, String schema, String table) {
    super(role);
    this.database = database;
    this.schema = schema;
    this.table = table;
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

  @Override
  public String getStringForLogging() {
    return String.format(
        "ChannelConfigureRequest(role=%s, db=%s, schema=%s, table=%s, file_name=%s)",
        getRole(), database, schema, table, getFileName());
  }
}
