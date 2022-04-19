/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/**
 * Indicates the status of a chunk registration, return from server as part of the register blob
 * REST_API
 */
class ChunkRegisterStatus {
  private List<ChannelRegisterStatus> channelsStatus;
  private String dbName;
  private String schemaName;
  private String tableName;

  @JsonProperty("channels")
  void setChannelsStatus(List<ChannelRegisterStatus> channelsStatus) {
    this.channelsStatus = channelsStatus;
  }

  List<ChannelRegisterStatus> getChannelsStatus() {
    return this.channelsStatus;
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
}
