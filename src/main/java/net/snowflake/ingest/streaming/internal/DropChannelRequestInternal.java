/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import net.snowflake.ingest.streaming.DropChannelRequest;
import net.snowflake.ingest.utils.Utils;

/** Class used to serialize the {@link DropChannelRequest} */
class DropChannelRequestInternal implements IStreamingIngestRequest {
  @JsonProperty("request_id")
  private String requestId;

  @JsonProperty("role")
  private String role;

  @JsonProperty("channel")
  private String channel;

  @JsonProperty("table")
  private String table;

  @JsonProperty("database")
  private String database;

  @JsonProperty("schema")
  private String schema;

  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonProperty("client_sequencer")
  Long clientSequencer;

  DropChannelRequestInternal(
      String requestId,
      String role,
      String database,
      String schema,
      String table,
      String channel,
      Long clientSequencer) {
    this.requestId = requestId;
    this.role = role;
    this.database = database;
    this.schema = schema;
    this.table = table;
    this.channel = channel;
    this.clientSequencer = clientSequencer;
  }

  String getRequestId() {
    return requestId;
  }

  String getRole() {
    return role;
  }

  String getChannel() {
    return channel;
  }

  String getTable() {
    return table;
  }

  String getDatabase() {
    return database;
  }

  String getSchema() {
    return schema;
  }

  Long getClientSequencer() {
    return clientSequencer;
  }

  String getFullyQualifiedTableName() {
    return Utils.getFullyQualifiedTableName(database, schema, table);
  }

  @Override
  public String getStringForLogging() {
    return String.format(
        "DropChannelRequest(requestId=%s, role=%s, db=%s, schema=%s, table=%s, channel=%s,"
            + " clientSequencer=%s)",
        requestId, role, database, schema, table, channel, clientSequencer);
  }
}
