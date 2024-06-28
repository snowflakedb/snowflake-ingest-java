/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import com.fasterxml.jackson.annotation.JsonProperty;
import net.snowflake.ingest.streaming.DropChannelRequest;
import net.snowflake.ingest.utils.Utils;

/** Class used to serialize the {@link DropChannelRequest} */
public class DropChannelRequestInternal implements StreamingIngestRequest {
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

  @JsonProperty("is_iceberg")
  private boolean isIceberg;

  @JsonProperty("client_sequencer")
  Long clientSequencer;

  DropChannelRequestInternal(
      String requestId, String role, DropChannelRequest request, boolean isIceberg) {
    this.requestId = requestId;
    this.role = role;
    this.channel = request.getChannelName();
    this.table = request.getTableName();
    this.database = request.getDBName();
    this.schema = request.getSchemaName();
    this.isIceberg = isIceberg;
    if (request instanceof DropChannelVersionRequest) {
      this.clientSequencer = ((DropChannelVersionRequest) request).getClientSequencer();
    }
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

  boolean getIsIceberg() {
    return isIceberg;
  }

  Long getClientSequencer() {
    return clientSequencer;
  }

  String getFullyQualifiedTableName() {
    return Utils.getFullyQualifiedTableName(database, schema, table);
  }
}
