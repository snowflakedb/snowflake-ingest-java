/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.utils.Constants;

/** Class used to serialize the {@link OpenChannelRequest} */
class OpenChannelRequestInternal implements IStreamingIngestRequest {
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

  @JsonProperty("write_mode")
  private String writeMode;

  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonProperty("offset_token")
  private String offsetToken;

  OpenChannelRequestInternal(
      String requestId,
      String role,
      String database,
      String schema,
      String table,
      String channel,
      Constants.WriteMode writeMode,
      String offsetToken) {
    this.requestId = requestId;
    this.role = role;
    this.database = database;
    this.schema = schema;
    this.table = table;
    this.channel = channel;
    this.writeMode = writeMode.name();
    this.offsetToken = offsetToken;
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

  String getWriteMode() {
    return writeMode;
  }

  String getOffsetToken() {
    return offsetToken;
  }

  @Override
  public String getStringForLogging() {
    return String.format(
        "OpenChannelRequestInternal(requestId=%s, role=%s, db=%s, schema=%s, table=%s, channel=%s,"
            + " writeMode=%s)",
        requestId, role, database, schema, table, channel, writeMode);
  }
}
