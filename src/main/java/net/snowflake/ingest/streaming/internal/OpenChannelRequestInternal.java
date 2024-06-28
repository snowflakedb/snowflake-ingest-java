/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.utils.Constants;
import net.snowflake.ingest.utils.Utils;

/** Class used to serialize the {@link OpenChannelRequest} */
public class OpenChannelRequestInternal implements StreamingIngestRequest {
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

  @JsonProperty("is_iceberg")
  private boolean isIceberg;

  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonProperty("offset_token")
  String offsetToken;

  OpenChannelRequestInternal(
      String requestId,
      String role,
      OpenChannelRequest request,
      Constants.WriteMode writeMode,
      boolean isIceberg) {
    this.requestId = requestId;
    this.role = role;
    this.channel = request.getChannelName();
    this.table = request.getTableName();
    this.database = request.getDBName();
    this.schema = request.getSchemaName();
    this.writeMode = writeMode.name();
    if (request.isOffsetTokenProvided()) {
      this.offsetToken = request.getOffsetToken();
    }
    this.isIceberg = isIceberg;
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

  boolean getIsIceberg() {
    return isIceberg;
  }

  String getOffsetToken() {
    return offsetToken;
  }

  String getFullyQualifiedTableName() {
    return Utils.getFullyQualifiedTableName(database, schema, table);
  }
}
