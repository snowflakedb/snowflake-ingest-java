/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/** Class to deserialize a request from a channel status request */
class ChannelsStatusRequest {

  // Used to deserialize a channel request
  static class ChannelStatusRequestDTO {
    // Database name
    private final String databaseName;

    // Schema name
    private final String schemaName;

    // Table Name
    private final String tableName;

    // Channel Name
    private final String channelName;

    // Client Sequencer
    private final Long clientSequencer;

    ChannelStatusRequestDTO(SnowflakeStreamingIngestChannelInternal channel) {
      this.channelName = channel.getName();
      this.databaseName = channel.getDBName();
      this.schemaName = channel.getSchemaName();
      this.tableName = channel.getTableName();
      this.clientSequencer = channel.getChannelSequencer();
    }

    @JsonProperty("table")
    String getTableName() {
      return tableName;
    }

    @JsonProperty("database")
    String getDatabaseName() {
      return databaseName;
    }

    @JsonProperty("schema")
    String getSchemaName() {
      return schemaName;
    }

    @JsonProperty("channel_name")
    String getChannelName() {
      return channelName;
    }

    @JsonProperty("client_sequencer")
    Long getClientSequencer() {
      return clientSequencer;
    }
  }

  // Optional Request ID. Used for diagnostic purposes.
  private String requestId;

  // Channels in request
  private List<ChannelStatusRequestDTO> channels;

  // Snowflake role used by client
  private String role;

  @JsonProperty("request_id")
  String getRequestId() {
    return requestId;
  }

  @JsonProperty("role")
  public String getRole() {
    return role;
  }

  @JsonProperty("role")
  public void setRole(String role) {
    this.role = role;
  }

  @JsonProperty("request_id")
  void setRequestId(String requestId) {
    this.requestId = requestId;
  }

  @JsonProperty("channels")
  void setChannels(List<ChannelStatusRequestDTO> channels) {
    this.channels = channels;
  }

  @JsonProperty("channels")
  List<ChannelStatusRequestDTO> getChannels() {
    return channels;
  }
}
