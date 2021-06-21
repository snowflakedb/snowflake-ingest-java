/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;

/** Class to deserialize a request from a channel status request */
class ChannelsStatusRequest {

  // Used to deserialize a channel request
  static class ChannelRequestDTO {

    /** Default constructor needed for Jackson serialization / deserialization */
    public ChannelRequestDTO() {}

    public ChannelRequestDTO(SnowflakeStreamingIngestChannel channel) {
      this.channelName = channel.getName();
      this.databaseName = channel.getDBName();
      this.schemaName = channel.getSchemaName();
      this.tableName = channel.getTableName();
      this.rowSequencerSent = true;
      this.clientSequencer =
          ((SnowflakeStreamingIngestChannelInternal) channel).getChannelSequencer();
      this.rowSequencer = ((SnowflakeStreamingIngestChannelInternal) channel).getRowSequencer();
    }

    // Database name
    private String databaseName;

    // Schema name
    private String schemaName;

    // Table Name
    private String tableName;

    // Channel Name
    private String channelName;

    // Client Sequencer
    private Long clientSequencer;

    // Optional Row Sequencer.
    private Long rowSequencer;
    private boolean rowSequencerSent;

    @JsonProperty("table")
    public String getTableName() {
      return tableName;
    }

    @JsonProperty("table")
    public void setTableName(String table) {
      this.tableName = table;
    }

    @JsonProperty("database")
    public void setDatabaseName(String databaseName) {
      this.databaseName = databaseName;
    }

    @JsonProperty("database")
    public String getDatabaseName() {
      return databaseName;
    }

    @JsonProperty("schema")
    public void setSchemaName(String schemaName) {
      this.schemaName = schemaName;
    }

    @JsonProperty("schema")
    public String getSchemaName() {
      return schemaName;
    }

    @JsonProperty("channel_name")
    public String getChannelName() {
      return channelName;
    }

    @JsonProperty("channel_name")
    public void setChannelName(String channelName) {
      this.channelName = channelName;
    }

    @JsonProperty("client_sequencer")
    public Long getClientSequencer() {
      return clientSequencer;
    }

    @JsonProperty("client_sequencer")
    public void setClientSequencer(long clientSequencer) {
      this.clientSequencer = clientSequencer;
    }

    @JsonInclude(Include.NON_EMPTY)
    @JsonProperty("row_sequencer")
    public void setRowSequencer(Long rowSequencer) {
      this.rowSequencer = rowSequencer;
      this.rowSequencerSent = true;
    }

    @JsonProperty("row_sequencer")
    public Long getRowSequencer() {
      return rowSequencer;
    }

    @JsonIgnore
    public boolean getRowSequencerSent() {
      return rowSequencerSent;
    }
  }

  // Optional Request ID. Used for diagnostic purposes.
  private String requestId;

  // Channels in request
  private ChannelRequestDTO[] channels;

  // Snowflake role used by client
  private String role;

  @JsonProperty("request_id")
  public String getRequestId() {
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
  public void setRequestId(String requestId) {
    this.requestId = requestId;
  }

  @JsonProperty("channels")
  public void setChannels(ChannelRequestDTO[] channels) {
    this.channels = channels;
  }

  @JsonProperty("channels")
  public ChannelRequestDTO[] getChannels() {
    return channels;
  }
}
