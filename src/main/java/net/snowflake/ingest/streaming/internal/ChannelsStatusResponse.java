/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/** Class used to serialize a response for the channels status endpoint */
class ChannelsStatusResponse {

  static class ChannelStatusResponseDTO {

    private Long statusCode;

    // latest persisted offset token
    private String persistedOffsetToken;

    // latest persisted client sequencer
    private long persistedClientSequencer;

    // latest persisted row sequencer
    private long persistedRowSequencer;

    //    /** Default constructor needed for Jackson */
    public ChannelStatusResponseDTO() {}

    /** Constructor when we only have a status code (failure case) */
    @JsonIgnore
    public ChannelStatusResponseDTO(final long statusCode) {
      this.statusCode = statusCode;
    }

    @JsonProperty("status_code")
    public Long getStatusCode() {
      return statusCode;
    }

    @JsonProperty("status_code")
    public void setStatusCode(Long statusCode) {
      this.statusCode = statusCode;
    }

    @JsonProperty("persisted_row_sequencer")
    public long getPersistedRowSequencer() {
      return persistedRowSequencer;
    }

    @JsonProperty("persisted_row_sequencer")
    public void setPersistedRowSequencer(long persistedRowSequencer) {
      this.persistedRowSequencer = persistedRowSequencer;
    }

    @JsonProperty("persisted_client_sequencer")
    public long getPersistedClientSequencer() {
      return persistedClientSequencer;
    }

    @JsonProperty("persisted_client_sequencer")
    public void setPersistedClientSequencer(long persistedClientSequencer) {
      this.persistedClientSequencer = persistedClientSequencer;
    }

    @JsonProperty("persisted_offset_token")
    public String getPersistedOffsetToken() {
      return persistedOffsetToken;
    }

    @JsonProperty("persisted_offset_token")
    public void setPersistedOffsetToken(String persistedOffsetToken) {
      this.persistedOffsetToken = persistedOffsetToken;
    }
  }

  // Default constructor needed for Jackson
  public ChannelsStatusResponse() {}

  // Channel array to return
  private ChannelStatusResponseDTO[] channels;
  private Long statusCode;
  private String message;

  @JsonProperty("status_code")
  void setStatusCode(Long statusCode) {
    this.statusCode = statusCode;
  }

  public long getStatusCode() {
    return this.statusCode;
  }

  @JsonProperty("message")
  void setMessage(String message) {
    this.message = message;
  }

  public String getMessage() {
    return this.message;
  }

  @JsonProperty("channels")
  public ChannelStatusResponseDTO[] getChannels() {
    return channels;
  }

  @JsonProperty("channels")
  public void setChannels(ChannelStatusResponseDTO[] channels) {
    this.channels = channels;
  }
}
