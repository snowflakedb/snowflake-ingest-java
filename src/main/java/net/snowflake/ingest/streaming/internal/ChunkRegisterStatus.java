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

  @JsonProperty("channels")
  void setChannelsStatus(List<ChannelRegisterStatus> channelsStatus) {
    this.channelsStatus = channelsStatus;
  }

  List<ChannelRegisterStatus> getChannelsStatus() {
    return this.channelsStatus;
  }
}
