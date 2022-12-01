/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import com.fasterxml.jackson.annotation.JsonProperty;
import javax.annotation.Nullable;
import net.snowflake.ingest.utils.Utils;

/**
 * Metadata for a Streaming Ingest channel that sends to Snowflake as part of the register blob
 * request
 */
class ChannelMetadata {
  private final String channelName;
  private final Long clientSequencer;
  private final Long rowSequencer;
  @Nullable private final String offsetToken;

  static Builder builder() {
    return new Builder();
  }

  /** Builder class to build a ChannelMetadata */
  static class Builder {
    private String channelName;
    private Long clientSequencer;
    private Long rowSequencer;
    @Nullable private String offsetToken; // offset token could be null

    Builder setOwningChannelFromContext(ChannelFlushContext channelFlushContext) {
      this.channelName = channelFlushContext.getName();
      this.clientSequencer = channelFlushContext.getChannelSequencer();
      return this;
    }

    Builder setRowSequencer(Long rowSequencer) {
      this.rowSequencer = rowSequencer;
      return this;
    }

    Builder setOffsetToken(String offsetToken) {
      this.offsetToken = offsetToken;
      return this;
    }

    ChannelMetadata build() {
      return new ChannelMetadata(this);
    }
  }

  private ChannelMetadata(Builder builder) {
    Utils.assertStringNotNullOrEmpty("channel name", builder.channelName);
    Utils.assertNotNull("channel client sequencer", builder.clientSequencer);
    Utils.assertNotNull("channel row sequencer", builder.rowSequencer);

    this.channelName = builder.channelName;
    this.clientSequencer = builder.clientSequencer;
    this.rowSequencer = builder.rowSequencer;
    this.offsetToken = builder.offsetToken;
  }

  @JsonProperty("channel_name")
  String getChannelName() {
    return this.channelName;
  }

  @JsonProperty("client_sequencer")
  Long getClientSequencer() {
    return this.clientSequencer;
  }

  @JsonProperty("row_sequencer")
  Long getRowSequencer() {
    return this.rowSequencer;
  }

  @Nullable
  @JsonProperty("offset_token")
  String getOffsetToken() {
    return this.offsetToken;
  }
}
