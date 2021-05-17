/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import com.fasterxml.jackson.annotation.JsonProperty;
import javax.annotation.Nullable;
import net.snowflake.ingest.utils.StreamingUtils;

/**
 * Metadata for a Streaming Ingest channel that sends to Snowflake as part of the register blob
 * request
 */
public class ChannelMetadata {
  private final String channelName;
  private final Long clientSequencer;
  private final Long rowSequencer;
  @Nullable private final String offsetToken;

  public static Builder builder() {
    return new Builder();
  }

  /** Builder class to build a ChannelMetadata */
  public static class Builder {
    private String channelName;
    private Long clientSequencer;
    private Long rowSequencer;
    @Nullable private String offsetToken; // offset token could be null

    public Builder setOwningChannel(SnowflakeStreamingIngestChannelInternal channel) {
      this.channelName = channel.getName();
      this.clientSequencer = channel.getChannelSequencer();
      return this;
    }

    public Builder setRowSequencer(Long rowSequencer) {
      this.rowSequencer = rowSequencer;
      return this;
    }

    public Builder setOffsetToken(String offsetToken) {
      this.offsetToken = offsetToken;
      return this;
    }

    public ChannelMetadata build() {
      return new ChannelMetadata(this);
    }
  }

  private ChannelMetadata(Builder builder) {
    StreamingUtils.assertStringNotNullOrEmpty("channel name", builder.channelName);
    StreamingUtils.assertNotNull("channel client sequencer", builder.clientSequencer);
    StreamingUtils.assertNotNull("channel row sequencer", builder.rowSequencer);

    this.channelName = builder.channelName;
    this.clientSequencer = builder.clientSequencer;
    this.rowSequencer = builder.rowSequencer;
    this.offsetToken = builder.offsetToken;
  }

  @JsonProperty("channel_name")
  public String getChannelName() {
    return this.channelName;
  }

  @JsonProperty("client_sequencer")
  public Long getClientSequencer() {
    return this.clientSequencer;
  }

  @JsonProperty("row_sequencer")
  public Long getRowSequencer() {
    return this.rowSequencer;
  }

  @Nullable
  @JsonProperty("offset_token")
  public String getOffsetToken() {
    return this.offsetToken;
  }
}
