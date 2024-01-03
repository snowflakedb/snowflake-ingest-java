package net.snowflake.ingest.streaming.internal;

import net.snowflake.ingest.streaming.DropChannelRequest;

/**
 * Same as DropChannelRequest but allows specifying a client sequencer to drop a specific version.
 */
class DropChannelVersionRequest extends DropChannelRequest {
  private final Long clientSequencer;

  public DropChannelVersionRequest(DropChannelRequestBuilder builder, long clientSequencer) {
    super(builder);
    this.clientSequencer = clientSequencer;
  }

  Long getClientSequencer() {
    return this.clientSequencer;
  }
}
