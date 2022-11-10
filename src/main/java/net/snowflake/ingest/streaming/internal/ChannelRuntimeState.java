/*
 * Copyright (c) 2022 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import java.util.concurrent.atomic.AtomicLong;

/** Internal state of channel that is used and mutated by both channel and its buffer. */
class ChannelRuntimeState {
  // Indicates whether the channel is still valid
  private volatile boolean isValid;

  // the channel's current offset token
  private volatile String offsetToken;

  // the channel's current row sequencer
  private final AtomicLong rowSequencer;

  ChannelRuntimeState(String offsetToken, long rowSequencer, boolean isValid) {
    this.offsetToken = offsetToken;
    this.rowSequencer = new AtomicLong(rowSequencer);
    this.isValid = isValid;
  }

  /**
   * Returns whether the channel is in a valid state.
   *
   * @return whether the channel is valid
   */
  boolean isValid() {
    return isValid;
  }

  /** Invalidate the channel that has this state object. */
  void invalidate() {
    isValid = false;
  }

  /** @return current offset token */
  String getOffsetToken() {
    return offsetToken;
  }

  /** @return current offset token after first incrementing it by one. */
  long incrementAndGetRowSequencer() {
    return rowSequencer.incrementAndGet();
  }

  /** @return row sequencer of the channel. */
  long getRowSequencer() {
    return rowSequencer.get();
  }

  /**
   * Updates the channel's offset token.
   *
   * @param offsetToken new offset token
   */
  void setOffsetToken(String offsetToken) {
    this.offsetToken = offsetToken;
  }
}
