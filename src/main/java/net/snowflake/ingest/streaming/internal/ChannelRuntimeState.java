/*
 * Copyright (c) 2022 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import java.util.concurrent.atomic.AtomicLong;

/** Internal state of channel that is used and mutated by both channel and its buffer. */
class ChannelRuntimeState {
  // Indicates whether the channel is still valid
  private volatile boolean isValid;

  // The channel's current start offset token
  private volatile String startOffsetToken;

  // The channel's current end offset token
  private volatile String endOffsetToken;

  // The channel's current row sequencer
  private final AtomicLong rowSequencer;

  // First and last insert time in ms, used for end2end latency measurement
  private Long firstInsertInMs;
  private Long lastInsertInMs;

  ChannelRuntimeState(String endOffsetToken, long rowSequencer, boolean isValid) {
    this.endOffsetToken = endOffsetToken;
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

  /** @return current end offset token */
  String getEndOffsetToken() {
    return endOffsetToken;
  }

  /** @return current start offset token */
  String getStartOffsetToken() {
    return startOffsetToken;
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
   * Updates the channel's start and end offset token.
   *
   * @param startOffsetToken new start offset token of the batch
   * @param endOffsetToken new end offset token
   */
  void updateOffsetToken(String startOffsetToken, String endOffsetToken, int rowCount) {
    if (rowCount == 0) {
      this.startOffsetToken = startOffsetToken;
    }
    this.endOffsetToken = endOffsetToken;
  }

  /** Update the insert stats for the current row buffer whenever needed */
  void updateInsertStats(long currentTimeInMs, int rowCount) {
    if (rowCount == 0) {
      this.firstInsertInMs = currentTimeInMs;
    }
    this.lastInsertInMs = currentTimeInMs;
  }

  /** Get the insert timestamp of the first row in the current row buffer */
  Long getFirstInsertInMs() {
    return this.firstInsertInMs;
  }

  /** Get the insert timestamp of the last row in the current row buffer */
  Long getLastInsertInMs() {
    return this.lastInsertInMs;
  }
}
