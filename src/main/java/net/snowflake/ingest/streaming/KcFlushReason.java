/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming;

public class KcFlushReason {
  public enum FlushReason {
    NONE("NONE"),
    BUFFER_FLUSH_TIME("buffer.flush.time"),
    BUFFER_BYTE_SIZE("buffer.size.bytes"),
    BUFFER_RECORD_COUNT("buffer.count.records"),
    ;

    private final String str;

    FlushReason(final String str) {
      this.str = str;
    }

    @Override
    public String toString() {
      return this.str;
    }
  }

  private final FlushReason flushReason;
  private final long flushValue;
  private final String firstOffsetToken;
  private final String lastOffsetToken;

  public KcFlushReason(FlushReason flushReason, long flushValue, String firstOffsetToken, String lastOffsetToken) {
    this.flushReason = flushReason;
    this.flushValue = flushValue;
    this.firstOffsetToken = firstOffsetToken;
    this.lastOffsetToken = lastOffsetToken;
  }

  public FlushReason getFlushReason() {
    return this.flushReason;
  }

  public long getFlushValue() {
    return this.flushValue;
  }

  @Override
  public String toString() {
    
  }
}
