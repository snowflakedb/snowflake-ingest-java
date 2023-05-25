/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming;

public class KcFlushReason {
  public static KcFlushReason getDefaultKcFlushReason() {
    return new KcFlushReason(FlushReason.NONE, -1);
  }

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

  private FlushReason flushReason;
  private long flushValue;

  public KcFlushReason(FlushReason flushReason, long flushValue) {
    this.flushReason = flushReason;
    this.flushValue = flushValue;
  }

  public FlushReason getFlushReason() {
    return this.flushReason;
  }

  public long getFlushValue() {
    return this.flushValue;
  }
}
