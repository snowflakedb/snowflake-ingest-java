/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import java.math.BigInteger;
import java.util.Objects;
import java.util.Optional;
import net.snowflake.client.jdbc.internal.snowflake.common.core.SFTimestamp;

class TimestampWrapper {
  private long epoch;
  private int fraction;
  private BigInteger timeInScale;
  private Optional<Integer> timezoneOffset = Optional.empty();
  private Optional<SFTimestamp> sfTimestamp = Optional.empty();

  public TimestampWrapper(long epoch, int fraction, BigInteger timeInScale) {
    this.epoch = epoch;
    this.fraction = fraction;
    this.timeInScale = timeInScale;
  }

  public TimestampWrapper(
      long epoch,
      int fraction,
      BigInteger timeInScale,
      int timezoneOffset,
      SFTimestamp sfTimestamp) {
    this.epoch = epoch;
    this.fraction = fraction;
    this.timeInScale = timeInScale;
    this.timezoneOffset = Optional.ofNullable(timezoneOffset);
    this.sfTimestamp = Optional.ofNullable(sfTimestamp);
  }

  public long getEpoch() {
    return epoch;
  }

  public int getFraction() {
    return fraction;
  }

  public BigInteger getTimeInScale() {
    return timeInScale;
  }

  public Optional<Integer> getTimezoneOffset() {
    return timezoneOffset;
  }

  public Optional<Integer> getTimeZoneIndex() {
    return timezoneOffset.map(t -> (t / 1000 / 60) + 1440);
  }

  public Optional<SFTimestamp> getSfTimestamp() {
    return sfTimestamp;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TimestampWrapper that = (TimestampWrapper) o;
    return epoch == that.epoch
        && fraction == that.fraction
        && Objects.equals(timeInScale, that.timeInScale)
        && timezoneOffset.equals(that.timezoneOffset)
        && sfTimestamp.equals(that.sfTimestamp);
  }

  @Override
  public int hashCode() {
    return Objects.hash(epoch, fraction, timeInScale, timezoneOffset, sfTimestamp);
  }
}
