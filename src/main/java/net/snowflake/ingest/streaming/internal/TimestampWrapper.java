/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import java.math.BigInteger;
import java.util.Objects;

class TimestampWrapper {
  private long epoch;
  private int fraction;
  private BigInteger timeInScale;

  public TimestampWrapper(long epoch, int fraction, BigInteger timeInScale) {
    this.epoch = epoch;
    this.fraction = fraction;
    this.timeInScale = timeInScale;
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

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TimestampWrapper that = (TimestampWrapper) o;
    return epoch == that.epoch
        && fraction == that.fraction
        && Objects.equals(timeInScale, that.timeInScale);
  }

  @Override
  public int hashCode() {
    return Objects.hash(epoch, fraction, timeInScale);
  }
}
