/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.core.arrow;

import net.snowflake.client.core.DataConversionContext;
import net.snowflake.client.core.ResultUtil;
import net.snowflake.client.core.SFException;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeTimestampWithTimezone;
import net.snowflake.client.jdbc.SnowflakeType;
import net.snowflake.client.jdbc.SnowflakeUtil;
import net.snowflake.common.core.SFTime;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.ValueVector;

import java.nio.ByteBuffer;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.TimeZone;

public class BigIntToTimeConverter extends AbstractArrowVectorConverter {
  private BigIntVector bigIntVector;
  protected ByteBuffer byteBuf = ByteBuffer.allocate(BigIntVector.TYPE_WIDTH);

  public BigIntToTimeConverter(
      ValueVector fieldVector, int columnIndex, DataConversionContext context) {
    super(SnowflakeType.TIME.name(), fieldVector, columnIndex, context);
    this.bigIntVector = (BigIntVector) fieldVector;
  }

  /**
   * parse long into SFTime
   *
   * @param index
   * @return
   */
  private SFTime toSFTime(int index) {
    long val = bigIntVector.getDataBuffer().getLong(index * BigIntVector.TYPE_WIDTH);
    return SFTime.fromFractionalSeconds(val, context.getScale(columnIndex));
  }

  @Override
  public Time toTime(int index) throws SFException {
    if (isNull(index)) {
      return null;
    } else {
      SFTime sfTime = toSFTime(index);
      if (sfTime == null) {
        return null;
      }
      Time ts =
          new Time(
              sfTime.getFractionalSeconds(ResultUtil.DEFAULT_SCALE_OF_SFTIME_FRACTION_SECONDS));
      if (useSessionTimezone) {
        ts =
            SnowflakeUtil.getTimeInSessionTimezone(
                SnowflakeUtil.getSecondsFromMillis(ts.getTime()),
                sfTime.getNanosecondsWithinSecond());
      }
      return ts;
    }
  }

  @Override
  public String toString(int index) throws SFException {
    if (context.getTimeFormatter() == null) {
      throw new SFException(ErrorCode.INTERNAL_ERROR, "missing time formatter");
    }
    return isNull(index)
        ? null
        : ResultUtil.getSFTimeAsString(
            toSFTime(index), context.getScale(columnIndex), context.getTimeFormatter());
  }

  @Override
  public Object toObject(int index) throws SFException {
    return isNull(index) ? null : toTime(index);
  }

  @Override
  public Timestamp toTimestamp(int index, TimeZone tz) throws SFException {
    if (isNull(index)) {
      return null;
    }
    if (useSessionTimezone) {
      SFTime sfTime = toSFTime(index);
      return new SnowflakeTimestampWithTimezone(
          sfTime.getFractionalSeconds(ResultUtil.DEFAULT_SCALE_OF_SFTIME_FRACTION_SECONDS),
          sfTime.getNanosecondsWithinSecond(),
          TimeZone.getTimeZone("UTC"));
    }
    return new Timestamp(toTime(index).getTime());
  }

  @Override
  public boolean toBoolean(int index) throws SFException {
    if (isNull(index)) {
      return false;
    }
    Time val = toTime(index);
    throw new SFException(
        ErrorCode.INVALID_VALUE_CONVERT, logicalTypeStr,
        SnowflakeUtil.BOOLEAN_STR, val);
  }
}
