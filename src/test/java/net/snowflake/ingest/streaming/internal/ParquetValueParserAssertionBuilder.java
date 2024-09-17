/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.Assert;

/** Builder that helps to assert parsing of values to parquet types */
class ParquetValueParserAssertionBuilder {
  private ParquetBufferValue parquetBufferValue;
  private RowBufferStats rowBufferStats;
  private Class valueClass;
  private Object value;
  private float size;
  private Object minMaxStat;
  private long currentNullCount;

  static ParquetValueParserAssertionBuilder newBuilder() {
    ParquetValueParserAssertionBuilder builder = new ParquetValueParserAssertionBuilder();
    return builder;
  }

  ParquetValueParserAssertionBuilder parquetBufferValue(ParquetBufferValue parquetBufferValue) {
    this.parquetBufferValue = parquetBufferValue;
    return this;
  }

  ParquetValueParserAssertionBuilder rowBufferStats(RowBufferStats rowBufferStats) {
    this.rowBufferStats = rowBufferStats;
    return this;
  }

  ParquetValueParserAssertionBuilder expectedValueClass(Class valueClass) {
    this.valueClass = valueClass;
    return this;
  }

  ParquetValueParserAssertionBuilder expectedParsedValue(Object value) {
    this.value = value;
    return this;
  }

  ParquetValueParserAssertionBuilder expectedSize(float size) {
    this.size = size;
    return this;
  }

  public ParquetValueParserAssertionBuilder expectedMinMax(Object minMaxStat) {
    this.minMaxStat = minMaxStat;
    return this;
  }

  public ParquetValueParserAssertionBuilder expectedNullCount(long currentNullCount) {
    this.currentNullCount = currentNullCount;
    return this;
  }

  void assertMatches() {
    Assert.assertEquals(valueClass, parquetBufferValue.getValue().getClass());
    if (valueClass.equals(byte[].class)) {
      Assert.assertArrayEquals((byte[]) value, (byte[]) parquetBufferValue.getValue());
    } else {
      assertValueEquals(value, parquetBufferValue.getValue());
    }
    Assert.assertEquals(size, parquetBufferValue.getSize(), 0);
    if (rowBufferStats != null) {
      if (minMaxStat instanceof BigInteger) {
        Assert.assertEquals(minMaxStat, rowBufferStats.getCurrentMinIntValue());
        Assert.assertEquals(minMaxStat, rowBufferStats.getCurrentMaxIntValue());
        return;
      } else if (minMaxStat instanceof byte[]) {
        Assert.assertArrayEquals((byte[]) minMaxStat, rowBufferStats.getCurrentMinStrValue());
        Assert.assertArrayEquals((byte[]) minMaxStat, rowBufferStats.getCurrentMaxStrValue());
        return;
      } else if (valueClass.equals(String.class)) {
        // String can have null min/max stats for variant data types
        Object min =
            rowBufferStats.getCurrentMinStrValue() != null
                ? new String(rowBufferStats.getCurrentMinStrValue(), StandardCharsets.UTF_8)
                : rowBufferStats.getCurrentMinStrValue();
        Object max =
            rowBufferStats.getCurrentMaxStrValue() != null
                ? new String(rowBufferStats.getCurrentMaxStrValue(), StandardCharsets.UTF_8)
                : rowBufferStats.getCurrentMaxStrValue();
        Assert.assertEquals(minMaxStat, min);
        Assert.assertEquals(minMaxStat, max);
        return;
      } else if (minMaxStat instanceof Double || minMaxStat instanceof BigDecimal) {
        Assert.assertEquals(minMaxStat, rowBufferStats.getCurrentMinRealValue());
        Assert.assertEquals(minMaxStat, rowBufferStats.getCurrentMaxRealValue());
        return;
      }
      throw new IllegalArgumentException(
          String.format("Unknown data type for min stat: %s", minMaxStat.getClass()));
    }
  }

  void assertNull() {
    Assert.assertNull(parquetBufferValue.getValue());
    Assert.assertEquals(currentNullCount, rowBufferStats.getCurrentNullCount());
  }

  void assertValueEquals(Object expectedValue, Object actualValue) {
    if (expectedValue == null) {
      Assert.assertNull(actualValue);
      return;
    }
    if (expectedValue instanceof List) {
      Assert.assertTrue(actualValue instanceof List);
      List<?> expectedList = (List<?>) expectedValue;
      List<?> actualList = (List<?>) actualValue;
      Assert.assertEquals(expectedList.size(), actualList.size());
      for (int i = 0; i < expectedList.size(); i++) {
        assertValueEquals(expectedList.get(i), actualList.get(i));
      }
    } else if (expectedValue.getClass().equals(byte[].class)) {
      Assert.assertEquals(byte[].class, actualValue.getClass());
      Assert.assertArrayEquals((byte[]) expectedValue, (byte[]) actualValue);
    } else {
      Assert.assertEquals(expectedValue, actualValue);
    }
  }
}
