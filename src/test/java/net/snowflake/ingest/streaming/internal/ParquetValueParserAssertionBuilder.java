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
  private Object minStat;
  private Object maxStat;
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
    this.minStat = minMaxStat;
    this.maxStat = minMaxStat;
    return this;
  }

  public ParquetValueParserAssertionBuilder expectedMin(Object minStat) {
    this.minStat = minStat;
    return this;
  }

  public ParquetValueParserAssertionBuilder expectedMax(Object maxStat) {
    this.maxStat = maxStat;
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
      if (minStat instanceof BigInteger) {
        Assert.assertEquals(minStat, rowBufferStats.getCurrentMinIntValue());
        Assert.assertEquals(maxStat, rowBufferStats.getCurrentMaxIntValue());
        return;
      } else if (minStat instanceof byte[]) {
        Assert.assertArrayEquals((byte[]) minStat, rowBufferStats.getCurrentMinStrValue());
        Assert.assertArrayEquals((byte[]) maxStat, rowBufferStats.getCurrentMaxStrValue());
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
        Assert.assertEquals(minStat, min);
        Assert.assertEquals(maxStat, max);
        return;
      } else if (minStat instanceof Double || minStat instanceof BigDecimal) {
        Assert.assertEquals(minStat, rowBufferStats.getCurrentMinRealValue());
        Assert.assertEquals(maxStat, rowBufferStats.getCurrentMaxRealValue());
        return;
      }
      throw new IllegalArgumentException(
          String.format("Unknown data type for min stat: %s", minStat.getClass()));
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
