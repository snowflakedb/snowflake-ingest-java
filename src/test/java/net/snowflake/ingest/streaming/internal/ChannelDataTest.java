/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class ChannelDataTest {
  @Parameterized.Parameters(name = "enableNDVAndNV: {0}")
  public static Object[] enableNDVAndNV() {
    return new Object[] {false, true};
  }

  @Parameterized.Parameter public boolean enableNDVAndNV;

  @Test
  public void testGetCombinedColumnStatsMapNulls() {
    Map<String, RowBufferStats> left = new HashMap<>();
    RowBufferStats leftStats1 =
        new RowBufferStats(
            "COL1", InternalParameterProvider.ENABLE_DISTINCT_VALUES_COUNT_DEFAULT, enableNDVAndNV);
    left.put("one", leftStats1);
    leftStats1.addIntValue(new BigInteger("10"));

    try {
      ChannelData.getCombinedColumnStatsMap(left, null);
      Assert.fail("Expected error for null input");
    } catch (SFException err) {
      Assert.assertEquals(ErrorCode.INTERNAL_ERROR.getMessageCode(), err.getVendorCode());
    }

    try {
      ChannelData.getCombinedColumnStatsMap(null, left);
      Assert.fail("Expected error for null input");
    } catch (SFException err) {
      Assert.assertEquals(ErrorCode.INTERNAL_ERROR.getMessageCode(), err.getVendorCode());
    }

    try {
      ChannelData.getCombinedColumnStatsMap(null, null);
      Assert.fail("Expected error for null input");
    } catch (SFException err) {
      Assert.assertEquals(ErrorCode.INTERNAL_ERROR.getMessageCode(), err.getVendorCode());
    }
  }

  @Test
  public void testGetCombinedColumnStatsMapMissingColumn() {
    Map<String, RowBufferStats> left = new HashMap<>();
    RowBufferStats leftStats1 =
        new RowBufferStats(
            "COL1", InternalParameterProvider.ENABLE_DISTINCT_VALUES_COUNT_DEFAULT, enableNDVAndNV);
    left.put("one", leftStats1);
    leftStats1.addIntValue(new BigInteger("10"));

    Map<String, RowBufferStats> right = new HashMap<>();
    RowBufferStats rightStats1 =
        new RowBufferStats(
            "COL1", InternalParameterProvider.ENABLE_DISTINCT_VALUES_COUNT_DEFAULT, enableNDVAndNV);
    right.put("foo", rightStats1);
    rightStats1.addIntValue(new BigInteger("11"));

    // Check for same size, key mismatch
    try {
      ChannelData.getCombinedColumnStatsMap(left, right);
      Assert.fail("Expected error for mismatched input");
    } catch (SFException err) {
      Assert.assertEquals(ErrorCode.INTERNAL_ERROR.getMessageCode(), err.getVendorCode());
    }

    // Check different sizes
    right.remove("foo");
    right.put("one", rightStats1);
    right.put("two", rightStats1);

    try {
      ChannelData.getCombinedColumnStatsMap(left, right);
      Assert.fail("Expected error for mismatched input");
    } catch (SFException err) {
      Assert.assertEquals(ErrorCode.INTERNAL_ERROR.getMessageCode(), err.getVendorCode());
    }
  }

  @Test
  public void testGetCombinedColumnStatsMap() {
    Map<String, RowBufferStats> left = new HashMap<>();
    Map<String, RowBufferStats> right = new HashMap<>();

    RowBufferStats leftStats1 =
        new RowBufferStats(
            "COL1", InternalParameterProvider.ENABLE_DISTINCT_VALUES_COUNT_DEFAULT, enableNDVAndNV);
    RowBufferStats rightStats1 =
        new RowBufferStats(
            "COL1", InternalParameterProvider.ENABLE_DISTINCT_VALUES_COUNT_DEFAULT, enableNDVAndNV);
    RowBufferStats leftStats2 =
        new RowBufferStats(
            "COL1", InternalParameterProvider.ENABLE_DISTINCT_VALUES_COUNT_DEFAULT, enableNDVAndNV);
    RowBufferStats rightStats2 =
        new RowBufferStats(
            "COL1", InternalParameterProvider.ENABLE_DISTINCT_VALUES_COUNT_DEFAULT, enableNDVAndNV);

    left.put("one", leftStats1);
    left.put("two", leftStats2);
    right.put("one", rightStats1);
    right.put("two", rightStats2);

    leftStats1.addIntValue(new BigInteger("10"));
    leftStats1.addIntValue(new BigInteger("15"));
    rightStats1.addIntValue(new BigInteger("11"));
    rightStats1.addIntValue(new BigInteger("13"));
    rightStats1.addIntValue(new BigInteger("17"));

    leftStats2.addStrValue("10");
    leftStats2.addStrValue("15");
    rightStats2.addStrValue("11");
    rightStats2.addStrValue("13");
    rightStats2.addStrValue("17");

    Map<String, RowBufferStats> result = ChannelData.getCombinedColumnStatsMap(left, right);

    RowBufferStats oneCombined = result.get("one");
    RowBufferStats twoCombined = result.get("two");

    Assert.assertEquals(new BigInteger("10"), oneCombined.getCurrentMinIntValue());
    Assert.assertEquals(new BigInteger("17"), oneCombined.getCurrentMaxIntValue());
    Assert.assertNull(oneCombined.getCurrentMinStrValue());
    Assert.assertNull(oneCombined.getCurrentMaxStrValue());
    Assert.assertNull(oneCombined.getCurrentMinRealValue());
    Assert.assertNull(oneCombined.getCurrentMaxRealValue());

    Assert.assertEquals(
        InternalParameterProvider.ENABLE_DISTINCT_VALUES_COUNT_DEFAULT ? 5 : -1,
        oneCombined.getDistinctValues());
    Assert.assertEquals(enableNDVAndNV ? 5 : -1, oneCombined.getNumberOfValues());

    Assert.assertArrayEquals(
        "10".getBytes(StandardCharsets.UTF_8), twoCombined.getCurrentMinStrValue());
    Assert.assertArrayEquals(
        "17".getBytes(StandardCharsets.UTF_8), twoCombined.getCurrentMaxStrValue());
    Assert.assertNull(twoCombined.getCurrentMinIntValue());
    Assert.assertNull(twoCombined.getCurrentMaxIntValue());
    Assert.assertNull(twoCombined.getCurrentMinRealValue());
    Assert.assertNull(twoCombined.getCurrentMaxRealValue());

    Assert.assertEquals(
        InternalParameterProvider.ENABLE_DISTINCT_VALUES_COUNT_DEFAULT ? 5 : -1,
        oneCombined.getDistinctValues());
    Assert.assertEquals(enableNDVAndNV ? 5 : -1, oneCombined.getNumberOfValues());
  }
}
