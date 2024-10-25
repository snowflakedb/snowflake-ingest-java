/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class RowBufferStatsTest {
  @Parameterized.Parameters(name = "enableNDVAndNV: {0}")
  public static Object[] enableNDVAndNV() {
    return new Object[] {false, true};
  }

  @Parameterized.Parameter public static boolean enableNDVAndNV;

  @Test
  public void testEmptyState() throws Exception {
    RowBufferStats stats = new RowBufferStats("COL1", enableNDVAndNV, enableNDVAndNV);

    Assert.assertNull(stats.getCollationDefinitionString());
    Assert.assertNull(stats.getCurrentMinRealValue());
    Assert.assertNull(stats.getCurrentMaxRealValue());
    Assert.assertNull(stats.getCurrentMinStrValue());
    Assert.assertNull(stats.getCurrentMaxStrValue());
    Assert.assertNull(stats.getCurrentMinIntValue());
    Assert.assertNull(stats.getCurrentMaxIntValue());
    Assert.assertEquals(0, stats.getCurrentNullCount());
    Assert.assertEquals(enableNDVAndNV ? 0 : -1, stats.getDistinctValues());
    Assert.assertEquals(enableNDVAndNV ? 0 : -1, stats.getNumberOfValues());
  }

  @Test
  public void testMinMaxStrNonCol() throws Exception {
    RowBufferStats stats = new RowBufferStats("COL1", enableNDVAndNV, enableNDVAndNV);

    stats.addStrValue("bob");
    Assert.assertArrayEquals("bob".getBytes(StandardCharsets.UTF_8), stats.getCurrentMinStrValue());
    Assert.assertArrayEquals("bob".getBytes(StandardCharsets.UTF_8), stats.getCurrentMaxStrValue());
    Assert.assertEquals(enableNDVAndNV ? 1 : -1, stats.getDistinctValues());
    Assert.assertEquals(enableNDVAndNV ? 1 : -1, stats.getNumberOfValues());

    stats.addStrValue("charlie");
    Assert.assertArrayEquals("bob".getBytes(StandardCharsets.UTF_8), stats.getCurrentMinStrValue());
    Assert.assertArrayEquals(
        "charlie".getBytes(StandardCharsets.UTF_8), stats.getCurrentMaxStrValue());
    Assert.assertEquals(enableNDVAndNV ? 2 : -1, stats.getDistinctValues());
    Assert.assertEquals(enableNDVAndNV ? 2 : -1, stats.getNumberOfValues());

    stats.addStrValue("alice");
    Assert.assertArrayEquals(
        "alice".getBytes(StandardCharsets.UTF_8), stats.getCurrentMinStrValue());
    Assert.assertArrayEquals(
        "charlie".getBytes(StandardCharsets.UTF_8), stats.getCurrentMaxStrValue());

    Assert.assertEquals(enableNDVAndNV ? 3 : -1, stats.getDistinctValues());
    Assert.assertEquals(enableNDVAndNV ? 3 : -1, stats.getNumberOfValues());

    Assert.assertNull(stats.getCurrentMinRealValue());
    Assert.assertNull(stats.getCurrentMaxRealValue());
    Assert.assertNull(stats.getCollationDefinitionString());
    Assert.assertNull(stats.getCurrentMinIntValue());
    Assert.assertNull(stats.getCurrentMaxIntValue());
    Assert.assertEquals(0, stats.getCurrentNullCount());
  }

  @Test
  public void testMinMaxInt() throws Exception {
    RowBufferStats stats = new RowBufferStats("COL1", enableNDVAndNV, enableNDVAndNV);

    stats.addIntValue(BigInteger.valueOf(5));
    Assert.assertEquals(BigInteger.valueOf((5)), stats.getCurrentMinIntValue());
    Assert.assertEquals(BigInteger.valueOf((5)), stats.getCurrentMaxIntValue());

    Assert.assertEquals(enableNDVAndNV ? 1 : -1, stats.getDistinctValues());
    Assert.assertEquals(enableNDVAndNV ? 1 : -1, stats.getNumberOfValues());

    stats.addIntValue(BigInteger.valueOf(6));
    Assert.assertEquals(BigInteger.valueOf((5)), stats.getCurrentMinIntValue());
    Assert.assertEquals(BigInteger.valueOf((6)), stats.getCurrentMaxIntValue());

    Assert.assertEquals(enableNDVAndNV ? 2 : -1, stats.getDistinctValues());
    Assert.assertEquals(enableNDVAndNV ? 2 : -1, stats.getNumberOfValues());

    stats.addIntValue(BigInteger.valueOf(4));
    Assert.assertEquals(BigInteger.valueOf((4)), stats.getCurrentMinIntValue());
    Assert.assertEquals(BigInteger.valueOf((6)), stats.getCurrentMaxIntValue());

    Assert.assertEquals(enableNDVAndNV ? 3 : -1, stats.getDistinctValues());
    Assert.assertEquals(enableNDVAndNV ? 3 : -1, stats.getNumberOfValues());

    Assert.assertNull(stats.getCurrentMinRealValue());
    Assert.assertNull(stats.getCurrentMaxRealValue());
    Assert.assertNull(stats.getCollationDefinitionString());
    Assert.assertNull(stats.getCurrentMinStrValue());
    Assert.assertNull(stats.getCurrentMaxStrValue());
    Assert.assertEquals(0, stats.getCurrentNullCount());
  }

  @Test
  public void testMinMaxReal() throws Exception {
    RowBufferStats stats = new RowBufferStats("COL1", enableNDVAndNV, enableNDVAndNV);

    stats.addRealValue(1.0);
    Assert.assertEquals(Double.valueOf(1), stats.getCurrentMinRealValue());
    Assert.assertEquals(Double.valueOf(1), stats.getCurrentMaxRealValue());

    Assert.assertEquals(enableNDVAndNV ? 1 : -1, stats.getDistinctValues());
    Assert.assertEquals(enableNDVAndNV ? 1 : -1, stats.getNumberOfValues());

    stats.addRealValue(1.5);
    Assert.assertEquals(Double.valueOf(1), stats.getCurrentMinRealValue());
    Assert.assertEquals(Double.valueOf(1.5), stats.getCurrentMaxRealValue());

    Assert.assertEquals(enableNDVAndNV ? 2 : -1, stats.getDistinctValues());
    Assert.assertEquals(enableNDVAndNV ? 2 : -1, stats.getNumberOfValues());

    stats.addRealValue(.8);
    Assert.assertEquals(Double.valueOf(.8), stats.getCurrentMinRealValue());
    Assert.assertEquals(Double.valueOf(1.5), stats.getCurrentMaxRealValue());

    Assert.assertEquals(enableNDVAndNV ? 3 : -1, stats.getDistinctValues());
    Assert.assertEquals(enableNDVAndNV ? 3 : -1, stats.getNumberOfValues());

    Assert.assertNull(stats.getCurrentMinIntValue());
    Assert.assertNull(stats.getCurrentMaxIntValue());
    Assert.assertNull(stats.getCollationDefinitionString());
    Assert.assertNull(stats.getCurrentMinStrValue());
    Assert.assertNull(stats.getCurrentMaxStrValue());
    Assert.assertEquals(0, stats.getCurrentNullCount());
  }

  @Test
  public void testIncCurrentNullCount() throws Exception {
    RowBufferStats stats = new RowBufferStats("COL1", enableNDVAndNV, enableNDVAndNV);

    Assert.assertEquals(0, stats.getCurrentNullCount());
    stats.incCurrentNullCount();
    Assert.assertEquals(1, stats.getCurrentNullCount());
    stats.incCurrentNullCount();
    Assert.assertEquals(2, stats.getCurrentNullCount());
  }

  @Test
  public void testMaxLength() throws Exception {
    RowBufferStats stats = new RowBufferStats("COL1", enableNDVAndNV, enableNDVAndNV);

    Assert.assertEquals(0, stats.getCurrentMaxLength());
    stats.setCurrentMaxLength(100L);
    Assert.assertEquals(100L, stats.getCurrentMaxLength());
    stats.setCurrentMaxLength(50L);
    Assert.assertEquals(100L, stats.getCurrentMaxLength());
  }

  @Test
  public void testGetCombinedStats() throws Exception {
    // Test for Integers
    RowBufferStats one = new RowBufferStats("COL1", enableNDVAndNV, enableNDVAndNV);
    RowBufferStats two = new RowBufferStats("COL1", enableNDVAndNV, enableNDVAndNV);

    one.addIntValue(BigInteger.valueOf(2));
    one.addIntValue(BigInteger.valueOf(4));
    one.addIntValue(BigInteger.valueOf(6));
    one.addIntValue(BigInteger.valueOf(8));
    one.incCurrentNullCount();
    one.incCurrentNullCount();

    two.addIntValue(BigInteger.valueOf(1));
    two.addIntValue(BigInteger.valueOf(3));
    two.addIntValue(BigInteger.valueOf(4));
    two.addIntValue(BigInteger.valueOf(5));

    RowBufferStats result = RowBufferStats.getCombinedStats(one, two);
    Assert.assertEquals(BigInteger.valueOf(1), result.getCurrentMinIntValue());
    Assert.assertEquals(BigInteger.valueOf(8), result.getCurrentMaxIntValue());
    Assert.assertEquals(enableNDVAndNV ? 7 : -1, result.getDistinctValues());
    Assert.assertEquals(enableNDVAndNV ? 10 : -1, result.getNumberOfValues());

    Assert.assertEquals(2, result.getCurrentNullCount());
    Assert.assertNull(result.getCurrentMinStrValue());
    Assert.assertNull(result.getCurrentMaxStrValue());
    Assert.assertNull(result.getCurrentMinRealValue());
    Assert.assertNull(result.getCurrentMaxRealValue());

    // Test for Reals
    one = new RowBufferStats("COL1", enableNDVAndNV, enableNDVAndNV);
    two = new RowBufferStats("COL1", enableNDVAndNV, enableNDVAndNV);

    one.addRealValue(2d);
    one.addRealValue(4d);
    one.addRealValue(6d);
    one.addRealValue(8d);

    two.addRealValue(1d);
    two.addRealValue(3d);
    two.addRealValue(4d);
    two.addRealValue(5d);

    result = RowBufferStats.getCombinedStats(one, two);
    Assert.assertEquals(Double.valueOf(1), result.getCurrentMinRealValue());
    Assert.assertEquals(Double.valueOf(8), result.getCurrentMaxRealValue());
    Assert.assertEquals(enableNDVAndNV ? 7 : -1, result.getDistinctValues());
    Assert.assertEquals(enableNDVAndNV ? 8 : -1, result.getNumberOfValues());

    Assert.assertEquals(0, result.getCurrentNullCount());
    Assert.assertNull(result.getCollationDefinitionString());
    Assert.assertNull(result.getCurrentMinStrValue());
    Assert.assertNull(result.getCurrentMaxStrValue());
    Assert.assertNull(result.getCurrentMinIntValue());
    Assert.assertNull(result.getCurrentMaxIntValue());

    // Test for Strings without collation
    one = new RowBufferStats("COL1", enableNDVAndNV, enableNDVAndNV);
    two = new RowBufferStats("COL1", enableNDVAndNV, enableNDVAndNV);

    one.addStrValue("alpha");
    one.addStrValue("d");
    one.addStrValue("f");
    one.addStrValue("g");
    one.incCurrentNullCount();
    one.setCurrentMaxLength(5);

    two.addStrValue("a");
    two.addStrValue("b");
    two.addStrValue("c");
    two.addStrValue("d");
    two.incCurrentNullCount();
    two.setCurrentMaxLength(1);

    result = RowBufferStats.getCombinedStats(one, two);
    Assert.assertArrayEquals("a".getBytes(StandardCharsets.UTF_8), result.getCurrentMinStrValue());
    Assert.assertArrayEquals("g".getBytes(StandardCharsets.UTF_8), result.getCurrentMaxStrValue());
    Assert.assertEquals(2, result.getCurrentNullCount());
    Assert.assertEquals(5, result.getCurrentMaxLength());
    Assert.assertEquals(enableNDVAndNV ? 7 : -1, result.getDistinctValues());
    Assert.assertEquals(enableNDVAndNV ? 10 : -1, result.getNumberOfValues());

    Assert.assertNull(result.getCurrentMinRealValue());
    Assert.assertNull(result.getCurrentMaxRealValue());
    Assert.assertNull(result.getCurrentMinIntValue());
    Assert.assertNull(result.getCurrentMaxIntValue());
  }

  @Test
  public void testGetCombinedStatsNull() throws Exception {
    // Test for Integers
    RowBufferStats one = new RowBufferStats("COL1", enableNDVAndNV, enableNDVAndNV);
    RowBufferStats two = new RowBufferStats("COL1", enableNDVAndNV, enableNDVAndNV);

    one.addIntValue(BigInteger.valueOf(2));
    one.addIntValue(BigInteger.valueOf(4));
    one.addIntValue(BigInteger.valueOf(6));
    one.addIntValue(BigInteger.valueOf(8));
    one.incCurrentNullCount();
    one.incCurrentNullCount();

    RowBufferStats result = RowBufferStats.getCombinedStats(one, two);
    Assert.assertEquals(BigInteger.valueOf(2), result.getCurrentMinIntValue());
    Assert.assertEquals(BigInteger.valueOf(8), result.getCurrentMaxIntValue());
    Assert.assertEquals(enableNDVAndNV ? 4 : -1, result.getDistinctValues());
    Assert.assertEquals(enableNDVAndNV ? 6 : -1, result.getNumberOfValues());

    Assert.assertEquals(2, result.getCurrentNullCount());

    Assert.assertNull(result.getCurrentMinStrValue());
    Assert.assertNull(result.getCurrentMaxStrValue());
    Assert.assertNull(result.getCurrentMinRealValue());
    Assert.assertNull(result.getCurrentMaxRealValue());

    // Test for Reals
    one = new RowBufferStats("COL1", enableNDVAndNV, enableNDVAndNV);

    one.addRealValue(2d);
    one.addRealValue(4d);
    one.addRealValue(6d);
    one.addRealValue(8d);

    result = RowBufferStats.getCombinedStats(one, two);
    Assert.assertEquals(Double.valueOf(2), result.getCurrentMinRealValue());
    Assert.assertEquals(Double.valueOf(8), result.getCurrentMaxRealValue());
    Assert.assertEquals(enableNDVAndNV ? 4 : -1, result.getDistinctValues());
    Assert.assertEquals(enableNDVAndNV ? 4 : -1, result.getNumberOfValues());
    Assert.assertEquals(0, result.getCurrentNullCount());

    Assert.assertNull(result.getCurrentMinStrValue());
    Assert.assertNull(result.getCurrentMaxStrValue());
    Assert.assertNull(result.getCurrentMinIntValue());
    Assert.assertNull(result.getCurrentMaxIntValue());

    // Test for Strings
    one = new RowBufferStats("COL1", enableNDVAndNV, enableNDVAndNV);
    two = new RowBufferStats("COL1", enableNDVAndNV, enableNDVAndNV);

    one.addStrValue("alpha");
    one.addStrValue("d");
    one.addStrValue("f");
    one.addStrValue("g");
    one.incCurrentNullCount();

    result = RowBufferStats.getCombinedStats(one, two);
    Assert.assertArrayEquals(
        "alpha".getBytes(StandardCharsets.UTF_8), result.getCurrentMinStrValue());
    Assert.assertArrayEquals("g".getBytes(StandardCharsets.UTF_8), result.getCurrentMaxStrValue());

    Assert.assertEquals(enableNDVAndNV ? 4 : -1, result.getDistinctValues());
    Assert.assertEquals(enableNDVAndNV ? 5 : -1, result.getNumberOfValues());
    Assert.assertEquals(1, result.getCurrentNullCount());

    Assert.assertNull(result.getCurrentMinRealValue());
    Assert.assertNull(result.getCurrentMaxRealValue());
    Assert.assertNull(result.getCurrentMinIntValue());
    Assert.assertNull(result.getCurrentMaxIntValue());
  }
}
