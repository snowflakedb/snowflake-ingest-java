package net.snowflake.ingest.streaming.internal;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import org.junit.Assert;
import org.junit.Test;

public class RowBufferStatsTest {

  @Test
  public void testEmptyState() throws Exception {
    RowBufferStats stats = new RowBufferStats("COL1");

    Assert.assertNull(stats.getCollationDefinitionString());
    Assert.assertNull(stats.getCurrentMinRealValue());
    Assert.assertNull(stats.getCurrentMaxRealValue());
    Assert.assertNull(stats.getCurrentMinStrValue());
    Assert.assertNull(stats.getCurrentMaxStrValue());
    Assert.assertNull(stats.getCurrentMinIntValue());
    Assert.assertNull(stats.getCurrentMaxIntValue());

    Assert.assertEquals(0, stats.getCurrentNullCount());
    Assert.assertEquals(-1, stats.getDistinctValues());
  }

  @Test
  public void testMinMaxStrNonCol() throws Exception {
    RowBufferStats stats = new RowBufferStats("COL1");

    stats.addBinaryValue("bob".getBytes(StandardCharsets.UTF_8));
    Assert.assertArrayEquals("bob".getBytes(StandardCharsets.UTF_8), stats.getCurrentMinStrValue());
    Assert.assertArrayEquals("bob".getBytes(StandardCharsets.UTF_8), stats.getCurrentMaxStrValue());
    Assert.assertEquals(-1, stats.getDistinctValues());

    stats.addBinaryValue("charlie".getBytes(StandardCharsets.UTF_8));
    Assert.assertArrayEquals("bob".getBytes(StandardCharsets.UTF_8), stats.getCurrentMinStrValue());
    Assert.assertArrayEquals(
        "charlie".getBytes(StandardCharsets.UTF_8), stats.getCurrentMaxStrValue());
    Assert.assertEquals(-1, stats.getDistinctValues());

    stats.addBinaryValue("alice".getBytes(StandardCharsets.UTF_8));
    Assert.assertArrayEquals(
        "alice".getBytes(StandardCharsets.UTF_8), stats.getCurrentMinStrValue());
    Assert.assertArrayEquals(
        "charlie".getBytes(StandardCharsets.UTF_8), stats.getCurrentMaxStrValue());
    Assert.assertEquals(-1, stats.getDistinctValues());

    Assert.assertNull(stats.getCurrentMinRealValue());
    Assert.assertNull(stats.getCurrentMaxRealValue());
    Assert.assertNull(stats.getCollationDefinitionString());
    Assert.assertNull(stats.getCurrentMinIntValue());
    Assert.assertNull(stats.getCurrentMaxIntValue());
    Assert.assertEquals(0, stats.getCurrentNullCount());
  }

  @Test
  public void testMinMaxInt() throws Exception {
    RowBufferStats stats = new RowBufferStats("COL1");

    stats.addIntValue(BigInteger.valueOf(5));
    Assert.assertEquals(BigInteger.valueOf((5)), stats.getCurrentMinIntValue());
    Assert.assertEquals(BigInteger.valueOf((5)), stats.getCurrentMaxIntValue());
    Assert.assertEquals(-1, stats.getDistinctValues());

    stats.addIntValue(BigInteger.valueOf(6));
    Assert.assertEquals(BigInteger.valueOf((5)), stats.getCurrentMinIntValue());
    Assert.assertEquals(BigInteger.valueOf((6)), stats.getCurrentMaxIntValue());
    Assert.assertEquals(-1, stats.getDistinctValues());

    stats.addIntValue(BigInteger.valueOf(4));
    Assert.assertEquals(BigInteger.valueOf((4)), stats.getCurrentMinIntValue());
    Assert.assertEquals(BigInteger.valueOf((6)), stats.getCurrentMaxIntValue());
    Assert.assertEquals(-1, stats.getDistinctValues());

    Assert.assertNull(stats.getCurrentMinRealValue());
    Assert.assertNull(stats.getCurrentMaxRealValue());
    Assert.assertNull(stats.getCollationDefinitionString());
    Assert.assertNull(stats.getCurrentMinStrValue());
    Assert.assertNull(stats.getCurrentMaxStrValue());
    Assert.assertEquals(0, stats.getCurrentNullCount());
  }

  @Test
  public void testMinMaxReal() throws Exception {
    RowBufferStats stats = new RowBufferStats("COL1");

    stats.addRealValue(1.0);
    Assert.assertEquals(Double.valueOf(1), stats.getCurrentMinRealValue());
    Assert.assertEquals(Double.valueOf(1), stats.getCurrentMaxRealValue());
    Assert.assertEquals(-1, stats.getDistinctValues());

    stats.addRealValue(1.5);
    Assert.assertEquals(Double.valueOf(1), stats.getCurrentMinRealValue());
    Assert.assertEquals(Double.valueOf(1.5), stats.getCurrentMaxRealValue());
    Assert.assertEquals(-1, stats.getDistinctValues());

    stats.addRealValue(.8);
    Assert.assertEquals(Double.valueOf(.8), stats.getCurrentMinRealValue());
    Assert.assertEquals(Double.valueOf(1.5), stats.getCurrentMaxRealValue());
    Assert.assertEquals(-1, stats.getDistinctValues());

    Assert.assertNull(stats.getCurrentMinIntValue());
    Assert.assertNull(stats.getCurrentMaxIntValue());
    Assert.assertNull(stats.getCollationDefinitionString());
    Assert.assertNull(stats.getCurrentMinStrValue());
    Assert.assertNull(stats.getCurrentMaxStrValue());
    Assert.assertEquals(0, stats.getCurrentNullCount());
  }

  @Test
  public void testIncCurrentNullCount() throws Exception {
    RowBufferStats stats = new RowBufferStats("COL1");

    Assert.assertEquals(0, stats.getCurrentNullCount());
    stats.incCurrentNullCount();
    Assert.assertEquals(1, stats.getCurrentNullCount());
    stats.incCurrentNullCount();
    Assert.assertEquals(2, stats.getCurrentNullCount());
  }

  @Test
  public void testMaxLength() throws Exception {
    RowBufferStats stats = new RowBufferStats("COL1");

    Assert.assertEquals(0, stats.getCurrentMaxLength());
    stats.setCurrentMaxLength(100L);
    Assert.assertEquals(100L, stats.getCurrentMaxLength());
    stats.setCurrentMaxLength(50L);
    Assert.assertEquals(100L, stats.getCurrentMaxLength());
  }

  @Test
  public void testGetCombinedStats() throws Exception {
    // Test for Integers
    RowBufferStats one = new RowBufferStats("COL1");
    RowBufferStats two = new RowBufferStats("COL1");

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
    Assert.assertEquals(-1, result.getDistinctValues());
    Assert.assertEquals(2, result.getCurrentNullCount());

    Assert.assertNull(result.getCurrentMinStrValue());
    Assert.assertNull(result.getCurrentMaxStrValue());
    Assert.assertNull(result.getCurrentMinRealValue());
    Assert.assertNull(result.getCurrentMaxRealValue());

    // Test for Reals
    one = new RowBufferStats("COL1");
    two = new RowBufferStats("COL1");

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
    Assert.assertEquals(-1, result.getDistinctValues());
    Assert.assertEquals(0, result.getCurrentNullCount());

    Assert.assertNull(result.getCollationDefinitionString());
    Assert.assertNull(result.getCurrentMinStrValue());
    Assert.assertNull(result.getCurrentMaxStrValue());
    Assert.assertNull(result.getCurrentMinIntValue());
    Assert.assertNull(result.getCurrentMaxIntValue());

    // Test for Strings without collation
    one = new RowBufferStats("COL1");
    two = new RowBufferStats("COL1");

    one.addBinaryValue("alpha".getBytes(StandardCharsets.UTF_8));
    one.addBinaryValue("d".getBytes(StandardCharsets.UTF_8));
    one.addBinaryValue("f".getBytes(StandardCharsets.UTF_8));
    one.addBinaryValue("g".getBytes(StandardCharsets.UTF_8));
    one.incCurrentNullCount();
    one.setCurrentMaxLength(5);

    two.addBinaryValue("a".getBytes(StandardCharsets.UTF_8));
    two.addBinaryValue("b".getBytes(StandardCharsets.UTF_8));
    two.addBinaryValue("c".getBytes(StandardCharsets.UTF_8));
    two.addBinaryValue("d".getBytes(StandardCharsets.UTF_8));
    two.incCurrentNullCount();
    two.setCurrentMaxLength(1);

    result = RowBufferStats.getCombinedStats(one, two);
    Assert.assertArrayEquals("a".getBytes(StandardCharsets.UTF_8), result.getCurrentMinStrValue());
    Assert.assertArrayEquals("g".getBytes(StandardCharsets.UTF_8), result.getCurrentMaxStrValue());
    Assert.assertEquals(-1, result.getDistinctValues());
    Assert.assertEquals(2, result.getCurrentNullCount());
    Assert.assertEquals(5, result.getCurrentMaxLength());

    Assert.assertNull(result.getCurrentMinRealValue());
    Assert.assertNull(result.getCurrentMaxRealValue());
    Assert.assertNull(result.getCurrentMinIntValue());
    Assert.assertNull(result.getCurrentMaxIntValue());
  }

  @Test
  public void testGetCombinedStatsNull() throws Exception {
    // Test for Integers
    RowBufferStats one = new RowBufferStats("COL1");
    RowBufferStats two = new RowBufferStats("COL1");

    one.addIntValue(BigInteger.valueOf(2));
    one.addIntValue(BigInteger.valueOf(4));
    one.addIntValue(BigInteger.valueOf(6));
    one.addIntValue(BigInteger.valueOf(8));
    one.incCurrentNullCount();
    one.incCurrentNullCount();

    RowBufferStats result = RowBufferStats.getCombinedStats(one, two);
    Assert.assertEquals(BigInteger.valueOf(2), result.getCurrentMinIntValue());
    Assert.assertEquals(BigInteger.valueOf(8), result.getCurrentMaxIntValue());
    Assert.assertEquals(-1, result.getDistinctValues());
    Assert.assertEquals(2, result.getCurrentNullCount());

    Assert.assertNull(result.getCurrentMinStrValue());
    Assert.assertNull(result.getCurrentMaxStrValue());
    Assert.assertNull(result.getCurrentMinRealValue());
    Assert.assertNull(result.getCurrentMaxRealValue());

    // Test for Reals
    one = new RowBufferStats("COL1");

    one.addRealValue(2d);
    one.addRealValue(4d);
    one.addRealValue(6d);
    one.addRealValue(8d);

    result = RowBufferStats.getCombinedStats(one, two);
    Assert.assertEquals(Double.valueOf(2), result.getCurrentMinRealValue());
    Assert.assertEquals(Double.valueOf(8), result.getCurrentMaxRealValue());
    Assert.assertEquals(-1, result.getDistinctValues());
    Assert.assertEquals(0, result.getCurrentNullCount());

    Assert.assertNull(result.getCurrentMinStrValue());
    Assert.assertNull(result.getCurrentMaxStrValue());
    Assert.assertNull(result.getCurrentMinIntValue());
    Assert.assertNull(result.getCurrentMaxIntValue());

    // Test for Strings
    one = new RowBufferStats("COL1");
    two = new RowBufferStats("COL1");

    one.addBinaryValue("alpha".getBytes(StandardCharsets.UTF_8));
    one.addBinaryValue("d".getBytes(StandardCharsets.UTF_8));
    one.addBinaryValue("f".getBytes(StandardCharsets.UTF_8));
    one.addBinaryValue("g".getBytes(StandardCharsets.UTF_8));
    one.incCurrentNullCount();

    result = RowBufferStats.getCombinedStats(one, two);
    Assert.assertArrayEquals(
        "alpha".getBytes(StandardCharsets.UTF_8), result.getCurrentMinStrValue());
    Assert.assertArrayEquals("g".getBytes(StandardCharsets.UTF_8), result.getCurrentMaxStrValue());
    Assert.assertEquals(-1, result.getDistinctValues());
    Assert.assertEquals(1, result.getCurrentNullCount());

    Assert.assertNull(result.getCurrentMinRealValue());
    Assert.assertNull(result.getCurrentMaxRealValue());
    Assert.assertNull(result.getCurrentMinIntValue());
    Assert.assertNull(result.getCurrentMaxIntValue());
  }
}
