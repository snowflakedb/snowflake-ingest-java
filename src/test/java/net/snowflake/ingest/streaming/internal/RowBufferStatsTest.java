package net.snowflake.ingest.streaming.internal;

import java.math.BigInteger;
import org.junit.Assert;
import org.junit.Test;

public class RowBufferStatsTest {

  @Test
  public void testEmptyState() throws Exception {
    RowBufferStats stats = new RowBufferStats();

    Assert.assertNull(stats.getCurrentMinRealValue());
    Assert.assertNull(stats.getCurrentMaxRealValue());
    Assert.assertNull(stats.getCurrentMinIntValue());
    Assert.assertNull(stats.getCurrentMaxIntValue());
    Assert.assertNull(stats.getCurrentMinStrValue());
    Assert.assertNull(stats.getCurrentMaxStrValue());

    Assert.assertEquals(0, stats.getCurrentNullCount());
    Assert.assertEquals(0, stats.getDistinctValues());
  }

  @Test
  public void testMinMaxStr() throws Exception {
    RowBufferStats stats = new RowBufferStats();

    stats.addStrValue("bob");
    Assert.assertEquals("bob", stats.getCurrentMinStrValue());
    Assert.assertEquals("bob", stats.getCurrentMaxStrValue());
    Assert.assertEquals(1, stats.getDistinctValues());

    stats.addStrValue("charlie");
    Assert.assertEquals("bob", stats.getCurrentMinStrValue());
    Assert.assertEquals("charlie", stats.getCurrentMaxStrValue());
    Assert.assertEquals(2, stats.getDistinctValues());

    stats.addStrValue("alice");
    Assert.assertEquals("alice", stats.getCurrentMinStrValue());
    Assert.assertEquals("charlie", stats.getCurrentMaxStrValue());
    Assert.assertEquals(3, stats.getDistinctValues());

    Assert.assertNull(stats.getCurrentMinRealValue());
    Assert.assertNull(stats.getCurrentMaxRealValue());
    Assert.assertNull(stats.getCurrentMinIntValue());
    Assert.assertNull(stats.getCurrentMaxIntValue());
    Assert.assertEquals(0, stats.getCurrentNullCount());
  }

  @Test
  public void testMinMaxInt() throws Exception {
    RowBufferStats stats = new RowBufferStats();

    stats.addIntValue(BigInteger.valueOf(5));
    Assert.assertEquals(BigInteger.valueOf((5)), stats.getCurrentMinIntValue());
    Assert.assertEquals(BigInteger.valueOf((5)), stats.getCurrentMaxIntValue());
    Assert.assertEquals(1, stats.getDistinctValues());

    stats.addIntValue(BigInteger.valueOf(6));
    Assert.assertEquals(BigInteger.valueOf((5)), stats.getCurrentMinIntValue());
    Assert.assertEquals(BigInteger.valueOf((6)), stats.getCurrentMaxIntValue());
    Assert.assertEquals(2, stats.getDistinctValues());

    stats.addIntValue(BigInteger.valueOf(4));
    Assert.assertEquals(BigInteger.valueOf((4)), stats.getCurrentMinIntValue());
    Assert.assertEquals(BigInteger.valueOf((6)), stats.getCurrentMaxIntValue());
    Assert.assertEquals(3, stats.getDistinctValues());

    Assert.assertNull(stats.getCurrentMinRealValue());
    Assert.assertNull(stats.getCurrentMaxRealValue());
    Assert.assertNull(stats.getCurrentMinStrValue());
    Assert.assertNull(stats.getCurrentMaxStrValue());
    Assert.assertEquals(0, stats.getCurrentNullCount());
  }

  @Test
  public void testMinMaxReal() throws Exception {
    RowBufferStats stats = new RowBufferStats();

    stats.addRealValue(1.0);
    Assert.assertEquals(Double.valueOf(1), stats.getCurrentMinRealValue());
    Assert.assertEquals(Double.valueOf(1), stats.getCurrentMaxRealValue());
    Assert.assertEquals(1, stats.getDistinctValues());

    stats.addRealValue(1.5);
    Assert.assertEquals(Double.valueOf(1), stats.getCurrentMinRealValue());
    Assert.assertEquals(Double.valueOf(1.5), stats.getCurrentMaxRealValue());
    Assert.assertEquals(2, stats.getDistinctValues());

    stats.addRealValue(.8);
    Assert.assertEquals(Double.valueOf(.8), stats.getCurrentMinRealValue());
    Assert.assertEquals(Double.valueOf(1.5), stats.getCurrentMaxRealValue());
    Assert.assertEquals(3, stats.getDistinctValues());

    Assert.assertNull(stats.getCurrentMinIntValue());
    Assert.assertNull(stats.getCurrentMaxIntValue());
    Assert.assertNull(stats.getCurrentMinStrValue());
    Assert.assertNull(stats.getCurrentMaxStrValue());
    Assert.assertEquals(0, stats.getCurrentNullCount());
  }

  @Test
  public void testIncCurrentNullCount() throws Exception {
    RowBufferStats stats = new RowBufferStats();

    Assert.assertEquals(0, stats.getCurrentNullCount());
    stats.incCurrentNullCount();
    Assert.assertEquals(1, stats.getCurrentNullCount());
    stats.incCurrentNullCount();
    Assert.assertEquals(2, stats.getCurrentNullCount());
  }

  @Test
  public void testMaxLength() throws Exception {
    RowBufferStats stats = new RowBufferStats();

    Assert.assertEquals(0, stats.getCurrentMaxLength());
    stats.setCurrentMaxLength(100L);
    Assert.assertEquals(100L, stats.getCurrentMaxLength());
    stats.setCurrentMaxLength(50L);
    Assert.assertEquals(100L, stats.getCurrentMaxLength());
  }
}
