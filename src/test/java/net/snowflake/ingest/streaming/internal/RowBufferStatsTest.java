package net.snowflake.ingest.streaming.internal;

import java.math.BigInteger;
import org.junit.Assert;
import org.junit.Test;

public class RowBufferStatsTest {

  @Test
  public void testCollationStates() throws Exception {
    RowBufferStats ai = new RowBufferStats("COL1", "en-ai");
    RowBufferStats as = new RowBufferStats("COL1", "en-as");
    RowBufferStats pi = new RowBufferStats("COL1", "en-pi");
    RowBufferStats ps = new RowBufferStats("COL1", "en-ps");
    RowBufferStats fu = new RowBufferStats("COL1", "en-fu");
    RowBufferStats fl = new RowBufferStats("COL1", "en-fl");
    RowBufferStats lower = new RowBufferStats("COL1", "lower");
    RowBufferStats upper = new RowBufferStats("COL1", "upper");
    RowBufferStats ltrim = new RowBufferStats("COL1", "ltrim");
    RowBufferStats rtrim = new RowBufferStats("COL1", "rtrim");
    RowBufferStats trim = new RowBufferStats("COL1", "trim");

    // Accents
    ai.addStrValue("a");
    ai.addStrValue("à");
    as.addStrValue("a");
    as.addStrValue("à");
    Assert.assertEquals("a", ai.getCurrentMinColStrValue());
    Assert.assertEquals("a", ai.getCurrentMaxColStrValue());
    Assert.assertEquals("a", as.getCurrentMinColStrValue());
    Assert.assertEquals("à", as.getCurrentMaxColStrValue());

    // Punctuation
    pi.addStrValue(".b");
    pi.addStrValue("a");
    ps.addStrValue(".b");
    ps.addStrValue("a");

    Assert.assertEquals("a", pi.getCurrentMinColStrValue());
    Assert.assertEquals(".b", ps.getCurrentMinColStrValue());

    // First Lower and Upper
    fl.addStrValue("C");
    fl.addStrValue("b");
    fu.addStrValue("b");
    fu.addStrValue("C");
    Assert.assertEquals("b", fl.getCurrentMinColStrValue());
    Assert.assertEquals("b", fu.getCurrentMinColStrValue());

    // Lower and Upper
    lower.addStrValue("AA");
    lower.addStrValue("a");
    upper.addStrValue("AA");
    upper.addStrValue("aaa");
    Assert.assertEquals("a", lower.getCurrentMinColStrValue());
    Assert.assertEquals("AA", upper.getCurrentMinColStrValue());

    // Trim settings
    trim.addStrValue(" z ");
    trim.addStrValue("b");
    ltrim.addStrValue(" z");
    ltrim.addStrValue("b");
    rtrim.addStrValue("z ");
    rtrim.addStrValue("b");
    Assert.assertEquals("b", trim.getCurrentMinColStrValue());
    Assert.assertEquals("b", ltrim.getCurrentMinColStrValue());
    Assert.assertEquals("b", rtrim.getCurrentMinColStrValue());
  }

  @Test
  public void testEmptyState() throws Exception {
    RowBufferStats stats = new RowBufferStats("COL1");

    Assert.assertNull(stats.getCollationDefinitionString());
    Assert.assertNull(stats.getCurrentMinRealValue());
    Assert.assertNull(stats.getCurrentMaxRealValue());
    Assert.assertNull(stats.getCurrentMinColStrValue());
    Assert.assertNull(stats.getCurrentMaxColStrValue());
    Assert.assertNull(stats.getCurrentMinIntValue());
    Assert.assertNull(stats.getCurrentMaxIntValue());

    Assert.assertEquals(0, stats.getCurrentNullCount());
    Assert.assertEquals(-1, stats.getDistinctValues());
  }

  @Test
  public void testMinMaxStrNonCol() throws Exception {
    RowBufferStats stats = new RowBufferStats("COL1");

    stats.addStrValue("bob");
    Assert.assertEquals("bob", stats.getCurrentMinColStrValue());
    Assert.assertEquals("bob", stats.getCurrentMaxColStrValue());
    Assert.assertEquals(-1, stats.getDistinctValues());

    stats.addStrValue("charlie");
    Assert.assertEquals("bob", stats.getCurrentMinColStrValue());
    Assert.assertEquals("charlie", stats.getCurrentMaxColStrValue());
    Assert.assertEquals(-1, stats.getDistinctValues());

    stats.addStrValue("alice");
    Assert.assertEquals("alice", stats.getCurrentMinColStrValue());
    Assert.assertEquals("charlie", stats.getCurrentMaxColStrValue());
    Assert.assertEquals(-1, stats.getDistinctValues());

    Assert.assertNull(stats.getCurrentMinRealValue());
    Assert.assertNull(stats.getCurrentMaxRealValue());
    Assert.assertNull(stats.getCollationDefinitionString());
    Assert.assertNull(stats.getCurrentMinIntValue());
    Assert.assertNull(stats.getCurrentMaxIntValue());
    Assert.assertEquals(0, stats.getCurrentNullCount());
  }

  @Test
  public void testStrTruncation() throws Exception {
    RowBufferStats stats = new RowBufferStats("COL1");
    stats.addStrValue("abcde|abcde|abcde|abcde|abcde|abcde|");
    Assert.assertEquals("abcde|abcde|abcde|abcde|abcde|ab", stats.getCurrentMinColStrValue());
    Assert.assertEquals("abcde|abcde|abcde|abcde|abcde|ac", stats.getCurrentMaxColStrValue());

    stats.addStrValue("zabcde|abcde|abcde|abcde|abcde|abcde|");
    Assert.assertEquals("abcde|abcde|abcde|abcde|abcde|ab", stats.getCurrentMinColStrValue());
    Assert.assertEquals("zabcde|abcde|abcde|abcde|abcde|b", stats.getCurrentMaxColStrValue());

    RowBufferStats ai = new RowBufferStats("COL1", "en-ai");
    ai.addStrValue("abcde|abcde|abcde|abcde|abcde|abcde|");
    Assert.assertEquals("abcde|abcde|abcde|abcde|abcde|ab", ai.getCurrentMinColStrValue());
    Assert.assertEquals("abcde|abcde|abcde|abcde|abcde|ac", ai.getCurrentMaxColStrValue());

    ai.addStrValue("zabcde|abcde|abcde|abcde|abcde|abcde|");
    Assert.assertEquals("abcde|abcde|abcde|abcde|abcde|ab", ai.getCurrentMinColStrValue());
    Assert.assertEquals("zabcde|abcde|abcde|abcde|abcde|b", ai.getCurrentMaxColStrValue());
  }

  @Test
  public void testMinMaxStrCol() throws Exception {
    RowBufferStats stats = new RowBufferStats("COL1", "en-ci");

    Assert.assertEquals("en-ci", stats.getCollationDefinitionString());

    stats.addStrValue("bob");
    Assert.assertEquals("bob", stats.getCurrentMinColStrValue());
    Assert.assertEquals("bob", stats.getCurrentMaxColStrValue());
    Assert.assertEquals(-1, stats.getDistinctValues());

    stats.addStrValue("Bob");
    Assert.assertEquals("bob", stats.getCurrentMinColStrValue());
    Assert.assertEquals("bob", stats.getCurrentMaxColStrValue());
    Assert.assertEquals(-1, stats.getDistinctValues());

    stats.addStrValue("Alice");
    Assert.assertEquals("Alice", stats.getCurrentMinColStrValue());
    Assert.assertEquals("bob", stats.getCurrentMaxColStrValue());
    Assert.assertEquals(-1, stats.getDistinctValues());

    Assert.assertNull(stats.getCurrentMinRealValue());
    Assert.assertNull(stats.getCurrentMaxRealValue());
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
    Assert.assertNull(stats.getCurrentMinColStrValue());
    Assert.assertNull(stats.getCurrentMaxColStrValue());
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
    Assert.assertNull(stats.getCurrentMinColStrValue());
    Assert.assertNull(stats.getCurrentMaxColStrValue());
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

    Assert.assertNull(result.getCurrentMinColStrValue());
    Assert.assertNull(result.getCurrentMaxColStrValue());
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
    Assert.assertNull(result.getCurrentMinColStrValue());
    Assert.assertNull(result.getCurrentMaxColStrValue());
    Assert.assertNull(result.getCurrentMinIntValue());
    Assert.assertNull(result.getCurrentMaxIntValue());

    // Test for Strings without collation
    one = new RowBufferStats("COL1");
    two = new RowBufferStats("COL1");

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
    Assert.assertEquals("a", result.getCurrentMinColStrValue());
    Assert.assertEquals("g", result.getCurrentMaxColStrValue());
    Assert.assertEquals(-1, result.getDistinctValues());
    Assert.assertEquals(2, result.getCurrentNullCount());
    Assert.assertEquals(5, result.getCurrentMaxLength());

    Assert.assertNull(result.getCurrentMinRealValue());
    Assert.assertNull(result.getCurrentMaxRealValue());
    Assert.assertNull(result.getCurrentMinIntValue());
    Assert.assertNull(result.getCurrentMaxIntValue());

    // Test for Strings with collation
    one = new RowBufferStats("COL1", "en-ci");
    two = new RowBufferStats("COL1", "en-ci");

    one.addStrValue("a");
    one.addStrValue("d");
    one.addStrValue("f");
    one.addStrValue("g");
    one.incCurrentNullCount();

    two.addStrValue("Alpha");
    two.addStrValue("b");
    two.addStrValue("c");
    two.addStrValue("d");
    two.incCurrentNullCount();

    result = RowBufferStats.getCombinedStats(one, two);
    Assert.assertEquals("a", result.getCurrentMinColStrValue());
    Assert.assertEquals("g", result.getCurrentMaxColStrValue());
    Assert.assertEquals(-1, result.getDistinctValues());
    Assert.assertEquals(2, result.getCurrentNullCount());

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

    Assert.assertNull(result.getCurrentMinColStrValue());
    Assert.assertNull(result.getCurrentMaxColStrValue());
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

    Assert.assertNull(result.getCurrentMinColStrValue());
    Assert.assertNull(result.getCurrentMaxColStrValue());
    Assert.assertNull(result.getCurrentMinIntValue());
    Assert.assertNull(result.getCurrentMaxIntValue());

    // Test for Strings
    one = new RowBufferStats("COL1");
    two = new RowBufferStats("COL1");

    one.addStrValue("alpha");
    one.addStrValue("d");
    one.addStrValue("f");
    one.addStrValue("g");
    one.incCurrentNullCount();

    result = RowBufferStats.getCombinedStats(one, two);
    Assert.assertEquals("alpha", result.getCurrentMinColStrValue());
    Assert.assertEquals("g", result.getCurrentMaxColStrValue());
    Assert.assertEquals(-1, result.getDistinctValues());
    Assert.assertEquals(1, result.getCurrentNullCount());

    Assert.assertNull(result.getCurrentMinRealValue());
    Assert.assertNull(result.getCurrentMaxRealValue());
    Assert.assertNull(result.getCurrentMinIntValue());
    Assert.assertNull(result.getCurrentMaxIntValue());
  }
}
