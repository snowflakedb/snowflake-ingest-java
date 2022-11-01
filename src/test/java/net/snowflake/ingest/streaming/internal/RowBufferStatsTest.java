package net.snowflake.ingest.streaming.internal;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.junit.Assert;
import org.junit.Test;

public class RowBufferStatsTest {

  @Test
  public void name() throws DecoderException {
    RowBufferStats ai = new RowBufferStats("en-ai");
    System.out.println(Hex.decodeHex("2901")[0]);
    System.out.println(Hex.decodeHex("2901")[1]);
    ai.addStrValue("a");
    //    ai.addStrValue2("à");

    System.out.println(ai.getCurrentMinColStrValueInBytes()[0]);
    System.out.println(ai.getCurrentMinColStrValueInBytes()[1]);
    Assert.assertArrayEquals(Hex.decodeHex("2901"), ai.getCurrentMinColStrValueInBytes());
  }

  @Test
  public void testCollationStates() throws Exception {
    RowBufferStats ai = new RowBufferStats("en-ai");
    RowBufferStats as = new RowBufferStats("en-as");
    RowBufferStats pi = new RowBufferStats("en-pi");
    RowBufferStats ps = new RowBufferStats("en-ps");
    RowBufferStats fu = new RowBufferStats("en-fu");
    RowBufferStats fl = new RowBufferStats("en-fl");
    RowBufferStats lower = new RowBufferStats("lower");
    RowBufferStats upper = new RowBufferStats("upper");
    RowBufferStats ltrim = new RowBufferStats("ltrim");
    RowBufferStats rtrim = new RowBufferStats("rtrim");
    RowBufferStats trim = new RowBufferStats("trim");

    // Accents
    ai.addStrValue("a");
    ai.addStrValue("à");
    as.addStrValue("a");
    as.addStrValue("à");
    Assert.assertArrayEquals(
        "a".getBytes(StandardCharsets.UTF_8), ai.getCurrentMinColStrValueInBytes());
    Assert.assertArrayEquals(
        "a".getBytes(StandardCharsets.UTF_8), ai.getCurrentMaxColStrValueInBytes());
    Assert.assertArrayEquals(
        "a".getBytes(StandardCharsets.UTF_8), as.getCurrentMinColStrValueInBytes());
    Assert.assertArrayEquals(
        "à".getBytes(StandardCharsets.UTF_8), as.getCurrentMaxColStrValueInBytes());

    // Punctuation
    pi.addStrValue(".b");
    pi.addStrValue("a");
    ps.addStrValue(".b");
    ps.addStrValue("a");

    Assert.assertEquals("a", pi.getCurrentMinColStrValueInBytes());
    Assert.assertEquals(".b", ps.getCurrentMinColStrValueInBytes());

    // First Lower and Upper
    fl.addStrValue("C");
    fl.addStrValue("b");
    fu.addStrValue("b");
    fu.addStrValue("C");
    Assert.assertEquals("b", fl.getCurrentMinColStrValueInBytes());
    Assert.assertEquals("b", fu.getCurrentMinColStrValueInBytes());

    // Lower and Upper
    lower.addStrValue("AA");
    lower.addStrValue("a");
    upper.addStrValue("AA");
    upper.addStrValue("aaa");
    Assert.assertEquals("a", lower.getCurrentMinColStrValueInBytes());
    Assert.assertEquals("AA", upper.getCurrentMinColStrValueInBytes());

    // Trim settings
    trim.addStrValue(" z ");
    trim.addStrValue("b");
    ltrim.addStrValue(" z");
    ltrim.addStrValue("b");
    rtrim.addStrValue("z ");
    rtrim.addStrValue("b");
    Assert.assertEquals("b", trim.getCurrentMinColStrValueInBytes());
    Assert.assertEquals("b", ltrim.getCurrentMinColStrValueInBytes());
    Assert.assertEquals("b", rtrim.getCurrentMinColStrValueInBytes());
  }

  @Test
  public void testEmptyState() throws Exception {
    RowBufferStats stats = new RowBufferStats();

    Assert.assertNull(stats.getCollationDefinitionString());
    Assert.assertNull(stats.getCurrentMinRealValue());
    Assert.assertNull(stats.getCurrentMaxRealValue());
    Assert.assertNull(stats.getCurrentMaxColStrValueInBytes());
    Assert.assertNull(stats.getCurrentMaxColStrValueInBytes());
    Assert.assertNull(stats.getCurrentMinIntValue());
    Assert.assertNull(stats.getCurrentMaxIntValue());

    Assert.assertEquals(0, stats.getCurrentNullCount());
    Assert.assertEquals(-1, stats.getDistinctValues());
  }

  @Test
  public void testMinMaxStrNonCol() throws Exception {
    RowBufferStats stats = new RowBufferStats();

    stats.addStrValue("bob");
    Assert.assertEquals("bob", stats.getCurrentMaxColStrValueInBytes());
    Assert.assertEquals("bob", stats.getCurrentMaxColStrValueInBytes());
    Assert.assertEquals(-1, stats.getDistinctValues());

    stats.addStrValue("charlie");
    Assert.assertEquals("bob", stats.getCurrentMaxColStrValueInBytes());
    Assert.assertEquals("charlie", stats.getCurrentMaxColStrValueInBytes());
    Assert.assertEquals(-1, stats.getDistinctValues());

    stats.addStrValue("alice");
    Assert.assertEquals("alice", stats.getCurrentMaxColStrValueInBytes());
    Assert.assertEquals("charlie", stats.getCurrentMaxColStrValueInBytes());
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
    RowBufferStats stats = new RowBufferStats();
    stats.addStrValue("abcde|abcde|abcde|abcde|abcde|abcde|");
    Assert.assertEquals(
        "abcde|abcde|abcde|abcde|abcde|ab", stats.getCurrentMinColStrValueInBytes());
    Assert.assertEquals(
        "abcde|abcde|abcde|abcde|abcde|ac", stats.getCurrentMaxColStrValueInBytes());

    stats.addStrValue("zabcde|abcde|abcde|abcde|abcde|abcde|");
    Assert.assertEquals(
        "abcde|abcde|abcde|abcde|abcde|ab", stats.getCurrentMinColStrValueInBytes());
    Assert.assertEquals(
        "zabcde|abcde|abcde|abcde|abcde|b", stats.getCurrentMaxColStrValueInBytes());

    RowBufferStats ai = new RowBufferStats("en-ai");
    ai.addStrValue("abcde|abcde|abcde|abcde|abcde|abcde|");
    Assert.assertEquals("abcde|abcde|abcde|abcde|abcde|ab", ai.getCurrentMinColStrValueInBytes());
    Assert.assertEquals("abcde|abcde|abcde|abcde|abcde|ac", ai.getCurrentMaxColStrValueInBytes());

    ai.addStrValue("zabcde|abcde|abcde|abcde|abcde|abcde|");
    Assert.assertEquals("abcde|abcde|abcde|abcde|abcde|ab", ai.getCurrentMinColStrValueInBytes());
    Assert.assertEquals("zabcde|abcde|abcde|abcde|abcde|b", ai.getCurrentMaxColStrValueInBytes());
  }

  @Test
  public void testMinMaxStrCol() throws Exception {
    RowBufferStats stats = new RowBufferStats("en-ci");

    Assert.assertEquals("en-ci", stats.getCollationDefinitionString());

    stats.addStrValue("bob");
    Assert.assertEquals("bob", stats.getCurrentMinColStrValueInBytes());
    Assert.assertEquals("bob", stats.getCurrentMaxColStrValueInBytes());
    Assert.assertEquals(-1, stats.getDistinctValues());

    stats.addStrValue("Bob");
    Assert.assertEquals("bob", stats.getCurrentMinColStrValueInBytes());
    Assert.assertEquals("bob", stats.getCurrentMaxColStrValueInBytes());
    Assert.assertEquals(-1, stats.getDistinctValues());

    stats.addStrValue("Alice");
    Assert.assertEquals("Alice", stats.getCurrentMinColStrValueInBytes());
    Assert.assertEquals("bob", stats.getCurrentMaxColStrValueInBytes());
    Assert.assertEquals(-1, stats.getDistinctValues());

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
    Assert.assertNull(stats.getCurrentMinColStrValueInBytes());
    Assert.assertNull(stats.getCurrentMaxColStrValueInBytes());
    Assert.assertEquals(0, stats.getCurrentNullCount());
  }

  @Test
  public void testMinMaxReal() throws Exception {
    RowBufferStats stats = new RowBufferStats();

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
    Assert.assertNull(stats.getCurrentMinColStrValueInBytes());
    Assert.assertNull(stats.getCurrentMaxColStrValueInBytes());
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

  @Test
  public void testGetCombinedStats() throws Exception {
    // Test for Integers
    RowBufferStats one = new RowBufferStats();
    RowBufferStats two = new RowBufferStats();

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

    Assert.assertNull(result.getCurrentMaxColStrValueInBytes());
    Assert.assertNull(result.getCurrentMaxColStrValueInBytes());
    Assert.assertNull(result.getCurrentMinRealValue());
    Assert.assertNull(result.getCurrentMaxRealValue());

    // Test for Reals
    one = new RowBufferStats();
    two = new RowBufferStats();

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
    Assert.assertNull(result.getCurrentMaxColStrValueInBytes());
    Assert.assertNull(result.getCurrentMaxColStrValueInBytes());
    Assert.assertNull(result.getCurrentMinIntValue());
    Assert.assertNull(result.getCurrentMaxIntValue());

    // Test for Strings without collation
    one = new RowBufferStats();
    two = new RowBufferStats();

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
    Assert.assertArrayEquals(
        "a".getBytes(StandardCharsets.UTF_8), result.getCurrentMinColStrValueInBytes());
    Assert.assertArrayEquals(
        "g".getBytes(StandardCharsets.UTF_8), result.getCurrentMaxColStrValueInBytes());
    Assert.assertEquals(-1, result.getDistinctValues());
    Assert.assertEquals(2, result.getCurrentNullCount());
    Assert.assertEquals(5, result.getCurrentMaxLength());

    Assert.assertNull(result.getCurrentMinRealValue());
    Assert.assertNull(result.getCurrentMaxRealValue());
    Assert.assertNull(result.getCurrentMinIntValue());
    Assert.assertNull(result.getCurrentMaxIntValue());

    // Test for Strings with collation
    one = new RowBufferStats("en-ci");
    two = new RowBufferStats("en-ci");

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
    Assert.assertArrayEquals(
        "a".getBytes(StandardCharsets.UTF_8), result.getCurrentMinColStrValueInBytes());
    Assert.assertArrayEquals(
        "g".getBytes(StandardCharsets.UTF_8), result.getCurrentMaxColStrValueInBytes());
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
    RowBufferStats one = new RowBufferStats();
    RowBufferStats two = new RowBufferStats();

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

    Assert.assertNull(result.getCurrentMinColStrValueInBytes());
    Assert.assertNull(result.getCurrentMaxColStrValueInBytes());
    Assert.assertNull(result.getCurrentMinRealValue());
    Assert.assertNull(result.getCurrentMaxRealValue());

    // Test for Reals
    one = new RowBufferStats();

    one.addRealValue(2d);
    one.addRealValue(4d);
    one.addRealValue(6d);
    one.addRealValue(8d);

    result = RowBufferStats.getCombinedStats(one, two);
    Assert.assertEquals(Double.valueOf(2), result.getCurrentMinRealValue());
    Assert.assertEquals(Double.valueOf(8), result.getCurrentMaxRealValue());
    Assert.assertEquals(-1, result.getDistinctValues());
    Assert.assertEquals(0, result.getCurrentNullCount());

    Assert.assertNull(result.getCurrentMinColStrValueInBytes());
    Assert.assertNull(result.getCurrentMaxColStrValueInBytes());
    Assert.assertNull(result.getCurrentMinIntValue());
    Assert.assertNull(result.getCurrentMaxIntValue());

    // Test for Strings
    one = new RowBufferStats();
    two = new RowBufferStats();

    one.addStrValue("alpha");
    one.addStrValue("d");
    one.addStrValue("f");
    one.addStrValue("g");
    one.incCurrentNullCount();

    result = RowBufferStats.getCombinedStats(one, two);
    Assert.assertArrayEquals(
        "alpha".getBytes(StandardCharsets.UTF_8), result.getCurrentMinColStrValueInBytes());
    Assert.assertArrayEquals(
        "g".getBytes(StandardCharsets.UTF_8), result.getCurrentMaxColStrValueInBytes());
    Assert.assertEquals(-1, result.getDistinctValues());
    Assert.assertEquals(1, result.getCurrentNullCount());

    Assert.assertNull(result.getCurrentMinRealValue());
    Assert.assertNull(result.getCurrentMaxRealValue());
    Assert.assertNull(result.getCurrentMinIntValue());
    Assert.assertNull(result.getCurrentMaxIntValue());
  }
}
