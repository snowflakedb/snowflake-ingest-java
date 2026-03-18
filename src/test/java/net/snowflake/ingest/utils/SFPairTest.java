/*
 * Ported from snowflake-jdbc: net.snowflake.client.util.SFPairTest
 */

package net.snowflake.ingest.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class SFPairTest {

  @Test
  public void testOf() {
    SFPair<String, Integer> pair = SFPair.of("hello", 42);
    assertEquals("hello", pair.left);
    assertEquals(Integer.valueOf(42), pair.right);
  }

  @Test
  public void testEquals() {
    SFPair<String, Integer> a = SFPair.of("a", 1);
    SFPair<String, Integer> b = SFPair.of("a", 1);
    SFPair<String, Integer> c = SFPair.of("a", 2);

    assertTrue(a.equals(b));
    assertFalse(a.equals(c));
    assertFalse(a.equals(null));
    assertTrue(a.equals(a));
    assertFalse(a.equals("not a pair"));
  }

  @Test
  public void testHashCode() {
    SFPair<String, Integer> a = SFPair.of("a", 1);
    SFPair<String, Integer> b = SFPair.of("a", 1);
    assertEquals(a.hashCode(), b.hashCode());
  }

  @Test
  public void testToString() {
    SFPair<String, Integer> pair = SFPair.of("x", 5);
    assertEquals("[ x, 5 ]", pair.toString());
  }

  @Test
  public void testNullValues() {
    SFPair<String, String> pair = SFPair.of(null, null);
    assertEquals(0, pair.hashCode());
    assertNotEquals(null, pair);
  }
}
