/*
 * Ported from snowflake-jdbc: net.snowflake.client.jdbc.internal.snowflake.common.util.Power10Test
 */

package net.snowflake.ingest.utils;

import static org.junit.Assert.assertEquals;

import java.math.BigInteger;
import org.junit.Test;

public class Power10Test {

  @Test
  public void testIntTable() {
    assertEquals(10, Power10.intSize);
    assertEquals(10, Power10.intTable.length);
    int expected = 1;
    for (int i = 0; i < Power10.intSize; i++) {
      assertEquals(expected, Power10.intTable[i]);
      expected *= 10;
    }
  }

  @Test
  public void testLongTable() {
    assertEquals(19, Power10.longSize);
    assertEquals(19, Power10.longTable.length);
    long expected = 1L;
    for (int i = 0; i < Power10.longSize; i++) {
      assertEquals(expected, Power10.longTable[i]);
      expected *= 10L;
    }
  }

  @Test
  public void testSb16Table() {
    assertEquals(39, Power10.sb16Size);
    assertEquals(39, Power10.sb16Table.length);
    BigInteger expected = BigInteger.ONE;
    for (int i = 0; i < Power10.sb16Size; i++) {
      assertEquals(expected, Power10.sb16Table[i]);
      expected = expected.multiply(BigInteger.TEN);
    }
  }
}
