/*
 * Replicated from snowflake-jdbc-thin (v3.25.1):
 *   net.snowflake.client.jdbc.internal.snowflake.common.util.Power10
 * Originally from net.snowflake:snowflake-common
 */

package net.snowflake.ingest.utils;

import java.math.BigInteger;

/** Powers-of-10 lookup tables for timestamp and decimal handling. */
public class Power10 {
  public static final int intSize = 10;
  public static final int[] intTable;
  public static final int longSize = 19;
  public static final long[] longTable;
  public static final int sb16Size = 39;
  public static final BigInteger[] sb16Table;

  static {
    int n;
    intTable = new int[10];
    intTable[0] = n = 1;
    for (int i = 1; i < 10; ++i) {
      intTable[i] = n *= 10;
    }
    long l;
    longTable = new long[19];
    longTable[0] = l = 1L;
    for (int i = 1; i < 19; ++i) {
      longTable[i] = l *= 10L;
    }
    BigInteger b;
    sb16Table = new BigInteger[39];
    sb16Table[0] = b = BigInteger.ONE;
    for (int i = 1; i < 39; ++i) {
      sb16Table[i] = b = b.multiply(BigInteger.TEN);
    }
  }
}
