package net.snowflake.ingest.streaming.internal.datatypes;

import org.junit.Ignore;
import org.junit.Test;

public class BinaryIT extends AbstractDataTypeTest {

  @Test
  public void testBinarySimple() throws Exception {
    testJdbcTypeCompatibility("BINARY", new byte[0], new ByteArrayProvider());
    testJdbcTypeCompatibility("BINARY", new byte[3], new ByteArrayProvider());

    testJdbcTypeCompatibility("BINARY", new byte[8 * 1024 * 1024], new ByteArrayProvider());

    testJdbcTypeCompatibility("BINARY", new byte[] {1, 2, 3, 4}, new ByteArrayProvider());
    testJdbcTypeCompatibility(
        "BINARY", "212D", new byte[] {33, 45}, new StringProvider(), new ByteArrayProvider());
    testJdbcTypeCompatibility(
        "BINARY",
        "0F00030A057F",
        new byte[] {15, 0, 3, 10, 5, 127},
        new StringProvider(),
        new ByteArrayProvider());
  }

  @Test
  @Ignore("SNOW-663704")
  public void testBinary() throws Exception {
    testJdbcTypeCompatibility("BINARY", new byte[] {-1}, new ByteArrayProvider());
  }
}
