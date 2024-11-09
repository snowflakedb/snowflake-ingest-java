package net.snowflake.ingest.streaming.internal.datatypes;

import org.bouncycastle.util.encoders.Hex;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class BinaryIT extends AbstractDataTypeTest {
  @Parameters(name = "{index}: {0}")
  public static Object[] parameters() {
    return new Object[] {"GZIP", "ZSTD"};
  }

  @Parameter public String compressionAlgorithm;

  @Before
  public void before() throws Exception {
    super.setUp(false, compressionAlgorithm, null);
  }

  @Test
  public void testBinary() throws Exception {
    testJdbcTypeCompatibility("BINARY", new byte[0], new ByteArrayProvider());
    testJdbcTypeCompatibility("BINARY", new byte[3], new ByteArrayProvider());
    testJdbcTypeCompatibility(
        "BINARY", new byte[] {Byte.MIN_VALUE, 0, Byte.MAX_VALUE}, new ByteArrayProvider());
    testJdbcTypeCompatibility("BINARY", Hex.decode("a0b0c0d0e0f0"), new ByteArrayProvider());
    testJdbcTypeCompatibility("BINARY", Hex.decode("aaff"), new ByteArrayProvider());
    testJdbcTypeCompatibility(
        "BINARY",
        Hex.decode("0000000000000000000000000000000000000000000000000000000000000000"),
        new ByteArrayProvider());
    testJdbcTypeCompatibility(
        "BINARY",
        Hex.decode("000000000000000000000000000000000000000000000000000000000000000000"),
        new ByteArrayProvider());
    testJdbcTypeCompatibility(
        "BINARY",
        Hex.decode("aaffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"),
        new ByteArrayProvider());
    testJdbcTypeCompatibility(
        "BINARY",
        Hex.decode("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"),
        new ByteArrayProvider());
    testJdbcTypeCompatibility(
        "BINARY",
        Hex.decode("aaffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"),
        new ByteArrayProvider());
    testJdbcTypeCompatibility(
        "BINARY",
        Hex.decode("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"),
        new ByteArrayProvider());

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
  public void testBinaryComparison() throws Exception {
    ingestManyAndMigrate(
        "BINARY", new byte[] {Byte.MIN_VALUE}, new byte[] {Byte.MAX_VALUE}, new byte[] {0});
    ingestManyAndMigrate(
        "BINARY", new byte[] {Byte.MAX_VALUE}, new byte[] {Byte.MIN_VALUE}, new byte[] {0});
    ingestManyAndMigrate(
        "BINARY",
        Hex.decode("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"),
        Hex.decode("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff00"));
    ingestManyAndMigrate(
        "BINARY",
        Hex.decode("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"),
        Hex.decode("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff00"));
  }

  @Test
  public void testMaxBinary() throws Exception {
    byte[] arr = new byte[8 * 1024 * 1024];
    testJdbcTypeCompatibility("BINARY", arr, new ByteArrayProvider());
  }
}
