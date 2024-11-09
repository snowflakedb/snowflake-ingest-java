package net.snowflake.ingest.streaming.internal.datatypes;

import java.math.BigDecimal;
import java.math.BigInteger;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class LogicalTypesIT extends AbstractDataTypeTest {
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
  public void testLogicalTypes() throws Exception {
    // Test booleans
    testJdbcTypeCompatibility("BOOLEAN", true, new BooleanProvider());
    testJdbcTypeCompatibility("BOOLEAN", false, new BooleanProvider());

    // Test strings
    testJdbcTypeCompatibility("BOOLEAN", "on", true, new StringProvider(), new BooleanProvider());
    testJdbcTypeCompatibility("BOOLEAN", "off", false, new StringProvider(), new BooleanProvider());

    // Test integers
    testJdbcTypeCompatibility(
        "BOOLEAN", Integer.MIN_VALUE, true, new IntProvider(), new BooleanProvider());
    testJdbcTypeCompatibility(
        "BOOLEAN", Integer.MAX_VALUE, true, new IntProvider(), new BooleanProvider());
    testJdbcTypeCompatibility("BOOLEAN", 0, false, new IntProvider(), new BooleanProvider());

    // Test longs
    testJdbcTypeCompatibility(
        "BOOLEAN", Long.MIN_VALUE, true, new LongProvider(), new BooleanProvider());
    testJdbcTypeCompatibility(
        "BOOLEAN", Long.MAX_VALUE, true, new LongProvider(), new BooleanProvider());
    testJdbcTypeCompatibility("BOOLEAN", 0, false, new IntProvider(), new BooleanProvider());

    // Test bytes
    testJdbcTypeCompatibility(
        "BOOLEAN", Byte.MIN_VALUE, true, new ByteProvider(), new BooleanProvider());
    testJdbcTypeCompatibility(
        "BOOLEAN", Byte.MAX_VALUE, true, new ByteProvider(), new BooleanProvider());
    testJdbcTypeCompatibility("BOOLEAN", 0, false, new IntProvider(), new BooleanProvider());

    // Test shorts
    testJdbcTypeCompatibility(
        "BOOLEAN", Short.MIN_VALUE, true, new ShortProvider(), new BooleanProvider());
    testJdbcTypeCompatibility(
        "BOOLEAN", Short.MAX_VALUE, true, new ShortProvider(), new BooleanProvider());
    testJdbcTypeCompatibility("BOOLEAN", 0, false, new IntProvider(), new BooleanProvider());

    // Test BigIntegers
    testIngestion("BOOLEAN", MAX_ALLOWED_BIG_INTEGER, true, new BooleanProvider());
    testIngestion("BOOLEAN", MIN_ALLOWED_BIG_INTEGER, true, new BooleanProvider());
    testIngestion("BOOLEAN", BigInteger.ZERO, false, new BooleanProvider());

    // Test BigDecimals
    testJdbcTypeCompatibility(
        "BOOLEAN", MAX_ALLOWED_BIG_DECIMAL, true, new BigDecimalProvider(), new BooleanProvider());
    testJdbcTypeCompatibility(
        "BOOLEAN", MIN_ALLOWED_BIG_DECIMAL, true, new BigDecimalProvider(), new BooleanProvider());
    testJdbcTypeCompatibility(
        "BOOLEAN", new BigDecimal("0.5"), true, new BigDecimalProvider(), new BooleanProvider());
    testJdbcTypeCompatibility(
        "BOOLEAN", BigDecimal.ZERO, false, new BigDecimalProvider(), new BooleanProvider());

    // Test floats
    testJdbcTypeCompatibility("BOOLEAN", 0.5f, true, new FloatProvider(), new BooleanProvider());
    testJdbcTypeCompatibility("BOOLEAN", -0.5f, true, new FloatProvider(), new BooleanProvider());
    testJdbcTypeCompatibility("BOOLEAN", 0f, false, new FloatProvider(), new BooleanProvider());
    testJdbcTypeCompatibility("BOOLEAN", 0.000f, false, new FloatProvider(), new BooleanProvider());

    // Test double
    testJdbcTypeCompatibility("BOOLEAN", 0.5, true, new DoubleProvider(), new BooleanProvider());
    testJdbcTypeCompatibility("BOOLEAN", -0.5, true, new DoubleProvider(), new BooleanProvider());
    testJdbcTypeCompatibility("BOOLEAN", 0d, false, new DoubleProvider(), new BooleanProvider());
    testJdbcTypeCompatibility("BOOLEAN", 0.000, false, new DoubleProvider(), new BooleanProvider());
  }
}
