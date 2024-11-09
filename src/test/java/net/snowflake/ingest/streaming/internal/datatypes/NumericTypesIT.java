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
public class NumericTypesIT extends AbstractDataTypeTest {
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
  public void testIntegers() throws Exception {
    // test bytes
    testJdbcTypeCompatibility("INT", (short) 0, new ShortProvider());
    testJdbcTypeCompatibility("INT", Short.MAX_VALUE, new ShortProvider());
    testJdbcTypeCompatibility("INT", Short.MIN_VALUE, new ShortProvider());

    // test shorts
    testJdbcTypeCompatibility("INT", (byte) 0, new ByteProvider());
    testJdbcTypeCompatibility("INT", Byte.MAX_VALUE, new ByteProvider());
    testJdbcTypeCompatibility("INT", Byte.MIN_VALUE, new ByteProvider());

    // test ints
    testJdbcTypeCompatibility("INT", 0, new IntProvider());
    testJdbcTypeCompatibility("INT", Integer.MAX_VALUE, new IntProvider());
    testJdbcTypeCompatibility("INT", Integer.MIN_VALUE, new IntProvider());

    // test longs
    testJdbcTypeCompatibility("INT", 0L, new LongProvider());
    testJdbcTypeCompatibility("INT", Long.MAX_VALUE, new LongProvider());
    testJdbcTypeCompatibility("INT", Long.MIN_VALUE, new LongProvider());

    // test floats
    testJdbcTypeCompatibility("INT", 51.000f, 51, new FloatProvider(), new IntProvider());
    testJdbcTypeCompatibility("INT", -51.000f, -51, new FloatProvider(), new IntProvider());
    testJdbcTypeCompatibility("INT", 1.3f, 1, new FloatProvider(), new IntProvider());
    testJdbcTypeCompatibility("INT", -1.3f, -1, new FloatProvider(), new IntProvider());

    // test doubles
    testJdbcTypeCompatibility("INT", 11.000, 11, new DoubleProvider(), new IntProvider());
    testJdbcTypeCompatibility("INT", -11.000, -11, new DoubleProvider(), new IntProvider());
    testJdbcTypeCompatibility("INT", 1.3, 1, new DoubleProvider(), new IntProvider());
    testJdbcTypeCompatibility("INT", -1.3, -1, new DoubleProvider(), new IntProvider());

    // test BigIntegers (JDBC does not support big integer, so we are reading back big decimals)
    testIngestion(
        "INT", MAX_ALLOWED_BIG_INTEGER, MAX_ALLOWED_BIG_DECIMAL, new BigDecimalProvider());
    testIngestion(
        "INT", MIN_ALLOWED_BIG_INTEGER, MIN_ALLOWED_BIG_DECIMAL, new BigDecimalProvider());
    testIngestion("INT", BigInteger.ZERO, BigDecimal.ZERO, new BigDecimalProvider());
    expectNumberOutOfRangeError("INT", MAX_ALLOWED_BIG_INTEGER.add(BigInteger.ONE), 38);
    expectNumberOutOfRangeError("INT", MIN_ALLOWED_BIG_INTEGER.subtract(BigInteger.ONE), 38);

    // test BigDecimals
    testJdbcTypeCompatibility("INT", BigDecimal.ZERO, new BigDecimalProvider());
    testJdbcTypeCompatibility("INT", MAX_ALLOWED_BIG_DECIMAL, new BigDecimalProvider());
    testJdbcTypeCompatibility("INT", MIN_ALLOWED_BIG_DECIMAL, new BigDecimalProvider());
    testJdbcTypeCompatibility("INT", new BigDecimal("51.000"), new BigDecimalProvider());
    testJdbcTypeCompatibility("INT", new BigDecimal("-51.000"), new BigDecimalProvider());
    expectNumberOutOfRangeError("INT", MAX_ALLOWED_BIG_DECIMAL.add(BigDecimal.ONE), 38);
    expectNumberOutOfRangeError("INT", MIN_ALLOWED_BIG_DECIMAL.subtract(BigDecimal.ONE), 38);
    testJdbcTypeCompatibility(
        "INT",
        new BigDecimal("9999999999999999999999999999999999.1234"),
        new BigDecimal("9999999999999999999999999999999999"),
        new BigDecimalProvider(),
        new BigDecimalProvider());
    testJdbcTypeCompatibility(
        "INT",
        new BigDecimal("-9999999999999999999999999999999999.1234"),
        new BigDecimal("-9999999999999999999999999999999999"),
        new BigDecimalProvider(),
        new BigDecimalProvider());
    testJdbcTypeCompatibility(
        "INT",
        new BigDecimal("0.4"),
        new BigDecimal("0"),
        new BigDecimalProvider(),
        new BigDecimalProvider());
    testJdbcTypeCompatibility(
        "INT",
        new BigDecimal("0.6"),
        new BigDecimal("1"),
        new BigDecimalProvider(),
        new BigDecimalProvider());
    testJdbcTypeCompatibility(
        "INT",
        new BigDecimal("-0.4"),
        new BigDecimal("0"),
        new BigDecimalProvider(),
        new BigDecimalProvider());
    testJdbcTypeCompatibility(
        "INT",
        new BigDecimal("-0.6"),
        new BigDecimal("-1"),
        new BigDecimalProvider(),
        new BigDecimalProvider());

    // test Strings (Pass strings, read back as big decimal)
    testJdbcTypeCompatibility(
        "INT", "1", BigDecimal.ONE, new StringProvider(), new BigDecimalProvider());
    testJdbcTypeCompatibility(
        "INT", "00001", BigDecimal.ONE, new StringProvider(), new BigDecimalProvider());
    testJdbcTypeCompatibility(
        "INT", "8e0", new BigDecimal(8), new StringProvider(), new BigDecimalProvider());
    testJdbcTypeCompatibility(
        "INT", "-10e4", new BigDecimal("-100000"), new StringProvider(), new BigDecimalProvider());
    testJdbcTypeCompatibility(
        "INT", "11.5", new BigDecimal("12"), new StringProvider(), new BigDecimalProvider());
    testJdbcTypeCompatibility(
        "INT", "12.4", new BigDecimal("12"), new StringProvider(), new BigDecimalProvider());
    testJdbcTypeCompatibility(
        "INT", "1.2e1", new BigDecimal("12"), new StringProvider(), new BigDecimalProvider());
    testJdbcTypeCompatibility(
        "INT", "1.25e1", new BigDecimal("13"), new StringProvider(), new BigDecimalProvider());
    testJdbcTypeCompatibility(
        "INT", "4.000", new BigDecimal("4"), new StringProvider(), new BigDecimalProvider());
    testIngestion(
        "INT",
        MAX_ALLOWED_BIG_INTEGER.toString(),
        MAX_ALLOWED_BIG_DECIMAL,
        new BigDecimalProvider());
    testIngestion(
        "INT",
        MIN_ALLOWED_BIG_INTEGER.toString(),
        MIN_ALLOWED_BIG_DECIMAL,
        new BigDecimalProvider());
  }

  @Test
  public void testNumbersWithLimitedPrecision() throws Exception {
    testJdbcTypeCompatibility("NUMBER(1, 0)", (byte) 0, new ByteProvider());
    testJdbcTypeCompatibility("NUMBER(1, 0)", (byte) 9, new ByteProvider());
    testJdbcTypeCompatibility("NUMBER(1, 0)", (byte) -9, new ByteProvider());
    expectNumberOutOfRangeError("NUMBER(1, 0)", 10, 1);
    expectNumberOutOfRangeError("NUMBER(1, 0)", -10, 1);
    expectNumberOutOfRangeError("NUMBER(1, 0)", -100, 1);
    expectNumberOutOfRangeError("NUMBER(1, 0)", -1000, 1);
  }

  @Test
  public void testNumbersWithScale() throws Exception {
    testJdbcTypeCompatibility("NUMBER(1, 1)", 0, new IntProvider());
    testJdbcTypeCompatibility("NUMBER(1, 1)", 0.00, new DoubleProvider());
    testJdbcTypeCompatibility("NUMBER(1, 1)", 0.2, new DoubleProvider());
    testJdbcTypeCompatibility("NUMBER(1, 1)", -0.2, new DoubleProvider());
    testJdbcTypeCompatibility(
        "NUMBER(1, 1)", 0.24, 0.2, new DoubleProvider(), new DoubleProvider());
    testJdbcTypeCompatibility(
        "NUMBER(1, 1)", -0.24, -0.2, new DoubleProvider(), new DoubleProvider());
    testJdbcTypeCompatibility(
        "NUMBER(1, 1)", 0.25, 0.3, new DoubleProvider(), new DoubleProvider());
    testJdbcTypeCompatibility(
        "NUMBER(1, 1)", -0.25, -0.3, new DoubleProvider(), new DoubleProvider());
    expectNumberOutOfRangeError("NUMBER(1, 1)", 1, 0);
    expectNumberOutOfRangeError("NUMBER(1, 1)", 10, 0);
    expectNumberOutOfRangeError("NUMBER(1, 1)", 1.25, 0);

    testJdbcTypeCompatibility("NUMBER(3, 1)", 0, new IntProvider());
    testJdbcTypeCompatibility("NUMBER(3, 1)", 99, new IntProvider());
    testJdbcTypeCompatibility("NUMBER(3, 1)", -99, new IntProvider());
    testJdbcTypeCompatibility(
        "NUMBER(3, 1)", 99.94, 99.9, new DoubleProvider(), new DoubleProvider());
    testJdbcTypeCompatibility(
        "NUMBER(3, 1)", -99.94, -99.9, new DoubleProvider(), new DoubleProvider());
    expectNumberOutOfRangeError("NUMBER(3, 1)", 99.95, 2);
    expectNumberOutOfRangeError("NUMBER(3, 1)", -99.95, 2);
    expectNumberOutOfRangeError("NUMBER(3, 1)", 100, 2);
    expectNumberOutOfRangeError("NUMBER(3, 1)", -100, 2);

    testJdbcTypeCompatibility(
        "NUMBER(38, 4)",
        "9999999999999999999999999999999000",
        new BigDecimal("9999999999999999999999999999999000"),
        new StringProvider(),
        new BigDecimalProvider());
    testJdbcTypeCompatibility(
        "NUMBER(38, 4)",
        "9999999999999999999999999999999999",
        new BigDecimal("9999999999999999999999999999999999"),
        new StringProvider(),
        new BigDecimalProvider());
    testJdbcTypeCompatibility(
        "NUMBER(38, 4)",
        "9999999999999999999999999999999999.000",
        new BigDecimal("9999999999999999999999999999999999"),
        new StringProvider(),
        new BigDecimalProvider());
    testJdbcTypeCompatibility(
        "NUMBER(38, 4)",
        "9999999999999999999999999999999999.9000",
        new BigDecimal("9999999999999999999999999999999999.9"),
        new StringProvider(),
        new BigDecimalProvider());
    testJdbcTypeCompatibility(
        "NUMBER(38, 4)",
        "9999999999999999999999999999999999.9999",
        new BigDecimal("9999999999999999999999999999999999.9999"),
        new StringProvider(),
        new BigDecimalProvider());
    testJdbcTypeCompatibility(
        "NUMBER(38, 37)",
        "0.0000000000000000000000000000000000000012",
        new BigDecimal("0.0000000000000000000000000000000000000"),
        new StringProvider(),
        new BigDecimalProvider());
    testJdbcTypeCompatibility(
        "NUMBER(38, 37)",
        "9.9999999999999999999999999999999999999",
        new BigDecimal("9.9999999999999999999999999999999999999"),
        new StringProvider(),
        new BigDecimalProvider());
    expectNumberOutOfRangeError(
        "NUMBER(38, 37)", new BigDecimal("9.99999999999999999999999999999999999999"), 1);

    testJdbcTypeCompatibility(
        "NUMBER(3, 1)",
        "12.5",
        new BigDecimal("12.5"),
        new StringProvider(),
        new BigDecimalProvider());
    testJdbcTypeCompatibility(
        "NUMBER(3, 1)",
        "1.25e1",
        new BigDecimal("12.5"),
        new StringProvider(),
        new BigDecimalProvider());
    testJdbcTypeCompatibility(
        "NUMBER(2, 0)",
        "12.5",
        new BigDecimal("13"),
        new StringProvider(),
        new BigDecimalProvider());
    testJdbcTypeCompatibility(
        "NUMBER(2, 0)",
        "1.25e1",
        new BigDecimal("13"),
        new StringProvider(),
        new BigDecimalProvider());
  }

  @Test
  public void testFloatingPointTypes() throws Exception {
    // Test double
    testJdbcTypeCompatibility("REAL", 1.35, 1.35, new DoubleProvider(), new DoubleProvider());
    testJdbcTypeCompatibility(
        "REAL", Double.NaN, Double.NaN, new DoubleProvider(), new DoubleProvider());
    testJdbcTypeCompatibility(
        "REAL",
        Double.NEGATIVE_INFINITY,
        Double.NEGATIVE_INFINITY,
        new DoubleProvider(),
        new DoubleProvider());
    testJdbcTypeCompatibility(
        "REAL",
        Double.POSITIVE_INFINITY,
        Double.POSITIVE_INFINITY,
        new DoubleProvider(),
        new DoubleProvider());

    // Test float
    testJdbcTypeCompatibility("REAL", 1.35f, 1.35, new FloatProvider(), new DoubleProvider());
    testJdbcTypeCompatibility(
        "REAL",
        Float.NEGATIVE_INFINITY,
        Double.NEGATIVE_INFINITY,
        new FloatProvider(),
        new DoubleProvider());
    testJdbcTypeCompatibility(
        "REAL",
        Float.POSITIVE_INFINITY,
        Double.POSITIVE_INFINITY,
        new FloatProvider(),
        new DoubleProvider());
    testJdbcTypeCompatibility(
        "REAL", Float.NaN, Double.NaN, new FloatProvider(), new DoubleProvider());

    // Test others
    testJdbcTypeCompatibility("REAL", 1, 1., new IntProvider(), new DoubleProvider());
    testJdbcTypeCompatibility("REAL", 1L, 1., new LongProvider(), new DoubleProvider());
    testJdbcTypeCompatibility("REAL", "1.35", 1.35, new StringProvider(), new DoubleProvider());

    testJdbcTypeCompatibility(
        "REAL", "Nan", Double.NaN, new StringProvider(), new DoubleProvider());
    testJdbcTypeCompatibility(
        "REAL", "Inf", Double.POSITIVE_INFINITY, new StringProvider(), new DoubleProvider());
    testJdbcTypeCompatibility(
        "REAL", "-Inf", Double.NEGATIVE_INFINITY, new StringProvider(), new DoubleProvider());

    testIngestion("REAL", new BigDecimal("1.35"), new BigDecimal("1.35"), new BigDecimalProvider());
    testIngestion("REAL", BigInteger.ONE, BigDecimal.ONE, new BigDecimalProvider());
  }
}
