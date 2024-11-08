package net.snowflake.ingest.streaming.internal.datatypes;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class SemiStructuredIT extends AbstractDataTypeTest {
  @Parameters(name = "{index}: {0}")
  public static Object[] parameters() {
    return new Object[] {"GZIP", "ZSTD"};
  }

  @Parameter public String compressionAlgorithm;

  @Before
  public void before() throws Exception {
    super.setUp(false, compressionAlgorithm, null);
  }

  // TODO SNOW-664249: There is a few-byte mismatch between the value sent by the user and its
  // server-side representation. Validation leaves a small buffer for this difference.
  private static final int MAX_ALLOWED_LENGTH = 16 * 1024 * 1024 - 64;

  @Test
  public void testVariant() throws Exception {
    // Test dates
    assertVariant("VARIANT", LocalTime.of(0, 0, 0, 123), "\"00:00:00.000000123\"", "VARCHAR");
    assertVariant(
        "VARIANT",
        OffsetTime.of(0, 0, 0, 123, ZoneOffset.ofHours(-1)),
        "\"00:00:00.000000123-01:00\"",
        "VARCHAR");
    assertVariant("VARIANT", LocalDate.of(1970, 1, 1), "\"1970-01-01\"", "VARCHAR");
    assertVariant(
        "VARIANT", LocalDateTime.of(1970, 1, 1, 0, 0, 0), "\"1970-01-01T00:00\"", "VARCHAR");
    assertVariant(
        "VARIANT",
        Instant.EPOCH.atOffset(ZoneOffset.ofHours(-1)),
        "\"1969-12-31T23:00-01:00\"",
        "VARCHAR");
    assertVariant(
        "VARIANT",
        Instant.EPOCH.atZone(ZoneId.of("America/Los_Angeles")),
        "\"1969-12-31T16:00-08:00\"",
        "VARCHAR");

    // Test arrays
    assertVariant("VARIANT", "[]", "[]", "ARRAY");
    assertVariant("VARIANT", "[1]", "[1]", "ARRAY");
    assertVariant(
        "VARIANT", new byte[] {Byte.MIN_VALUE, 0, Byte.MAX_VALUE}, "[-128,0,127]", "ARRAY");
    assertVariant(
        "VARIANT", Arrays.asList(Byte.MIN_VALUE, 0, Byte.MAX_VALUE), "[-128,0,127]", "ARRAY");
    assertVariant(
        "VARIANT", new short[] {Short.MIN_VALUE, 0, Short.MAX_VALUE}, "[-32768,0,32767]", "ARRAY");
    assertVariant(
        "VARIANT", Arrays.asList(Short.MIN_VALUE, 0, Short.MAX_VALUE), "[-32768,0,32767]", "ARRAY");
    assertVariant(
        "VARIANT",
        new int[] {Integer.MIN_VALUE, 0, Integer.MAX_VALUE},
        "[-2147483648,0,2147483647]",
        "ARRAY");
    assertVariant(
        "VARIANT",
        Arrays.asList(Integer.MIN_VALUE, 0, Integer.MAX_VALUE),
        "[-2147483648,0,2147483647]",
        "ARRAY");
    assertVariant(
        "VARIANT",
        new long[] {Long.MIN_VALUE, 0L, Long.MAX_VALUE},
        "[-9223372036854775808,0,9223372036854775807]",
        "ARRAY");
    assertVariant(
        "VARIANT",
        Arrays.asList(Long.MIN_VALUE, 0L, Long.MAX_VALUE),
        "[-9223372036854775808,0,9223372036854775807]",
        "ARRAY");
    assertVariant(
        "VARIANT",
        new Object[] {
          Byte.MIN_VALUE,
          Short.MAX_VALUE,
          0,
          Long.MAX_VALUE,
          new Object[2],
          Collections.emptyMap(),
          null
        },
        "[-128,32767,0,9223372036854775807,[null,null],{}, null]",
        "ARRAY");
    assertVariant(
        "VARIANT",
        Arrays.asList(
            Byte.MIN_VALUE,
            Short.MAX_VALUE,
            0,
            Long.MAX_VALUE,
            new Object[2],
            Collections.emptyMap(),
            null),
        "[-128,32767,0,9223372036854775807,[null,null],{}, null]",
        "ARRAY");
    assertVariant("VARIANT", new BigInteger[] {BigInteger.TEN}, "[10]", "ARRAY");
    assertVariant("VARIANT", Collections.singletonList(BigInteger.TEN), "[10]", "ARRAY");
    assertVariant("VARIANT", new BigDecimal[] {new BigDecimal("10.5")}, "[10.5]", "ARRAY");
    assertVariant("VARIANT", Collections.singletonList(new BigDecimal("10.5")), "[10.5]", "ARRAY");

    // Test booleans
    assertVariant("VARIANT", "false", "false", "BOOLEAN");
    assertVariant("VARIANT", false, "false", "BOOLEAN");
    assertVariant("VARIANT", true, "true", "BOOLEAN");

    // Test numbers
    assertVariant("VARIANT", 34, "34", "INTEGER");
    assertVariant("VARIANT", "34", "34", "INTEGER");

    assertVariant("VARIANT", Byte.MAX_VALUE, String.valueOf(Byte.MAX_VALUE), "INTEGER");
    assertVariant("VARIANT", Short.MAX_VALUE, String.valueOf(Short.MAX_VALUE), "INTEGER");
    assertVariant("VARIANT", Integer.MAX_VALUE, String.valueOf(Integer.MAX_VALUE), "INTEGER");
    assertVariant("VARIANT", Long.MAX_VALUE, String.valueOf(Long.MAX_VALUE), "INTEGER");
    assertVariant("VARIANT", Long.MIN_VALUE, String.valueOf(Long.MIN_VALUE), "INTEGER");
    assertVariant("VARIANT", BigInteger.TEN, BigInteger.TEN.toString(), "INTEGER");
    assertVariant(
        "VARIANT", new BigDecimal("10.54"), new BigDecimal("10.54").toString(), "DECIMAL");
    assertVariant("VARIANT", "-1000.25", "-1000.25", "DECIMAL");
    assertVariant("VARIANT", -1000.25f, "-1000.25", "DECIMAL");
    assertVariant("VARIANT", -1000.25, "-1000.25", "DECIMAL");

    // Test objects
    assertVariant("VARIANT", "{}", "{}", "OBJECT");
    String testObject =
        "{\"a\": \"foo\", \"b\":15, \"c\": false, \"d\": [1, 2, 3], \"e\": {\"q\": false}, \"f\":"
            + " null}";
    assertVariant("VARIANT", testObject, testObject, "OBJECT");
    assertVariant(
        "VARIANT",
        Collections.singletonMap(
            "A",
            Collections.singletonMap("B", Collections.singletonMap("C", new String[] {"foo"}))),
        "{\"A\": {\"B\": {\"C\": [\"foo\"]}}}",
        "OBJECT");

    // Test strings
    assertVariant("VARIANT", "\"foo\"", "\"foo\"", "VARCHAR");
    assertVariant("VARIANT", '1', "\"1\"", "VARCHAR");
    assertVariant("VARIANT", 'd', "\"d\"", "VARCHAR");
    assertVariant(
        "VARIANT", "\"ž, š, č, ř, c, j, ď, ť, ň\"", "\"ž, š, č, ř, c, j, ď, ť, ň\"", "VARCHAR");

    // Date/time strings are ingested as varchars
    assertVariant("VARIANT", "\"13:05:33.299094\"", "\"13:05:33.299094\"", "VARCHAR");
    assertVariant("VARIANT", "\"13:05:55.684003Z\"", "\"13:05:55.684003Z\"", "VARCHAR");
    assertVariant("VARIANT", "\"2022-09-14\"", "\"2022-09-14\"", "VARCHAR");
    assertVariant(
        "VARIANT", "\"2022-09-14T13:04:53.578667\"", "\"2022-09-14T13:04:53.578667\"", "VARCHAR");
    assertVariant(
        "VARIANT",
        "\"2022-09-14T06:16:09.797704-07:00[America/Los_Angeles]\"",
        "\"2022-09-14T06:16:09.797704-07:00[America/Los_Angeles]\"",
        "VARCHAR");

    // Test JSON null
    assertVariant("VARIANT", "null", "null", "NULL_VALUE");

    // Test SQL null, if the value is SQL NULL, the value returned is null
    assertVariant("VARIANT", "", null, null);
    assertVariant("VARIANT", " ", null, null);
    assertVariant("VARIANT", null, null, null);
  }

  /**
   * For JDBC version > 3.13.3 we dont verify the value returned from max variant, max array and max
   * object because of
   * https://github.com/snowflakedb/snowflake-sdks-drivers-issues-teamwork/issues/819
   *
   * @throws Exception
   */
  @Test
  public void testMaxVariantAndObject() throws Exception {
    String maxObject = createLargeVariantObject(MAX_ALLOWED_LENGTH);
    assertVariant("VARIANT", maxObject, maxObject, "OBJECT", true);
    assertVariant("OBJECT", maxObject, maxObject, "OBJECT", true);
  }

  /**
   * For JDBC version > 3.13.3 we dont verify the value returned from max variant, max array and max
   * object because of
   * https://github.com/snowflakedb/snowflake-sdks-drivers-issues-teamwork/issues/819
   *
   * @throws Exception
   */
  @Test
  public void testMaxArray() throws Exception {
    String maxArray = "[" + createLargeVariantObject(MAX_ALLOWED_LENGTH - 2) + "]";
    assertVariant("ARRAY", maxArray, maxArray, "ARRAY", true);
  }

  @Test
  public void testObject() throws Exception {
    assertVariant("OBJECT", "{}", "{}", "OBJECT");
    assertVariant("OBJECT", Collections.emptyMap(), "{}", "OBJECT");
    assertVariant("OBJECT", Collections.singletonMap("1", 2), "{\"1\": 2}", "OBJECT");
    assertVariant(
        "OBJECT", Collections.singletonMap("1", new byte[] {1, 2}), "{\"1\": [1,2]}", "OBJECT");
  }

  @Test
  public void testArray() throws Exception {
    assertVariant("ARRAY", "[]", "[]", "ARRAY");
    assertVariant("ARRAY", Collections.emptyList(), "[]", "ARRAY");
    assertVariant("ARRAY", new Object[0], "[]", "ARRAY");
    assertVariant("ARRAY", new int[0], "[]", "ARRAY");
    assertVariant("ARRAY", new double[0], "[]", "ARRAY");

    assertVariant("ARRAY", 1, "[1]", "ARRAY");
    assertVariant("ARRAY", "null", "[null]", "ARRAY");
    assertVariant("ARRAY", "{}", "[{}]", "ARRAY");
    assertVariant("ARRAY", Collections.emptyMap(), "[{}]", "ARRAY");
    assertVariant("ARRAY", Collections.singletonMap("1", "2"), "[{\"1\": \"2\"}]", "ARRAY");
  }

  @Test
  public void testNumberScientificNotation() throws Exception {
    assertVariantLiterally("VARIANT", " 12.34\t\n", "12.34", "DECIMAL");

    assertVariantLiterally("VARIANT", " 1.234e1\t\n", "1.234000000000000e+01", "DOUBLE");
    assertVariantLiterally("VARIANT", " 1.234E1\t\n", "1.234000000000000e+01", "DOUBLE");
    assertVariantLiterally("VARIANT", " 123.4e-1\t\n", "1.234000000000000e+01", "DOUBLE");
    assertVariantLiterally("VARIANT", " 123.4E-1\t\n", "1.234000000000000e+01", "DOUBLE");

    assertVariantLiterally("VARIANT", " 1234e1\t\n", "1.234000000000000e+04", "DOUBLE");
    assertVariantLiterally("VARIANT", " 1234E1\t\n", "1.234000000000000e+04", "DOUBLE");
    assertVariantLiterally("VARIANT", " 1234e-1\t\n", "1.234000000000000e+02", "DOUBLE");
    assertVariantLiterally("VARIANT", " 1234E-1\t\n", "1.234000000000000e+02", "DOUBLE");

    assertVariantLiterally(
        "OBJECT",
        " {\"key\": 1.234E1\t\n}",
        "{\n" + "  \"key\": 1.234000000000000e+01\n" + "}",
        "OBJECT");

    assertVariantLiterally(
        "ARRAY", " [1.234E1\t\n]\n", "[\n" + "  1.234000000000000e+01\n" + "]", "ARRAY");
  }

  private String createLargeVariantObject(int size) throws JsonProcessingException {
    char[] stringContent = new char[size - 17]; // {"a":"11","b":""}
    Arrays.fill(stringContent, 'c');
    Map<String, Object> m = new HashMap<>();
    m.put("a", "11");
    m.put("b", new String(stringContent));
    String s = new ObjectMapper().writeValueAsString(m);
    Assert.assertEquals(s.length(), size);
    return s;
  }
}
