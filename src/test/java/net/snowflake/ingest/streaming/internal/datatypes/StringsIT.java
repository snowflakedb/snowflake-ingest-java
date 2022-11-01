package net.snowflake.ingest.streaming.internal.datatypes;

import java.math.BigDecimal;
import java.math.BigInteger;
import org.junit.Test;

public class StringsIT extends AbstractDataTypeTest {
  @Test
  public void testStrings() throws Exception {
    testJdbcTypeCompatibility("VARCHAR", "", new StringProvider());
    testJdbcTypeCompatibility("VARCHAR", "foo", new StringProvider());

    // Test strings with limited size
    testJdbcTypeCompatibility("VARCHAR(2)", "", new StringProvider());
    testJdbcTypeCompatibility("VARCHAR(2)", "ab", new StringProvider());
    expectArrowNotSupported("VARCHAR(2)", "abc");

    // test booleans
    testJdbcTypeCompatibility("CHAR(5)", true, "true", new BooleanProvider(), new StringProvider());
    testJdbcTypeCompatibility(
        "CHAR(5)", false, "false", new BooleanProvider(), new StringProvider());
    expectArrowNotSupported("CHAR(4)", false);

    // test numbers
    testJdbcTypeCompatibility(
        "CHAR(4)", (byte) 123, "123", new ByteProvider(), new StringProvider());
    testJdbcTypeCompatibility(
        "CHAR(4)", (short) 1111, "1111", new ShortProvider(), new StringProvider());
    testJdbcTypeCompatibility("CHAR(4)", 1111, "1111", new IntProvider(), new StringProvider());
    testJdbcTypeCompatibility("CHAR(4)", 1111L, "1111", new LongProvider(), new StringProvider());
    testIngestion("CHAR(4)", BigInteger.valueOf(1111), "1111", new StringProvider());
    testJdbcTypeCompatibility("CHAR(3)", 1.5f, "1.5", new FloatProvider(), new StringProvider());
    testJdbcTypeCompatibility("CHAR(3)", 1.500f, "1.5", new FloatProvider(), new StringProvider());
    testJdbcTypeCompatibility("CHAR(3)", 1.5d, "1.5", new DoubleProvider(), new StringProvider());
    testJdbcTypeCompatibility("CHAR(3)", 1.500d, "1.5", new DoubleProvider(), new StringProvider());

    // BigDecimal
    testJdbcTypeCompatibility(
        "CHAR(4)",
        BigDecimal.valueOf(1111),
        "1111",
        new BigDecimalProvider(),
        new StringProvider());
    testJdbcTypeCompatibility(
        "CHAR(1)", new BigDecimal("4.0000"), "4", new BigDecimalProvider(), new StringProvider());
    testJdbcTypeCompatibility(
        "VARCHAR",
        new BigDecimal("4e10"),
        "40000000000",
        new BigDecimalProvider(),
        new StringProvider());

    // char
    testIngestion("CHAR(4)", 'c', "c", new StringProvider());
  }

  @Test
  public void testNonAsciiStrings() throws Exception {
    testIngestion(
        "VARCHAR", "ž, š, č, ř, c, j, ď, ť, ň", "ž, š, č, ř, c, j, ď, ť, ň", new StringProvider());
    String times16 = "čččččččččččččččč";
    String times17 = "ččččččččččččččččč";
    testIngestion("VARCHAR", times16, times16, new StringProvider());
    testIngestion("VARCHAR", times17, times17, new StringProvider());

    String input = "❄❄❄öüß0öüä++ěšíáýšěčíáýřž+šáříé+ýšěáéíščžýříéě+ž❄❄❄";
    testIngestion("VARCHAR", input, input, new StringProvider());
  }

  @Test
  public void testMaxAllowedString() throws Exception {
    StringBuilder maxAllowedStringBuilder = buildString('a', 16 * 1024 * 1024);
    String maxString = maxAllowedStringBuilder.toString();
    testIngestion("VARCHAR", maxString, maxString, new StringProvider());
    expectArrowNotSupported("VARCHAR", maxAllowedStringBuilder.append('a').toString());
  }

  @Test
  public void testMaxAllowedMultiByteString() throws Exception {
    // 'š' is a 2-byte unicode character
    StringBuilder maxAllowedMultiByteStringBuilder = buildString('š', 8 * 1024 * 1024);
    String maxAllowedMultiByteString = maxAllowedMultiByteStringBuilder.toString();
    testIngestion(
        "VARCHAR", maxAllowedMultiByteString, maxAllowedMultiByteString, new StringProvider());

    expectArrowNotSupported("VARCHAR", maxAllowedMultiByteStringBuilder.append('a').toString());
  }

  private StringBuilder buildString(char character, int count) {
    StringBuilder maxStringBuilder = new StringBuilder(count);
    for (int i = 0; i < count; i++) {
      maxStringBuilder.append(character);
    }
    return maxStringBuilder;
  }
}
