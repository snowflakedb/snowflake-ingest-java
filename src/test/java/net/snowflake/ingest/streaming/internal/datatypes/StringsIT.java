package net.snowflake.ingest.streaming.internal.datatypes;

import static net.snowflake.ingest.TestUtils.buildString;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class StringsIT extends AbstractDataTypeTest {

  private static final int MB_16 = 16 * 1024 * 1024;

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
  public void testStrings() throws Exception {
    testJdbcTypeCompatibility("VARCHAR", "", new StringProvider());
    testJdbcTypeCompatibility("VARCHAR", "foo", new StringProvider());
    testJdbcTypeCompatibility("VARCHAR", "  foo  \t\n", new StringProvider());

    // Test strings with limited size
    testJdbcTypeCompatibility("VARCHAR(1)", "", new StringProvider());
    testJdbcTypeCompatibility("VARCHAR(2)", "ab", new StringProvider());
    testJdbcTypeCompatibility("VARCHAR(2)", "🍞❄", new StringProvider());

    // test booleans
    testJdbcTypeCompatibility("CHAR(5)", true, "true", new BooleanProvider(), new StringProvider());
    testJdbcTypeCompatibility(
        "CHAR(5)", false, "false", new BooleanProvider(), new StringProvider());
    expectNotSupported("CHAR(4)", false);

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
    testJdbcTypeCompatibility(
        "VARCHAR", "❄😃öüß0ö😃üä++ěšíáýšěčí🍞áýřž+šář+🍞ýšš😃čžýříéě+ž❄", new StringProvider());
  }

  @Test
  public void testStringCreatedFromInvalidBytes() throws Exception {
    byte[] bytes = new byte[256];
    int counter = 0;
    while (counter < 256) {
      bytes[counter] = (byte) (Byte.MIN_VALUE + counter);
      counter++;
    }

    String s = new String(bytes, StandardCharsets.UTF_8);
    testJdbcTypeCompatibility("VARCHAR", s, new StringProvider());
  }

  @Test
  public void testMaxAllowedString() throws Exception {
    // 1-byte chars
    String maxString = buildString("a", MB_16);
    testIngestion("VARCHAR", maxString, new StringProvider());
    expectNotSupported("VARCHAR", maxString + "a");

    // 2-byte chars
    maxString = buildString("š", MB_16 / 2);
    testIngestion("VARCHAR", maxString, new StringProvider());

    expectNotSupported("VARCHAR", maxString + "a");

    // 3-byte chars
    maxString = buildString("❄", MB_16 / 3);
    testIngestion("VARCHAR", maxString, new StringProvider());
    expectNotSupported("VARCHAR", maxString + "aa");

    // 4-byte chars
    maxString = buildString("🍞", MB_16 / 4);
    testIngestion("VARCHAR", maxString, new StringProvider());
    expectNotSupported("VARCHAR", maxString + "a");
  }

  @Test
  public void testPrefixFF() throws Exception {

    // 11x \xFFFF
    testIngestion(
        "VARCHAR",
        "\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF",
        new StringProvider());
    // 10x \xFFFF + chars
    testIngestion(
        "VARCHAR",
        "\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFFaaaaaaaaaaaaaaaaaaaaaaaaaa",
        new StringProvider());

    // chars + 15+ times \uFFFF
    ingestManyAndMigrate(
        "VARCHAR",
        "aaaaaaaaa\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF");

    // chars + 15+ times \uFFFF + chars
    ingestManyAndMigrate(
        "VARCHAR",
        "aaaaaaaaa\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFFaaaaaaaaa");

    // 15+ times \uFFFF
    ingestManyAndMigrate(
        "VARCHAR",
        "\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF");

    // 15+ times \uFFFF + chars
    ingestManyAndMigrate(
        "VARCHAR",
        "\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFFaaaaaaaaa");
  }

  @Test
  public void testMultiByteCharComparison() throws Exception {
    ingestManyAndMigrate("VARCHAR", "a", "❄");
    ingestManyAndMigrate("VARCHAR", "❄", "a");

    ingestManyAndMigrate(
        "VARCHAR",
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄");
    ingestManyAndMigrate(
        "VARCHAR",
        "❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄",
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
  }

  /**
   * Ingests string with length around EP-truncation point and asserts that both shorter, equal and
   * longer strings are ingested correctly
   */
  @Test
  public void testTruncationAndIncrementation() throws Exception {
    // Test 1-byte
    testIngestion("VARCHAR", buildString("a", 31), new StringProvider());
    testIngestion("VARCHAR", buildString("a", 32), new StringProvider());
    testIngestion("VARCHAR", buildString("a", 33), new StringProvider());

    // Test 2-byte
    testIngestion("VARCHAR", buildString("š", 15), new StringProvider());
    testIngestion("VARCHAR", buildString("š", 16), new StringProvider());
    testIngestion("VARCHAR", buildString("š", 17), new StringProvider());
    testIngestion("VARCHAR", "a" + buildString("š", 15), new StringProvider());
    testIngestion("VARCHAR", "a" + buildString("š", 16), new StringProvider());

    // Test 3-byte
    testIngestion("VARCHAR", buildString("❄", 10), new StringProvider());
    testIngestion("VARCHAR", buildString("❄", 11), new StringProvider());
    testIngestion("VARCHAR", buildString("❄", 12), new StringProvider());

    // Test 4-byte
    testIngestion("VARCHAR", buildString("🍞", 6), new StringProvider());
    testIngestion("VARCHAR", buildString("🍞", 7), new StringProvider());
    testIngestion("VARCHAR", buildString("🍞", 8), new StringProvider());

    testIngestion("VARCHAR", "a" + buildString("🍞", 7), new StringProvider());
  }

  @Test
  @Ignore("Failing due to GS SNOW-690281")
  public void testByteSplit() throws Exception {
    testIngestion("VARCHAR", "a" + buildString("🍞", 8), new StringProvider());
    testIngestion("VARCHAR", "a" + buildString("🍞", 9), new StringProvider());
  }

  /**
   * Verifies that non-nullable collated columns are not supported at all and an exception is thrown
   * already while creating the channel.
   */
  @Test
  public void testCollatedColumnsNotSupported() throws SQLException {
    String tableName = getRandomIdentifier();
    conn.createStatement()
        .execute(
            String.format(
                "create or replace table %s (\"create\" string collate 'en-ci')", tableName));
    try {
      openChannel(tableName);
      Assert.fail("Opening a channel shouldn't have succeeded");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.OPEN_CHANNEL_FAILURE.getMessageCode(), e.getVendorCode());
    }
  }
}
