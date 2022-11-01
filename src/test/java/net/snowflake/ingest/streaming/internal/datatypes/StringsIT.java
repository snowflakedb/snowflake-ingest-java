package net.snowflake.ingest.streaming.internal.datatypes;

import java.math.BigDecimal;
import java.math.BigInteger;
import net.snowflake.ingest.TestUtils;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.utils.Constants;
import org.junit.Ignore;
import org.junit.Test;

public class StringsIT extends AbstractDataTypeTest {

  public StringsIT(String name, Constants.BdecVersion bdecVersion) {
    super(name, bdecVersion);
  }

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
    testIngestion("VARCHAR", "ž, š, č, ř, c, j, ď, ť, ň", new StringProvider());
    testIngestion("VARCHAR", "čččččččččččččččč", new StringProvider()); // 16x
    testIngestion("VARCHAR", "ačččččččččččččččč", new StringProvider()); // 1x + 16x
    testIngestion("VARCHAR", "ččččččččččččččččč", new StringProvider()); // 17x
    testIngestion(
        "VARCHAR", "❄😃öüß0ö😃üä++ěšíáýšěčí🍞áýřž+šář+🍞ýšš😃čžýříéě+ž❄", new StringProvider());
  }

  @Test
  public void testMaxAllowedString() throws Exception {
    // 1-byte chars
    String maxString = buildString("a", 16 * 1024 * 1024);
    testIngestion("VARCHAR", maxString, new StringProvider());
    expectArrowNotSupported("VARCHAR", maxString + "a");

    // 2-byte chars
    maxString = buildString("š", 8 * 1024 * 1024);
    testIngestion("VARCHAR", maxString, new StringProvider());

    expectArrowNotSupported("VARCHAR", maxString + "a");

    // 3-byte chars
    maxString = buildString("❄", (16 * 1024 * 1024 - 1) / 3);
    testIngestion("VARCHAR", maxString, new StringProvider());
    expectArrowNotSupported("VARCHAR", maxString + "aa");

    // 4-byte chars
    maxString = buildString("🍞", 4 * 1024 * 1024);
    testIngestion("VARCHAR", maxString, new StringProvider());
    expectArrowNotSupported("VARCHAR", maxString + "a");
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
    ingestAndMigrate(
        "aaaaaaaaa\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF");

    // chars + 15+ times \uFFFF + chars
    ingestAndMigrate(
        "aaaaaaaaa\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFFaaaaaaaaa");

    // 15+ times \uFFFF
    ingestAndMigrate(
        "\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF");

    // 15+ times \uFFFF + chars
    ingestAndMigrate(
        "\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFFaaaaaaaaa");
  }

  @Test
  public void testMultiByteCharComparison() throws Exception {
    ingestAndMigrate("a", "B");
    ingestAndMigrate("b", "A");

    ingestAndMigrate("a", "❄");
    ingestAndMigrate("❄", "a");

    ingestAndMigrate(
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄");
    ingestAndMigrate(
        "❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄",
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
  }

  /**
   * Ingests string with length around truncation size and assert that both shorter, equal and
   * longer strings are injected correctly
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
    testIngestion("VARCHAR", buildString("a", 1, "š", 15), new StringProvider());
    testIngestion("VARCHAR", buildString("a", 1, "š", 16), new StringProvider());

    // Test 3-byte
    testIngestion("VARCHAR", buildString("❄", 10), new StringProvider());
    testIngestion("VARCHAR", buildString("❄", 11), new StringProvider());
    testIngestion("VARCHAR", buildString("❄", 12), new StringProvider());

    // Test 4-byte
    testIngestion("VARCHAR", buildString("🍞", 6), new StringProvider());
    testIngestion("VARCHAR", buildString("🍞", 7), new StringProvider());
    testIngestion("VARCHAR", buildString("🍞", 8), new StringProvider());

    testIngestion("VARCHAR", buildString("a", 1, "🍞", 7), new StringProvider());
  }

  @Test
  @Ignore("Failing due to GS SNOW-690281")
  public void testByteSplit() throws Exception {
    testIngestion("VARCHAR", buildString("a", 1, "🍞", 8), new StringProvider());
    testIngestion("VARCHAR", buildString("a", 1, "🍞", 9), new StringProvider());
  }

  /**
   * Creates a string from a certain number of concatenated strings e.g. buildString("ab", 2) =>
   * abab
   */
  private String buildString(String str, int count) {
    StringBuilder sb = new StringBuilder(count);
    for (int i = 0; i < count; i++) {
      sb.append(str);
    }
    return sb.toString();
  }

  /**
   * Creates a string concatenated from two strings, each consisting of a certain number of
   * concatenated strings e.g. buildString("a", 2, "eb", 3) => aaebebeb
   */
  private String buildString(String str1, int count1, String str2, int count2) {
    String sb1 = buildString(str1, count1);
    String sb2 = buildString(str2, count2);
    return sb1 + sb2;
  }

  /**
   * Ingest two values, wait for the latest offset to be committed, migrate the table and assert no
   * errors have been thrown. Useful to test that EP values are generated correctly because if they
   * weren't, migration would fail and create an incident.
   */
  protected <STREAMING_INGEST_WRITE> void ingestAndMigrate(STREAMING_INGEST_WRITE... values)
      throws Exception {
    String tableName = createTable("VARCHAR");
    SnowflakeStreamingIngestChannel channel = openChannel(tableName);
    String offsetToken = null;
    for (int i = 0; i < values.length; i++) {
      offsetToken = String.format("offsetToken%d", i);
      channel.insertRow(createStreamingIngestRow(values[i]), offsetToken);
    }

    TestUtils.waitForOffset(channel, offsetToken);
    migrateTable(tableName); // migration should always succeed
  }
}
