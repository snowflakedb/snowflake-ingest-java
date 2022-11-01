package net.snowflake.ingest.streaming.internal.datatypes;

import net.snowflake.ingest.TestUtils;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.utils.Constants;
import org.junit.Test;

public class StringCollationsIT extends AbstractDataTypeTest {

  private static final String[] COLLATIONS =
      new String[] {
        "en-fl",
        "en-fu",
        "en-fu-forced",
        "trim",
        "ltrim",
        "rtrim",
        "lower",
        "lower-trim",
        "upper",
        "upper-trim",
        "en",
        "en-ai",
        "en-ci",
        "en-fl",
        "en-fu",
        "en-pi",
        "en-ai-pi-ci",
        "en-ai-pi-ci-upper-trim",
        "pl",
        "pl-ai-ci-pi"
      };

  public StringCollationsIT(String name, Constants.BdecVersion bdecVersion) {
    super(name, bdecVersion);
  }

  @Test
  public void testCollatedStrings() throws Exception {
    for (String collation : new String[] {"lower", "upper"}) {
      runCollatedTest(collation, "B", "š");
      runCollatedTest(collation, "Š", "b");
    }

    for (String collation : new String[] {"trim", "ltrim", "rtrim"}) {
      runCollatedTest(collation, "A", " š ");
      runCollatedTest(collation, "A", "š ");
      runCollatedTest(collation, "A", " š");
    }

    for (String collation : new String[] {"en_US-fl", "en_US-fu"}) {
      runCollatedTest(collation, "Baa", "šaa");
      runCollatedTest(collation, "Šaa", "baa");
    }

    for (String collation : new String[] {"es_ES", "es_ES-trim", "es_ES-upper", "es_ES-lower"}) {
      runCollatedTest(collation, "  piña colada  ", "pinta");
      runCollatedTest(collation, "  Piña colada  ", "PINTA");
    }
  }

  @Test
  public void testCollatedFFPrefix() throws Exception {
    String collation = "en_US-ai";

    // 11x \xFFFF
    runCollatedTest(
        collation, "\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF");

    // 10x \xFFFF + chars
    runCollatedTest(
        collation,
        "\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFFaaaaaaaaaaaaaaaaaaaaaaaaaa");

    // chars + 15+ times \uFFFF
    runCollatedTest(
        collation,
        "aaaaaaaaa\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF");

    // chars + 15+ times \uFFFF + chars
    runCollatedTest(
        collation,
        "aaaaaaaaa\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFFaaaaaaaaa");

    // 15+ times \uFFFF
    runCollatedTest(
        collation,
        "\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF");

    // 15+ times \uFFFF + chars
    runCollatedTest(
        collation,
        "\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFFaaaaaaaaa");
  }

  /** Inspired by t_collations/src/collations-basic.sql */
  @Test
  public void basicCollationsTest() throws Exception {
    String[] values = new String[] {"abc", "Ąbc", "ĄBC", "ABC", " ABC", "a-b-c", "A_b C", "ąbć"};

    for (String collation : COLLATIONS) {
      runCollatedTest(collation, values);
    }
  }

  @Test
  public void testCollatedStringsWithMultiByteChars() throws Exception {
    for (String collation : COLLATIONS) {
      runCollatedTest(
          collation,
          // less than 32 bytes
          "ěéčář",
          // 32 bytes
          "ěéčářýíáčšřýěšří",
          // more than 32 bytes
          "ěéčářýíáčšřýěšýříěšýčířýěščíéářýíěášýčř=+ýčřáíýěščířáýěščíářýíěáščýšýčěířžě");
    }
  }

  private void runCollatedTest(String collation, String... values) throws Exception {
    String tableName = createTable("VARCHAR", collation);
    SnowflakeStreamingIngestChannel channel = openChannel(tableName);
    String offsetToken = null;
    for (int i = 0; i < values.length; i++) {
      offsetToken = String.format("foo%d", i);
      channel.insertRow(createStreamingIngestRow(values[i]), offsetToken);
    }
    TestUtils.waitForOffset(channel, offsetToken);
    migrateTable(tableName); // migration should always succeed
  }
}
