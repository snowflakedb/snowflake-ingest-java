package net.snowflake.ingest.streaming.internal.datatypes;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import net.snowflake.ingest.TestUtils;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.utils.Constants;
import org.junit.Test;

/**
 * This test generates batches of random unicode strings, ingests and migrates them. Migration
 * uncovers any potential inconsistencies between data and metadata. If there is a failure, it
 * prints the random strings from the batch as a reproducer.
 */
public class RandomizedUnicodeStringsIT extends AbstractDataTypeTest {
  public RandomizedUnicodeStringsIT(String name, Constants.BdecVersion bdecVersion) {
    super(name, bdecVersion);
  }

  private static final int MAX_STRING_LENGTH = 64;
  private static final int BATCH_COUNT = 100;
  private static final int MAX_BATCH_SIZE = 10;

  private static final Random random = new Random();

  @Test
  public void testRandomValidUnicodeStrings() throws Exception {
    for (int i = 0; i < BATCH_COUNT; i++) {
      System.out.printf("Running batch %d / %d%n", i, BATCH_COUNT);
      runBatch(this::randomUnicodeString);
    }
  }

  @Test
  public void testStringsFromRandomByteArrays() throws Exception {
    for (int i = 0; i < BATCH_COUNT; i++) {
      System.out.printf("Running batch %d / %d%n", i, BATCH_COUNT);
      runBatch(this::utf8StringFromRandomBytes);
    }
  }

  private void runBatch(Supplier<String> stringSupplier) throws Exception {
    String tableName = createTable("VARCHAR");
    SnowflakeStreamingIngestChannel channel = openChannel(tableName);
    List<String> strings = new ArrayList<>();

    try {
      int i = 0;
      int batchSize = random.nextInt(MAX_BATCH_SIZE) + 1;
      while (i < batchSize) {
        i++;
        String stringInput = stringSupplier.get();
        strings.add(stringInput);
        channel.insertRow(createStreamingIngestRow(stringInput), String.valueOf(i));
      }
      TestUtils.waitForOffset(channel, String.valueOf(i));
      migrateTable(tableName); // migration should always succeed
    } catch (Exception e) {
      System.err.println("StringsRandomizedIT: Failed for the following batch of strings:");
      for (String input : strings) {
        char[] z = input.toCharArray();
        List<String> charCodeList =
            IntStream.range(0, z.length)
                .map(id -> z[id])
                .mapToObj(x -> String.format("\\u%04X", x))
                .collect(Collectors.toList());
        System.err.println(String.join("", charCodeList));
      }
      throw e;
    }
  }

  /** Generate random unicode string of random length between 0 and MAX_STRING_LENGTH. */
  private String randomUnicodeString() {
    int targetLength = random.nextInt(MAX_STRING_LENGTH);

    StringBuilder sb = new StringBuilder();

    int length = 0;
    while (length < targetLength) {
      int codePoint = random.nextInt(Character.MAX_CODE_POINT);
      int codePointType = Character.getType(codePoint);
      if (codePointType == Character.UNASSIGNED
          || codePointType == Character.PRIVATE_USE
          || codePointType == Character.SURROGATE) {
        continue;
      }
      length++;
      sb.appendCodePoint(codePoint);
    }
    return sb.toString();
  }

  private String utf8StringFromRandomBytes() {
    byte[] bytes = new byte[random.nextInt(MAX_STRING_LENGTH)];
    random.nextBytes(bytes);
    return new String(bytes, StandardCharsets.UTF_8);
  }
}
