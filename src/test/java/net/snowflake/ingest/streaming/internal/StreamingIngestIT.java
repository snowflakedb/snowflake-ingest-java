package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.utils.Constants.BLOB_NO_HEADER;
import static net.snowflake.ingest.utils.Constants.COMPRESS_BLOB_TWICE;
import static net.snowflake.ingest.utils.Constants.ROLE;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import java.util.stream.IntStream;
import net.snowflake.ingest.TestUtils;
import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.ParameterProvider;
import net.snowflake.ingest.utils.SFException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** Example streaming ingest sdk integration test */
public class StreamingIngestIT {
  private static final String TEST_TABLE = "STREAMING_INGEST_TEST_TABLE";
  private static final String TEST_DB_PREFIX = "STREAMING_INGEST_TEST_DB";
  private static final String TEST_SCHEMA = "STREAMING_INGEST_TEST_SCHEMA";

  private static final String INTERLEAVED_TABLE_PREFIX = "t_interleaved_test_";
  private static final String INTERLEAVED_CHANNEL_TABLE = "t_interleaved_channel_test";
  private static final int INTERLEAVED_CHANNEL_NUMBER = 3;

  private Properties prop;

  private SnowflakeStreamingIngestClientInternal<?> client;
  private Connection jdbcConnection;
  private String testDb;

  @Before
  public void beforeAll() throws Exception {
    testDb = TEST_DB_PREFIX + "_" + UUID.randomUUID().toString().substring(0, 4);
    // Create a streaming ingest client
    jdbcConnection = TestUtils.getConnection(true);
    jdbcConnection
        .createStatement()
        .execute(String.format("create or replace database %s;", testDb));
    jdbcConnection
        .createStatement()
        .execute(String.format("create or replace schema %s.%s;", testDb, TEST_SCHEMA));
    jdbcConnection
        .createStatement()
        .execute(
            String.format(
                "create or replace table %s.%s.%s (c1 char(10));",
                testDb, TEST_SCHEMA, TEST_TABLE));
    // Set timezone to UTC
    jdbcConnection.createStatement().execute("alter session set timezone = 'UTC';");
    jdbcConnection
        .createStatement()
        .execute(String.format("use warehouse %s", TestUtils.getWarehouse()));

    prop = TestUtils.getProperties();
    if (prop.getProperty(ROLE).equals("DEFAULT_ROLE")) {
      prop.setProperty(ROLE, "ACCOUNTADMIN");
    }
    client =
        (SnowflakeStreamingIngestClientInternal<?>)
            SnowflakeStreamingIngestClientFactory.builder("client1").setProperties(prop).build();
  }

  @After
  public void afterAll() throws Exception {
    client.close();
    jdbcConnection.createStatement().execute(String.format("drop database %s", testDb));
  }

  @Test
  public void testSimpleIngest() throws Exception {
    OpenChannelRequest request1 =
        OpenChannelRequest.builder("CHANNEL")
            .setDBName(testDb)
            .setSchemaName(TEST_SCHEMA)
            .setTableName(TEST_TABLE)
            .setOnErrorOption(OpenChannelRequest.OnErrorOption.CONTINUE)
            .build();

    // Open a streaming ingest channel from the given client
    SnowflakeStreamingIngestChannel channel1 = client.openChannel(request1);
    for (int val = 0; val < 1000; val++) {
      Map<String, Object> row = new HashMap<>();
      row.put("c1", Integer.toString(val));
      verifyInsertValidationResponse(channel1.insertRow(row, Integer.toString(val)));
    }

    // Close the channel after insertion
    channel1.close().get();

    for (int i = 1; i < 15; i++) {
      if (channel1.getLatestCommittedOffsetToken() != null
          && channel1.getLatestCommittedOffsetToken().equals("999")) {
        ResultSet result =
            jdbcConnection
                .createStatement()
                .executeQuery(
                    String.format(
                        "select count(*) from %s.%s.%s", testDb, TEST_SCHEMA, TEST_TABLE));
        result.next();
        Assert.assertEquals(1000, result.getLong(1));

        ResultSet result2 =
            jdbcConnection
                .createStatement()
                .executeQuery(
                    String.format(
                        "select * from %s.%s.%s order by c1 limit 2",
                        testDb, TEST_SCHEMA, TEST_TABLE));
        result2.next();
        Assert.assertEquals("0", result2.getString(1));
        result2.next();
        Assert.assertEquals("1", result2.getString(1));

        // Verify perf metrics
        if (client.getParameterProvider().hasEnabledSnowpipeStreamingMetrics()) {
          Assert.assertEquals(1, client.blobSizeHistogram.getCount());
          if (BLOB_NO_HEADER && COMPRESS_BLOB_TWICE) {
            Assert.assertEquals(3445, client.blobSizeHistogram.getSnapshot().getMax());
          } else if (BLOB_NO_HEADER) {
            Assert.assertEquals(3600, client.blobSizeHistogram.getSnapshot().getMax());
          } else if (COMPRESS_BLOB_TWICE) {
            Assert.assertEquals(3981, client.blobSizeHistogram.getSnapshot().getMax());
          } else {
            Assert.assertEquals(4115, client.blobSizeHistogram.getSnapshot().getMax());
          }
        }
        return;
      }
      Thread.sleep(500);
    }
    Assert.fail("Row sequencer not updated before timeout");
  }

  @Test
  public void testParameterOverrides() throws Exception {
    Map<String, Object> parameterMap = new HashMap<>();
    parameterMap.put(ParameterProvider.BUFFER_FLUSH_INTERVAL_IN_MILLIS, 30L);
    parameterMap.put(ParameterProvider.BUFFER_FLUSH_CHECK_INTERVAL_IN_MILLIS, 50L);
    parameterMap.put(ParameterProvider.INSERT_THROTTLE_THRESHOLD_IN_PERCENTAGE, 1);
    parameterMap.put(ParameterProvider.INSERT_THROTTLE_INTERVAL_IN_MILLIS, 1L);
    parameterMap.put(ParameterProvider.ENABLE_SNOWPIPE_STREAMING_METRICS, true);
    parameterMap.put(ParameterProvider.IO_TIME_CPU_RATIO, 1);
    parameterMap.put(ParameterProvider.BLOB_UPLOAD_MAX_RETRY_COUNT, 1);
    client =
        (SnowflakeStreamingIngestClientInternal<?>)
            SnowflakeStreamingIngestClientFactory.builder("testParameterOverridesClient")
                .setProperties(prop)
                .setParameterOverrides(parameterMap)
                .build();

    OpenChannelRequest request1 =
        OpenChannelRequest.builder("CHANNEL")
            .setDBName(testDb)
            .setSchemaName(TEST_SCHEMA)
            .setTableName(TEST_TABLE)
            .setOnErrorOption(OpenChannelRequest.OnErrorOption.CONTINUE)
            .build();

    // Open a streaming ingest channel from the given client
    SnowflakeStreamingIngestChannel channel1 = client.openChannel(request1);
    for (int val = 0; val < 10; val++) {
      Map<String, Object> row = new HashMap<>();
      row.put("c1", Integer.toString(val));
      verifyInsertValidationResponse(channel1.insertRow(row, Integer.toString(val)));
    }

    // Close the channel after insertion
    channel1.close().get();

    for (int i = 1; i < 15; i++) {
      if (channel1.getLatestCommittedOffsetToken() != null
          && channel1.getLatestCommittedOffsetToken().equals("9")) {
        ResultSet result =
            jdbcConnection
                .createStatement()
                .executeQuery(
                    String.format(
                        "select count(*) from %s.%s.%s", testDb, TEST_SCHEMA, TEST_TABLE));
        result.next();
        Assert.assertEquals(10, result.getLong(1));
        return;
      }
      Thread.sleep(500);
    }
    Assert.fail("Row sequencer not updated before timeout");
  }

  @Test
  public void testInterleavedIngest() {
    Consumer<IntConsumer> iter =
        f -> IntStream.rangeClosed(1, INTERLEAVED_CHANNEL_NUMBER).forEach(f);

    iter.accept(i -> createTableForInterleavedTest(INTERLEAVED_TABLE_PREFIX + i));

    SnowflakeStreamingIngestChannel[] channels =
        new SnowflakeStreamingIngestChannel[INTERLEAVED_CHANNEL_NUMBER];
    iter.accept(i -> channels[i - 1] = openChannel(INTERLEAVED_TABLE_PREFIX + i, "CHANNEL"));

    iter.accept(
        i ->
            produceRowsForInterleavedTest(
                channels[i - 1], INTERLEAVED_TABLE_PREFIX + i, 1 << (i + 1)));
    iter.accept(i -> waitChannelFlushed(channels[i - 1], 1 << (i + 1)));

    iter.accept(i -> verifyTableRowCount(1 << (i + 1), INTERLEAVED_TABLE_PREFIX + i));
    iter.accept(
        i ->
            verifyInterleavedResult(
                1 << (i + 1), INTERLEAVED_TABLE_PREFIX + i, INTERLEAVED_TABLE_PREFIX + i));
  }

  @Test
  public void testMultiChannelChunk() {
    Consumer<IntConsumer> iter =
        f -> IntStream.rangeClosed(1, INTERLEAVED_CHANNEL_NUMBER).forEach(f);

    createTableForInterleavedTest(INTERLEAVED_CHANNEL_TABLE);

    SnowflakeStreamingIngestChannel[] channels =
        new SnowflakeStreamingIngestChannel[INTERLEAVED_CHANNEL_NUMBER];
    iter.accept(i -> channels[i - 1] = openChannel(INTERLEAVED_CHANNEL_TABLE, "CHANNEL_" + i));

    iter.accept(
        i ->
            produceRowsForInterleavedTest(
                channels[i - 1], INTERLEAVED_CHANNEL_TABLE + "_channel_" + i, 1 << (i + 1)));
    iter.accept(i -> waitChannelFlushed(channels[i - 1], 1 << (i + 1)));

    int rowNumber =
        IntStream.rangeClosed(1, INTERLEAVED_CHANNEL_NUMBER).map(i -> 1 << (i + 1)).sum();
    verifyTableRowCount(rowNumber, INTERLEAVED_CHANNEL_TABLE);

    iter.accept(
        i ->
            verifyInterleavedResult(
                1 << (i + 1),
                INTERLEAVED_CHANNEL_TABLE,
                INTERLEAVED_CHANNEL_TABLE + "_channel_" + i));
  }

  @Test
  public void testCollation() throws Exception {
    String collationTable = "collation_table";
    jdbcConnection
        .createStatement()
        .execute(
            String.format(
                "create or replace table %s (noncol char(10), col char(10) collate 'en-ci');",
                collationTable));

    OpenChannelRequest request1 =
        OpenChannelRequest.builder("CHANNEL")
            .setDBName(testDb)
            .setSchemaName(TEST_SCHEMA)
            .setTableName(collationTable)
            .setOnErrorOption(OpenChannelRequest.OnErrorOption.CONTINUE)
            .build();

    // Open a streaming ingest channel from the given client
    SnowflakeStreamingIngestChannel channel1 = client.openChannel(request1);
    Map<String, Object> row = new HashMap<>();
    row.put("col", "AA");
    row.put("noncol", "AA");
    verifyInsertValidationResponse(channel1.insertRow(row, "1"));
    row.put("col", "a");
    row.put("noncol", "a");
    verifyInsertValidationResponse(channel1.insertRow(row, "2"));

    // Close the channel after insertion
    channel1.close().get();

    for (int i = 1; i < 15; i++) {
      if (channel1.getLatestCommittedOffsetToken() != null
          && channel1.getLatestCommittedOffsetToken().equals("2")) {
        ResultSet result =
            jdbcConnection
                .createStatement()
                .executeQuery(
                    String.format(
                        "select min(col), min(noncol) from %s.%s.%s",
                        testDb, TEST_SCHEMA, collationTable));
        result.next();
        Assert.assertEquals("a", result.getString(1));
        Assert.assertEquals("AA", result.getString(2));
        return;
      }
      Thread.sleep(1000);
    }
    Assert.fail("Row sequencer not updated before timeout");
  }

  @Test
  public void testDecimalColumnIngest() throws Exception {
    String decimalTableName = "decimal_table";
    jdbcConnection
        .createStatement()
        .execute(
            String.format(
                "create or replace table %s (tinyfloat NUMBER(38,2));", decimalTableName));
    OpenChannelRequest request1 =
        OpenChannelRequest.builder("CHANNEL_DECI")
            .setDBName(testDb)
            .setSchemaName(TEST_SCHEMA)
            .setTableName(decimalTableName)
            .setOnErrorOption(OpenChannelRequest.OnErrorOption.CONTINUE)
            .build();

    // Open a streaming ingest channel from the given client
    SnowflakeStreamingIngestChannel channel1 = client.openChannel(request1);

    Map<String, Object> row = new HashMap<>();
    row.put("tinyfloat", -1.1);
    verifyInsertValidationResponse(channel1.insertRow(row, null));
    row.put("tinyfloat", 2.2);
    verifyInsertValidationResponse(channel1.insertRow(row, null));
    row.put("tinyfloat", BigInteger.valueOf(10).pow(35));
    verifyInsertValidationResponse(channel1.insertRow(row, "1"));

    // Close the channel after insertion
    channel1.close().get();

    for (int i = 1; i < 15; i++) {
      if (channel1.getLatestCommittedOffsetToken() != null
          && channel1.getLatestCommittedOffsetToken().equals("1")) {
        ResultSet result = null;
        result =
            jdbcConnection
                .createStatement()
                .executeQuery(
                    String.format("select * from %s.%s.%s", testDb, TEST_SCHEMA, decimalTableName));

        result.next();
        Assert.assertEquals(-1.1, result.getFloat("TINYFLOAT"), 0.001);
        result.next();
        Assert.assertEquals(2.2, result.getFloat("TINYFLOAT"), 0.001);
        result.next();
        Assert.assertEquals(
            BigInteger.valueOf(10).pow(35).floatValue(), result.getFloat("TINYFLOAT"), 10);
        return;
      } else {
        Thread.sleep(2000);
      }
    }
    Assert.fail("Row sequencer not updated before timeout");
  }

  @Test
  public void testTimeColumnIngest() throws Exception {
    String timeTableName = "time_table";
    jdbcConnection
        .createStatement()
        .execute(
            String.format(
                "create or replace table %s (tsmall TIME(3), tntzsmall TIMESTAMP_NTZ(3), ttzsmall"
                    + " TIMESTAMP_TZ(3), tbig TIME(9), tntzbig TIMESTAMP_NTZ(9), ttzbig"
                    + " TIMESTAMP_TZ(9) );",
                timeTableName));
    OpenChannelRequest request1 =
        OpenChannelRequest.builder("CHANNEL_TIME")
            .setDBName(testDb)
            .setSchemaName(TEST_SCHEMA)
            .setTableName(timeTableName)
            .setOnErrorOption(OpenChannelRequest.OnErrorOption.CONTINUE)
            .build();

    // Open a streaming ingest channel from the given client
    SnowflakeStreamingIngestChannel channel1 = client.openChannel(request1);

    Map<String, Object> row = new HashMap<>();
    row.put("ttzsmall", "2021-01-01 01:00:00.123 -0300");
    row.put("ttzbig", "2021-01-01 09:00:00.12345678 -0300");
    row.put("tsmall", "01:00:00.123");
    row.put("tbig", "09:00:00.12345678");
    row.put("tntzsmall", "1609462800123");
    row.put("tntzbig", "1609462800123450000");
    verifyInsertValidationResponse(channel1.insertRow(row, null));
    row.put("ttzsmall", "2021-01-01 10:00:00.123 +0700");
    row.put("ttzbig", "2021-01-01 19:00:00.12345678 -0300");
    row.put("tsmall", "02:00:00.123");
    row.put("tbig", "10:00:00.12345678");
    row.put("tntzsmall", "1709462800123");
    row.put("tntzbig", "170946280212345000");
    verifyInsertValidationResponse(channel1.insertRow(row, null));
    row.put("ttzsmall", "2021-01-01 05:00:00 +0100");
    row.put("ttzbig", "2021-01-01 23:00:00.12345678 -0300");
    row.put("tsmall", "03:00:00.123");
    row.put("tbig", "11:00:00.12345678");
    row.put("tntzsmall", "1809462800123");
    row.put("tntzbig", "2031-01-01 09:00:00.123456780");
    verifyInsertValidationResponse(channel1.insertRow(row, "1"));

    // Close the channel after insertion
    channel1.close().get();

    Calendar cal = Calendar.getInstance();
    cal.setTimeZone(TimeZone.getTimeZone("UTC"));
    for (int i = 1; i < 15; i++) {
      if (channel1.getLatestCommittedOffsetToken() != null
          && channel1.getLatestCommittedOffsetToken().equals("1")) {
        ResultSet result = null;
        result =
            jdbcConnection
                .createStatement()
                .executeQuery(
                    String.format("select * from %s.%s.%s", testDb, TEST_SCHEMA, timeTableName));

        result.next();
        Assert.assertEquals(1609473600123l, result.getTimestamp("TTZSMALL").getTime());
        Assert.assertEquals(1609502400123l, result.getTimestamp("TTZBIG").getTime());
        Assert.assertEquals(123456780, result.getTimestamp("TTZBIG").getNanos());
        Assert.assertEquals(3600123, result.getTimestamp("TSMALL").getTime());
        Assert.assertEquals(32400123, result.getTimestamp("TBIG").getTime());
        Assert.assertEquals(123456780, result.getTimestamp("TBIG").getNanos());
        Assert.assertEquals(1609462800123L, result.getTimestamp("TNTZSMALL", cal).getTime());
        Assert.assertEquals(1609462800123L, result.getTimestamp("TNTZBIG", cal).getTime());
        Assert.assertEquals(123450000, result.getTimestamp("TNTZBIG", cal).getNanos());

        result =
            jdbcConnection
                .createStatement()
                .executeQuery(
                    String.format(
                        "select "
                            + "max(ttzsmall) as mttzsmall,"
                            + "max(ttzbig) as mttzbig,"
                            + "max(tsmall) as mtsmall,"
                            + "max(tbig) as mtbig,"
                            + "max(tntzsmall) as mtntzsmall,"
                            + "max(tntzbig) as mtntzbig"
                            + " from %s.%s.%s",
                        testDb, TEST_SCHEMA, timeTableName));

        result.next();
        Assert.assertEquals(1609473600123L, result.getTimestamp("MTTZSMALL").getTime());
        Assert.assertEquals(1609552800123L, result.getTimestamp("MTTZBIG").getTime());
        Assert.assertEquals(123456780, result.getTimestamp("MTTZBIG").getNanos());
        Assert.assertEquals(10800123, result.getTimestamp("MTSMALL").getTime());
        Assert.assertEquals(39600123, result.getTimestamp("MTBIG").getTime());
        Assert.assertEquals(123456780, result.getTimestamp("MTBIG").getNanos());
        Assert.assertEquals(1809462800123L, result.getTimestamp("MTNTZSMALL", cal).getTime());
        Assert.assertEquals(1925024400123L, result.getTimestamp("MTNTZBIG", cal).getTime());
        Assert.assertEquals(123456780, result.getTimestamp("MTNTZBIG", cal).getNanos());

        return;
      } else {
        Thread.sleep(2000);
      }
    }
    Assert.fail("Row sequencer not updated before timeout");
  }

  @Test
  public void testMultiColumnIngest() throws Exception {
    String multiTableName = "multi_column";
    jdbcConnection
        .createStatement()
        .execute(
            String.format(
                "create or replace table %s (s text, i integer, f float, var variant, t"
                    + " timestamp_ntz, tinyfloat NUMBER(3,1), d DATE);",
                multiTableName));
    OpenChannelRequest request1 =
        OpenChannelRequest.builder("CHANNEL_MULTI")
            .setDBName(testDb)
            .setSchemaName(TEST_SCHEMA)
            .setTableName(multiTableName)
            .setOnErrorOption(OpenChannelRequest.OnErrorOption.CONTINUE)
            .build();

    // Open a streaming ingest channel from the given client
    SnowflakeStreamingIngestChannel channel1 = client.openChannel(request1);
    long timestamp = System.currentTimeMillis();
    timestamp = timestamp / 1000;
    Map<String, Object> row = new HashMap<>();
    row.put("s", "honk");
    row.put("i", 1);
    row.put("f", 3.14);
    row.put("tinyfloat", 1.1);
    row.put("var", "{\"e\":2.7}");
    row.put("t", String.valueOf(timestamp));
    row.put("d", "1969-12-31 00:00:00");
    verifyInsertValidationResponse(channel1.insertRow(row, "1"));

    // Close the channel after insertion
    channel1.close().get();

    Calendar cal = Calendar.getInstance();
    cal.setTimeZone(TimeZone.getTimeZone("UTC"));
    for (int i = 1; i < 15; i++) {
      if (channel1.getLatestCommittedOffsetToken() != null
          && channel1.getLatestCommittedOffsetToken().equals("1")) {
        ResultSet result = null;
        result =
            jdbcConnection
                .createStatement()
                .executeQuery(
                    String.format("select * from %s.%s.%s", testDb, TEST_SCHEMA, multiTableName));

        result.next();
        Assert.assertEquals("honk", result.getString("S"));
        Assert.assertEquals(1, result.getLong("I"));
        Assert.assertEquals(3.14, result.getFloat("F"), 0.0001);
        Assert.assertEquals(1.1, result.getFloat("TINYFLOAT"), 0.001);
        Assert.assertEquals("{\n" + "  \"e\": 2.7\n" + "}", result.getString("VAR"));
        Assert.assertEquals(timestamp * 1000, result.getTimestamp("T", cal).getTime());
        Assert.assertEquals(-1, TimeUnit.MILLISECONDS.toDays(result.getDate("D", cal).getTime()));
        return;
      } else {
        Thread.sleep(2000);
      }
    }
    Assert.fail("Row sequencer not updated before timeout");
  }

  @Test
  public void testNullableColumns() throws Exception {
    String multiTableName = "multi_column";
    jdbcConnection
        .createStatement()
        .execute(
            String.format(
                "create or replace table %s (s text, notnull text NOT NULL);", multiTableName));
    OpenChannelRequest request1 =
        OpenChannelRequest.builder("CHANNEL_MULTI1")
            .setDBName(testDb)
            .setSchemaName(TEST_SCHEMA)
            .setTableName(multiTableName)
            .setOnErrorOption(OpenChannelRequest.OnErrorOption.CONTINUE)
            .build();

    // Open a streaming ingest channel from the given client
    SnowflakeStreamingIngestChannel channel1 = client.openChannel(request1);

    OpenChannelRequest request2 =
        OpenChannelRequest.builder("CHANNEL_MULTI2")
            .setDBName(testDb)
            .setSchemaName(TEST_SCHEMA)
            .setTableName(multiTableName)
            .setOnErrorOption(OpenChannelRequest.OnErrorOption.CONTINUE)
            .build();

    // Open a streaming ingest channel from the given client
    SnowflakeStreamingIngestChannel channel2 = client.openChannel(request2);

    Map<String, Object> row1 = new HashMap<>();
    row1.put("s", "honk");
    row1.put("notnull", "1");
    verifyInsertValidationResponse(channel1.insertRow(row1, "1"));

    Map<String, Object> row2 = new HashMap<>();
    row2.put("s", null);
    row2.put("notnull", "2");
    verifyInsertValidationResponse(channel1.insertRow(row2, "2"));

    Map<String, Object> row3 = new HashMap<>();
    row3.put("notnull", "3");
    verifyInsertValidationResponse(channel1.insertRow(row3, "3"));

    verifyInsertValidationResponse(channel2.insertRow(row3, "1"));

    // Close the channel after insertion
    channel1.close().get();
    channel2.close().get();

    for (int i = 1; i < 15; i++) {
      if (channel1.getLatestCommittedOffsetToken() != null
          && channel1.getLatestCommittedOffsetToken().equals("3")
          && channel2.getLatestCommittedOffsetToken() != null
          && channel2.getLatestCommittedOffsetToken().equals("1")) {
        ResultSet result = null;
        result =
            jdbcConnection
                .createStatement()
                .executeQuery(
                    String.format(
                        "select * from %s.%s.%s order by notnull",
                        testDb, TEST_SCHEMA, multiTableName));

        result.next();
        Assert.assertEquals("honk", result.getString("S"));
        Assert.assertEquals("1", result.getString("NOTNULL"));

        result.next();
        Assert.assertNull(result.getString("S"));
        Assert.assertEquals("2", result.getString("NOTNULL"));

        result.next();
        Assert.assertNull(result.getString("S"));
        Assert.assertEquals("3", result.getString("NOTNULL"));

        result.next();
        Assert.assertNull(result.getString("S"));
        Assert.assertEquals("3", result.getString("NOTNULL"));
        return;
      } else {
        Thread.sleep(2000);
      }
    }
    Assert.fail("Row sequencer not updated before timeout");
  }

  @Test
  public void testMultiThread() throws Exception {
    String multiThreadTable = "multi_thread";
    jdbcConnection
        .createStatement()
        .execute(
            String.format("create or replace table %s (numcol NUMBER(10,2));", multiThreadTable));
    int numThreads = 20;
    int numRows = 10000;
    ExecutorService testThreadPool = Executors.newFixedThreadPool(numThreads);
    CompletableFuture[] futures = new CompletableFuture[numThreads];
    List<SnowflakeStreamingIngestChannel> channelList = new ArrayList<>();
    for (int i = 0; i < numThreads; i++) {
      final String channelName = "CHANNEL" + i;
      futures[i] =
          CompletableFuture.runAsync(
              () -> {
                OpenChannelRequest request =
                    OpenChannelRequest.builder(channelName)
                        .setDBName(testDb)
                        .setSchemaName(TEST_SCHEMA)
                        .setTableName(multiThreadTable)
                        .setOnErrorOption(OpenChannelRequest.OnErrorOption.CONTINUE)
                        .build();
                SnowflakeStreamingIngestChannel channel = client.openChannel(request);
                channelList.add(channel);
                for (int val = 1; val <= numRows; val++) {
                  Map<String, Object> row = new HashMap<>();
                  row.put("numcol", val);
                  verifyInsertValidationResponse(channel.insertRow(row, Integer.toString(val)));
                }
              },
              testThreadPool);
    }
    CompletableFuture joined = CompletableFuture.allOf(futures);
    joined.get();

    // Close the channel after insertion
    for (SnowflakeStreamingIngestChannel channel : channelList) {
      channel.close().get();
    }

    for (int i = 1; i < 15; i++) {
      if (channelList.stream()
          .allMatch(
              c ->
                  c.getLatestCommittedOffsetToken() != null
                      && c.getLatestCommittedOffsetToken().equals(Integer.toString(numRows)))) {
        ResultSet result =
            jdbcConnection
                .createStatement()
                .executeQuery(
                    String.format(
                        "select count(*), max(numcol), min(numcol) from %s.%s.%s",
                        testDb, TEST_SCHEMA, multiThreadTable));

        result.next();
        Assert.assertEquals(numRows * numThreads, result.getInt(1));
        Assert.assertEquals(Double.valueOf(numRows), result.getDouble(2), 10);
        Assert.assertEquals(1D, result.getDouble(3), 10);
        return;
      } else {
        Thread.sleep(3000);
      }
    }
    Assert.fail("Row sequencer not updated before timeout");
  }

  /**
   * Tests client's handling of invalidated channels
   *
   * @throws Exception
   */
  @Test
  public void testTwoClientsOneChannel() throws Exception {
    SnowflakeStreamingIngestClientInternal<?> clientA =
        (SnowflakeStreamingIngestClientInternal<?>)
            SnowflakeStreamingIngestClientFactory.builder("clientA").setProperties(prop).build();
    SnowflakeStreamingIngestClientInternal<?> clientB =
        (SnowflakeStreamingIngestClientInternal<?>)
            SnowflakeStreamingIngestClientFactory.builder("clientB").setProperties(prop).build();

    OpenChannelRequest requestA =
        OpenChannelRequest.builder("CHANNEL")
            .setDBName(testDb)
            .setSchemaName(TEST_SCHEMA)
            .setTableName(TEST_TABLE)
            .setOnErrorOption(OpenChannelRequest.OnErrorOption.CONTINUE)
            .build();

    // Open a streaming ingest channel from the given client
    SnowflakeStreamingIngestChannel channelA = clientA.openChannel(requestA);
    Map<String, Object> row = new HashMap<>();
    row.put("c1", "1");
    verifyInsertValidationResponse(channelA.insertRow(row, "1"));
    clientA.flush(false).get();

    // ClientB opens channel and invalidates ClientA
    SnowflakeStreamingIngestChannel channelB = clientB.openChannel(requestA);
    row.put("c1", "2");
    verifyInsertValidationResponse(channelB.insertRow(row, "2"));
    clientB.flush(false).get();

    // ClientA tries to write, but will fail to register because it's invalid
    row.put("c1", "3");
    verifyInsertValidationResponse(channelA.insertRow(row, "3"));
    clientA.flush(false).get();

    // ClientA will error trying to write to invalid channel
    try {
      row.put("c1", "4");
      verifyInsertValidationResponse(channelA.insertRow(row, "4"));
      Assert.fail();
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.INVALID_CHANNEL.getMessageCode(), e.getVendorCode());
    }

    // ClientA will not be kept down, reopens the channel, invalidating ClientB
    SnowflakeStreamingIngestChannel channelA2 = clientA.openChannel(requestA);

    // ClientB tries to write but the register will be rejected, invalidating the channel
    row.put("c1", "5");
    verifyInsertValidationResponse(channelB.insertRow(row, "5"));
    clientB.flush(false).get();

    // ClientB fails to write to invalid channel
    try {
      row.put("c1", "6");
      verifyInsertValidationResponse(channelB.insertRow(row, "6"));
      Assert.fail();
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.INVALID_CHANNEL.getMessageCode(), e.getVendorCode());
    }

    // ClientA is victorious, writes to table
    row.put("c1", "7");
    verifyInsertValidationResponse(channelA2.insertRow(row, "7"));
    clientA.flush(false).get();
    row.put("c1", "8");
    verifyInsertValidationResponse(channelA2.insertRow(row, "8"));

    // Close the channel after insertion
    channelA2.close().get();

    for (int i = 1; i < 15; i++) {
      if (channelA2.getLatestCommittedOffsetToken() != null
          && channelA2.getLatestCommittedOffsetToken().equals("8")) {
        ResultSet result =
            jdbcConnection
                .createStatement()
                .executeQuery(
                    String.format(
                        "select count(*) from %s.%s.%s", testDb, TEST_SCHEMA, TEST_TABLE));
        result.next();
        Assert.assertEquals(4, result.getLong(1));

        ResultSet result2 =
            jdbcConnection
                .createStatement()
                .executeQuery(
                    String.format(
                        "select * from %s.%s.%s order by c1", testDb, TEST_SCHEMA, TEST_TABLE));
        result2.next();
        Assert.assertEquals("1", result2.getString(1));
        result2.next();
        Assert.assertEquals("2", result2.getString(1));
        result2.next();
        Assert.assertEquals("7", result2.getString(1));
        result2.next();
        Assert.assertEquals("8", result2.getString(1));
        return;
      }
      Thread.sleep(500);
    }
  }

  @Test
  public void testAbortOnErrorOption() throws Exception {
    String onErrorOptionTable = "abort_on_error_option";
    jdbcConnection
        .createStatement()
        .execute(String.format("create or replace table %s (c1 int);", onErrorOptionTable));

    OpenChannelRequest request =
        OpenChannelRequest.builder("CHANNEL")
            .setDBName(testDb)
            .setSchemaName(TEST_SCHEMA)
            .setTableName(onErrorOptionTable)
            .setOnErrorOption(OpenChannelRequest.OnErrorOption.ABORT)
            .build();

    // Open a streaming ingest channel from the given client
    SnowflakeStreamingIngestChannel channel = client.openChannel(request);
    Map<String, Object> row1 = new HashMap<>();
    row1.put("c1", 1);
    channel.insertRow(row1, "1");
    Map<String, Object> row2 = new HashMap<>();
    row2.put("c1", 2);
    channel.insertRow(row2, "2");
    Map<String, Object> row3 = new HashMap<>();
    row3.put("c1", "a");
    try {
      channel.insertRow(row3, "3");
      Assert.fail("insert should fail");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.INVALID_ROW.getMessageCode(), e.getVendorCode());
    }
    try {
      channel.insertRows(Arrays.asList(row1, row2, row3), "6");
      Assert.fail("insert should fail");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.INVALID_ROW.getMessageCode(), e.getVendorCode());
    }
    Map<String, Object> row7 = new HashMap<>();
    row7.put("c1", 7);
    channel.insertRow(row7, "7");

    // Close the channel after insertion
    channel.close().get();

    for (int i = 1; i < 15; i++) {
      if (channel.getLatestCommittedOffsetToken() != null
          && channel.getLatestCommittedOffsetToken().equals("7")) {
        ResultSet result =
            jdbcConnection
                .createStatement()
                .executeQuery(
                    String.format(
                        "select count(c1), min(c1), max(c1) from %s.%s.%s",
                        testDb, TEST_SCHEMA, onErrorOptionTable));
        result.next();
        Assert.assertEquals("3", result.getString(1));
        Assert.assertEquals("1", result.getString(2));
        Assert.assertEquals("7", result.getString(3));
        return;
      }
      Thread.sleep(1000);
    }
    Assert.fail("Row sequencer not updated before timeout");
  }

  @Test
  public void testChannelClose() throws Exception {
    OpenChannelRequest request1 =
        OpenChannelRequest.builder("CHANNEL")
            .setDBName(testDb)
            .setSchemaName(TEST_SCHEMA)
            .setTableName(TEST_TABLE)
            .setOnErrorOption(OpenChannelRequest.OnErrorOption.CONTINUE)
            .build();

    // Open a streaming ingest channel from the given client
    SnowflakeStreamingIngestChannel channel1 = client.openChannel(request1);
    for (int val = 0; val < 1000; val++) {
      Map<String, Object> row = new HashMap<>();
      row.put("c1", Integer.toString(val));
      verifyInsertValidationResponse(channel1.insertRow(row, Integer.toString(val)));
    }

    // Close the channel to make sure everything is committed
    channel1.close().get();
  }

  @Test
  public void testNullValuesOnMultiDataTypes() throws Exception {
    String nullValuesOnMultiDataTypesTable = "test_null_values_on_multi_data_types";
    jdbcConnection
        .createStatement()
        .execute(
            String.format(
                "create or replace table %s (c0 int, c1 number, c2 decimal, c3 bigint, c4 float,"
                    + " c5 real, c6 varchar, c7 char, c8 string, c9 text, c10 binary, c11 boolean,"
                    + " c12 date, c13 time, c14 timestamp, c15 variant, c16 object, c17 array);",
                nullValuesOnMultiDataTypesTable));

    OpenChannelRequest request =
        OpenChannelRequest.builder("CHANNEL")
            .setDBName(testDb)
            .setSchemaName(TEST_SCHEMA)
            .setTableName(nullValuesOnMultiDataTypesTable)
            .setOnErrorOption(OpenChannelRequest.OnErrorOption.CONTINUE)
            .build();

    // Open a streaming ingest channel from the given client
    SnowflakeStreamingIngestChannel channel1 = client.openChannel(request);
    for (int val = 0; val < 1000; val++) {
      Map<String, Object> row = new HashMap<>();
      row.put("c0", val);
      verifyInsertValidationResponse(channel1.insertRow(row, Integer.toString(val)));
    }

    // Sleep for a few seconds to make sure a flush has finished
    Thread.sleep(5000);

    // Insert one row with all NULLs to make sure it works
    Map<String, Object> row = new HashMap<>();
    for (int idx = 0; idx < 18; idx++) {
      row.put("c" + idx, null);
    }
    verifyInsertValidationResponse(channel1.insertRow(row, "0"));

    // Close the channel to make sure everything is committed
    channel1.close().get();

    // Query the table in the end to make sure everything is readable
    ResultSet result =
        jdbcConnection
            .createStatement()
            .executeQuery(
                String.format(
                    "select top 1 c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14,"
                        + " c15, c16, c17 from %s.%s.%s",
                    testDb, TEST_SCHEMA, nullValuesOnMultiDataTypesTable));
    result.next();
    for (int idx = 1; idx < 18; idx++) {
      Assert.assertNull(result.getObject(idx));
    }
  }

  /** Verify the insert validation response and throw the exception if needed */
  private void verifyInsertValidationResponse(InsertValidationResponse response) {
    if (response.hasErrors()) {
      throw response.getInsertErrors().get(0).getException();
    }
  }

  private void createTableForInterleavedTest(String tableName) {
    try {
      jdbcConnection
          .createStatement()
          .execute(
              String.format(
                  "create or replace table %s (num NUMBER(38,0), str VARCHAR);", tableName));
    } catch (SQLException e) {
      throw new RuntimeException("Cannot create table " + tableName, e);
    }
  }

  private SnowflakeStreamingIngestChannel openChannel(String tableName, String channelName) {
    OpenChannelRequest request =
        OpenChannelRequest.builder(channelName)
            .setDBName(testDb)
            .setSchemaName(TEST_SCHEMA)
            .setTableName(tableName)
            .setOnErrorOption(OpenChannelRequest.OnErrorOption.CONTINUE)
            .build();

    // Open a streaming ingest channel from the given client
    return client.openChannel(request);
  }

  private void produceRowsForInterleavedTest(
      SnowflakeStreamingIngestChannel channel, String strPrefix, int rowNumber) {
    // Insert a few rows into the channel
    for (int val = 0; val < rowNumber; val++) {
      Map<String, Object> row = new HashMap<>();
      row.put("num", val);
      row.put("str", strPrefix + "_value_" + val);
      verifyInsertValidationResponse(channel.insertRow(row, Integer.toString(val)));
    }
  }

  private void waitChannelFlushed(SnowflakeStreamingIngestChannel channel, int numberOfRows) {
    String latestCommittedOffsetToken = null;
    for (int i = 1; i < 15; i++) {
      latestCommittedOffsetToken = channel.getLatestCommittedOffsetToken();
      if (latestCommittedOffsetToken != null
          && latestCommittedOffsetToken.equals(Integer.toString(numberOfRows - 1))) {
        return;
      }
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        throw new RuntimeException(
            "Interrupted waitChannelFlushed for " + numberOfRows + " rows", e);
      }
    }
    Assert.fail(
        "Row sequencer not updated before timeout, latestCommittedOffsetToken: "
            + latestCommittedOffsetToken);
  }

  private void verifyInterleavedResult(int rowNumber, String tableName, String strPrefix) {
    try {
      ResultSet result =
          jdbcConnection
              .createStatement()
              .executeQuery(
                  String.format(
                      "select * from %s.%s.%s where str ilike '%s%%' order by num",
                      testDb, TEST_SCHEMA, tableName, strPrefix));
      for (int val = 0; val < rowNumber; val++) {
        result.next();
        Assert.assertEquals(val, result.getLong("NUM"));
        Assert.assertEquals(strPrefix + "_value_" + val, result.getString("STR"));
      }
    } catch (SQLException e) {
      throw new RuntimeException("Cannot verifyInterleavedResult for " + tableName, e);
    }
  }

  private void verifyTableRowCount(int rowNumber, String tableName) {
    try {
      ResultSet resultCount =
          jdbcConnection
              .createStatement()
              .executeQuery(
                  String.format("select count(*) from %s.%s.%s", testDb, TEST_SCHEMA, tableName));
      resultCount.next();
      Assert.assertEquals(rowNumber, resultCount.getLong(1));
    } catch (SQLException e) {
      throw new RuntimeException("Cannot verifyTableRowCount for " + tableName, e);
    }
  }

  @Test
  public void testDataTypes() {
    String tableName = "t_data_types";
    try {
      jdbcConnection
          .createStatement()
          .execute(
              String.format(
                  "create or replace table %s (num NUMBER, -- default FIXED\n"
                      + "                                    num_10_5 NUMBER(10, 5), -- FIXED with"
                      + " non zero scale: big dec\n"
                      + "                                    num_38_0 NUMBER(38, 0), -- FIXED sb16:"
                      + " big dec\n"
                      + "                                    num_2_0 NUMBER(2, 0), -- FIXED sb1\n"
                      + "                                    num_4_0 NUMBER(4, 0), -- FIXED sb2\n"
                      + "                                    num_9_0 NUMBER(9, 0), -- FIXED sb4\n"
                      + "                                    num_18_0 NUMBER(18, 0), -- FIXED sb8\n"
                      + "                                    num_float FLOAT, -- REAL\n"
                      + "                                    str_varchar VARCHAR(256), -- varchar,"
                      + " char, any, string, text\n"
                      + "                                    bin BINARY(256), -- BINARY, VARBINARY:"
                      + " hex\n"
                      + "                                    bl BOOLEAN, -- BOOLEAN: true/false\n"
                      + "                                    var VARIANT, -- VARIANT: json\n"
                      + "                                    obj OBJECT, -- OBJECT: json map\n"
                      + "                                    arr ARRAY, -- ARRAY: array of"
                      + " variants\n"
                      + "                                    epochdays DATE,\n"
                      + "                                    timesec TIME(0),\n"
                      + "                                    timenano TIME(9),\n"
                      + "                                    epochsec TIMESTAMP_NTZ(0),\n"
                      + "                                    epochnano TIMESTAMP_NTZ(9));",
                  tableName));
    } catch (SQLException e) {
      throw new RuntimeException("Cannot create table " + tableName, e);
    }

    SnowflakeStreamingIngestChannel channel = openChannel(tableName, "CHANNEL");

    Random r = new Random();
    for (int val = 0; val < 10000; val++) {
      verifyInsertValidationResponse(channel.insertRow(getRandomRow(r), Integer.toString(val)));
    }
    waitChannelFlushed(channel, 10000);
  }

  private static Map<String, Object> getRandomRow(Random r) {
    Map<String, Object> row = new HashMap<>();
    row.put("num", r.nextInt());
    row.put("num_10_5", nextFloat(r));
    row.put(
        "num_38_0",
        new BigDecimal("" + nextLongOfPrecision(r, 18) + Math.abs(nextLongOfPrecision(r, 18))));
    row.put("num_2_0", r.nextInt(100));
    row.put("num_4_0", r.nextInt(10000));
    row.put("num_9_0", r.nextInt(1000000000));
    row.put("num_18_0", nextLongOfPrecision(r, 18));
    row.put("num_float", nextFloat(r));
    row.put("str_varchar", nextString(r));
    row.put("bin", nextBytes(r));
    row.put("bl", r.nextBoolean());
    row.put("var", nextJson(r));
    row.put("obj", nextJson(r));
    row.put("arr", Arrays.asList(r.nextInt(100), r.nextInt(100), r.nextInt(100)));
    row.put(
        "epochdays",
        String.valueOf(Math.abs(r.nextInt()) % (18963 * 24 * 60 * 60))); // DATE, 02.12.2021
    row.put(
        "timesec",
        String.valueOf(
            (r.nextInt(11) * 60 * 60 + r.nextInt(59) * 60 + r.nextInt(59)) * 10000
                + r.nextInt(9999))); // TIME(4), 05:12:43.4536
    row.put(
        "timenano",
        String.valueOf(
            (14 * 60 * 60 + 26 * 60 + 34) * 1000000000L
                + 437582643)); // TIME(9), 14:26:34.437582643
    row.put(
        "epochsec",
        String.valueOf(
            Math.abs(r.nextLong()) % 1638459438)); // TIMESTAMP_LTZ(0), 02.12.2021 15:37:18
    row.put(
        "epochnano",
        String.format(
            "%d%d",
            Math.abs(r.nextInt()) % 1999999999,
            100000000
                + Math.abs(
                    r.nextInt(899999999)))); // TIMESTAMP_LTZ(9), 18.05.2033 03:33:19.999999999
    return row;
  }

  private static long nextLongOfPrecision(Random r, int precision) {
    return r.nextLong() % Math.round(Math.pow(10, precision));
  }

  private static String nextString(Random r) {
    return new String(nextBytes(r));
  }

  private static byte[] nextBytes(Random r) {
    byte[] bin = new byte[128];
    r.nextBytes(bin);
    for (int i = 0; i < bin.length; i++) {
      bin[i] = (byte) (Math.abs(bin[i]) % 25 + 97); // ascii letters
    }
    return bin;
  }

  private static double nextFloat(Random r) {
    return (r.nextLong() % Math.round(Math.pow(10, 10))) / 100000d;
  }

  private static String nextJson(Random r) {
    return String.format(
        "{ \"%s\": %d, \"%s\": \"%s\", \"%s\": null, \"%s\": { \"%s\": %f, \"%s\": \"%s\", \"%s\":"
            + " null } }",
        nextString(r),
        r.nextInt(),
        nextString(r),
        nextString(r),
        nextString(r),
        nextString(r),
        nextString(r),
        r.nextFloat(),
        nextString(r),
        nextString(r),
        nextString(r));
  }
}
