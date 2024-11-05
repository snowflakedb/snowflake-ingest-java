package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.utils.Constants.BLOB_NO_HEADER;
import static net.snowflake.ingest.utils.Constants.COMPRESS_BLOB_TWICE;
import static net.snowflake.ingest.utils.Constants.DROP_CHANNEL_ENDPOINT;
import static net.snowflake.ingest.utils.Constants.REGISTER_BLOB_ENDPOINT;
import static net.snowflake.ingest.utils.Constants.USER;
import static net.snowflake.ingest.utils.ParameterProvider.BDEC_PARQUET_COMPRESSION_ALGORITHM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.atLeastOnce;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
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
import net.snowflake.ingest.connection.RequestBuilder;
import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.OffsetTokenVerificationFunction;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import net.snowflake.ingest.utils.Constants;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.HttpUtil;
import net.snowflake.ingest.utils.ParameterProvider;
import net.snowflake.ingest.utils.SFException;
import net.snowflake.ingest.utils.SnowflakeURL;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

/** Example streaming ingest sdk integration test */
@RunWith(Parameterized.class)
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

  @Parameters
  public static Iterable<Object[]> getParameterPermutations() {
    return Arrays.asList(
        new Object[][] {{"GZIP", false}, {"ZSTD", false}, {"GZIP", true}, {"ZSTD", true}});
  }

  @Parameter(0)
  public String compressionAlgorithm;

  @Parameter(1)
  public boolean enableIcebergStreaming;

  private static final OffsetTokenVerificationFunction offsetTokenVerificationFunction =
      (prevBatchEndOffset, curBatchStartOffset, curBatchEndOffset, rowCount) -> {
        boolean isMatch = true;

        if (curBatchStartOffset != null) {
          try {
            long curStart = Long.parseLong(curBatchStartOffset);
            long curEnd = Long.parseLong(curBatchEndOffset);

            // We verify that the end_offset - start_offset + 1 = row_count
            if (curEnd - curStart + 1 != rowCount) {
              isMatch = false;
            }

            // We verify that start_offset_of_current_batch = end_offset_of_previous_batch+1
            if (prevBatchEndOffset != null) {
              long prevEnd = Long.parseLong(prevBatchEndOffset);
              if (curStart != prevEnd + 1) {
                isMatch = false;
              }
            }
          } catch (NumberFormatException ignored) {
            // Do nothing since we can't compare the offset
          }
        }

        return isMatch;
      };

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

    // Test without role param
    prop = TestUtils.getProperties(Constants.BdecVersion.THREE, true);
    prop.setProperty(BDEC_PARQUET_COMPRESSION_ALGORITHM, compressionAlgorithm);
    client =
        (SnowflakeStreamingIngestClientInternal<?>)
            SnowflakeStreamingIngestClientFactory.builder("client1").setProperties(prop).build();
  }

  @After
  public void afterAll() throws Exception {
    if (client != null) {
      client.close();
    }
    jdbcConnection.createStatement().execute(String.format("drop database %s", testDb));
  }

  @Test
  public void testSimpleIngest() throws Exception {
    // TODO @rcheng - dont want to change factory, so inject
    SnowflakeURL url = new SnowflakeURL(TestUtils.getAccountURL());
    RequestBuilder requestBuilder =
        Mockito.spy(
            new RequestBuilder(
                url,
                TestUtils.getUser(),
                TestUtils.getKeyPair(),
                HttpUtil.getHttpClient(url.getAccount()),
                enableIcebergStreaming,
                "testrequestbuilder"));
    client.injectRequestBuilder(requestBuilder);

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

    // verify expected request sent to server
    String[] expectedPayloadParams = {"request_id", "blobs", "role", "blob_stats"};
    for (String expectedParam : expectedPayloadParams) {
      Mockito.verify(requestBuilder, atLeastOnce())
          .generateStreamingIngestPostRequest(
              ArgumentMatchers.contains(expectedParam),
              ArgumentMatchers.refEq(REGISTER_BLOB_ENDPOINT),
              ArgumentMatchers.refEq("register blob"));
    }

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
  public void testDropChannel() throws Exception {
    SnowflakeURL url = new SnowflakeURL(TestUtils.getAccountURL());
    RequestBuilder requestBuilder =
        Mockito.spy(
            new RequestBuilder(
                url,
                TestUtils.getUser(),
                TestUtils.getKeyPair(),
                HttpUtil.getHttpClient(url.getAccount()),
                enableIcebergStreaming,
                "testrequestbuilder"));
    client.injectRequestBuilder(requestBuilder);

    OpenChannelRequest request1 =
        OpenChannelRequest.builder("CHANNEL")
            .setDBName(testDb)
            .setSchemaName(TEST_SCHEMA)
            .setTableName(TEST_TABLE)
            .setOnErrorOption(OpenChannelRequest.OnErrorOption.CONTINUE)
            .build();

    // Open a streaming ingest channel from the given client
    SnowflakeStreamingIngestChannel channel1 = client.openChannel(request1);
    // Close the channel after insertion
    channel1.close(true).get();

    // verify expected request sent to server
    Mockito.verify(requestBuilder)
        .generateStreamingIngestPostRequest(
            ArgumentMatchers.contains("client_sequencer"),
            ArgumentMatchers.refEq(DROP_CHANNEL_ENDPOINT),
            ArgumentMatchers.refEq("drop channel"));
  }

  @Test
  public void testParameterOverrides() throws Exception {
    Map<String, Object> parameterMap = new HashMap<>();
    parameterMap.put(ParameterProvider.MAX_CLIENT_LAG, "3 seconds");
    parameterMap.put(ParameterProvider.BUFFER_FLUSH_CHECK_INTERVAL_IN_MILLIS, 50L);
    parameterMap.put(ParameterProvider.INSERT_THROTTLE_THRESHOLD_IN_PERCENTAGE, 1);
    parameterMap.put(ParameterProvider.INSERT_THROTTLE_THRESHOLD_IN_BYTES, 1024);
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
    iter.accept(
        i ->
            channels[i - 1] =
                openChannel(
                    INTERLEAVED_TABLE_PREFIX + i,
                    "CHANNEL",
                    OpenChannelRequest.OnErrorOption.ABORT));

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
    iter.accept(
        i ->
            channels[i - 1] =
                openChannel(
                    INTERLEAVED_CHANNEL_TABLE,
                    "CHANNEL_" + i,
                    OpenChannelRequest.OnErrorOption.ABORT));

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
    row.put("ttzsmall", "2021-01-01T01:00:00.123-03:00");
    row.put("ttzbig", "2021-01-01T09:00:00.12345678-03:00");
    row.put("tsmall", "01:00:00.123");
    row.put("tbig", "09:00:00.12345678");
    row.put("tntzsmall", "1609462800123");
    row.put("tntzbig", "1609462800123450000");
    verifyInsertValidationResponse(channel1.insertRow(row, null));
    row.put("ttzsmall", "2021-01-01T10:00:00.123+07:00");
    row.put("ttzbig", "2021-01-01T19:00:00.12345678-03:00");
    row.put("tsmall", "02:00:00.123");
    row.put("tbig", "10:00:00.12345678");
    row.put("tntzsmall", "1709462800123");
    row.put("tntzbig", "170946280212345000");
    verifyInsertValidationResponse(channel1.insertRow(row, null));
    row.put("ttzsmall", "2021-01-01T05:00:00+01:00");
    row.put("ttzbig", "2021-01-01T23:00:00.12345678-03:00");
    row.put("tsmall", "03:00:00.123");
    row.put("tbig", "11:00:00.12345678");
    row.put("tntzsmall", "1809462800123");
    row.put("tntzbig", "2031-01-01T09:00:00.123456780");
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
    row.put("d", "1969-12-31T00:00:00");
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
  public void testOpenChannelOffsetToken() throws Exception {
    String tableName = "offsetTokenTest";
    jdbcConnection
        .createStatement()
        .execute(String.format("create or replace table %s (s text);", tableName));
    OpenChannelRequest request1 =
        OpenChannelRequest.builder("TEST_CHANNEL")
            .setDBName(testDb)
            .setSchemaName(TEST_SCHEMA)
            .setTableName(tableName)
            .setOnErrorOption(OpenChannelRequest.OnErrorOption.CONTINUE)
            .setOffsetToken("TEST_OFFSET")
            .build();

    // Open a streaming ingest channel from the given client
    SnowflakeStreamingIngestChannel channel1 = client.openChannel(request1);

    // Close the channel after insertion
    channel1.close().get();

    for (int i = 1; i < 15; i++) {
      if (channel1.getLatestCommittedOffsetToken() != null
          && channel1.getLatestCommittedOffsetToken().equals("TEST_OFFSET")) {
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
    List<SnowflakeStreamingIngestChannel> channelList =
        Collections.synchronizedList(new ArrayList<>());
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
      Assert.assertEquals(ErrorCode.INVALID_VALUE_ROW.getMessageCode(), e.getVendorCode());
    }
    try {
      channel.insertRows(Arrays.asList(row1, row2, row3), "6");
      Assert.fail("insert should fail");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.INVALID_VALUE_ROW.getMessageCode(), e.getVendorCode());
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
  public void testAbortOnErrorSkipBatch() throws Exception {
    String onErrorOptionTable = "skip_batch_on_error_option";
    jdbcConnection
        .createStatement()
        .execute(String.format("create or replace table %s (c1 int);", onErrorOptionTable));

    OpenChannelRequest request =
        OpenChannelRequest.builder("CHANNEL")
            .setDBName(testDb)
            .setSchemaName(TEST_SCHEMA)
            .setTableName(onErrorOptionTable)
            .setOnErrorOption(OpenChannelRequest.OnErrorOption.SKIP_BATCH)
            .build();

    // Open a streaming ingest channel from the given client
    SnowflakeStreamingIngestChannel channel = client.openChannel(request);
    Map<String, Object> row1 = new HashMap<>();
    row1.put("c1", 1);
    Map<String, Object> row2 = new HashMap<>();
    row2.put("c1", 2);
    Map<String, Object> row3 = new HashMap<>();
    row3.put("c1", "a");

    verifyInsertValidationResponse(channel.insertRow(row1, "1"));

    InsertValidationResponse response = channel.insertRows(Arrays.asList(row1, row2, row3), "3");
    Assert.assertTrue(response.hasErrors());

    Map<String, Object> row4 = new HashMap<>();
    row4.put("c1", 4);
    verifyInsertValidationResponse(channel.insertRow(row4, "4"));

    // Close the channel after insertion
    channel.close().get();

    for (int i = 1; i < 15; i++) {
      if (channel.getLatestCommittedOffsetToken() != null
          && channel.getLatestCommittedOffsetToken().equals("4")) {
        ResultSet result =
            jdbcConnection
                .createStatement()
                .executeQuery(
                    String.format(
                        "select count(c1), min(c1), max(c1) from %s.%s.%s",
                        testDb, TEST_SCHEMA, onErrorOptionTable));
        result.next();
        Assert.assertEquals("2", result.getString(1));
        Assert.assertEquals("1", result.getString(2));
        Assert.assertEquals("4", result.getString(3));
        return;
      }
      Thread.sleep(1000);
    }
    Assert.fail("Row sequencer not updated before timeout");
  }

  @Test
  public void testInsertRowsWithValidStartOffsetToken() throws Exception {
    String insertRowsWithValidStartOffsetToken = "insert_rows_with_valid_start_offset_token";
    jdbcConnection
        .createStatement()
        .execute(
            String.format(
                "create or replace table %s (c1 int);", insertRowsWithValidStartOffsetToken));

    OpenChannelRequest request =
        OpenChannelRequest.builder("CHANNEL")
            .setDBName(testDb)
            .setSchemaName(TEST_SCHEMA)
            .setTableName(insertRowsWithValidStartOffsetToken)
            .setOnErrorOption(OpenChannelRequest.OnErrorOption.CONTINUE)
            .setOffsetTokenVerificationFunction(offsetTokenVerificationFunction)
            .build();

    // Open a streaming ingest channel from the given client
    SnowflakeStreamingIngestChannel channel = client.openChannel(request);
    Map<String, Object> row1 = new HashMap<>();
    row1.put("c1", 1);
    Map<String, Object> row2 = new HashMap<>();
    row2.put("c1", 2);
    Map<String, Object> row3 = new HashMap<>();
    row3.put("c1", "a");

    verifyInsertValidationResponse(channel.insertRow(row1, "1"));

    InsertValidationResponse response =
        channel.insertRows(Arrays.asList(row1, row2, row3), "2", "4");
    Assert.assertTrue(response.hasErrors());

    // Close the channel after insertion
    channel.close().get();

    for (int i = 1; i < 15; i++) {
      if (channel.getLatestCommittedOffsetToken() != null
          && channel.getLatestCommittedOffsetToken().equals("4")) {
        ResultSet result =
            jdbcConnection
                .createStatement()
                .executeQuery(
                    String.format(
                        "select count(c1), min(c1), max(c1) from %s.%s.%s",
                        testDb, TEST_SCHEMA, insertRowsWithValidStartOffsetToken));
        result.next();
        Assert.assertEquals("3", result.getString(1));
        Assert.assertEquals("1", result.getString(2));
        Assert.assertEquals("2", result.getString(3));
        return;
      }
      Thread.sleep(1000);
    }
    Assert.fail("Row sequencer not updated before timeout");
  }

  @Test
  public void testInsertRowsWithInvalidStartOffsetToken() throws Exception {
    String insertRowsWithInvalidStartOffsetToken = "insert_rows_with_invalid_start_offset_token";
    jdbcConnection
        .createStatement()
        .execute(
            String.format(
                "create or replace table %s (c1 int);", insertRowsWithInvalidStartOffsetToken));

    OpenChannelRequest request =
        OpenChannelRequest.builder("CHANNEL")
            .setDBName(testDb)
            .setSchemaName(TEST_SCHEMA)
            .setTableName(insertRowsWithInvalidStartOffsetToken)
            .setOnErrorOption(OpenChannelRequest.OnErrorOption.CONTINUE)
            .setOffsetTokenVerificationFunction(offsetTokenVerificationFunction)
            .build();

    // Open a streaming ingest channel from the given client
    SnowflakeStreamingIngestChannel channel = client.openChannel(request);
    Map<String, Object> row1 = new HashMap<>();
    row1.put("c1", 1);
    Map<String, Object> row2 = new HashMap<>();
    row2.put("c1", 2);

    // end-start != rowcount
    channel.insertRows(Arrays.asList(row1, row2), "1", "4");

    // offset != prevEnd+1
    channel.insertRow(row1, "7");

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
                        testDb, TEST_SCHEMA, insertRowsWithInvalidStartOffsetToken));
        result.next();
        Assert.assertEquals("3", result.getString(1));
        Assert.assertEquals("1", result.getString(2));
        Assert.assertEquals("2", result.getString(3));
        return;
      }
      Thread.sleep(1000);
    }
    Assert.fail("Row sequencer not updated before timeout");
  }

  @Test
  public void testOffsetTokenVerificationFunction() {
    Assert.assertTrue(offsetTokenVerificationFunction.verify("1", "2", "4", 3));
    Assert.assertTrue(offsetTokenVerificationFunction.verify(null, "2", "4", 3));
    Assert.assertTrue(offsetTokenVerificationFunction.verify("1", "2", null, 3));
    Assert.assertTrue(offsetTokenVerificationFunction.verify("a", "2", "4", 3));
    Assert.assertFalse(offsetTokenVerificationFunction.verify("1", "3", "4", 3));
    Assert.assertFalse(offsetTokenVerificationFunction.verify("2", "1", "4", 3));
    Assert.assertFalse(offsetTokenVerificationFunction.verify("1", "2", "4", 2));
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

  @Test
  public void testFailureHalfwayThroughRowProcessing() throws Exception {
    String tableName = "failure_halfway_through_table";
    jdbcConnection
        .createStatement()
        .execute(
            String.format("create or replace table %s(c1 varchar(1), c2 varchar(1));", tableName));
    SnowflakeStreamingIngestChannel channel =
        openChannel(tableName, "channel1", OpenChannelRequest.OnErrorOption.CONTINUE);

    // Row will be ingested
    Map<String, Object> row = new LinkedHashMap<>();
    row.put("c1", null);
    row.put("c2", "b");
    channel.insertRow(row, "offset0");

    // Row will not be ingested, its contribution to nullCount and other statistics should be
    // discarded
    row = new LinkedHashMap<>();
    row.put("c1", null);
    row.put("c2", "qa");
    channel.insertRow(row, "offset1");

    // Row will be ingested
    row = new LinkedHashMap<>();
    row.put("c1", null);
    row.put("c2", null);
    channel.insertRow(row, "offset2");

    TestUtils.waitForOffset(channel, "offset2");

    jdbcConnection.createStatement().execute(String.format("alter table %s migrate;", tableName));

    ResultSet rs =
        jdbcConnection.createStatement().executeQuery(String.format("select * from %s", tableName));
    rs.next();
    Assert.assertEquals(null, rs.getString(1));
    Assert.assertEquals("b", rs.getString(2));

    rs.next();
    Assert.assertEquals(null, rs.getString(1));
    Assert.assertEquals(null, rs.getString(2));
  }

  @Test
  public void testColumnNameQuotes() throws Exception {
    final String tableName = "T_COLUMN_WITH_QUOTES";
    final String unquotedColumn = "c1"; // user name <C1>
    final String quotedColumn = "\"c 2\""; // user name <c 2>
    final String doubleQuotedColumn = "\"c\"\"a\\ \"\"\\ 3\""; // user name <c"a\ "\ 3>
    final String escapedSpacesColumn1 =
        "c\\ 4"; // space is escaped with '\' in Snowflake SQL, user name <C 4>
    final String escapedSpacesColumn2 =
        "c\\ 5"; // space is escaped with '\' in Snowflake SQL, user name <C 5>
    jdbcConnection
        .createStatement()
        .execute(
            String.format(
                "create or replace table %s.%s.%s (%s NUMBER(38,0), %s NUMBER(38,0), %s"
                    + " NUMBER(38,0), %s NUMBER(38,0), %s NUMBER(38,0));",
                testDb,
                TEST_SCHEMA,
                tableName,
                unquotedColumn,
                quotedColumn,
                doubleQuotedColumn,
                escapedSpacesColumn1,
                escapedSpacesColumn2));

    OpenChannelRequest request1 =
        OpenChannelRequest.builder("CHANNEL")
            .setDBName(testDb)
            .setSchemaName(TEST_SCHEMA)
            .setTableName(tableName)
            .setOnErrorOption(OpenChannelRequest.OnErrorOption.CONTINUE)
            .build();

    // Open a streaming ingest channel from the given client
    SnowflakeStreamingIngestChannel channel1 = client.openChannel(request1);
    for (int val = 0; val < 10; val++) {
      Map<String, Object> row = new HashMap<>();
      row.put(unquotedColumn, val);
      row.put(quotedColumn, val);
      row.put(doubleQuotedColumn, val);
      row.put(escapedSpacesColumn1, val);
      // space escape '\' is not really required, because Java always has its own outer double
      // quotes.
      // hence, it happens to also work w/o the space escape after the internal literal
      // normalisation.
      String escapedSpacesColumn2NoSpaceEscape = escapedSpacesColumn2.replace("\\", "");
      row.put(escapedSpacesColumn2NoSpaceEscape, val);
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
                        "select %s, %s, %s, %s, %s from %s.%s.%s order by %s",
                        unquotedColumn,
                        quotedColumn,
                        doubleQuotedColumn,
                        escapedSpacesColumn1,
                        escapedSpacesColumn2,
                        testDb,
                        TEST_SCHEMA,
                        tableName,
                        unquotedColumn));
        for (int val = 0; val < 10; val++) {
          result.next();
          for (int index = 1; index <= 5; index++) Assert.assertEquals(val, result.getInt(index));
        }
        return;
      }
      Thread.sleep(500);
    }
    Assert.fail("Row sequencer not updated before timeout");
  }

  @Test
  public void testTableColumnEvolution() throws Exception {
    final int rowNum = 100;
    final String tableName = TEST_TABLE + "_TEST_COL_EVOL";
    final String tableFullName = String.format("%s.%s.%s", testDb, TEST_SCHEMA, tableName);
    final String streamName =
        String.format("%s.%s.%s", testDb, TEST_SCHEMA, "STREAM_TEST_COL_EVOL");

    executeQuery("create or replace table %s (c1 varchar, c2 varchar, c3 varchar);", tableName);
    int channelNum = 1;

    // insert first 3 columns
    ingestByColIndexAndVerifyTable(tableName, channelNum, Arrays.asList(1, 2, 3), rowNum);
    channelNum++;

    // add one column
    executeQuery(String.format("alter table %s add column c4 varchar;", tableFullName));
    ingestByColIndexAndVerifyTable(tableName, channelNum, Arrays.asList(1, 2, 3, 4), rowNum);
    channelNum++;

    // remove first column
    executeQuery(String.format("alter table %s drop column c1;", tableFullName));
    ingestByColIndexAndVerifyTable(tableName, channelNum, Arrays.asList(2, 3, 4), rowNum);
    channelNum++;

    // remove last column
    executeQuery(String.format("alter table %s drop column c4;", tableFullName));
    ingestByColIndexAndVerifyTable(tableName, channelNum, Arrays.asList(2, 3), rowNum);
    channelNum++;

    // add last column
    executeQuery(String.format("alter table %s add column c5 varchar;", tableFullName));
    ingestByColIndexAndVerifyTable(tableName, channelNum, Arrays.asList(2, 3, 5), rowNum);
    channelNum++;

    // create stream on table to append change tracking columns
    executeQuery(String.format("create or replace stream %s on table %s;", streamName, tableName));
    ingestByColIndexAndVerifyTable(tableName, channelNum, Arrays.asList(2, 3, 5), rowNum);
    channelNum++;

    // add one more column after change tracking columns
    executeQuery(String.format("alter table %s add column c6 varchar;", tableFullName));
    ingestByColIndexAndVerifyTable(tableName, channelNum, Arrays.asList(2, 3, 5, 6), rowNum);
  }

  @Test
  public void testLeakedHTTPConnectionFix() throws Exception {
    prop = TestUtils.getProperties(Constants.BdecVersion.THREE, true);
    prop.setProperty(BDEC_PARQUET_COMPRESSION_ALGORITHM, compressionAlgorithm);
    prop.setProperty(USER, "badUser");

    for (int i = 0; i < 20; i++) {
      try (SnowflakeStreamingIngestClient client =
          SnowflakeStreamingIngestClientFactory.builder("MY_CLIENT").setProperties(prop).build()) {
      } catch (Exception ignored) {

      }
      Thread.sleep(5000);
    }
  }

  /**
   * Ingests rows into a table and verifies them by querying the table.
   *
   * <p>The test opens channel 'channel<channelNum>' for ingestion.
   *
   * <p>Expected table schema is ('C<index1>' varchar, 'C<index2>' varchar) where indexes are from
   * {@code columnIndexes}.
   *
   * <p>The ingested text values have the format: 'v<col_index1>_<row_index>'. The row index is from
   * range [(channelNum - 1) * numberOfRows, numberOfRows].
   *
   * @param tableName table full name
   * @param channelNum channel number to open
   * @param columnIndexes indexes of columns to ingest
   * @param numberOfRows to ingest
   */
  private void ingestByColIndexAndVerifyTable(
      String tableName, int channelNum, List<Integer> columnIndexes, int numberOfRows)
      throws InterruptedException, SQLException {
    final String valueFormat = "v%01d_%03d";
    final String channelName = "channel" + channelNum;
    final int startIndex = (channelNum - 1) * numberOfRows;

    // ingest rows
    SnowflakeStreamingIngestChannel channel =
        openChannel(tableName, channelName, OpenChannelRequest.OnErrorOption.ABORT);
    for (int i = 0; i < numberOfRows; i++) {
      Map<String, Object> row = new HashMap<>();
      for (int col : columnIndexes) {
        String value = String.format(valueFormat, col, startIndex + i);
        row.put("c" + col, value);
      }
      verifyInsertValidationResponse(channel.insertRow(row, Integer.toString(i)));
    }
    TestUtils.waitForOffset(channel, Integer.toString(numberOfRows - 1));
    channel.close();

    // query them and verify
    int firstColNum = columnIndexes.get(0);
    try (ResultSet result =
        jdbcConnection
            .createStatement()
            .executeQuery(
                String.format(
                    "select * from %s.%s.%s where c%d >= 'v%01d_%03d' and c%d < 'v%01d_%03d' order"
                        + " by c%d",
                    testDb,
                    TEST_SCHEMA,
                    tableName,
                    firstColNum,
                    firstColNum,
                    startIndex,
                    firstColNum,
                    firstColNum,
                    startIndex + numberOfRows,
                    firstColNum))) {
      for (int i = 0; i < numberOfRows; i++) {
        result.next();
        for (int col : columnIndexes) {
          String value = result.getString("C" + col);
          String expectedValue = String.format(valueFormat, col, startIndex + i);

          assertThat(value)
              .describedAs(
                  "Ingested value mismatch for table %s, startIndex %d, column c%d, index %d",
                  tableName, startIndex, col, i)
              .isEqualTo(expectedValue);
        }
      }
    }
  }

  private void executeQuery(String sqlFmt, Object... args) {
    String sql = String.format(sqlFmt, args);
    try {
      jdbcConnection.createStatement().executeQuery(sql);
    } catch (SQLException e) {
      throw new RuntimeException("SQL query failed : " + sql, e);
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

  private SnowflakeStreamingIngestChannel openChannel(
      String tableName, String channelName, OpenChannelRequest.OnErrorOption onErrorOption) {
    OpenChannelRequest request =
        OpenChannelRequest.builder(channelName)
            .setDBName(testDb)
            .setSchemaName(TEST_SCHEMA)
            .setTableName(tableName)
            .setOnErrorOption(onErrorOption)
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
  public void testDataTypes() throws SQLException, ParseException {
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

    SnowflakeStreamingIngestChannel channel =
        openChannel(tableName, "CHANNEL", OpenChannelRequest.OnErrorOption.ABORT);

    Map<String, Object> negRow = getNegRow();
    verifyInsertValidationResponse(channel.insertRow(negRow, Integer.toString(0)));

    Map<String, Object> nullRow = getNullRow();
    verifyInsertValidationResponse(channel.insertRow(nullRow, Integer.toString(1)));

    Map<String, Object> posRow = getPosRow();
    verifyInsertValidationResponse(channel.insertRow(posRow, Integer.toString(2)));

    waitChannelFlushed(channel, 3);
    verifyTableRowCount(3, tableName);

    ResultSet result =
        jdbcConnection
            .createStatement()
            .executeQuery(
                String.format(
                    "select * from %s.%s.%s order by num", testDb, TEST_SCHEMA, tableName));

    result.next(); // negRow
    assertNegRow(negRow, result);

    result.next(); // nullRow
    assertNullRow(nullRow, result);

    result.next(); // posRow
    assertPosRow(posRow, result);
  }

  private Map<String, Object> getNegRow() {
    Map<String, Object> negRow = new HashMap<>();
    negRow.put("num", -123);
    negRow.put("num_10_5", new BigDecimal("-12345.54321"));
    negRow.put("num_38_0", new BigDecimal("-91234567899876543219876543211234567891"));
    negRow.put("num_2_0", -12);
    negRow.put("num_4_0", -1234);
    negRow.put("num_9_0", -123456789);
    negRow.put("num_18_0", -123456789987654321L);
    negRow.put("num_float", -12345.54321f);
    negRow.put("str_varchar", "zxccvbnm,.");
    negRow.put("bin", new byte[] {1, 2, 3});
    negRow.put("bl", true);
    negRow.put("var", "{}");
    negRow.put("obj", "{}");
    negRow.put("arr", Arrays.asList());
    negRow.put("epochdays", "0");
    negRow.put("epochsec", "0");
    negRow.put("epochnano", "0");
    negRow.put("timesec", "0");
    negRow.put("timenano", "0");

    return negRow;
  }

  private Map<String, Object> getNullRow() {
    Map<String, Object> nullRow = new HashMap<>();
    nullRow.put("num", 0);
    nullRow.put("num_10_5", new BigDecimal("0.00000"));
    nullRow.put("num_38_0", new BigDecimal("0"));
    nullRow.put("num_2_0", 0);
    nullRow.put("num_4_0", 0);
    nullRow.put("num_9_0", 0);
    nullRow.put("num_18_0", 0L);
    nullRow.put("num_float", 0.0f);
    nullRow.put("str_varchar", null);
    nullRow.put("bin", null);
    nullRow.put("bl", false);
    nullRow.put("var", null);
    nullRow.put("obj", null);
    nullRow.put("arr", null);
    nullRow.put("epochdays", null);
    nullRow.put("epochsec", null);
    nullRow.put("epochnano", null);
    nullRow.put("timesec", null);
    nullRow.put("timenano", null);

    return nullRow;
  }

  private Map<String, Object> getPosRow() {
    Map<String, Object> posRow = new HashMap<>();
    posRow.put("num", 123);
    posRow.put("num_10_5", new BigDecimal("12345.54321"));
    posRow.put("num_38_0", new BigDecimal("91234567899876543219876543211234567891"));
    posRow.put("num_2_0", 12);
    posRow.put("num_4_0", 1234);
    posRow.put("num_9_0", 123456789);
    posRow.put("num_18_0", 123456789987654321L);
    posRow.put("num_float", 12345.54321f);
    posRow.put("str_varchar", "abcd123456.");
    posRow.put("bin", new byte[] {4, 5, 6});
    posRow.put("bl", true);
    posRow.put(
        "var",
        "{ \"a\": 1, \"b\": \"qwerty\", \"c\": null, \"d\": { \"e\": 2, \"f\": \"asdf\", \"g\":"
            + " null } }");
    posRow.put(
        "obj",
        "{ \"a\": 1, \"b\": \"qwerty\", \"c\": null, \"d\": { \"e\": 2, \"f\": \"asdf\", \"g\":"
            + " null } }");
    posRow.put("arr", Arrays.asList("{ \"a\": 1}", "{ \"b\": 2 }", "{ \"c\": 3 }"));
    posRow.put("epochdays", "2022-09-18T20:05:07"); // DATE, 18.09.2022
    posRow.put("epochsec", "2022-09-18T20:05:07"); // TIMESTAMP_NTZ(0)
    posRow.put("epochnano", "2022-09-18T20:05:07.999999999"); // TIMESTAMP_NTZ(9)
    posRow.put("timesec", "01:00:01.999999999"); // TIME(0)
    posRow.put("timenano", "01:00:01.999999999"); // TIME(9)

    return posRow;
  }

  private void assertNegRow(Map<String, Object> expectedNegRow, ResultSet actualResult)
      throws SQLException {
    assertNonTimeAndVarFields(expectedNegRow, actualResult);
    Assert.assertEquals("{}", actualResult.getString("VAR"));
    Assert.assertEquals("{}", actualResult.getString("OBJ"));
    Assert.assertEquals("[]", actualResult.getString("ARR"));
    Assert.assertEquals(0, actualResult.getDate("EPOCHDAYS").getTime());
    Assert.assertEquals(0, actualResult.getTimestamp("EPOCHSEC").getTime());
    Assert.assertEquals(0, actualResult.getTimestamp("EPOCHNANO").getNanos());
    Assert.assertEquals(0, actualResult.getTimestamp("EPOCHNANO").getTime());
    Assert.assertEquals(0, actualResult.getTimestamp("TIMESEC").getTime());
    Assert.assertEquals(0, actualResult.getTimestamp("TIMENANO").getNanos());
    Assert.assertEquals(0, actualResult.getTimestamp("TIMENANO").getTime());
  }

  private void assertNullRow(Map<String, Object> expectedNullRow, ResultSet actualResult)
      throws SQLException {
    Assert.assertNotNull(actualResult);
    assertNonTimeAndVarFields(expectedNullRow, actualResult);
    Assert.assertNull(actualResult.getString("VAR"));
    Assert.assertNull(actualResult.getString("OBJ"));
    Assert.assertNull(actualResult.getString("ARR"));
    Assert.assertNull(actualResult.getDate("EPOCHDAYS"));
    Assert.assertNull(actualResult.getTimestamp("EPOCHSEC"));
    Assert.assertNull(actualResult.getTimestamp("EPOCHNANO"));
    Assert.assertNull(actualResult.getTimestamp("TIMESEC"));
    Assert.assertNull(actualResult.getTimestamp("TIMENANO"));
  }

  private void assertPosRow(Map<String, Object> expectedPosRow, ResultSet actualResult)
      throws SQLException {
    String formattedJSON =
        "{\n"
            + "  \"a\": 1,\n"
            + "  \"b\": \"qwerty\",\n"
            + "  \"c\": null,\n"
            + "  \"d\": {\n"
            + "    \"e\": 2,\n"
            + "    \"f\": \"asdf\",\n"
            + "    \"g\": null\n"
            + "  }\n"
            + "}";

    String formattedArray =
        "[\n"
            + "  \"{ \\\"a\\\": 1}\",\n"
            + "  \"{ \\\"b\\\": 2 }\",\n"
            + "  \"{ \\\"c\\\": 3 }\"\n"
            + "]";
    Assert.assertNotNull(actualResult);
    assertNonTimeAndVarFields(expectedPosRow, actualResult);
    Assert.assertEquals(formattedJSON, actualResult.getString("VAR"));
    Assert.assertEquals(formattedJSON, actualResult.getString("OBJ"));
    Assert.assertEquals(formattedArray, actualResult.getString("ARR"));
    Assert.assertEquals(
        1663459200000l, actualResult.getDate("EPOCHDAYS").getTime()); // in ms, 18.09.2022 00:00:00
    Assert.assertEquals(1663531507000L, actualResult.getTimestamp("EPOCHSEC").getTime());
    Assert.assertEquals(999999999L, actualResult.getTimestamp("EPOCHNANO").getNanos());
    // 1663531507000 ms + 999 ms (from ns)
    Assert.assertEquals(1663531507999L, actualResult.getTimestamp("EPOCHNANO").getTime());
    Assert.assertEquals(3601000, actualResult.getTimestamp("TIMESEC").getTime()); // 1h + 1s in ms
    Assert.assertEquals(999999999, actualResult.getTimestamp("TIMENANO").getNanos());
    // 1h + 1s + 999 ms (from ns)
    Assert.assertEquals(3601999, actualResult.getTimestamp("TIMENANO").getTime());
  }

  private void assertNonTimeAndVarFields(Map<String, Object> row, ResultSet result)
      throws SQLException {
    Assert.assertNotNull(result);
    Assert.assertEquals(row.get("num"), result.getInt("NUM"));
    Assert.assertEquals(row.get("num_10_5"), result.getBigDecimal("NUM_10_5"));
    Assert.assertEquals(row.get("num_38_0"), result.getBigDecimal("NUM_38_0"));
    Assert.assertEquals(row.get("num_2_0"), result.getInt("NUM_2_0"));
    Assert.assertEquals(row.get("num_4_0"), result.getInt("NUM_4_0"));
    Assert.assertEquals(row.get("num_9_0"), result.getInt("NUM_9_0"));
    Assert.assertEquals((long) row.get("num_18_0"), result.getLong("NUM_18_0"));
    Assert.assertEquals((float) row.get("num_float"), result.getFloat("NUM_FLOAT"), 0);
    Assert.assertEquals(row.get("str_varchar"), result.getString("STR_VARCHAR"));
    Assert.assertArrayEquals((byte[]) row.get("bin"), result.getBytes("BIN"));
    Assert.assertEquals(row.get("bl"), result.getBoolean("BL"));
  }
}
