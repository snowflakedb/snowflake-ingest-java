package net.snowflake.ingest.streaming.example;

import static net.snowflake.ingest.utils.Constants.ROLE;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;
import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import net.snowflake.ingest.streaming.internal.SnowflakeStreamingIngestClientInternal;
import net.snowflake.ingest.utils.Constants;
import net.snowflake.ingest.utils.ParameterProvider;

/** Streaming ingest sdk perf test */
public class SnowflakeStreamingIngestParquetPerfRunner {

  private static final String TEST_TABLE = "STREAMING_INGEST_TEST_TABLE";
  private static final String TEST_DB_PREFIX = "STREAMING_INGEST_TEST_DB";
  private static final String TEST_SCHEMA = "STREAMING_INGEST_TEST_SCHEMA";

  private Properties prop;

  private SnowflakeStreamingIngestClientInternal<?> client;
  private Connection jdbcConnection;
  private String testDb;

  private final Constants.BdecVersion bdecVersion;

  private final boolean enableInternalParquetBuffering;

  private final int batchSize;
  private final int iterations;
  private final int numChannels;
  private String clientName = "GDOCI_PERF_";

  public SnowflakeStreamingIngestParquetPerfRunner(
      @SuppressWarnings("unused") String name,
      boolean enableInternalParquetBuffering,
      Constants.BdecVersion bdecVersion,
      int batchSize,
      int iterations,
      int numChannels) {
    this.bdecVersion = bdecVersion;
    this.enableInternalParquetBuffering = enableInternalParquetBuffering;
    this.batchSize = batchSize;
    this.iterations = iterations;
    this.numChannels = numChannels;
    this.clientName +=
        enableInternalParquetBuffering
            + "_"
            + bdecVersion.name()
            + "_"
            + (this.batchSize * iterations)
            + "x"
            + numChannels;
  }

  public void setup() throws Exception {
    testDb = TEST_DB_PREFIX;

    prop = Util.getProperties(bdecVersion);
    jdbcConnection = Util.getConnection();

    jdbcConnection
        .createStatement()
        .execute(String.format("use role %s;", prop.getProperty("role")));

    jdbcConnection
        .createStatement()
        .execute(String.format("create or replace database %s;", testDb));
    jdbcConnection
        .createStatement()
        .execute(String.format("create or replace schema %s.%s;", testDb, TEST_SCHEMA));

    // Set timezone to UTC
    jdbcConnection.createStatement().execute("alter session set timezone = 'UTC';");
    jdbcConnection
        .createStatement()
        .execute(String.format("use warehouse %s", prop.getProperty("warehouse")));

    if (prop.getProperty(ROLE).equals("DEFAULT_ROLE")) {
      prop.setProperty(ROLE, "ACCOUNTADMIN");
    }

    Map<String, Object> parameterMap = new HashMap<>();
    parameterMap.put(
        ParameterProvider.ENABLE_PARQUET_INTERNAL_BUFFERING, enableInternalParquetBuffering);

    client =
        (SnowflakeStreamingIngestClientInternal<?>)
            SnowflakeStreamingIngestClientFactory.builder(clientName)
                .setProperties(prop)
                .setParameterOverrides(parameterMap)
                .build();
  }

  public void tearDown() throws Exception {
    client.close();
    jdbcConnection.createStatement().execute(String.format("drop database %s", testDb));
  }

  public void runPerfExperiment() throws ExecutionException, InterruptedException {
    try {
      jdbcConnection
          .createStatement()
          .execute(
              String.format(
                  "create or replace table %s (\n"
                      + "                                    num_2_1 NUMBER(2, 1),\n"
                      + "                                    num_4_2 NUMBER(4, 2),\n"
                      + "                                    num_9_4 NUMBER(9, 4),\n"
                      + "                                    num_18_7 NUMBER(18, 7),\n"
                      + "                                    num_38_15 NUMBER(38, 15),\n"
                      + "                                    num_float FLOAT,\n"
                      + "                                    str VARCHAR(256),\n"
                      + "                                    bin BINARY(256));",
                  TEST_TABLE));
    } catch (SQLException e) {
      throw new RuntimeException("Cannot create table " + TEST_TABLE, e);
    }

    List<Map<String, Object>> rows = new ArrayList<>();
    for (int i = 0; i < batchSize; i++) {
      Random r = new Random();
      rows.add(Util.getRandomRow(r));
    }

    ExecutorService testThreadPool = Executors.newFixedThreadPool(numChannels);
    CompletableFuture[] futures = new CompletableFuture[numChannels];
    List<SnowflakeStreamingIngestChannel> channelList = new ArrayList<>();
    for (int i = 0; i < numChannels; i++) {
      final String channelName = "CHANNEL" + i;
      futures[i] =
          CompletableFuture.runAsync(
              () -> {
                SnowflakeStreamingIngestChannel channel = openChannel(TEST_TABLE, channelName);
                channelList.add(channel);
                for (int val = 0; val < iterations; val++) {
                  verifyInsertValidationResponse(channel.insertRows(rows, Integer.toString(val)));
                }
                // waitChannelFlushed(channel, batchSize * iterations);
              },
              testThreadPool);
    }
    CompletableFuture joined = CompletableFuture.allOf(futures);
    joined.get();
    testThreadPool.shutdown();

    // verifyTableRowCount(batchSize * iterations * numChannels, tableName);
  }

  /** Verify the insert validation response and throw the exception if needed */
  private void verifyInsertValidationResponse(InsertValidationResponse response) {
    if (response.hasErrors()) {
      throw response.getInsertErrors().get(0).getException();
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
  }

  private void verifyTableRowCount(int rowNumber, String tableName) {
    try {
      ResultSet resultCount =
          jdbcConnection
              .createStatement()
              .executeQuery(
                  String.format("select count(*) from %s.%s.%s", testDb, TEST_SCHEMA, tableName));
      resultCount.next();
      if (rowNumber != resultCount.getLong(1)) {
        throw new IllegalArgumentException("Number of rows is not as expected!");
      }
    } catch (SQLException e) {
      throw new RuntimeException("Cannot verifyTableRowCount for " + tableName, e);
    }
  }
}
