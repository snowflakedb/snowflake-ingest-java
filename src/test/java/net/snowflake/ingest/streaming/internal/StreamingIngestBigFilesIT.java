package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.TestUtils.verifyTableRowCount;
import static net.snowflake.ingest.utils.Constants.ROLE;
import static net.snowflake.ingest.utils.ParameterProvider.BDEC_PARQUET_COMPRESSION_ALGORITHM;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;
import net.snowflake.ingest.TestUtils;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import net.snowflake.ingest.utils.Constants;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

/** Ingest large amount of rows. */
@RunWith(Parameterized.class)
public class StreamingIngestBigFilesIT {
  private static final String TEST_DB_PREFIX = "STREAMING_INGEST_TEST_DB";
  private static final String TEST_SCHEMA = "STREAMING_INGEST_TEST_SCHEMA";

  private Properties prop;

  private SnowflakeStreamingIngestClientInternal<?> client;
  private Connection jdbcConnection;
  private String testDb;

  @Parameters(name = "{index}: {0}")
  public static Object[] compressionAlgorithms() {
    return new Object[] {"GZIP", "ZSTD"};
  }

  @Parameter public String compressionAlgorithm;

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
    // Set timezone to UTC
    jdbcConnection.createStatement().execute("alter session set timezone = 'UTC';");
    jdbcConnection
        .createStatement()
        .execute(String.format("use warehouse %s", TestUtils.getWarehouse()));

    prop = TestUtils.getProperties(Constants.BdecVersion.THREE, false);
    if (prop.getProperty(ROLE).equals("DEFAULT_ROLE")) {
      prop.setProperty(ROLE, "ACCOUNTADMIN");
    }
    prop.setProperty(BDEC_PARQUET_COMPRESSION_ALGORITHM, compressionAlgorithm);
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
  public void testManyRowsMultipleChannelsToMultipleTable()
      throws SQLException, ExecutionException, InterruptedException {
    String tableNamePrefix = "t_big_table_";

    int numTables = 2;
    int numChannels = 4; // channels are assigned round-robin to tables.
    int batchSize = 10000;
    int numBatches = 10; // number of rows PER CHANNEL is batchSize * numBatches
    boolean isNullable = false;

    Map<Integer, Integer> tableIdToNumChannels = new HashMap<>();
    for (int i = 0; i < numChannels; i++) {
      tableIdToNumChannels.put(
          i % numTables, tableIdToNumChannels.getOrDefault(i % numTables, 0) + 1);
    }
    for (int i = 0; i < numTables; i++) {
      String tableName = tableNamePrefix + i;
      createTableForTest(tableName);
    }

    ingestRandomRowsToTable(
        tableNamePrefix, numTables, numChannels, batchSize, numBatches, isNullable);

    for (int i = 0; i < numTables; i++) {
      int numChannelsToTable = tableIdToNumChannels.get(i);
      verifyTableRowCount(
          batchSize * numBatches * numChannelsToTable,
          jdbcConnection,
          testDb,
          TEST_SCHEMA,
          tableNamePrefix + i);

      // select * to ensure scanning works
      ResultSet result =
          jdbcConnection
              .createStatement()
              .executeQuery(
                  String.format(
                      "select * from %s.%s.%s", testDb, TEST_SCHEMA, tableNamePrefix + i));
      result.next();
      Assert.assertNotNull(result.getString("STR"));
    }
  }

  private void ingestRandomRowsToTable(
      String tablePrefix,
      int numTables,
      int numChannels,
      int batchSize,
      int iterations,
      boolean isNullable)
      throws ExecutionException, InterruptedException {

    final List<Map<String, Object>> rows = Collections.synchronizedList(new ArrayList<>());
    for (int i = 0; i < batchSize; i++) {
      Random r = new Random();
      rows.add(TestUtils.getRandomRow(r, isNullable));
    }

    ExecutorService testThreadPool = Executors.newFixedThreadPool(numChannels);
    CompletableFuture[] futures = new CompletableFuture[numChannels];
    List<SnowflakeStreamingIngestChannel> channelList =
        Collections.synchronizedList(new ArrayList<>());
    for (int i = 0; i < numChannels; i++) {
      final String channelName = "CHANNEL" + i;
      int finalI = i;
      futures[i] =
          CompletableFuture.runAsync(
              () -> {
                int targetTable = finalI % numTables;
                SnowflakeStreamingIngestChannel channel =
                    openChannel(tablePrefix + targetTable, channelName);
                channelList.add(channel);
                for (int val = 0; val < iterations; val++) {
                  TestUtils.verifyInsertValidationResponse(
                      channel.insertRows(rows, Integer.toString(val)));
                }
              },
              testThreadPool);
    }
    CompletableFuture joined = CompletableFuture.allOf(futures);
    joined.get();
    channelList.forEach(channel -> TestUtils.waitChannelFlushed(channel, iterations));
    testThreadPool.shutdown();
  }

  private void createTableForTest(String tableName) {
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
                  tableName));
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
}
