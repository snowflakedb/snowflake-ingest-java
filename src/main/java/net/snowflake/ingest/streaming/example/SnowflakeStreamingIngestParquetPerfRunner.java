package net.snowflake.ingest.streaming.example;

import static net.snowflake.ingest.utils.Constants.ROLE;

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
  private static final String TEST_DB = "STREAMING_INGEST_TEST_DB";
  private static final String TEST_SCHEMA = "STREAMING_INGEST_TEST_SCHEMA";

  private Properties prop;

  private SnowflakeStreamingIngestClientInternal<?> client;

  private final Constants.BdecVersion bdecVersion;

  private final boolean enableInternalParquetBuffering;

  private final int batchSize;
  private final int iterations;
  private final int numChannels;

  private boolean nullable;
  private String clientName = "GZIP_EC2_GDOCI_PERF_";

  public SnowflakeStreamingIngestParquetPerfRunner(
      @SuppressWarnings("unused") String name,
      boolean enableInternalParquetBuffering,
      Constants.BdecVersion bdecVersion,
      int batchSize,
      int iterations,
      int numChannels,
      boolean nullable) {
    this.bdecVersion = bdecVersion;
    this.enableInternalParquetBuffering = enableInternalParquetBuffering;
    this.batchSize = batchSize;
    this.iterations = iterations;
    this.numChannels = numChannels;
    this.nullable = nullable;
    this.clientName +=
        enableInternalParquetBuffering
            + "_"
            + bdecVersion.name()
            + "_"
            + (this.batchSize * iterations)
            + "x"
            + numChannels
            + "_nullable_"
            + nullable;
  }

  public void setup() throws Exception {
    prop = Util.getProperties(bdecVersion);

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
  }

  public void runPerfExperiment() throws ExecutionException, InterruptedException {
    /* try {
     */
    /*jdbcConnection
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
            TEST_TABLE));*/
    /*
    } catch (SQLException e) {
      throw new RuntimeException("Cannot create table " + TEST_TABLE, e);
    }*/

    List<Map<String, Object>> rows = new ArrayList<>();
    for (int i = 0; i < batchSize; i++) {
      Random r = new Random();
      rows.add(Util.getRandomRow(r, nullable));
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
            .setDBName(TEST_DB)
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
}
