package net.snowflake.ingest.streaming.example;

import static net.snowflake.ingest.utils.Constants.ROLE;

import java.io.IOException;
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
public class Runner {

  private static String TEST_TABLE = "STREAMING_INGEST_TEST_TABLE";
  private static final String TEST_DB = "STREAMING_INGEST_TEST_DB";
  private static final String TEST_SCHEMA = "STREAMING_INGEST_TEST_SCHEMA";

  private Properties prop;

  private SnowflakeStreamingIngestClientInternal<?> client;

  private final Constants.BdecVersion bdecVersion;

  private final boolean enableInternalParquetBuffering;

  private int batchSize;
  private final int iterations;
  private final int numChannels;

  private boolean nullable;
  private String clientName = "GZIP_EC2_GDOCI_PERF_";

  private boolean useCriteoDataset= false;

  public Runner(
      @SuppressWarnings("unused") String name,
      boolean enableInternalParquetBuffering,
      Constants.BdecVersion bdecVersion,
      int batchSize,
      int iterations,
      int numChannels,
      boolean nullable,
      boolean useCriteoDataset) {
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
    this.useCriteoDataset = useCriteoDataset;
    this.TEST_TABLE = useCriteoDataset ? "STREAMING_INGEST_TEST_TABLE_CRITEO" : TEST_TABLE;
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

  public void runPerfExperiment() throws ExecutionException, InterruptedException, IOException {
    List<Map<String, Object>> rows;
    if(useCriteoDataset) {
      rows = CriteoUtil.readData();
      batchSize = rows.size();
    } else {
      rows = new ArrayList<>();
      for (int i = 0; i < batchSize; i++) {
        Random r = new Random();
        rows.add(Util.getRandomRow(r, nullable));
      }
    }

    ExecutorService testThreadPool = Executors.newFixedThreadPool(numChannels);
    CompletableFuture[] futures = new CompletableFuture[numChannels];
    List<SnowflakeStreamingIngestChannel> channelList = new ArrayList<>();
    for (int i = 0; i < numChannels; i++) {
      final String channelName = "CHANNEL" + i;
      futures[i] =
          CompletableFuture.runAsync(
              () -> {
                SnowflakeStreamingIngestChannel channel = Util.openChannel(TEST_DB, TEST_SCHEMA, TEST_TABLE, channelName, client);
                channelList.add(channel);
                for (int val = 0; val < iterations; val++) {
                  Util.verifyInsertValidationResponse(channel.insertRows(rows, Integer.toString(val)));
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
}
