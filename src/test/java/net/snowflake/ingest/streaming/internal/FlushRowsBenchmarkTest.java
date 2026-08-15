/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import static java.time.ZoneOffset.UTC;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import net.snowflake.ingest.connection.RequestBuilder;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.utils.ParameterProvider;
import net.snowflake.ingest.utils.Utils;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.parquet.column.ParquetProperties;
import org.junit.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

@State(Scope.Thread)
public class FlushRowsBenchmarkTest {
  @Param({"false", "true"})
  public boolean enableIcebergStreaming;

  @Param({"false", "true"})
  public boolean enableReadbackVerification;

  @Param({"1000", "10000", "100000"})
  public int numRows;

  private SnowflakeStreamingIngestChannelInternal<?> channel;
  private SnowflakeStreamingIngestClientInternal<?> client;

  @Setup(Level.Trial)
  public void setUpBeforeAll() {
    CloseableHttpClient httpClient = MockSnowflakeServiceClient.createHttpClient();
    RequestBuilder requestBuilder =
        MockSnowflakeServiceClient.createRequestBuilder(httpClient, enableIcebergStreaming);
    Properties prop = new Properties();
    prop.setProperty(
        ParameterProvider.ENABLE_ICEBERG_STREAMING, String.valueOf(enableIcebergStreaming));
    prop.setProperty(
        ParameterProvider.ENABLE_PARQUET_READBACK_VERIFICATION,
        String.valueOf(enableReadbackVerification));
    // Prevent insertRow from signaling the FlushService during buffer pre-fill;
    // the benchmark controls flushing directly via buffer.flush()
    prop.setProperty(ParameterProvider.MAX_CHANNEL_SIZE_IN_BYTES, String.valueOf(Long.MAX_VALUE));
    client =
        new SnowflakeStreamingIngestClientInternal<>(
            "client_PARQUET", null, prop, httpClient, true, requestBuilder, new HashMap<>());

    channel =
        new SnowflakeStreamingIngestChannelInternal<>(
            "channel",
            "db",
            "schema",
            "table",
            "0",
            0L,
            0L,
            client,
            "key",
            1234L,
            OpenChannelRequest.OnErrorOption.CONTINUE,
            UTC,
            null /* offsetTokenVerificationFunction */,
            enableIcebergStreaming
                ? ParquetProperties.WriterVersion.PARQUET_2_0
                : ParquetProperties.WriterVersion.PARQUET_1_0);

    ColumnMetadata col = new ColumnMetadata();
    col.setOrdinal(1);
    col.setName("COL");
    col.setPhysicalType("SB16");
    col.setNullable(false);
    col.setLogicalType("FIXED");
    col.setPrecision(38);
    col.setScale(0);

    channel.setupSchema(Collections.singletonList(col));
    assert Utils.getProvider() != null;
  }

  @TearDown(Level.Trial)
  public void tearDownAfterAll() throws Exception {
    channel.close();
    client.close();
  }

  @Setup(Level.Invocation)
  public void fillBufferForFlush() {
    Map<String, Object> row = new HashMap<>();
    row.put("col", 1);
    for (int i = 0; i < numRows; i++) {
      channel.insertRow(row, String.valueOf(i));
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Benchmark
  public void testFlushRows() throws Exception {
    RowBuffer buffer = channel.getRowBuffer();
    ChannelData data = buffer.flush();
    if (data != null) {
      data.setChannelContext(new ChannelFlushContext("ch", "db", "schema", "table", 1L, "key", 0L));
      Flusher flusher = buffer.createFlusher();
      flusher.serialize(
          (List<ChannelData>) (List<?>) Collections.singletonList(data),
          "bench.bdec",
          0,
          FileMetadataTestingOverrides.none());
    }
  }

  @Test
  public void launchBenchmark() throws RunnerException {
    Options opt =
        new OptionsBuilder()
            .include(this.getClass().getName() + ".*")
            .mode(Mode.AverageTime)
            .timeUnit(TimeUnit.MICROSECONDS)
            .warmupTime(TimeValue.seconds(1))
            .warmupIterations(2)
            .measurementTime(TimeValue.seconds(1))
            .measurementIterations(10)
            .threads(2)
            .forks(1)
            .shouldFailOnError(true)
            .shouldDoGC(true)
            .build();

    new Runner(opt).run();
  }
}
