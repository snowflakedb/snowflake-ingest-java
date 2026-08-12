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
import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.utils.ParameterProvider;
import net.snowflake.ingest.utils.Utils;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.parquet.column.ParquetProperties;
import org.junit.Assert;
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
public class InsertRowsBenchmarkTest {
  @Param({"false", "true"})
  public boolean enableIcebergStreaming;

  // package-private so FlushFixture can access it
  SnowflakeStreamingIngestChannelInternal<?> channel;
  private SnowflakeStreamingIngestClientInternal<?> client;

  @Param({"100000"})
  private int numRows;

  @Param({"false", "true"})
  public boolean enableReadbackVerification;

  @Setup(Level.Trial)
  public void setUpBeforeAll() {
    // SNOW-1490151: Testing gaps
    CloseableHttpClient httpClient = MockSnowflakeServiceClient.createHttpClient();
    RequestBuilder requestBuilder =
        MockSnowflakeServiceClient.createRequestBuilder(httpClient, enableIcebergStreaming);
    Properties prop = new Properties();
    prop.setProperty(
        ParameterProvider.ENABLE_ICEBERG_STREAMING, String.valueOf(enableIcebergStreaming));
    prop.setProperty(
        ParameterProvider.ENABLE_PARQUET_INTERNAL_READBACK_VERIFICATION,
        String.valueOf(enableReadbackVerification));
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
    // Setup column fields and vectors
    ColumnMetadata col = new ColumnMetadata();
    col.setOrdinal(1);
    col.setName("COL");
    col.setPhysicalType("SB16");
    col.setNullable(false);
    col.setLogicalType("FIXED");
    col.setPrecision(38);
    col.setScale(0);

    channel.setupSchema(Collections.singletonList(col));
    // Register channel so ChannelCache.setNeedFlush works when buffer threshold is reached
    @SuppressWarnings({"unchecked", "rawtypes"})
    ChannelCache rawCache = client.getChannelCache();
    rawCache.addChannel(channel);
    assert Utils.getProvider() != null;
  }

  @TearDown(Level.Trial)
  public void tearDownAfterAll() throws Exception {
    channel.close();
    client.close();
  }

  /**
   * Auxiliary state that pre-fills the buffer before each testFlushRows invocation only.
   * Using a separate @State class ensures fillBuffer does NOT run before testInsertRow.
   */
  @State(Scope.Thread)
  public static class FlushFixture {
    @Setup(Level.Invocation)
    public void fillBuffer(InsertRowsBenchmarkTest benchmark) {
      Map<String, Object> row = new HashMap<>();
      row.put("col", 1);
      for (int i = 0; i < 100000; i++) {
        benchmark.channel.insertRow(row, String.valueOf(i));
      }
    }
  }

  /**
   * Benchmarks the full serialize+verify pipeline. Run with enableReadbackVerification=false vs
   * true to measure the cost of readback verification.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  @Benchmark
  public void testFlushRows(FlushFixture fixture) throws Exception {
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

  @Benchmark
  public void testInsertRow() {
    Map<String, Object> row = new HashMap<>();
    row.put("col", 1);

    for (int i = 0; i < numRows; i++) {
      InsertValidationResponse response = channel.insertRow(row, String.valueOf(i));
      Assert.assertFalse(response.hasErrors());
    }
  }

  @Test
  public void insertRow() throws Exception {
    setUpBeforeAll();
    Map<String, Object> row = new HashMap<>();
    row.put("col", 1);

    for (int i = 0; i < 1000000; i++) {
      InsertValidationResponse response = channel.insertRow(row, String.valueOf(i));
      Assert.assertFalse(response.hasErrors());
    }
    tearDownAfterAll();
  }

  @Test
  public void launchBenchmark() throws RunnerException {
    Options opt =
        new OptionsBuilder()
            // Specify which benchmarks to run.
            // You can be more specific if you'd like to run only one benchmark per test.
            .include(this.getClass().getName() + ".*")
            // Set the following options as needed
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
            // .jvmArgs("-XX:+UnlockDiagnosticVMOptions", "-XX:+PrintInlining")
            // .addProfiler(WinPerfAsmProfiler.class)
            .build();

    new Runner(opt).run();
  }
}
