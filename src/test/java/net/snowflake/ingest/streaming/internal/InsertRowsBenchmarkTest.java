/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import static java.time.ZoneOffset.UTC;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.utils.Utils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
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
@RunWith(Parameterized.class)
public class InsertRowsBenchmarkTest {
  @Parameterized.Parameters(name = "isIcebergMode: {0}")
  public static Object[] isIcebergMode() {
    return new Object[] {false, true};
  }

  @Parameterized.Parameter public boolean isIcebergMode;

  private SnowflakeStreamingIngestChannelInternal<?> channel;
  private SnowflakeStreamingIngestClientInternal<?> client;

  @Param({"100000"})
  private int numRows;

  @Setup(Level.Trial)
  public void setUpBeforeAll() {
    // SNOW-1490151: Testing gaps
    client =
        new SnowflakeStreamingIngestClientInternal<ParquetChunkData>(
            "client_PARQUET", isIcebergMode);
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
            UTC);
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
    assert Utils.getProvider() != null;
  }

  @TearDown(Level.Trial)
  public void tearDownAfterAll() throws Exception {
    channel.close();
    client.close();
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
