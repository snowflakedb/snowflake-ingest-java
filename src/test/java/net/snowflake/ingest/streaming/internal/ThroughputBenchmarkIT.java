/*
 * Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import com.codahale.metrics.Snapshot;
import com.sun.management.OperatingSystemMXBean;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.sql.Connection;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import net.snowflake.ingest.TestUtils;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import net.snowflake.ingest.utils.Constants;
import net.snowflake.ingest.utils.ParameterProvider;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Manual E2E latency benchmark comparing ENABLE_PARQUET_READBACK_VERIFICATION on vs off.
 *
 * <p>Runs two schema configurations (17-column narrow, 1151-column LexisNexis approximation). Each
 * schema is benchmarked NUM_RUNS times. Per-flush build latency is read directly from the {@code
 * buildLatency} Codahale timer on the client internals after each pass. Results are written
 * incrementally to RESULTS_CSV_PATH in CSV format (Excel-compatible).
 *
 * <p>Remove @Ignore and provide profile.json pointing at a quiet preprod6 account to run.
 */
@org.junit.Ignore
public class ThroughputBenchmarkIT {

  private static final OffsetDateTime BENCH_TS =
      OffsetDateTime.of(2024, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);

  private static final int WARMUP_SECONDS = 30;
  private static final int BENCHMARK_SECONDS = 60;
  private static final int NUM_RUNS = 10;
  private static final String RESULTS_CSV_PATH = "target/benchmark_results.csv";

  private static final String BENCH_DB = "STREAMING_INGEST_BENCHMARK_DB";
  private static final String BENCH_SCHEMA = "PUBLIC";

  // ---------- schema configuration ----------

  private static class SchemaConfig {
    final String tableName;
    final int nNumber, nFloat, nVarchar, nTimestampTz, nObject;

    SchemaConfig(
        String tableName, int nNumber, int nFloat, int nVarchar, int nTimestampTz, int nObject) {
      this.tableName = tableName;
      this.nNumber = nNumber;
      this.nFloat = nFloat;
      this.nVarchar = nVarchar;
      this.nTimestampTz = nTimestampTz;
      this.nObject = nObject;
    }

    int totalColumns() {
      return nNumber + nFloat + nVarchar + nTimestampTz + nObject;
    }

    String buildColumnDefs() {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < nNumber; i++) sb.append("num_").append(i).append(" NUMBER,");
      for (int i = 0; i < nFloat; i++) sb.append("flt_").append(i).append(" FLOAT,");
      for (int i = 0; i < nVarchar; i++) sb.append("str_").append(i).append(" VARCHAR,");
      for (int i = 0; i < nTimestampTz; i++) sb.append("ts_").append(i).append(" TIMESTAMP_TZ,");
      for (int i = 0; i < nObject; i++) sb.append("obj_").append(i).append(" OBJECT,");
      sb.setLength(sb.length() - 1);
      return sb.toString();
    }

    Map<String, Object> buildRow(long i) {
      Map<String, Object> row = new HashMap<>(totalColumns() * 2);
      for (int j = 0; j < nNumber; j++) row.put("num_" + j, (i * j) % 1_000_000L);
      for (int j = 0; j < nFloat; j++) row.put("flt_" + j, i * j * 0.001);
      for (int j = 0; j < nVarchar; j++) row.put("str_" + j, "value_" + (j % 100));
      for (int j = 0; j < nTimestampTz; j++) row.put("ts_" + j, BENCH_TS);
      for (int j = 0; j < nObject; j++) row.put("obj_" + j, "{\"k\":\"v\"}");
      return row;
    }
  }

  // 17 columns: 5 NUMBER + 4 FLOAT + 5 VARCHAR + 2 TIMESTAMP_TZ + 1 OBJECT
  private static final SchemaConfig NARROW = new SchemaConfig("BENCH_NARROW_17", 5, 4, 5, 2, 1);

  // 1,151 columns: 400 NUMBER + 400 FLOAT + 200 VARCHAR + 150 TIMESTAMP_TZ + 1 OBJECT
  // (close enough to the 1160-col LexisNexis approximation used in the prior run)
  private static final SchemaConfig WIDE =
      new SchemaConfig("BENCH_WIDE_1151", 400, 400, 200, 150, 1);

  private static final SchemaConfig[] SCHEMAS = {NARROW, WIDE};

  // ---------- lifecycle ----------

  @BeforeClass
  public static void setup() throws Exception {
    try (Connection conn = TestUtils.getConnection()) {
      conn.createStatement().execute("USE WAREHOUSE " + TestUtils.getWarehouse());
      conn.createStatement().execute("CREATE OR REPLACE DATABASE " + BENCH_DB);
      for (SchemaConfig s : SCHEMAS) {
        conn.createStatement()
            .execute(
                "CREATE OR REPLACE TABLE "
                    + BENCH_DB
                    + "."
                    + BENCH_SCHEMA
                    + "."
                    + s.tableName
                    + " ("
                    + s.buildColumnDefs()
                    + ")");
      }
    }
  }

  @AfterClass
  public static void teardown() throws Exception {
    try (Connection conn = TestUtils.getConnection()) {
      conn.createStatement().execute("DROP DATABASE IF EXISTS " + BENCH_DB);
    }
  }

  // ---------- test ----------

  @Test
  public void benchmarkReadbackVerificationLatency() throws Exception {
    try (PrintWriter csv = new PrintWriter(new FileWriter(RESULTS_CSV_PATH))) {
      writeCsvHeader(csv);

      for (SchemaConfig schema : SCHEMAS) {
        System.out.printf(
            "%n============================================================%n"
                + "Schema: %s  (%d columns)%n"
                + "============================================================%n",
            schema.tableName, schema.totalColumns());

        System.out.printf("%nWarmup (verify=false, %ds)...%n", WARMUP_SECONDS);
        runPass("warmup", false, WARMUP_SECONDS, schema);

        for (int run = 1; run <= NUM_RUNS; run++) {
          System.out.printf(
              "%nRun %d/%d — without verification (%ds)...%n", run, NUM_RUNS, BENCHMARK_SECONDS);
          Result baseline = runPass("no_verify_r" + run, false, BENCHMARK_SECONDS, schema);

          System.out.printf(
              "%nRun %d/%d — with verification (%ds)...%n", run, NUM_RUNS, BENCHMARK_SECONDS);
          Result withVerify = runPass("verify_r" + run, true, BENCHMARK_SECONDS, schema);

          printComparison(run, schema, baseline, withVerify);
          writeCsvRow(csv, schema, run, baseline, withVerify);
          csv.flush();
        }
      }
    }
    System.out.printf("%nResults written to %s%n", RESULTS_CSV_PATH);
  }

  // ---------- pass runner ----------

  private static Result runPass(
      String label, boolean verificationOn, int durationSeconds, SchemaConfig schema)
      throws Exception {
    Properties props = TestUtils.getProperties(Constants.BdecVersion.THREE, false);
    Map<String, Object> overrides = new HashMap<>();
    overrides.put(
        ParameterProvider.ENABLE_PARQUET_READBACK_VERIFICATION, String.valueOf(verificationOn));

    try (SnowflakeStreamingIngestClient client =
        SnowflakeStreamingIngestClientFactory.builder("bench_" + label)
            .setProperties(props)
            .setParameterOverrides(overrides)
            .build()) {

      SnowflakeStreamingIngestChannel channel =
          client.openChannel(
              OpenChannelRequest.builder("BENCH_CH")
                  .setDBName(BENCH_DB)
                  .setSchemaName(BENCH_SCHEMA)
                  .setTableName(schema.tableName)
                  .setOnErrorOption(OpenChannelRequest.OnErrorOption.CONTINUE)
                  .build());

      OperatingSystemMXBean osBean =
          (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
      long cpuStart = osBean.getProcessCpuTime();
      long t0 = System.nanoTime();
      long deadline = t0 + (long) durationSeconds * 1_000_000_000L;
      long rowCount = 0;

      while (System.nanoTime() < deadline) {
        channel.insertRow(schema.buildRow(rowCount), String.valueOf(rowCount));
        rowCount++;
      }
      long insertNs = System.nanoTime() - t0;

      channel.close().get();
      long drainNs = System.nanoTime() - t0 - insertNs;
      long cpuMs = (osBean.getProcessCpuTime() - cpuStart) / 1_000_000;

      // Read per-flush build latency directly from the client's Codahale timer.
      // buildLatency is package-private on SnowflakeStreamingIngestClientInternal.
      SnowflakeStreamingIngestClientInternal clientInternal =
          (SnowflakeStreamingIngestClientInternal) client;
      long flushCount = clientInternal.buildLatency.getCount();
      Snapshot snap = clientInternal.buildLatency.getSnapshot();
      // Codahale Timer stores durations in nanoseconds; convert to ms.
      long meanMs = Math.round(snap.getMean() / 1_000_000.0);
      long p50Ms = Math.round(snap.getMedian() / 1_000_000.0);
      long p95Ms = Math.round(snap.get95thPercentile() / 1_000_000.0);

      Result r =
          new Result(
              rowCount,
              insertNs / 1_000_000,
              drainNs / 1_000_000,
              cpuMs,
              flushCount,
              meanMs,
              p50Ms,
              p95Ms);
      System.out.printf(
          "[%s] rows=%,d rate=%,.0f/s drain=%,dms cpu=%,dms  |  flushes=%d"
              + " buildMean=%dms buildP50=%dms buildP95=%dms%n",
          label,
          r.rowCount,
          r.insertRowsPerSec(),
          r.drainMs,
          r.cpuMs,
          r.flushCount,
          r.buildMeanMs,
          r.buildP50Ms,
          r.buildP95Ms);
      return r;
    }
  }

  // ---------- console reporting ----------

  private static void printComparison(
      int run, SchemaConfig schema, Result baseline, Result measured) {
    System.out.printf(
        "%n--- Run %d  %s (%d cols)  verify=false vs verify=true ---%n",
        run, schema.tableName, schema.totalColumns());
    System.out.printf("%-32s %10s %10s %10s%n", "", "no-verify", "verify", "delta");
    printStat("Build latency mean (ms)", baseline.buildMeanMs, measured.buildMeanMs);
    printStat("Build latency p50  (ms)", baseline.buildP50Ms, measured.buildP50Ms);
    printStat("Build latency p95  (ms)", baseline.buildP95Ms, measured.buildP95Ms);
    printStat("Flush drain time   (ms)", baseline.drainMs, measured.drainMs);
    printStat("CPU usage          (ms)", baseline.cpuMs, measured.cpuMs);
    System.out.printf("%-32s %10d %10d%n", "Flush count", baseline.flushCount, measured.flushCount);
  }

  private static void printStat(String label, long baseline, long measured) {
    double pct = baseline == 0 ? 0 : 100.0 * (measured - baseline) / baseline;
    System.out.printf("%-32s %10d %10d %+9.1f%%%n", label, baseline, measured, pct);
  }

  // ---------- CSV reporting ----------

  private static void writeCsvHeader(PrintWriter csv) {
    csv.println(
        "schema,columns,run,"
            + "no_verify_rows,no_verify_rate_per_s,no_verify_drain_ms,no_verify_cpu_ms,"
            + "no_verify_flushes,no_verify_build_mean_ms,no_verify_build_p50_ms,no_verify_build_p95_ms,"
            + "verify_rows,verify_rate_per_s,verify_drain_ms,verify_cpu_ms,"
            + "verify_flushes,verify_build_mean_ms,verify_build_p50_ms,verify_build_p95_ms,"
            + "build_mean_delta_pct,build_p50_delta_pct,build_p95_delta_pct,"
            + "drain_delta_pct,cpu_delta_pct");
  }

  private static void writeCsvRow(
      PrintWriter csv, SchemaConfig schema, int run, Result b, Result v) {
    csv.printf(
        "%s,%d,%d,"
            + "%d,%.0f,%d,%d,%d,%d,%d,%d,"
            + "%d,%.0f,%d,%d,%d,%d,%d,%d,"
            + "%.1f,%.1f,%.1f,%.1f,%.1f%n",
        schema.tableName,
        schema.totalColumns(),
        run,
        b.rowCount,
        b.insertRowsPerSec(),
        b.drainMs,
        b.cpuMs,
        b.flushCount,
        b.buildMeanMs,
        b.buildP50Ms,
        b.buildP95Ms,
        v.rowCount,
        v.insertRowsPerSec(),
        v.drainMs,
        v.cpuMs,
        v.flushCount,
        v.buildMeanMs,
        v.buildP50Ms,
        v.buildP95Ms,
        delta(b.buildMeanMs, v.buildMeanMs),
        delta(b.buildP50Ms, v.buildP50Ms),
        delta(b.buildP95Ms, v.buildP95Ms),
        delta(b.drainMs, v.drainMs),
        delta(b.cpuMs, v.cpuMs));
  }

  private static double delta(long baseline, long measured) {
    return baseline == 0 ? 0 : 100.0 * (measured - baseline) / baseline;
  }

  // ---------- result record ----------

  private static class Result {
    final long rowCount, insertMs, drainMs, cpuMs;
    final long flushCount, buildMeanMs, buildP50Ms, buildP95Ms;

    Result(
        long rowCount,
        long insertMs,
        long drainMs,
        long cpuMs,
        long flushCount,
        long buildMeanMs,
        long buildP50Ms,
        long buildP95Ms) {
      this.rowCount = rowCount;
      this.insertMs = insertMs;
      this.drainMs = drainMs;
      this.cpuMs = cpuMs;
      this.flushCount = flushCount;
      this.buildMeanMs = buildMeanMs;
      this.buildP50Ms = buildP50Ms;
      this.buildP95Ms = buildP95Ms;
    }

    double insertRowsPerSec() {
      return rowCount * 1000.0 / insertMs;
    }
  }
}
