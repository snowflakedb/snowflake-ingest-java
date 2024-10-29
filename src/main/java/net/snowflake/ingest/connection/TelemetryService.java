/*
 * Copyright (c) 2022 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.connection;

import static net.snowflake.ingest.connection.RequestBuilder.DEFAULT_VERSION;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.google.common.util.concurrent.RateLimiter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import net.snowflake.client.jdbc.internal.apache.http.impl.client.CloseableHttpClient;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.ObjectNode;
import net.snowflake.client.jdbc.telemetry.TelemetryClient;
import net.snowflake.client.jdbc.telemetry.TelemetryUtil;
import net.snowflake.ingest.utils.Logging;

/**
 * Telemetry service to collect logs in the SDK and send them to Snowflake through the JDBC client
 * telemetry API
 */
public class TelemetryService {
  private final String enableIcebergStreaming;

  // Enum for different client telemetries
  enum TelemetryType {
    STREAMING_INGEST_LATENCY_IN_SEC("streaming_ingest_latency_in_ms"),
    STREAMING_INGEST_CLIENT_FAILURE("streaming_ingest_client_failure"),
    STREAMING_INGEST_THROUGHPUT_BYTES_PER_SEC("streaming_ingest_throughput_bytes_per_sec"),
    STREAMING_INGEST_CPU_MEMORY_USAGE("streaming_ingest_cpu_memory_usage"),
    STREAMING_INGEST_BATCH_OFFSET_MISMATCH("streaming_ingest_batch_offset_mismatch");

    private final String name;

    TelemetryType(String name) {
      this.name = name;
    }

    @Override
    public String toString() {
      return this.name;
    }
  }

  private static final Logging logger = new Logging(TelemetryService.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final String TYPE = "type";
  private static final String CLIENT_NAME = "client_name";
  private static final String ENABLE_ICEBERG_STREAMING = "enable_iceberg_streaming";
  private static final String COUNT = "count";
  private static final String MAX = "max";
  private static final String MIN = "min";
  private static final String MEDIAN = "median";
  private static final String MEAN = "mean";
  private static final String PERCENTILE99TH = "99thPercentile";
  private final TelemetryClient telemetry;
  private final String clientName;
  private final Map<String, RateLimiter> rateLimitersMap;

  /**
   * Default constructor
   *
   * @param httpClient http client
   * @param enableIcebergStreaming whether the ingestion client is running in iceberg mode
   * @param clientName name of the client
   * @param url account url
   */
  TelemetryService(
      CloseableHttpClient httpClient,
      boolean enableIcebergStreaming,
      String clientName,
      String url) {
    this.clientName = clientName;
    this.enableIcebergStreaming = String.valueOf(enableIcebergStreaming);
    this.telemetry = (TelemetryClient) TelemetryClient.createSessionlessTelemetry(httpClient, url);
    this.rateLimitersMap = new HashMap<>();
  }

  /** Flush the telemetry buffer and close the telemetry service */
  public void close() {
    this.telemetry.close();
  }

  /** Report the Streaming Ingest latency metrics */
  public void reportLatencyInSec(
      Timer buildLatency, Timer uploadLatency, Timer registerLatency, Timer flushLatency) {
    if (flushLatency.getCount() > 0) {
      ObjectNode msg = MAPPER.createObjectNode();
      msg.set("build_latency_ms", buildMsgFromTimer(buildLatency));
      msg.set("upload_latency_ms", buildMsgFromTimer(uploadLatency));
      msg.set("register_latency_ms", buildMsgFromTimer(registerLatency));
      msg.set("flush_latency_ms", buildMsgFromTimer(flushLatency));
      send(TelemetryType.STREAMING_INGEST_LATENCY_IN_SEC, msg);
    }
  }

  /** Report the Streaming Ingest failure metrics */
  public void reportClientFailure(String summary, String exception) {
    ObjectNode msg = MAPPER.createObjectNode();
    msg.put("summary", summary);
    msg.put("client_version", DEFAULT_VERSION);
    msg.put("exception", exception);
    send(TelemetryType.STREAMING_INGEST_CLIENT_FAILURE, msg);
  }

  /** Report the Streaming Ingest throughput metrics */
  public void reportThroughputBytesPerSecond(Meter inputThroughput, Meter uploadThroughput) {
    if (inputThroughput.getCount() > 0) {
      ObjectNode msg = MAPPER.createObjectNode();
      msg.put(COUNT, inputThroughput.getCount());
      msg.put("input_mean_rate_bytes_per_sec", inputThroughput.getMeanRate());
      msg.put("upload_mean_rate_bytes_per_sec", uploadThroughput.getMeanRate());
      send(TelemetryType.STREAMING_INGEST_THROUGHPUT_BYTES_PER_SEC, msg);
    }
  }

  /** Report the Streaming Ingest CUP/memory usage metrics */
  public void reportCpuMemoryUsage(Histogram cpuUsage) {
    if (cpuUsage.getCount() > 0) {
      ObjectNode msg = MAPPER.createObjectNode();
      Snapshot cpuSnapshot = cpuUsage.getSnapshot();
      Runtime runTime = Runtime.getRuntime();
      msg.put(COUNT, cpuUsage.getCount());
      msg.put("cpu_max", cpuSnapshot.getMax());
      msg.put("cpu_mean", cpuSnapshot.getMean());
      msg.put("max_memory", runTime.maxMemory());
      msg.put("total_memory", runTime.totalMemory());
      msg.put("free_memory", runTime.freeMemory());
      send(TelemetryType.STREAMING_INGEST_CPU_MEMORY_USAGE, msg);
    }
  }

  /** Report the offset token mismatch in a batch */
  public void reportBatchOffsetMismatch(
      String channelName,
      String prevBatchEndOffset,
      String startOffset,
      String endOffset,
      long rowCount) {
    // Add a rate limiter to report the mismatch at most once every second per channel
    RateLimiter rateLimiter =
        rateLimitersMap.computeIfAbsent(channelName, v -> RateLimiter.create(1.0));
    if (rateLimiter.tryAcquire()) {
      ObjectNode msg = MAPPER.createObjectNode();
      msg.put("channel_name", channelName);
      msg.put("prev_batch_end_offset", prevBatchEndOffset);
      msg.put("start_offset", startOffset);
      msg.put("end_offset", endOffset);
      msg.put("row_count", rowCount);
      send(TelemetryType.STREAMING_INGEST_BATCH_OFFSET_MISMATCH, msg);
    } else {
      logger.logDebug(
          "Rate limit exceeded on reportBatchOffsetMismatch, skipping report it to SF.");
    }
  }

  /** Send log to Snowflake asynchronously through JDBC client telemetry */
  void send(TelemetryType type, ObjectNode msg) {
    try {
      msg.put(TYPE, type.toString());
      msg.put(CLIENT_NAME, clientName);
      msg.put(ENABLE_ICEBERG_STREAMING, enableIcebergStreaming);
      telemetry.addLogToBatch(TelemetryUtil.buildJobData(msg));
    } catch (Exception e) {
      logger.logWarn("Failed to send telemetry data, error: {}", e.getMessage());
    }
  }

  /** Build message from a Timer metric */
  private ObjectNode buildMsgFromTimer(Timer timer) {
    ObjectNode msg = MAPPER.createObjectNode();
    Snapshot buildSnapshot = timer.getSnapshot();
    msg.put(COUNT, timer.getCount());
    msg.put(MAX, TimeUnit.NANOSECONDS.toMillis(buildSnapshot.getMax()));
    msg.put(MIN, TimeUnit.NANOSECONDS.toMillis(buildSnapshot.getMin()));
    msg.put(MEAN, TimeUnit.NANOSECONDS.toMillis((long) buildSnapshot.getMean()));
    msg.put(MEDIAN, TimeUnit.NANOSECONDS.toMillis((long) buildSnapshot.getMedian()));
    msg.put(
        PERCENTILE99TH, TimeUnit.NANOSECONDS.toMillis((long) buildSnapshot.get99thPercentile()));
    return msg;
  }

  /** Refresh JWT token stored in the telemetry client */
  public void refreshToken(String token) {
    telemetry.refreshToken(token);
  }
}
