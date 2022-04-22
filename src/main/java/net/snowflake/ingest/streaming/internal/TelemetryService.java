/*
 * Copyright (c) 2022 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import net.snowflake.client.jdbc.internal.apache.http.impl.client.CloseableHttpClient;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.ObjectNode;
import net.snowflake.client.jdbc.telemetry.Telemetry;
import net.snowflake.client.jdbc.telemetry.TelemetryClient;
import net.snowflake.client.jdbc.telemetry.TelemetryUtil;
import net.snowflake.ingest.utils.Logging;

/**
 * Telemetry service to collect logs in the SDK and send them to Snowflake through JDBC client
 * telemetry
 */
class TelemetryService {
  private enum TelemetryType {
    STREAMING_INGEST_LATENCY_IN_SEC("streaming_ingest_latency_in_sec"),
    STREAMING_INGEST_CLIENT_FAILURE("streaming_ingest_client_failure"),
    STREAMING_INGEST_THROUGHPUT_BYTES_PER_SEC("streaming_ingest_throughput_bytes_per_sec"),
    STREAMING_INGEST_CPU_MEMORY_USAGE("streaming_ingest_cpu_memory_usage");

    private final String name;

    TelemetryType(String name) {
      this.name = name;
    }

    @Override
    public String toString() {
      return this.name;
    }
  }

  private static final Logging logger = new Logging(FlushService.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final String TYPE = "type";
  private static final String CLIENT_NAME = "client_name";
  private static final String COUNT = "count";
  private static final String MAX = "max";
  private static final String MIN = "min";
  private static final String MEDIAN = "median";
  private static final String MEAN = "mean";
  private static final String PERCENTILE99TH = "99thPercentile";
  private final Telemetry telemetry;
  private final String clientName;

  /**
   * Default constructor
   *
   * @param httpClient http client
   * @param clientName name of the client
   * @param url account url
   */
  TelemetryService(CloseableHttpClient httpClient, String clientName, String url) {
    this.clientName = clientName;
    this.telemetry = TelemetryClient.createSessionlessTelemetry(httpClient, url);
  }

  /** Report the SDK latency metrics */
  void reportLatencyInSec(
      Timer buildLatency, Timer uploadLatency, Timer registerLatency, Timer flushLatency) {
    ObjectNode msg = MAPPER.createObjectNode();
    msg.set("build_latency_sec", buildMsgFromTimer(buildLatency));
    msg.set("upload_latency_sec", buildMsgFromTimer(uploadLatency));
    msg.set("register_latency_sec", buildMsgFromTimer(registerLatency));
    msg.set("flush_latency_sec", buildMsgFromTimer(flushLatency));
    send(TelemetryType.STREAMING_INGEST_LATENCY_IN_SEC, msg);
  }

  /** Report the SDK latency metrics */
  void reportClientFailure(String fileName, String exception) {
    ObjectNode msg = MAPPER.createObjectNode();
    msg.put("file_name", fileName);
    msg.put("exception", exception);
    send(TelemetryType.STREAMING_INGEST_CLIENT_FAILURE, msg);
  }

  /** Report the SDK latency metrics */
  void reportThroughputBytesPerSecond(Meter inputThroughput, Meter uploadThrough) {
    ObjectNode msg = MAPPER.createObjectNode();
    msg.put("input_mean_rate_bytes_per_sec", inputThroughput.getMeanRate());
    msg.put("upload_mean_rate_bytes_per_sec", uploadThrough.getMeanRate());
    send(TelemetryType.STREAMING_INGEST_THROUGHPUT_BYTES_PER_SEC, msg);
  }

  /** Report the SDK latency metrics */
  void reportCpuMemoryUsage(Histogram cpuUsage) {
    ObjectNode msg = MAPPER.createObjectNode();
    Snapshot cpuSnapshot = cpuUsage.getSnapshot();
    Runtime runTime = Runtime.getRuntime();
    msg.put("cpu_max", cpuSnapshot.getMax());
    msg.put("cpu_mean", cpuSnapshot.getMean());
    msg.put("max_memory", runTime.maxMemory());
    msg.put("total_memory", runTime.totalMemory());
    msg.put("free_memory", runTime.freeMemory());
    send(TelemetryType.STREAMING_INGEST_CPU_MEMORY_USAGE, msg);
  }

  /** Send log to Snowflake asynchronously through JDBC client telemetry */
  private void send(TelemetryType type, ObjectNode msg) {
    try {
      msg.put(TYPE, type.toString());
      msg.put(CLIENT_NAME, clientName);

      telemetry.addLogToBatch(TelemetryUtil.buildJobData(msg));
      // telemetry.sendBatchAsync(); // TODO: REMOVE
    } catch (Exception e) {
      logger.logWarn("Failed to send telemetry data, error: {}", e.getMessage());
    }
  }

  private ObjectNode buildMsgFromTimer(Timer timer) {
    ObjectNode msg = MAPPER.createObjectNode();
    Snapshot buildSnapshot = timer.getSnapshot();
    msg.put(COUNT, timer.getCount());
    msg.put(MAX, buildSnapshot.getMax());
    msg.put(MIN, buildSnapshot.getMin());
    msg.put(MEAN, buildSnapshot.getMean());
    msg.put(MEDIAN, buildSnapshot.getMedian());
    msg.put(PERCENTILE99TH, buildSnapshot.get99thPercentile());
    return msg;
  }
}
