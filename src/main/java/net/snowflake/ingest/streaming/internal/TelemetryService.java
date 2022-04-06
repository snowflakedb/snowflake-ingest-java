/*
 * Copyright (c) 2022 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.ObjectNode;
import net.snowflake.client.jdbc.telemetry.Telemetry;
import net.snowflake.client.jdbc.telemetry.TelemetryClient;
import net.snowflake.client.jdbc.telemetry.TelemetryUtil;
import net.snowflake.ingest.utils.Logging;
import org.apache.http.impl.client.CloseableHttpClient;

/**
 * Telemetry service to collect logs in the SDK and send them to Snowflake through JDBC client
 * telemetry
 */
class TelemetryService {
  private enum TelemetryType {
    STREAMING_INGEST_BLOB_SIZE("streaming_ingest_blob_size");

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
  private static final String PERCENTILE75TH = "75thPercentile";
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
    this.telemetry =
        TelemetryClient.createSessionlessTelemetry(
            ((net.snowflake.client.jdbc.internal.apache.http.impl.client.CloseableHttpClient)
                httpClient),
            url);
  }

  /** Report the blob size histogram */
  void reportBlobSize(Histogram blobSizeHistogram) {
    ObjectNode msg = MAPPER.createObjectNode();
    Snapshot snapshot = blobSizeHistogram.getSnapshot();
    msg.put(COUNT, blobSizeHistogram.getCount());
    msg.put(MAX, snapshot.getMax());
    msg.put(MIN, snapshot.getMin());
    msg.put(MEDIAN, snapshot.getMedian());
    msg.put(MEAN, snapshot.getMean());
    msg.put(PERCENTILE75TH, snapshot.get75thPercentile());
    msg.put(PERCENTILE99TH, snapshot.get99thPercentile());

    send(TelemetryType.STREAMING_INGEST_BLOB_SIZE, msg);
  }

  /** Send log to Snowflake asynchronously through JDBC client telemetry */
  private void send(TelemetryType type, ObjectNode msg) {
    try {
      msg.put(TYPE, type.toString());
      msg.put(CLIENT_NAME, clientName);

      telemetry.addLogToBatch(TelemetryUtil.buildJobData(msg));
      telemetry.sendBatchAsync();
    } catch (Exception e) {
      logger.logError("Failed to send telemetry data, error: {}", e.getMessage());
    }
  }
}
