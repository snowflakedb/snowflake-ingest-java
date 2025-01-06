package net.snowflake.ingest.streaming.internal;

import net.snowflake.ingest.connection.TelemetryService;
import net.snowflake.ingest.streaming.ObservabilityClusteringKey;
import net.snowflake.ingest.streaming.OffsetTokenVerificationFunction;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import org.apache.parquet.column.ParquetProperties;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.function.Consumer;

class ObservabilityParquetRowBuffer extends ParquetRowBuffer {
  private final ObservabilityClusteringKey bufferClusteringKey;

  /**
   * Construct a ObservabilityParquetRowBuffer object.
   */
  ObservabilityParquetRowBuffer(
      ObservabilityClusteringKey bufferClusteringKey,
      OpenChannelRequest.OnErrorOption onErrorOption,
      ZoneId defaultTimezone,
      String fullyQualifiedChannelName,
      Consumer<Float> rowSizeMetric,
      ChannelRuntimeState channelRuntimeState,
      ClientBufferParameters clientBufferParameters,
      OffsetTokenVerificationFunction offsetTokenVerificationFunction,
      ParquetProperties.WriterVersion parquetWriterVersion,
      TelemetryService telemetryService) {
    super(onErrorOption, defaultTimezone, fullyQualifiedChannelName, rowSizeMetric, channelRuntimeState, clientBufferParameters, offsetTokenVerificationFunction, parquetWriterVersion, telemetryService);

    this.bufferClusteringKey = bufferClusteringKey;
  }

  @Override
  protected ChannelData<ParquetChunkData> newChannelData() {
    ObservabilityChannelData data = new ObservabilityChannelData();
    data.setClusteringKey(bufferClusteringKey);

    return data;
  }
}
