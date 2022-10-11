/*
 * Copyright (c) 2022 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Interface to convert {@link ChannelData} buffered in {@link RowBuffer} to the underlying format
 * implementation for faster processing.
 *
 * @param <T> type of column data (Arrow {@link org.apache.arrow.vector.VectorSchemaRoot} or {@link
 *     ParquetChunkData})
 */
public interface Flusher<T> {
  /**
   * Serialize buffered rows into the underlying format.
   *
   * @param channelsDataPerTable buffered rows
   * @param chunkData output
   * @param filePath file path
   * @return {@link SerializationResult}
   * @throws IOException
   */
  SerializationResult serialize(
      List<ChannelData<T>> channelsDataPerTable, ByteArrayOutputStream chunkData, String filePath)
      throws IOException;

  /** Holds result of the buffered rows conversion: channel metadata and stats. */
  class SerializationResult {
    final List<ChannelMetadata> channelsMetadataList;
    final Map<String, RowBufferStats> columnEpStatsMapCombined;
    final long rowCount;

    public SerializationResult(
        List<ChannelMetadata> channelsMetadataList,
        Map<String, RowBufferStats> columnEpStatsMapCombined,
        long rowCount) {
      this.channelsMetadataList = channelsMetadataList;
      this.columnEpStatsMapCombined = columnEpStatsMapCombined;
      this.rowCount = rowCount;
    }
  }
}
