/*
 * Copyright (c) 2022-2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import net.snowflake.ingest.utils.Pair;

/**
 * Interface to convert {@link ChannelData} buffered in {@link RowBuffer} to the underlying format
 * implementation for faster processing.
 *
 * @param <T> type of column data ({@link ParquetChunkData})
 */
public interface Flusher<T> {
  /**
   * Serialize buffered rows into the underlying format.
   *
   * @param fullyQualifiedTableName
   * @param channelsDataPerTable buffered rows
   * @param filePath file path
   * @param chunkStartOffset
   * @return {@link SerializationResult}
   * @throws IOException
   */
  SerializationResult serialize(
      List<ChannelData<T>> channelsDataPerTable, String filePath, long chunkStartOffset)
      throws IOException;

  /** Holds result of the buffered rows conversion: channel metadata and stats. */
  class SerializationResult {
    final List<ChannelMetadata> channelsMetadataList;
    final Map<String, RowBufferStats> columnEpStatsMapCombined;
    final long rowCount;
    final float chunkEstimatedUncompressedSize;
    final ByteArrayOutputStream chunkData;
    final Pair<Long, Long> chunkMinMaxInsertTimeInMs;
    final long extendedMetadataSize;

    public SerializationResult(
        List<ChannelMetadata> channelsMetadataList,
        Map<String, RowBufferStats> columnEpStatsMapCombined,
        long rowCount,
        float chunkEstimatedUncompressedSize,
        ByteArrayOutputStream chunkData,
        Pair<Long, Long> chunkMinMaxInsertTimeInMs,
        long extendedMetadataSize) {
      this.channelsMetadataList = channelsMetadataList;
      this.columnEpStatsMapCombined = columnEpStatsMapCombined;
      this.rowCount = rowCount;
      this.chunkEstimatedUncompressedSize = chunkEstimatedUncompressedSize;
      this.chunkData = chunkData;
      this.chunkMinMaxInsertTimeInMs = chunkMinMaxInsertTimeInMs;
      this.extendedMetadataSize = extendedMetadataSize;
    }
  }
}
