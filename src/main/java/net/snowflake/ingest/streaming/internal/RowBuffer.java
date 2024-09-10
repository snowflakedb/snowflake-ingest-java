/*
 * Copyright (c) 2022 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import java.util.List;
import java.util.Map;
import net.snowflake.ingest.streaming.InsertValidationResponse;

/**
 * Interface for the buffer in the Streaming Ingest channel that holds the un-flushed rows, these
 * rows will be converted to the underlying format implementation for faster processing
 *
 * @param <T> type of column data ({@link ParquetChunkData})
 */
interface RowBuffer<T> {
  /**
   * Set up the column fields and vectors using the column metadata from the server
   *
   * @param columns list of column metadata
   */
  void setupSchema(List<ColumnMetadata> columns);

  /**
   * Insert a batch of rows into the row buffer
   *
   * @param rows input row
   * @param startOffsetToken start offset token of the batch
   * @param endOffsetToken offset token of the latest row in the batch
   * @return insert response that possibly contains errors because of insertion failures
   */
  InsertValidationResponse insertRows(
      Iterable<Map<String, Object>> rows, String startOffsetToken, String endOffsetToken);

  /**
   * Flush the data in the row buffer by taking the ownership of the old vectors and pass all the
   * required info back to the flush service to build the blob
   *
   * @return A ChannelData object that contains the info needed by the flush service to build a blob
   */
  ChannelData<T> flush();

  /**
   * Close the row buffer and release resources. Note that the caller needs to handle
   * synchronization
   */
  void close(String name);

  /**
   * Get the current buffer size
   *
   * @return the current buffer size
   */
  float getSize();

  /**
   * Create {@link Flusher} implementation to flush the buffered rows to the underlying format
   * implementation for faster processing.
   *
   * @return flusher
   */
  Flusher<T> createFlusher();
}
