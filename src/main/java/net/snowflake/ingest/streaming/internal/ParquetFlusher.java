/*
 * Copyright (c) 2022 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.Logging;
import net.snowflake.ingest.utils.SFException;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.parquet.hadoop.BdecParquetReader;
import org.apache.parquet.hadoop.BdecParquetWriter;
import org.apache.parquet.schema.MessageType;

/**
 * Converts {@link ChannelData} buffered in {@link RowBuffer} to the Parquet format for faster
 * processing.
 */
public class ParquetFlusher implements Flusher<ParquetChunkData> {
  private static final Logging logger = new Logging(ParquetFlusher.class);
  private final MessageType schema;
  private final BufferAllocator allocator;

  /** Construct parquet flusher from its schema. */
  public ParquetFlusher(MessageType schema, BufferAllocator allocator) {
    this.schema = schema;
    this.allocator = allocator;
  }

  @Override
  public SerializationResult serialize(
      List<ChannelData<ParquetChunkData>> channelsDataPerTable, String filePath)
      throws IOException {
    List<ChannelMetadata> channelsMetadataList = new ArrayList<>();
    long rowCount = 0L;
    String firstChannelFullyQualifiedTableName = null;
    Map<String, RowBufferStats> columnEpStatsMapCombined = null;
    BdecParquetWriter mergedChannelWriter = null;
    ByteArrayOutputStream chunkData = new ByteArrayOutputStream();

    for (ChannelData<ParquetChunkData> data : channelsDataPerTable) {
      // Create channel metadata
      ChannelMetadata channelMetadata =
          ChannelMetadata.builder()
              .setOwningChannelFromContext(data.getChannelContext())
              .setRowSequencer(data.getRowSequencer())
              .setOffsetToken(data.getOffsetToken())
              .build();
      // Add channel metadata to the metadata list
      channelsMetadataList.add(channelMetadata);

      logger.logDebug(
          "Parquet Flusher: Start building channel={}, rowCount={}, bufferSize={} in blob={}",
          data.getChannelContext().fullyQualifiedName,
          data.getRowCount(),
          data.getBufferSize(),
          filePath);

      if (mergedChannelWriter == null) {
        columnEpStatsMapCombined = data.getColumnEps();
        mergedChannelWriter = data.getVectors().parquetWriter;
        chunkData = data.getVectors().output;
        firstChannelFullyQualifiedTableName = data.getChannelContext().fullyQualifiedTableName;
      } else {
        // This method assumes that channelsDataPerTable is grouped by table. We double check
        // here and throw an error if the assumption is violated
        if (!data.getChannelContext()
            .fullyQualifiedTableName
            .equals(firstChannelFullyQualifiedTableName)) {
          throw new SFException(ErrorCode.INVALID_DATA_IN_CHUNK);
        }

        columnEpStatsMapCombined =
            ChannelData.getCombinedColumnStatsMap(columnEpStatsMapCombined, data.getColumnEps());

        data.getVectors().parquetWriter.close();
        BdecParquetReader.readFileIntoWriter(
            data.getVectors().output.toByteArray(), mergedChannelWriter, allocator);
      }

      rowCount += data.getRowCount();

      logger.logDebug(
          "Parquet Flusher: Finish building channel={}, rowCount={}, bufferSize={} in blob={}",
          data.getChannelContext().fullyQualifiedName,
          data.getRowCount(),
          data.getBufferSize(),
          filePath);
    }

    // Map<String, String> metadata = channelsDataPerTable.get(0).getVectors().metadata;
    // flushToParquetBdecChunk(chunkData, rows, metadata, channelsMetadataList);

    if (mergedChannelWriter != null) {
      mergedChannelWriter.close();
    }
    return new SerializationResult(
        channelsMetadataList, columnEpStatsMapCombined, rowCount, chunkData);
  }
}
