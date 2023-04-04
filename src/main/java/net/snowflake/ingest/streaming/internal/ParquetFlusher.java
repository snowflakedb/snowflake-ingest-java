/*
 * Copyright (c) 2022 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import net.snowflake.ingest.utils.*;
import org.apache.parquet.hadoop.BdecParquetBufferWriter;
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
  private final ParquetRowBuffer.BufferingType parquetBufferingType;

  /**
   * Construct parquet flusher from its schema and set flag that indicates whether Parquet memory
   * optimization is enabled, i.e. rows will be buffered in internal Parquet buffer.
   */
  public ParquetFlusher(MessageType schema, ParquetRowBuffer.BufferingType parquetBufferingType) {
    this.schema = schema;
    this.parquetBufferingType = parquetBufferingType;
  }

  @Override
  public SerializationResult serialize(
      List<ChannelData<ParquetChunkData>> channelsDataPerTable, String filePath)
      throws IOException {
    if (parquetBufferingType == ParquetRowBuffer.BufferingType.PARQUET_WRITER) {
      return serializeFromParquetWriteBuffers(channelsDataPerTable, filePath);
    } else if (parquetBufferingType == ParquetRowBuffer.BufferingType.PARQUET_BUFFERS) {
      return serializeFromParquetWriteBdecBuffers(channelsDataPerTable, filePath);
    } else {
      return serializeFromJavaObjects(channelsDataPerTable, filePath);
    }
  }

  private SerializationResult serializeFromParquetWriteBuffers(
      List<ChannelData<ParquetChunkData>> channelsDataPerTable, String filePath)
      throws IOException {
    List<ChannelMetadata> channelsMetadataList = new ArrayList<>();
    long rowCount = 0L;
    String firstChannelFullyQualifiedTableName = null;
    Map<String, RowBufferStats> columnEpStatsMapCombined = null;
    BdecParquetWriter mergedChannelWriter = null;
    ByteArrayOutputStream mergedChunkData = new ByteArrayOutputStream();
    Pair<Long, Long> chunkMinMaxInsertTimeInMs = null;

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
          data.getChannelContext().getFullyQualifiedName(),
          data.getRowCount(),
          data.getBufferSize(),
          filePath);

      if (mergedChannelWriter == null) {
        columnEpStatsMapCombined = data.getColumnEps();
        mergedChannelWriter = data.getVectors().parquetWriter;
        mergedChunkData = data.getVectors().output;
        firstChannelFullyQualifiedTableName = data.getChannelContext().getFullyQualifiedTableName();
        chunkMinMaxInsertTimeInMs = data.getMinMaxInsertTimeInMs();
      } else {
        // This method assumes that channelsDataPerTable is grouped by table. We double check
        // here and throw an error if the assumption is violated
        if (!data.getChannelContext()
            .getFullyQualifiedTableName()
            .equals(firstChannelFullyQualifiedTableName)) {
          throw new SFException(ErrorCode.INVALID_DATA_IN_CHUNK);
        }

        columnEpStatsMapCombined =
            ChannelData.getCombinedColumnStatsMap(columnEpStatsMapCombined, data.getColumnEps());
        data.getVectors().parquetWriter.close();
        BdecParquetReader.readFileIntoWriter(
            data.getVectors().output.toByteArray(), mergedChannelWriter);
        chunkMinMaxInsertTimeInMs =
            ChannelData.getCombinedMinMaxInsertTimeInMs(
                chunkMinMaxInsertTimeInMs, data.getMinMaxInsertTimeInMs());
      }

      rowCount += data.getRowCount();

      logger.logDebug(
          "Parquet Flusher: Finish building channel={}, rowCount={}, bufferSize={} in blob={}",
          data.getChannelContext().getFullyQualifiedName(),
          data.getRowCount(),
          data.getBufferSize(),
          filePath);
    }

    if (mergedChannelWriter != null) {
      mergedChannelWriter.close();
    }
    return new SerializationResult(
        channelsMetadataList,
        columnEpStatsMapCombined,
        rowCount,
        mergedChunkData,
        chunkMinMaxInsertTimeInMs);
  }

  private SerializationResult serializeFromJavaObjects(
      List<ChannelData<ParquetChunkData>> channelsDataPerTable, String filePath)
      throws IOException {
    List<ChannelMetadata> channelsMetadataList = new ArrayList<>();
    long rowCount = 0L;
    String firstChannelFullyQualifiedTableName = null;
    Map<String, RowBufferStats> columnEpStatsMapCombined = null;
    List<List<Object>> rows = null;
    BdecParquetWriter parquetWriter;
    ByteArrayOutputStream mergedData = new EnhancedByteArrayOutputStream();
    Pair<Long, Long> chunkMinMaxInsertTimeInMs = null;

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
          "Parquet Flusher: Start building channel={}, rowCount={}, bufferSize={} in blob={},"
              + " parquetBufferingType={}",
          data.getChannelContext().getFullyQualifiedName(),
          data.getRowCount(),
          data.getBufferSize(),
          filePath,
          parquetBufferingType);

      if (rows == null) {
        columnEpStatsMapCombined = data.getColumnEps();
        rows = new ArrayList<>();
        firstChannelFullyQualifiedTableName = data.getChannelContext().getFullyQualifiedTableName();
        chunkMinMaxInsertTimeInMs = data.getMinMaxInsertTimeInMs();
      } else {
        // This method assumes that channelsDataPerTable is grouped by table. We double check
        // here and throw an error if the assumption is violated
        if (!data.getChannelContext()
            .getFullyQualifiedTableName()
            .equals(firstChannelFullyQualifiedTableName)) {
          throw new SFException(ErrorCode.INVALID_DATA_IN_CHUNK);
        }

        columnEpStatsMapCombined =
            ChannelData.getCombinedColumnStatsMap(columnEpStatsMapCombined, data.getColumnEps());
        chunkMinMaxInsertTimeInMs =
            ChannelData.getCombinedMinMaxInsertTimeInMs(
                chunkMinMaxInsertTimeInMs, data.getMinMaxInsertTimeInMs());
      }

      rows.addAll(data.getVectors().rows);

      rowCount += data.getRowCount();

      logger.logDebug(
          "Parquet Flusher: Finish building channel={}, rowCount={}, bufferSize={} in blob={},"
              + " enableParquetMemoryOptimization={}",
          data.getChannelContext().getFullyQualifiedName(),
          data.getRowCount(),
          data.getBufferSize(),
          filePath);
    }

    Map<String, String> metadata = channelsDataPerTable.get(0).getVectors().metadata;
    parquetWriter =
        new BdecParquetWriter(mergedData, schema, metadata, firstChannelFullyQualifiedTableName);
    rows.forEach(parquetWriter::writeRow);
    parquetWriter.close();

    return new SerializationResult(
        channelsMetadataList,
        columnEpStatsMapCombined,
        rowCount,
        mergedData,
        chunkMinMaxInsertTimeInMs);
  }

  private SerializationResult serializeFromParquetWriteBdecBuffers(
          List<ChannelData<ParquetChunkData>> channelsDataPerTable, String filePath)
          throws IOException {
    List<ChannelMetadata> channelsMetadataList = new ArrayList<>();
    long rowCount = 0L;
    String firstChannelFullyQualifiedTableName = null;
    Map<String, RowBufferStats> columnEpStatsMapCombined = null;
    BdecParquetBufferWriter mergedChannelWriter = null;
    ByteArrayOutputStream mergedChunkData = new ByteArrayOutputStream();
    Pair<Long, Long> chunkMinMaxInsertTimeInMs = null;

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
              data.getChannelContext().getFullyQualifiedName(),
              data.getRowCount(),
              data.getBufferSize(),
              filePath);

      if (mergedChannelWriter == null) {
        columnEpStatsMapCombined = data.getColumnEps();
        mergedChannelWriter = data.getVectors().parquetBufferWriter;
        mergedChunkData = data.getVectors().output;
        firstChannelFullyQualifiedTableName = data.getChannelContext().getFullyQualifiedTableName();
        chunkMinMaxInsertTimeInMs = data.getMinMaxInsertTimeInMs();
      } else {
        // This method assumes that channelsDataPerTable is grouped by table. We double check
        // here and throw an error if the assumption is violated
        if (!data.getChannelContext()
                .getFullyQualifiedTableName()
                .equals(firstChannelFullyQualifiedTableName)) {
          throw new SFException(ErrorCode.INVALID_DATA_IN_CHUNK);
        }

        columnEpStatsMapCombined =
                ChannelData.getCombinedColumnStatsMap(columnEpStatsMapCombined, data.getColumnEps());
        mergedChannelWriter.appendWriter(data.getVectors().parquetBufferWriter);
        chunkMinMaxInsertTimeInMs =
                ChannelData.getCombinedMinMaxInsertTimeInMs(
                        chunkMinMaxInsertTimeInMs, data.getMinMaxInsertTimeInMs());
      }

      rowCount += data.getRowCount();

      logger.logDebug(
              "Parquet Flusher: Finish building channel={}, rowCount={}, bufferSize={} in blob={}",
              data.getChannelContext().getFullyQualifiedName(),
              data.getRowCount(),
              data.getBufferSize(),
              filePath);
    }

    if (mergedChannelWriter != null) {
      mergedChannelWriter.close();
    }
    return new SerializationResult(
            channelsMetadataList,
            columnEpStatsMapCombined,
            rowCount,
            mergedChunkData,
            chunkMinMaxInsertTimeInMs);
  }
}
