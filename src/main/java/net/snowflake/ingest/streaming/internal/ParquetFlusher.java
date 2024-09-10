/*
 * Copyright (c) 2022 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import net.snowflake.ingest.utils.Constants;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.Logging;
import net.snowflake.ingest.utils.Pair;
import net.snowflake.ingest.utils.SFException;
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
  private final boolean enableParquetInternalBuffering;
  private final long maxChunkSizeInBytes;

  private final Constants.BdecParquetCompression bdecParquetCompression;

  /**
   * Construct parquet flusher from its schema and set flag that indicates whether Parquet memory
   * optimization is enabled, i.e. rows will be buffered in internal Parquet buffer.
   */
  public ParquetFlusher(
      MessageType schema,
      boolean enableParquetInternalBuffering,
      long maxChunkSizeInBytes,
      Constants.BdecParquetCompression bdecParquetCompression) {
    this.schema = schema;
    this.enableParquetInternalBuffering = enableParquetInternalBuffering;
    this.maxChunkSizeInBytes = maxChunkSizeInBytes;
    this.bdecParquetCompression = bdecParquetCompression;
  }

  @Override
  public SerializationResult serialize(
      List<ChannelData<ParquetChunkData>> channelsDataPerTable, String filePath)
      throws IOException {
    if (enableParquetInternalBuffering) {
      return serializeFromParquetWriteBuffers(channelsDataPerTable, filePath);
    }
    return serializeFromJavaObjects(channelsDataPerTable, filePath);
  }

  private SerializationResult serializeFromParquetWriteBuffers(
      List<ChannelData<ParquetChunkData>> channelsDataPerTable, String filePath)
      throws IOException {
    List<ChannelMetadata> channelsMetadataList = new ArrayList<>();
    long rowCount = 0L;
    float chunkEstimatedUncompressedSize = 0f;
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
              .setOffsetToken(data.getEndOffsetToken())
              .setStartOffsetToken(data.getStartOffsetToken())
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
      chunkEstimatedUncompressedSize += data.getBufferSize();

      logger.logDebug(
          "Parquet Flusher: Finish building channel={}, rowCount={}, bufferSize={} in blob={}",
          data.getChannelContext().getFullyQualifiedName(),
          data.getRowCount(),
          data.getBufferSize(),
          filePath);
    }

    if (mergedChannelWriter != null) {
      mergedChannelWriter.close();
      this.verifyRowCounts(
          "serializeFromParquetWriteBuffers",
          mergedChannelWriter,
          rowCount,
          channelsDataPerTable,
          -1);
    }
    return new SerializationResult(
        channelsMetadataList,
        columnEpStatsMapCombined,
        rowCount,
        chunkEstimatedUncompressedSize,
        mergedChunkData,
        chunkMinMaxInsertTimeInMs);
  }

  private SerializationResult serializeFromJavaObjects(
      List<ChannelData<ParquetChunkData>> channelsDataPerTable, String filePath)
      throws IOException {
    List<ChannelMetadata> channelsMetadataList = new ArrayList<>();
    long rowCount = 0L;
    float chunkEstimatedUncompressedSize = 0f;
    String firstChannelFullyQualifiedTableName = null;
    Map<String, RowBufferStats> columnEpStatsMapCombined = null;
    List<List<Object>> rows = null;
    BdecParquetWriter parquetWriter;
    ByteArrayOutputStream mergedData = new ByteArrayOutputStream();
    Pair<Long, Long> chunkMinMaxInsertTimeInMs = null;

    for (ChannelData<ParquetChunkData> data : channelsDataPerTable) {
      // Create channel metadata
      ChannelMetadata channelMetadata =
          ChannelMetadata.builder()
              .setOwningChannelFromContext(data.getChannelContext())
              .setRowSequencer(data.getRowSequencer())
              .setOffsetToken(data.getEndOffsetToken())
              .setStartOffsetToken(data.getStartOffsetToken())
              .build();
      // Add channel metadata to the metadata list
      channelsMetadataList.add(channelMetadata);

      logger.logDebug(
          "Parquet Flusher: Start building channel={}, rowCount={}, bufferSize={} in blob={},"
              + " enableParquetMemoryOptimization={}",
          data.getChannelContext().getFullyQualifiedName(),
          data.getRowCount(),
          data.getBufferSize(),
          filePath,
          enableParquetInternalBuffering);

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
      chunkEstimatedUncompressedSize += data.getBufferSize();

      logger.logDebug(
          "Parquet Flusher: Finish building channel={}, rowCount={}, bufferSize={} in blob={},"
              + " enableParquetMemoryOptimization={}",
          data.getChannelContext().getFullyQualifiedName(),
          data.getRowCount(),
          data.getBufferSize(),
          filePath,
          enableParquetInternalBuffering);
    }

    Map<String, String> metadata = channelsDataPerTable.get(0).getVectors().metadata;
    // We insert the filename in the file itself as metadata so that streams can work on replicated
    // mixed tables. For a more detailed discussion on the topic see SNOW-561447 and
    // http://go/streams-on-replicated-mixed-tables
    metadata.put(Constants.PRIMARY_FILE_ID_KEY, StreamingIngestUtils.getShortname(filePath));
    parquetWriter =
        new BdecParquetWriter(
            mergedData,
            schema,
            metadata,
            firstChannelFullyQualifiedTableName,
            maxChunkSizeInBytes,
            bdecParquetCompression);
    rows.forEach(parquetWriter::writeRow);
    parquetWriter.close();

    this.verifyRowCounts(
        "serializeFromJavaObjects", parquetWriter, rowCount, channelsDataPerTable, rows.size());

    return new SerializationResult(
        channelsMetadataList,
        columnEpStatsMapCombined,
        rowCount,
        chunkEstimatedUncompressedSize,
        mergedData,
        chunkMinMaxInsertTimeInMs);
  }

  /**
   * Validates that rows count in metadata matches the row count in Parquet footer and the row count
   * written by the parquet writer
   *
   * @param serializationType Serialization type, used for logging purposes only
   * @param writer Parquet writer writing the data
   * @param channelsDataPerTable Channel data
   * @param totalMetadataRowCount Row count calculated during metadata collection
   * @param javaSerializationTotalRowCount Total row count when java object serialization is used.
   *     Used only for logging purposes if there is a mismatch.
   */
  private void verifyRowCounts(
      String serializationType,
      BdecParquetWriter writer,
      long totalMetadataRowCount,
      List<ChannelData<ParquetChunkData>> channelsDataPerTable,
      long javaSerializationTotalRowCount) {
    long parquetTotalRowsWritten = writer.getRowsWritten();

    List<Long> parquetFooterRowsPerBlock = writer.getRowCountsFromFooter();
    long parquetTotalRowsInFooter = 0;
    for (long perBlockCount : parquetFooterRowsPerBlock) {
      parquetTotalRowsInFooter += perBlockCount;
    }

    if (parquetTotalRowsInFooter != totalMetadataRowCount
        || parquetTotalRowsWritten != totalMetadataRowCount) {

      final String perChannelRowCountsInMetadata =
          channelsDataPerTable.stream()
              .map(x -> String.valueOf(x.getRowCount()))
              .collect(Collectors.joining(","));

      final String channelNames =
          channelsDataPerTable.stream()
              .map(x -> String.valueOf(x.getChannelContext().getName()))
              .collect(Collectors.joining(","));

      final String perBlockRowCountsInFooter =
          parquetFooterRowsPerBlock.stream().map(String::valueOf).collect(Collectors.joining(","));

      final long channelsCountInMetadata = channelsDataPerTable.size();

      throw new SFException(
          ErrorCode.INTERNAL_ERROR,
          String.format(
              "[%s]The number of rows in Parquet does not match the number of rows in metadata. "
                  + "parquetTotalRowsInFooter=%d "
                  + "totalMetadataRowCount=%d "
                  + "parquetTotalRowsWritten=%d "
                  + "perChannelRowCountsInMetadata=%s "
                  + "perBlockRowCountsInFooter=%s "
                  + "channelsCountInMetadata=%d "
                  + "countOfSerializedJavaObjects=%d "
                  + "channelNames=%s",
              serializationType,
              parquetTotalRowsInFooter,
              totalMetadataRowCount,
              parquetTotalRowsWritten,
              perChannelRowCountsInMetadata,
              perBlockRowCountsInFooter,
              channelsCountInMetadata,
              javaSerializationTotalRowCount,
              channelNames));
    }
  }
}
