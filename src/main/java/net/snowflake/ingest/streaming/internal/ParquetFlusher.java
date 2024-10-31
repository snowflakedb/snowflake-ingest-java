/*
 * Copyright (c) 2022-2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import net.snowflake.ingest.utils.Constants;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.Logging;
import net.snowflake.ingest.utils.Pair;
import net.snowflake.ingest.utils.SFException;
import org.apache.parquet.Preconditions;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.SnowflakeParquetWriter;
import org.apache.parquet.schema.MessageType;

/**
 * Converts {@link ChannelData} buffered in {@link RowBuffer} to the Parquet format for faster
 * processing.
 */
public class ParquetFlusher implements Flusher<ParquetChunkData> {
  private static final Logging logger = new Logging(ParquetFlusher.class);
  private final MessageType schema;
  private final long maxChunkSizeInBytes;
  private final Optional<Integer> maxRowGroups;

  private final Constants.BdecParquetCompression bdecParquetCompression;
  private final ParquetProperties.WriterVersion parquetWriterVersion;
  private final boolean enableDictionaryEncoding;
  private final boolean enableIcebergStreaming;

  /** Construct parquet flusher from its schema. */
  public ParquetFlusher(
      MessageType schema,
      long maxChunkSizeInBytes,
      Optional<Integer> maxRowGroups,
      Constants.BdecParquetCompression bdecParquetCompression,
      ParquetProperties.WriterVersion parquetWriterVersion,
      boolean enableDictionaryEncoding,
      boolean enableIcebergStreaming) {
    this.schema = schema;
    this.maxChunkSizeInBytes = maxChunkSizeInBytes;
    this.maxRowGroups = maxRowGroups;
    this.bdecParquetCompression = bdecParquetCompression;
    this.parquetWriterVersion = parquetWriterVersion;
    this.enableDictionaryEncoding = enableDictionaryEncoding;
    this.enableIcebergStreaming = enableIcebergStreaming;
  }

  @Override
  public SerializationResult serialize(
      List<ChannelData<ParquetChunkData>> channelsDataPerTable,
      String filePath,
      long chunkStartOffset)
      throws IOException {
    return serializeFromJavaObjects(channelsDataPerTable, filePath, chunkStartOffset);
  }

  private SerializationResult serializeFromJavaObjects(
      List<ChannelData<ParquetChunkData>> channelsDataPerTable,
      String filePath,
      long chunkStartOffset)
      throws IOException {
    List<ChannelMetadata> channelsMetadataList = new ArrayList<>();
    long rowCount = 0L;
    float chunkEstimatedUncompressedSize = 0f;
    String firstChannelFullyQualifiedTableName = null;
    Map<String, RowBufferStats> columnEpStatsMapCombined = null;
    List<List<Object>> rows = null;
    SnowflakeParquetWriter parquetWriter;
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
          "Parquet Flusher: Start building channel={}, rowCount={}, bufferSize={} in blob={}",
          data.getChannelContext().getFullyQualifiedName(),
          data.getRowCount(),
          data.getBufferSize(),
          filePath);

      if (rows == null) {
        columnEpStatsMapCombined = data.getColumnEps();
        rows = new ArrayList<>();
        firstChannelFullyQualifiedTableName = data.getChannelContext().getFullyQualifiedTableName();
        chunkMinMaxInsertTimeInMs = data.getMinMaxInsertTimeInMs();
      } else {
        // This method assumes that channelsDataPerTable is grouped by table. We double-check
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
          "Parquet Flusher: Finish building channel={}, rowCount={}, bufferSize={} in blob={}",
          data.getChannelContext().getFullyQualifiedName(),
          data.getRowCount(),
          data.getBufferSize(),
          filePath);
    }

    Map<String, String> metadata = channelsDataPerTable.get(0).getVectors().metadata;
    addFileIdToMetadata(filePath, chunkStartOffset, metadata);
    parquetWriter =
        new SnowflakeParquetWriter(
            mergedData,
            schema,
            metadata,
            firstChannelFullyQualifiedTableName,
            maxChunkSizeInBytes,
            maxRowGroups,
            bdecParquetCompression,
            parquetWriterVersion,
            enableDictionaryEncoding);
    rows.forEach(parquetWriter::writeRow);
    parquetWriter.close();

    this.verifyRowCounts(parquetWriter, rowCount, channelsDataPerTable, rows.size());

    return new SerializationResult(
        channelsMetadataList,
        columnEpStatsMapCombined,
        rowCount,
        chunkEstimatedUncompressedSize,
        mergedData,
        chunkMinMaxInsertTimeInMs,
        parquetWriter.getExtendedMetadataSize());
  }

  private void addFileIdToMetadata(
      String filePath, long chunkStartOffset, Map<String, String> metadata) {
    // We insert the filename in the file itself as metadata so that streams can work on replicated
    // mixed tables. For a more detailed discussion on the topic see SNOW-561447 and
    // http://go/streams-on-replicated-mixed-tables,  and
    // http://go/managed-iceberg-replication-change-tracking
    // Using chunk offset as suffix ensures that for interleaved tables, the file
    // id key is unique for each chunk. Each chunk is logically a separate Parquet file that happens
    // to be bundled together.
    if (chunkStartOffset == 0) {
      metadata.put(
          enableIcebergStreaming
              ? Constants.ASSIGNED_FULL_FILE_NAME_KEY
              : Constants.PRIMARY_FILE_ID_KEY,
          StreamingIngestUtils.getShortname(filePath));
    } else {
      Preconditions.checkState(
          !enableIcebergStreaming, "Iceberg streaming is not supported with non-zero offsets");
      String shortName = StreamingIngestUtils.getShortname(filePath);
      final String[] parts = shortName.split("\\.");
      Preconditions.checkState(parts.length == 2, "Invalid file name format");
      metadata.put(
          Constants.PRIMARY_FILE_ID_KEY,
          String.format("%s_%d.%s", parts[0], chunkStartOffset, parts[1]));
    }
  }

  /**
   * Validates that rows count in metadata matches the row count in Parquet footer and the row count
   * written by the parquet writer
   *
   * @param writer Parquet writer writing the data
   * @param channelsDataPerTable Channel data
   * @param totalMetadataRowCount Row count calculated during metadata collection
   * @param javaSerializationTotalRowCount Total row count when java object serialization is used.
   *     Used only for logging purposes if there is a mismatch.
   */
  private void verifyRowCounts(
      SnowflakeParquetWriter writer,
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
              "The number of rows in Parquet does not match the number of rows in metadata. "
                  + "parquetTotalRowsInFooter=%d "
                  + "totalMetadataRowCount=%d "
                  + "parquetTotalRowsWritten=%d "
                  + "perChannelRowCountsInMetadata=%s "
                  + "perBlockRowCountsInFooter=%s "
                  + "channelsCountInMetadata=%d "
                  + "countOfSerializedJavaObjects=%d "
                  + "channelNames=%s",
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
