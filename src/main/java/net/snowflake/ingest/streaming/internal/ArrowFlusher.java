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
import net.snowflake.ingest.utils.Pair;
import net.snowflake.ingest.utils.SFException;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.ArrowWriter;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;

/**
 * Converts {@link ChannelData} buffered in {@link RowBuffer} to the Arrow format for faster
 * processing.
 */
public class ArrowFlusher implements Flusher<VectorSchemaRoot> {
  private static final Logging logger = new Logging(ArrowFlusher.class);

  @Override
  public Flusher.SerializationResult serialize(
      List<ChannelData<VectorSchemaRoot>> channelsDataPerTable,
      ByteArrayOutputStream chunkData,
      String filePath)
      throws IOException {
    List<ChannelMetadata> channelsMetadataList = new ArrayList<>();
    long rowCount = 0L;
    VectorSchemaRoot root = null;
    ArrowWriter arrowWriter = null;
    VectorLoader loader = null;
    SnowflakeStreamingIngestChannelInternal<VectorSchemaRoot> firstChannel = null;
    Map<String, RowBufferStats> columnEpStatsMapCombined = null;
    Pair<Long, Long> chunkMinMaxInsertTimeInMs = null;

    try {
      for (ChannelData<VectorSchemaRoot> data : channelsDataPerTable) {
        // Create channel metadata
        ChannelMetadata channelMetadata =
            ChannelMetadata.builder()
                .setOwningChannel(data.getChannel())
                .setRowSequencer(data.getRowSequencer())
                .setOffsetToken(data.getOffsetToken())
                .build();
        // Add channel metadata to the metadata list
        channelsMetadataList.add(channelMetadata);

        logger.logDebug(
            "Start building channel={}, rowCount={}, bufferSize={} in blob={}",
            data.getChannel().getFullyQualifiedName(),
            data.getRowCount(),
            data.getBufferSize(),
            filePath);

        if (root == null) {
          columnEpStatsMapCombined = data.getColumnEps();
          root = data.getVectors();
          arrowWriter = new ArrowStreamWriter(root, null, chunkData);
          loader = new VectorLoader(root);
          firstChannel = data.getChannel();
          arrowWriter.start();
          chunkMinMaxInsertTimeInMs = data.getMinMaxInsertTimeInMs();
        } else {
          // This method assumes that channelsDataPerTable is grouped by table. We double check
          // here and throw an error if the assumption is violated
          if (!data.getChannel()
              .getFullyQualifiedTableName()
              .equals(firstChannel.getFullyQualifiedTableName())) {
            throw new SFException(ErrorCode.INVALID_DATA_IN_CHUNK);
          }

          columnEpStatsMapCombined =
              ChannelData.getCombinedColumnStatsMap(columnEpStatsMapCombined, data.getColumnEps());
          chunkMinMaxInsertTimeInMs =
              ChannelData.getCombinedMinMaxInsertTimeInMs(
                  chunkMinMaxInsertTimeInMs, data.getMinMaxInsertTimeInMs());

          VectorUnloader unloader = new VectorUnloader(data.getVectors());
          ArrowRecordBatch recordBatch = unloader.getRecordBatch();
          loader.load(recordBatch);
          recordBatch.close();
          data.getVectors().close();
        }

        // Write channel data using the stream writer
        arrowWriter.writeBatch();
        rowCount += data.getRowCount();

        logger.logDebug(
            "Finish building channel={}, rowCount={}, bufferSize={} in blob={}",
            data.getChannel().getFullyQualifiedName(),
            data.getRowCount(),
            data.getBufferSize(),
            filePath);
      }
    } finally {
      if (arrowWriter != null) {
        arrowWriter.close();
        root.close();
      }
    }
    return new Flusher.SerializationResult(
        channelsMetadataList, columnEpStatsMapCombined, rowCount, chunkMinMaxInsertTimeInMs);
  }
}
