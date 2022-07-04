package net.snowflake.ingest.streaming.internal;

import net.snowflake.ingest.utils.*;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.ArrowWriter;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ArrowFlusher implements Flusher {
    private static final Logging logger = new Logging(ArrowFlusher.class);

    private final int bdecVersion;

    public ArrowFlusher(int bdecVersion) {
        this.bdecVersion = bdecVersion;
    }

    @Override
    public Flusher.SerializationResult serialize(List<ChannelData> channelsDataPerTable, ByteArrayOutputStream chunkData, String filePath) throws IOException {
        List<ChannelMetadata> channelsMetadataList = new ArrayList<>();
        long rowCount = 0L;
        VectorSchemaRoot root = null;
        ArrowWriter arrowWriter = null;
        VectorLoader loader = null;
        SnowflakeStreamingIngestChannelInternal firstChannel = null;
        Map<String, RowBufferStats> columnEpStatsMapCombined = null;

        try {
            for (ChannelData dataUncasted : channelsDataPerTable) {
                ChannelData<VectorSchemaRoot> data = (ChannelData<VectorSchemaRoot>)dataUncasted;
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
                    arrowWriter =
                            getArrowBatchWriteMode() == Constants.ArrowBatchWriteMode.STREAM
                                    ? new ArrowStreamWriter(root, null, chunkData)
                                    : new ArrowFileWriterWithCompression(
                                    root,
                                    Channels.newChannel(chunkData),
                                    new CustomCompressionCodec(CompressionUtil.CodecType.ZSTD));
                    loader = new VectorLoader(root);
                    firstChannel = data.getChannel();
                    arrowWriter.start();
                } else {
                    // This method assumes that channelsDataPerTable is grouped by table. We double check
                    // here and throw an error if the assumption is violated
                    if (!data.getChannel()
                            .getFullyQualifiedTableName()
                            .equals(firstChannel.getFullyQualifiedTableName())) {
                        throw new SFException(ErrorCode.INVALID_DATA_IN_CHUNK);
                    }

                    columnEpStatsMapCombined =
                            ChannelData.getCombinedColumnStatsMap(
                                    columnEpStatsMapCombined, data.getColumnEps());

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
        return new Flusher.SerializationResult(channelsMetadataList, columnEpStatsMapCombined, rowCount);
    }

    private Constants.ArrowBatchWriteMode getArrowBatchWriteMode() {
        switch (bdecVersion) {
            case 1:
                return Constants.ArrowBatchWriteMode.STREAM;
            case 2:
                return Constants.ArrowBatchWriteMode.FILE;
            default:
                throw new IllegalArgumentException(
                        String.format(
                                "Unsupported BLOB_FORMAT_VERSION = '%d', allowed values are in range [%d, %d]"
                                        + " inclusively",
                                bdecVersion,
                                ParameterProvider.BLOB_FORMAT_START_SUPPORTED_VERSION,
                                ParameterProvider.BLOB_FORMAT_END_SUPPORTED_VERSION));
        }
    }
}
