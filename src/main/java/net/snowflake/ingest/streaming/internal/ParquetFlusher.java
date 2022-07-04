package net.snowflake.ingest.streaming.internal;

import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.Logging;
import net.snowflake.ingest.utils.SFException;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.*;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static net.snowflake.ingest.utils.Constants.MAX_CHUNK_SIZE_IN_BYTES;

public class ParquetFlusher implements Flusher {
    private static final Logging logger = new Logging(ParquetFlusher.class);
    private final MessageType schema;

    public ParquetFlusher(MessageType schema) {
        this.schema = schema;
    }

    @Override
    public SerializationResult serialize(List<ChannelData> channelsDataPerTable, ByteArrayOutputStream chunkData, String filePath) throws IOException {
        List<ChannelMetadata> channelsMetadataList = new ArrayList<>();
        long rowCount = 0L;
        SnowflakeStreamingIngestChannelInternal firstChannel = null;
        Map<String, RowBufferStats> columnEpStatsMapCombined = null;
        List<List<Object>> rows = null;

        for (ChannelData dataUncasted : channelsDataPerTable) {
            ChannelData<ParquetRowBuffer.ParquetChunkData> data = (ChannelData<ParquetRowBuffer.ParquetChunkData>)dataUncasted;
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
                    "Parquet Flusher: Start building channel={}, rowCount={}, bufferSize={} in blob={}",
                    data.getChannel().getFullyQualifiedName(),
                    data.getRowCount(),
                    data.getBufferSize(),
                    filePath);

            if (rows == null) {
                columnEpStatsMapCombined = data.getColumnEps();
                rows = new ArrayList<>();
                firstChannel = data.getChannel();
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
            }
            rows.addAll(data.getVectors().rows);

            rowCount += data.getRowCount();

            logger.logDebug(
                    "Parquet Flusher: Finish building channel={}, rowCount={}, bufferSize={} in blob={}",
                    data.getChannel().getFullyQualifiedName(),
                    data.getRowCount(),
                    data.getBufferSize(),
                    filePath);
        }

        Map<String, String> metadata = ((ChannelData<ParquetRowBuffer.ParquetChunkData>)channelsDataPerTable.get(0)).getVectors().metadata;

        try {
            ParquetWriter<List<Object>> writer = new BdecParquetWriterBuilder(chunkData, schema, metadata)
                    .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
                    //.withDictionaryPageSize((int)MAX_CHUNK_SIZE_IN_BYTES)
                    .withDictionaryEncoding(false)
                    .enableValidation()
                    .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                    //.withPageSize((int)MAX_CHUNK_SIZE_IN_BYTES)
                    //.withRowGroupSize((int)MAX_CHUNK_SIZE_IN_BYTES)
                    .withWriteMode(ParquetFileWriter.Mode.CREATE)
                    .build();

            for (List<Object> row : rows) {
                writer.write(row);
            }
            writer.close();
        } catch (Throwable t) {
            logger.logError("Parquet Flusher: failed to write", t);
            throw t;
        }

        return new Flusher.SerializationResult(channelsMetadataList, columnEpStatsMapCombined, rowCount);
    }

    public static class BdecParquetWriterBuilder extends ParquetWriter.Builder<List<Object>, BdecParquetWriterBuilder> {
        private final MessageType schema;
        private final Map<String, String> extraMetaData;

        protected BdecParquetWriterBuilder(ByteArrayOutputStream stream, MessageType schema, Map<String, String> extraMetaData) {
            super(new ByteArrayOutputFile(stream));
            this.schema = schema;
            this.extraMetaData = extraMetaData;
        }

        @Override
        protected BdecParquetWriterBuilder self() {
            return this;
        }

        @Override
        protected WriteSupport<List<Object>> getWriteSupport(Configuration conf) {
            return new BdecWriteSupport(schema, extraMetaData);
        }
    }

    private static class ByteArrayOutputFile implements OutputFile {
        private final ByteArrayOutputStream stream;

        private ByteArrayOutputFile(ByteArrayOutputStream stream) {
            this.stream = stream;
        }

        @Override
        public PositionOutputStream create(long blockSizeHint) throws IOException {
            stream.reset();
            return new ByteArrayDelegatingPositionOutputStream(stream);
        }

        @Override
        public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
            return create(blockSizeHint);
        }

        @Override
        public boolean supportsBlockSize() {
            return false;
        }

        @Override
        public long defaultBlockSize() {
            return (int)MAX_CHUNK_SIZE_IN_BYTES;
        }
    }

    private static class ByteArrayDelegatingPositionOutputStream extends DelegatingPositionOutputStream {
        private final ByteArrayOutputStream stream;

        public ByteArrayDelegatingPositionOutputStream(ByteArrayOutputStream stream) {
            super(stream);
            this.stream = stream;
        }

        @Override
        public long getPos() {
            return stream.size();
        }
    }

    private static class BdecWriteSupport extends WriteSupport<List<Object>> {
        MessageType schema;
        RecordConsumer recordConsumer;
        Map<String, String> extraMetaData;

        // TODO: support specifying encodings and compression
        BdecWriteSupport(MessageType schema, Map<String, String> extraMetaData) {
            this.schema = schema;
            this.extraMetaData = extraMetaData;
        }

        @Override
        public WriteContext init(Configuration config) {
            // TODO: metadata
            return new WriteContext(schema, extraMetaData);
        }

        @Override
        public void prepareForWrite(RecordConsumer recordConsumer) {
            this.recordConsumer = recordConsumer;
        }

        @Override
        public void write(List<Object> values) {
            List<ColumnDescriptor> cols = schema.getColumns();
            if (values.size() != cols.size()) {
                throw new ParquetEncodingException("Invalid input data. Expecting " +
                        cols.size() + " columns. Input had " + values.size() + " columns (" + cols + ") : " + values);
            }

            recordConsumer.startMessage();
            for (int i = 0; i < cols.size(); ++i) {
                Object val = values.get(i);
                // val.length() == 0 indicates a NULL value.
                if (val != null) {
                    String fieldName = cols.get(i).getPath()[0];
                    recordConsumer.startField(fieldName, i);
                    PrimitiveType.PrimitiveTypeName typeName = cols.get(i).getPrimitiveType().getPrimitiveTypeName();
                    switch (typeName) {
                        case BOOLEAN:
                            recordConsumer.addBoolean((boolean)val);
                            break;
                        case FLOAT:
                            recordConsumer.addFloat((float)val);
                            break;
                        case DOUBLE:
                            recordConsumer.addDouble((double)val);
                            break;
                        case INT32:
                            recordConsumer.addInteger((int)val);
                            break;
                        case INT64:
                            recordConsumer.addLong((long)val);
                            break;
                        case BINARY:
                            recordConsumer.addBinary(Binary.fromString((String)val));
                            break;
                        default:
                            throw new ParquetEncodingException(
                                    "Unsupported column type: " + cols.get(i).getType());
                    }
                    recordConsumer.endField(fieldName, i);
                }
            }
            recordConsumer.endMessage();
        }
    }
}
