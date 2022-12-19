/*
 * Copyright (c) 2022 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.utils.Constants.MAX_CHUNK_SIZE_IN_BYTES;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.Logging;
import net.snowflake.ingest.utils.SFException;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.BdecParquetReader;
import org.apache.parquet.hadoop.BdecParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.DelegatingPositionOutputStream;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.ParquetEncodingException;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;

/**
 * Converts {@link ChannelData} buffered in {@link RowBuffer} to the Parquet format for faster
 * processing.
 */
public class ParquetFlusher implements Flusher<ParquetChunkData> {
  private static final Logging logger = new Logging(ParquetFlusher.class);
  private final MessageType schema;

  /** Construct parquet flusher from its schema. */
  public ParquetFlusher(MessageType schema) {
    this.schema = schema;
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
    ByteArrayOutputStream mergedChunkData = new ByteArrayOutputStream();

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
        channelsMetadataList, columnEpStatsMapCombined, rowCount, mergedChunkData);
  }

  /**
   * A parquet specific write builder.
   *
   * <p>This class is implemented as parquet library API requires, mostly to provide {@link
   * BdecWriteSupport} implementation.
   */
  private static class BdecParquetWriterBuilder
      extends ParquetWriter.Builder<List<Object>, BdecParquetWriterBuilder> {
    private final MessageType schema;
    private final Map<String, String> extraMetaData;
    private final List<ChannelMetadata> channelsMetadataList;

    protected BdecParquetWriterBuilder(
        ByteArrayOutputStream stream,
        MessageType schema,
        Map<String, String> extraMetaData,
        List<ChannelMetadata> channelsMetadataList) {
      super(new ByteArrayOutputFile(stream));
      this.schema = schema;
      this.extraMetaData = extraMetaData;
      this.channelsMetadataList = channelsMetadataList;
    }

    @Override
    protected BdecParquetWriterBuilder self() {
      return this;
    }

    @Override
    protected WriteSupport<List<Object>> getWriteSupport(Configuration conf) {
      return new BdecWriteSupport(schema, extraMetaData, channelsMetadataList);
    }
  }

  /**
   * A parquet specific file output implementation.
   *
   * <p>This class is implemented as parquet library API requires, mostly to create our {@link
   * ByteArrayDelegatingPositionOutputStream} implementation.
   */
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
      return (int) MAX_CHUNK_SIZE_IN_BYTES;
    }
  }

  /**
   * A parquet specific output stream implementation.
   *
   * <p>This class is implemented as parquet library API requires, mostly to wrap our BDEC output
   * {@link ByteArrayOutputStream}.
   */
  private static class ByteArrayDelegatingPositionOutputStream
      extends DelegatingPositionOutputStream {
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

  /**
   * A parquet specific write support implementation.
   *
   * <p>This class is implemented as parquet library API requires, mostly to serialize user column
   * values depending on type into Parquet {@link RecordConsumer} in {@link
   * BdecWriteSupport#write(List)}.
   */
  private static class BdecWriteSupport extends WriteSupport<List<Object>> {
    MessageType schema;
    RecordConsumer recordConsumer;
    Map<String, String> extraMetadata;
    List<ChannelMetadata> channelsMetadataList;

    // TODO SNOW-672156: support specifying encodings and compression
    BdecWriteSupport(
        MessageType schema,
        Map<String, String> extraMetadata,
        List<ChannelMetadata> channelsMetadataList) {
      this.schema = schema;
      this.extraMetadata = extraMetadata;
      this.channelsMetadataList = channelsMetadataList;
    }

    @Override
    public WriteContext init(Configuration config) {
      return new WriteContext(schema, extraMetadata);
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
      this.recordConsumer = recordConsumer;
    }

    @Override
    public void write(List<Object> values) {
      List<ColumnDescriptor> cols = schema.getColumns();
      if (values.size() != cols.size()) {
        List<String> channelNames =
            this.channelsMetadataList.stream()
                .map(ChannelMetadata::getChannelName)
                .collect(Collectors.toList());
        throw new ParquetEncodingException(
            "Invalid input data in channels "
                + channelNames
                + ". Expecting "
                + cols.size()
                + " columns. Input had "
                + values.size()
                + " columns ("
                + cols
                + ") : "
                + values);
      }

      recordConsumer.startMessage();
      for (int i = 0; i < cols.size(); ++i) {
        Object val = values.get(i);
        // val.length() == 0 indicates a NULL value.
        if (val != null) {
          String fieldName = cols.get(i).getPath()[0];
          recordConsumer.startField(fieldName, i);
          PrimitiveType.PrimitiveTypeName typeName =
              cols.get(i).getPrimitiveType().getPrimitiveTypeName();
          switch (typeName) {
            case BOOLEAN:
              recordConsumer.addBoolean((boolean) val);
              break;
            case FLOAT:
              recordConsumer.addFloat((float) val);
              break;
            case DOUBLE:
              recordConsumer.addDouble((double) val);
              break;
            case INT32:
              recordConsumer.addInteger((int) val);
              break;
            case INT64:
              recordConsumer.addLong((long) val);
              break;
            case BINARY:
              Binary binVal =
                  val instanceof String
                      ? Binary.fromString((String) val)
                      : Binary.fromConstantByteArray((byte[]) val);
              recordConsumer.addBinary(binVal);
              break;
            case FIXED_LEN_BYTE_ARRAY:
              Binary binary = Binary.fromConstantByteArray((byte[]) val);
              recordConsumer.addBinary(binary);
              break;
            default:
              throw new ParquetEncodingException(
                  "Unsupported column type: " + cols.get(i).getPrimitiveType());
          }
          recordConsumer.endField(fieldName, i);
        }
      }
      recordConsumer.endMessage();
    }
  }
}
