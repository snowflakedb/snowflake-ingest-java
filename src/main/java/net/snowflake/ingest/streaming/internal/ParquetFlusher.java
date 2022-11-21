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
import net.snowflake.ingest.utils.Constants;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.Logging;
import net.snowflake.ingest.utils.Pair;
import net.snowflake.ingest.utils.SFException;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
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
      List<ChannelData<ParquetChunkData>> channelsDataPerTable,
      ByteArrayOutputStream chunkData,
      String filePath)
      throws IOException {
    List<ChannelMetadata> channelsMetadataList = new ArrayList<>();
    long rowCount = 0L;
    SnowflakeStreamingIngestChannelInternal<ParquetChunkData> firstChannel = null;
    Map<String, RowBufferStats> columnEpStatsMapCombined = null;
    List<List<Object>> rows = null;
    Pair<Long, Long> chunkMinMaxInsertTimeInMs = null;

    for (ChannelData<ParquetChunkData> data : channelsDataPerTable) {
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

    Map<String, String> metadata = channelsDataPerTable.get(0).getVectors().metadata;

    flushToParquetBdecChunk(chunkData, rows, metadata, channelsMetadataList);
    return new SerializationResult(
        channelsMetadataList, columnEpStatsMapCombined, rowCount, chunkMinMaxInsertTimeInMs);
  }

  /**
   * Flushes a parquet row chunk to the given BDEC output stream.
   *
   * @param bdecOutput BDEC output stream
   * @param chunkRows chunk rows
   * @param metadata chunk metadata
   * @param channelsMetadataList metadata of the channels the chunk rows belong to
   * @throws IOException thrown from Parquet library in case of writing problems
   */
  private void flushToParquetBdecChunk(
      ByteArrayOutputStream bdecOutput,
      List<List<Object>> chunkRows,
      Map<String, String> metadata,
      List<ChannelMetadata> channelsMetadataList)
      throws IOException {
    try {
      ParquetWriter<List<Object>> writer =
          new BdecParquetWriterBuilder(bdecOutput, schema, metadata, channelsMetadataList)
              // PARQUET_2_0 uses Encoding.DELTA_BYTE_ARRAY for byte arrays (e.g. SF sb16)
              // server side does not support it TODO: SNOW-657238
              .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)

              // the dictionary encoding (Encoding.*_DICTIONARY) is not supported by server side
              // scanner yet
              .withDictionaryEncoding(false)

              // Historically server side scanner supports only the case when the row number in all
              // pages is the same.
              // The quick fix is to effectively disable the page size/row limit
              // to always have one page per chunk until server side is generalised.
              .withPageSize((int) Constants.MAX_CHUNK_SIZE_IN_BYTES * 2)
              .withPageRowCountLimit(chunkRows.size() + 1)
              .enableValidation()
              .withCompressionCodec(CompressionCodecName.GZIP)
              .withWriteMode(ParquetFileWriter.Mode.CREATE)
              .build();

      // We can use lower level column writers and custom ValuesWriterFactory that uses plain byte
      // array encoding used by PARQUET_1_0 and supported by server side
      // TODO: SNOW-672143
      for (List<Object> row : chunkRows) {
        writer.write(row);
      }
      writer.close();
    } catch (Throwable t) {
      logger.logError("Parquet Flusher: failed to write", t);
      throw t;
    }
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
              recordConsumer.addBinary(Binary.fromString((String) val));
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
