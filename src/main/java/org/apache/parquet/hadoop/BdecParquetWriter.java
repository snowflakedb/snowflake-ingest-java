package org.apache.parquet.hadoop;

import static net.snowflake.ingest.utils.Constants.MAX_CHUNK_SIZE_IN_BYTES;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import net.snowflake.ingest.utils.Constants;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.values.factory.DefaultV1ValuesWriterFactory;
import org.apache.parquet.crypto.FileEncryptionProperties;
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
 * BDEC specific parquet writer.
 *
 * <p>Resides in parquet package because, it uses {@link InternalParquetRecordWriter} that is
 * package private.
 */
public class BdecParquetWriter implements AutoCloseable {
  private final InternalParquetRecordWriter<List<Object>> writer;
  private final ArrowByteBufferAllocator allocator;
  private final CodecFactory codecFactory;

  public BdecParquetWriter(
      ByteArrayOutputStream stream,
      MessageType schema,
      Map<String, String> extraMetaData,
      String channelName,
      BufferAllocator allocator)
      throws IOException {
    OutputFile file = new ByteArrayOutputFile(stream);
    this.allocator = new ArrowByteBufferAllocator(allocator, true);
    ParquetProperties encodingProps = createParquetProperties(this.allocator);
    Configuration conf = new Configuration();
    WriteSupport<List<Object>> writeSupport =
        new BdecWriteSupport(schema, extraMetaData, channelName);
    WriteSupport.WriteContext writeContext = writeSupport.init(conf);

    ParquetFileWriter fileWriter =
        new ParquetFileWriter(
            file,
            schema,
            ParquetFileWriter.Mode.CREATE,
            ParquetWriter.DEFAULT_BLOCK_SIZE,
            ParquetWriter.MAX_PADDING_SIZE_DEFAULT,
            encodingProps.getColumnIndexTruncateLength(),
            encodingProps.getStatisticsTruncateLength(),
            encodingProps.getPageWriteChecksumEnabled(),
            (FileEncryptionProperties) null);
    fileWriter.start();

    codecFactory = new CodecFactory(conf, ParquetWriter.DEFAULT_PAGE_SIZE);
    @SuppressWarnings("deprecation") // Parquet does not support the new one now
    CodecFactory.BytesCompressor compressor = codecFactory.getCompressor(CompressionCodecName.GZIP);
    writer =
        new InternalParquetRecordWriter<>(
            fileWriter,
            writeSupport,
            schema,
            writeContext.getExtraMetaData(),
            ParquetWriter.DEFAULT_BLOCK_SIZE,
            compressor,
            true,
            encodingProps);
  }

  public void writeRow(List<Object> row) {
    try {
      writer.write(row);
    } catch (InterruptedException | IOException e) {
      throw new SFException(ErrorCode.INTERNAL_ERROR, "parquet row write failed", e);
    }
  }

  @Override
  public void close() throws IOException {
    try {
      writer.close();
    } catch (InterruptedException e) {
      throw new IOException(e);
    } finally {
      codecFactory.release();
      allocator.close();
    }
  }

  private static ParquetProperties createParquetProperties(ArrowByteBufferAllocator allocator) {
    return ParquetProperties.builder()
        // PARQUET_2_0 uses Encoding.DELTA_BYTE_ARRAY for byte arrays (e.g. SF sb16)
        // server side does not support it TODO: SNOW-657238
        .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
        .withValuesWriterFactory(new DefaultV1ValuesWriterFactory())

        // the dictionary encoding (Encoding.*_DICTIONARY) is not supported by server side
        // scanner yet
        .withDictionaryEncoding(false)

        // Historically server side scanner supports only the case when the row number in all
        // pages is the same.
        // The quick fix is to effectively disable the page size/row limit
        // to always have one page per chunk until server side is generalised.
        .withPageSize((int) Constants.MAX_CHUNK_SIZE_IN_BYTES * 2)
        .withPageRowCountLimit(Integer.MAX_VALUE)
        .withAllocator(allocator)
        .build();
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
    private final String channelName;

    // TODO SNOW-672156: support specifying encodings and compression
    BdecWriteSupport(MessageType schema, Map<String, String> extraMetadata, String channelName) {
      this.schema = schema;
      this.extraMetadata = extraMetadata;
      this.channelName = channelName;
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
        throw new ParquetEncodingException(
            "Invalid input data in channel '"
                + channelName
                + "'. Expecting "
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
