/*
 * Copyright (c) 2022 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.parquet.hadoop;

import static net.snowflake.ingest.utils.Constants.MAX_CHUNK_SIZE_IN_BYTES;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.Logging;
import net.snowflake.ingest.utils.SFException;
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
 * <p>Resides in parquet package because, it uses {@link InternalParquetRecordWriter} and {@link
 * CodecFactory} that are package private.
 */
public class BdecParquetWriter implements AutoCloseable {

  private static final Logging logger = new Logging(BdecParquetWriter.class);
  private final InternalParquetRecordWriter<List<Object>> writer;
  private final CodecFactory codecFactory;

  /**
   * Creates a BDEC specific parquet writer.
   *
   * @param stream output
   * @param schema row schema
   * @param extraMetaData extra metadata
   * @param channelName name of the channel that is using the writer
   * @param estimatedUncompressedChunkSizeInBytes estimated uncompressed chunk size in bytes
   * @throws IOException
   */
  public BdecParquetWriter(
      ByteArrayOutputStream stream,
      MessageType schema,
      Map<String, String> extraMetaData,
      String channelName,
      float estimatedUncompressedChunkSizeInBytes)
      throws IOException {
    OutputFile file = new ByteArrayOutputFile(stream);
    ParquetProperties encodingProps =
        createParquetProperties(estimatedUncompressedChunkSizeInBytes);
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

    /*
    Internally parquet writer initialises CodecFactory with the configured page size.
    We set the page size to the max chunk size that is quite big in general.
    CodecFactory allocates a byte buffer of that size on heap during initialisation.

    If we use Parquet writer for buffering, there will be one writer per channel on each flush.
    The memory will be allocated for each writer at the beginning even if we don't write anything with each writer,
    which is the case when we enable parquet writer buffering.
    Hence, to avoid huge memory allocations, we have to internally initialise CodecFactory with `ParquetWriter.DEFAULT_PAGE_SIZE` as it usually happens.
    To get code access to this internal initialisation, we have to move the BdecParquetWriter class in the parquet.hadoop package.
    */
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
    }
  }

  private static ParquetProperties createParquetProperties(
      float estimatedUncompressedChunkSizeInBytes) {
    /**
     * There are two main limitations on the server side that we have to overcome by tweaking
     * Parquet limits:
     *
     * <p>1. Scanner supports only the case when the row number in pages is the same. Remember that
     * a page has data from only one column.
     *
     * <p>2. Scanner supports only 1 row group per Parquet file.
     *
     * <p>We can't guarantee that each page will have the same number of rows, because we have no
     * internal control over Parquet lib. That's why to satisfy 1., we will generate one page per
     * column by changing the page size limit and increasing the page row limit. To satisfy 2., we
     * will disable a check that decides when to flush buffered rows to row groups. The check
     * happens after a configurable amount of row counts and by setting it to Integer.MAX_VALUE,
     * Parquet lib will never perform the check and flush all buffered rows to one row group on
     * close().
     *
     * <p>TODO: Remove the enforcements of single row group SNOW-738040 and single page (per column)
     * SNOW-737331 *
     */

    // an estimate of the final page size after compression and encoding.
    // For every column/value, Parquet stores other metadata like: nullability, repetition which
    // require more space. To be on the safe side, we are multiplying the estimated uncompressed
    // size with a factor.
    int pageSizeLimit =
        (int)
            Math.max((int) MAX_CHUNK_SIZE_IN_BYTES, estimatedUncompressedChunkSizeInBytes);
    logger.logInfo(
        "Setting Parquet page limit={}, estimatedUncompressedChunkSizeInBytes={},"
            + " MAX_CHUNK_SIZE_IN_BYTES={}.",
        pageSizeLimit,
        estimatedUncompressedChunkSizeInBytes,
        MAX_CHUNK_SIZE_IN_BYTES);
    return ParquetProperties.builder()
        // PARQUET_2_0 uses Encoding.DELTA_BYTE_ARRAY for byte arrays (e.g. SF sb16)
        // server side does not support it TODO: SNOW-657238
        .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
        .withValuesWriterFactory(new DefaultV1ValuesWriterFactory())

        // the dictionary encoding (Encoding.*_DICTIONARY) is not supported by server side
        // scanner yet
        .withDictionaryEncoding(false)
        .withPageRowCountLimit(20_000)
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
