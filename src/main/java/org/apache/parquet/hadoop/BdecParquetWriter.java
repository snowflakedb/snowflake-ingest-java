/*
 * Copyright (c) 2022 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.parquet.hadoop;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import net.snowflake.ingest.utils.Constants;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.values.factory.DefaultV1ValuesWriterFactory;
import org.apache.parquet.crypto.FileEncryptionProperties;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
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
  private final InternalParquetRecordWriter<List<Object>> writer;
  private final CodecFactory codecFactory;
  private long rowsWritten = 0;

  /**
   * Creates a BDEC specific parquet writer.
   *
   * @param stream output
   * @param schema row schema
   * @param extraMetaData extra metadata
   * @param channelName name of the channel that is using the writer
   * @throws IOException
   */
  public BdecParquetWriter(
      ByteArrayOutputStream stream,
      MessageType schema,
      Map<String, String> extraMetaData,
      String channelName,
      long maxChunkSizeInBytes,
      Constants.BdecParquetCompression bdecParquetCompression)
      throws IOException {
    OutputFile file = new ByteArrayOutputFile(stream, maxChunkSizeInBytes);
    ParquetProperties encodingProps = createParquetProperties();
    Configuration conf = new Configuration();
    WriteSupport<List<Object>> writeSupport =
        new BdecWriteSupport(schema, extraMetaData, channelName);
    WriteSupport.WriteContext writeContext = writeSupport.init(conf);

    ParquetFileWriter fileWriter =
        new ParquetFileWriter(
            file,
            schema,
            ParquetFileWriter.Mode.CREATE,
            Constants.MAX_BLOB_SIZE_IN_BYTES * 2,
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
    CodecFactory.BytesCompressor compressor =
        codecFactory.getCompressor(bdecParquetCompression.getCompressionCodec());
    writer =
        new InternalParquetRecordWriter<>(
            fileWriter,
            writeSupport,
            schema,
            writeContext.getExtraMetaData(),
            Constants.MAX_BLOB_SIZE_IN_BYTES * 2,
            compressor,
            true,
            encodingProps);
  }

  /** @return List of row counts per block stored in the parquet footer */
  public List<Long> getRowCountsFromFooter() {
    final List<Long> blockRowCounts = new ArrayList<>();
    for (BlockMetaData metadata : writer.getFooter().getBlocks()) {
      blockRowCounts.add(metadata.getRowCount());
    }
    return blockRowCounts;
  }

  public void writeRow(List<Object> row) {
    try {
      writer.write(row);
      rowsWritten++;
    } catch (InterruptedException | IOException e) {
      throw new SFException(ErrorCode.INTERNAL_ERROR, "parquet row write failed", e);
    }
  }

  public long getRowsWritten() {
    return rowsWritten;
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

  private static ParquetProperties createParquetProperties() {
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
     * column.
     *
     * <p>To satisfy 1. and 2., we will disable a check that decides when to flush buffered rows to
     * row groups and pages. The check happens after a configurable amount of row counts and by
     * setting it to Integer.MAX_VALUE, Parquet lib will never perform the check and flush all
     * buffered rows to one row group on close(). The same check decides when to flush a rowgroup to
     * page. So by disabling it, we are not checking any page limits as well and flushing all
     * buffered data of the rowgroup to one page.
     *
     * <p>TODO: Remove the enforcements of single row group SNOW-738040 and single page (per column)
     * SNOW-737331. TODO: Revisit block and page size estimate after limitation (1) is removed
     * SNOW-738614 *
     */
    return ParquetProperties.builder()
        // PARQUET_2_0 uses Encoding.DELTA_BYTE_ARRAY for byte arrays (e.g. SF sb16)
        // server side does not support it TODO: SNOW-657238
        .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
        .withValuesWriterFactory(new DefaultV1ValuesWriterFactory())
        // the dictionary encoding (Encoding.*_DICTIONARY) is not supported by server side
        // scanner yet
        .withDictionaryEncoding(false)
        .withPageRowCountLimit(Integer.MAX_VALUE)
        .withMinRowCountForPageSizeCheck(Integer.MAX_VALUE)
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
    private final long maxChunkSizeInBytes;

    private ByteArrayOutputFile(ByteArrayOutputStream stream, long maxChunkSizeInBytes) {
      this.stream = stream;
      this.maxChunkSizeInBytes = maxChunkSizeInBytes;
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
      return maxChunkSizeInBytes;
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
