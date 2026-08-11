/*
 * Copyright (c) 2022-2024 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.parquet.hadoop;

import com.google.common.annotations.VisibleForTesting;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;

/**
 * BDEC specific parquet reader.
 *
 * <p>Resides in parquet package because, it uses {@link InternalParquetRecordReader} that is
 * package private.
 */
public class BdecParquetReader implements AutoCloseable {
  private final InternalParquetRecordReader<List<Object>> reader;
  private final ParquetFileReader fileReader;

  /**
   * @param data buffer where the data that has to be read resides.
   * @throws IOException
   */
  public BdecParquetReader(byte[] data) throws IOException {
    ParquetReadOptions options = ParquetReadOptions.builder().build();
    fileReader = ParquetFileReader.open(new BdecInputFile(data), options);
    reader = new InternalParquetRecordReader<>(new BdecReadSupport(), options.getRecordFilter());
    reader.initialize(fileReader, options);
  }

  /**
   * Reads the current row, i.e. list of values.
   *
   * @return current row
   * @throws IOException
   */
  public List<Object> read() throws IOException {
    try {
      return reader.nextKeyValue() ? reader.getCurrentValue() : null;
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  /** Get the key value metadata in the file */
  public Map<String, String> getKeyValueMetadata() {
    return fileReader.getFileMetaData().getKeyValueMetaData();
  }

  /**
   * Close the reader.
   *
   * @throws IOException
   */
  @Override
  public void close() throws IOException {
    reader.close();
  }

  /**
   * Verifies that all data in a BDEC parquet blob can be decompressed and has the expected row
   * count. Uses {@code parquet-format-structures} to walk the file structure and {@code
   * CodecFactory} to decompress each page, catching both compressed-data corruption (GZIP CRC32
   * failures) and page-header corruption (num_values=0).
   */
  public static void verify(byte[] data, long expectedRowCount) throws IOException {
    int totalLen = data.length;
    int footerLen =
        ByteBuffer.wrap(data, totalLen - 8, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
    int footerStart = totalLen - 8 - footerLen;
    FileMetaData footer =
        Util.readFileMetaData(new ByteArrayInputStream(data, footerStart, footerLen));

    CodecFactory codecFactory = new CodecFactory(new Configuration(), 0);
    try {
      long actualRowCount = 0;
      for (RowGroup rowGroup : footer.getRow_groups()) {
        boolean countedRowGroup = false;
        for (ColumnChunk chunk : rowGroup.getColumns()) {
          ColumnMetaData meta = chunk.getMeta_data();
          CompressionCodecName codecName = CompressionCodecName.valueOf(meta.getCodec().name());
          CodecFactory.BytesDecompressor decompressor =
              codecName == CompressionCodecName.UNCOMPRESSED
                  ? null
                  : codecFactory.getDecompressor(codecName);

          long chunkStart = meta.getData_page_offset();
          if (meta.isSetDictionary_page_offset()
              && meta.getDictionary_page_offset() < chunkStart) {
            chunkStart = meta.getDictionary_page_offset();
          }
          ByteArrayInputStream chunkStream =
              new ByteArrayInputStream(data, (int) chunkStart, (int) meta.getTotal_compressed_size());

          while (chunkStream.available() > 0) {
            PageHeader pageHeader = Util.readPageHeader(chunkStream);
            int compressedSize = pageHeader.getCompressed_page_size();
            if (compressedSize <= 0) break;
            byte[] compressedPage = new byte[compressedSize];
            if (chunkStream.read(compressedPage, 0, compressedSize) != compressedSize) {
              throw new IOException("Truncated page data in parquet column chunk");
            }

            if (!countedRowGroup) {
              if (pageHeader.isSetData_page_header()) {
                actualRowCount += pageHeader.getData_page_header().getNum_values();
              } else if (pageHeader.isSetData_page_header_v2()) {
                actualRowCount += pageHeader.getData_page_header_v2().getNum_values();
              }
            }

            if (decompressor != null) {
              int valuesOffset = 0;
              int uncompressedSize = pageHeader.getUncompressed_page_size();
              if (pageHeader.isSetData_page_header_v2()) {
                valuesOffset =
                    pageHeader.getData_page_header_v2().getRepetition_levels_byte_length()
                        + pageHeader.getData_page_header_v2().getDefinition_levels_byte_length();
                uncompressedSize -= valuesOffset;
              }
              int valuesLen = compressedSize - valuesOffset;
              if (valuesLen > 0) {
                decompressor.decompress(
                    BytesInput.from(compressedPage, valuesOffset, valuesLen), uncompressedSize);
              }
            }
          }
          countedRowGroup = true;
        }
      }

      if (actualRowCount != expectedRowCount) {
        throw new IOException(
            String.format(
                "Row count mismatch: expected %d, got %d", expectedRowCount, actualRowCount));
      }
    } finally {
      codecFactory.release();
    }
  }

  /**
   * Reads the input data using Parquet reader and writes them using a Parquet Writer.
   *
   * @param data input data to be read first and then written with outputWriter
   * @param outputWriter output parquet writer
   */
  public static void readFileIntoWriter(byte[] data, SnowflakeParquetWriter outputWriter) {
    try (BdecParquetReader reader = new BdecParquetReader(data)) {
      for (List<Object> record = reader.read(); record != null; record = reader.read()) {
        outputWriter.writeRow(record);
      }
    } catch (IOException e) {
      throw new SFException(ErrorCode.INTERNAL_ERROR, "Failed to merge parquet files", e);
    }
  }

  @VisibleForTesting
  public static class BdecInputFile implements InputFile {
    private final byte[] data;

    public BdecInputFile(byte[] data) {
      this.data = data;
    }

    @Override
    public long getLength() {
      return data.length;
    }

    @Override
    public SeekableInputStream newStream() {
      return new BdecSeekableInputStream(new BdecByteArrayInputStream(data));
    }
  }

  private static class BdecSeekableInputStream extends DelegatingSeekableInputStream {
    private final BdecByteArrayInputStream stream;

    public BdecSeekableInputStream(BdecByteArrayInputStream stream) {
      super(stream);
      this.stream = stream;
    }

    @Override
    public long getPos() {
      return stream.getPos();
    }

    @Override
    public void seek(long newPos) {
      stream.seek(newPos);
    }
  }

  private static class BdecByteArrayInputStream extends ByteArrayInputStream {
    public BdecByteArrayInputStream(byte[] buf) {
      super(buf);
    }

    long getPos() {
      return pos;
    }

    void seek(long newPos) {
      pos = (int) newPos;
    }
  }

  private static class BdecReadSupport extends ReadSupport<List<Object>> {
    @Override
    public RecordMaterializer<List<Object>> prepareForRead(
        Configuration conf, Map<String, String> metaData, MessageType schema, ReadContext context) {
      return new BdecRecordMaterializer(schema);
    }

    @Override
    public ReadContext init(InitContext context) {
      return new ReadContext(context.getFileSchema());
    }
  }

  private static class BdecRecordMaterializer extends RecordMaterializer<List<Object>> {
    public final BdecRecordConverter root;

    public BdecRecordMaterializer(MessageType schema) {
      this.root = new BdecRecordConverter(schema);
    }

    @Override
    public List<Object> getCurrentRecord() {
      return root.getCurrentRecord();
    }

    @Override
    public GroupConverter getRootConverter() {
      return root;
    }
  }

  private static class BdecRecordConverter extends GroupConverter {
    private final Converter[] converters;
    private final int fieldNumber;
    private Object[] record;

    public BdecRecordConverter(GroupType schema) {
      this.converters = new Converter[schema.getFieldCount()];
      this.fieldNumber = schema.getFields().size();
      for (int i = 0; i < fieldNumber; i++) {
        converters[i] = new BdecPrimitiveConverter(i);
      }
    }

    @Override
    public Converter getConverter(int fieldIndex) {
      return converters[fieldIndex];
    }

    List<Object> getCurrentRecord() {
      return Arrays.asList(record);
    }

    @Override
    public void start() {
      record = new Object[fieldNumber];
    }

    @Override
    public void end() {}

    private class BdecPrimitiveConverter extends PrimitiveConverter {
      protected final int index;

      public BdecPrimitiveConverter(int index) {
        this.index = index;
      }

      @Override
      public void addBinary(Binary value) {
        record[index] = value.getBytes();
      }

      @Override
      public void addBoolean(boolean value) {
        record[index] = value;
      }

      @Override
      public void addDouble(double value) {
        record[index] = value;
      }

      @Override
      public void addFloat(float value) {
        record[index] = value;
      }

      @Override
      public void addInt(int value) {
        record[index] = value;
      }

      @Override
      public void addLong(long value) {
        record[index] = value;
      }
    }
  }
}
