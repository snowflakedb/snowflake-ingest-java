/*
 * Copyright (c) 2022 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.parquet.hadoop;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
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
   * Reads the input data using Parquet reader and writes them using a Parquet Writer.
   *
   * @param data input data to be read first and then written with outputWriter
   * @param outputWriter output parquet writer
   */
  public static void readFileIntoWriter(byte[] data, BdecParquetWriter outputWriter) {
    try (BdecParquetReader reader = new BdecParquetReader(data)) {
      for (List<Object> record = reader.read(); record != null; record = reader.read()) {
        outputWriter.writeRow(record);
      }
    } catch (IOException e) {
      throw new SFException(ErrorCode.INTERNAL_ERROR, "Failed to merge parquet files", e);
    }
  }

  private static class BdecInputFile implements InputFile {
    private final byte[] data;

    private BdecInputFile(byte[] data) {
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
