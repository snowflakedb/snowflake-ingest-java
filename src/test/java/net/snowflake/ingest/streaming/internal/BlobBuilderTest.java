/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import net.snowflake.ingest.utils.Constants;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.Pair;
import net.snowflake.ingest.utils.SFException;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.SnowflakeParquetWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

@RunWith(Parameterized.class)
public class BlobBuilderTest {
  @Parameterized.Parameters(name = "enableIcebergStreaming: {0}")
  public static Object[] enableIcebergStreaming() {
    return new Object[] {false, true};
  }

  @Parameterized.Parameter public boolean enableIcebergStreaming;

  @Test
  public void testSerializationErrors() throws Exception {
    Map<FullyQualifiedTableName, EncryptionKey> encryptionKeysPerTable = new ConcurrentHashMap<>();
    encryptionKeysPerTable.put(
        new FullyQualifiedTableName("DB", "SCHEMA", "TABLE"),
        new EncryptionKey("DB", "SCHEMA", "TABLE", "KEY", 1234L));

    // Construction succeeds if both data and metadata contain 1 row
    BlobBuilder.constructBlobAndMetadata(
        "a.bdec",
        Collections.singletonList(createChannelDataPerTable(1)),
        Constants.BdecVersion.THREE,
        new InternalParameterProvider(enableIcebergStreaming),
        encryptionKeysPerTable);

    // Construction fails if metadata contains 0 rows and data 1 row
    try {
      BlobBuilder.constructBlobAndMetadata(
          "a.bdec",
          Collections.singletonList(createChannelDataPerTable(0)),
          Constants.BdecVersion.THREE,
          new InternalParameterProvider(enableIcebergStreaming),
          encryptionKeysPerTable);
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.INTERNAL_ERROR.getMessageCode(), e.getVendorCode());
      Assert.assertTrue(e.getMessage().contains("parquetTotalRowsInFooter=1"));
      Assert.assertTrue(e.getMessage().contains("totalMetadataRowCount=0"));
      Assert.assertTrue(e.getMessage().contains("parquetTotalRowsWritten=1"));
      Assert.assertTrue(e.getMessage().contains("perChannelRowCountsInMetadata=0"));
      Assert.assertTrue(e.getMessage().contains("perBlockRowCountsInFooter=1"));
      Assert.assertTrue(e.getMessage().contains("channelsCountInMetadata=1"));
      Assert.assertTrue(e.getMessage().contains("countOfSerializedJavaObjects=1"));
    }
  }

  @Test
  public void testMetadataAndExtendedMetadataSize() throws Exception {
    if (!enableIcebergStreaming) {
      return;
    }

    BlobBuilder.Blob blob =
        BlobBuilder.constructBlobAndMetadata(
            "a.parquet",
            Collections.singletonList(createChannelDataPerTable(1)),
            Constants.BdecVersion.THREE,
            new InternalParameterProvider(enableIcebergStreaming),
            new ConcurrentHashMap<>());

    InputFile blobInputFile = new InMemoryInputFile(blob.blobBytes);
    ParquetFileReader reader = ParquetFileReader.open(blobInputFile);
    ParquetMetadata footer = reader.getFooter();

    int extendedMetadataSize = 0;
    long extendedMetadaOffset = 0;
    for (BlockMetaData block : footer.getBlocks()) {
      for (ColumnChunkMetaData column : block.getColumns()) {
        extendedMetadataSize +=
            (column.getColumnIndexReference() != null
                    ? column.getColumnIndexReference().getLength()
                    : 0)
                + (column.getOffsetIndexReference() != null
                    ? column.getOffsetIndexReference().getLength()
                    : 0)
                + (column.getBloomFilterLength() == -1 ? 0 : column.getBloomFilterLength());
        extendedMetadaOffset =
            Math.max(column.getFirstDataPageOffset() + column.getTotalSize(), extendedMetadaOffset);
      }
    }
    Assertions.assertThat(blob.chunksMetadataList.size()).isEqualTo(1);
    Assertions.assertThat(blob.chunksMetadataList.get(0).getExtendedMetadataSize())
        .isEqualTo(extendedMetadataSize);
    Assertions.assertThat(blob.chunksMetadataList.get(0).getMetadataSize())
        .isEqualTo(
            blob.blobBytes.length
                - extendedMetadaOffset
                - extendedMetadataSize
                - ParquetFileWriter.MAGIC.length
                - Integer.BYTES);
  }

  /**
   * Creates a channel data configurable number of rows in metadata and 1 physical row (using both
   * with and without internal buffering optimization)
   */
  private List<ChannelData<ParquetChunkData>> createChannelDataPerTable(int metadataRowCount)
      throws IOException {
    String columnName = "C1";
    ChannelData<ParquetChunkData> channelData = Mockito.spy(new ChannelData<>());
    MessageType schema = createSchema(columnName);
    Mockito.doReturn(
            new ParquetFlusher(
                schema,
                100L,
                enableIcebergStreaming ? Optional.of(1) : Optional.empty(),
                Constants.BdecParquetCompression.GZIP,
                enableIcebergStreaming
                    ? ParquetProperties.WriterVersion.PARQUET_2_0
                    : ParquetProperties.WriterVersion.PARQUET_1_0,
                enableIcebergStreaming,
                enableIcebergStreaming))
        .when(channelData)
        .createFlusher();

    channelData.setRowSequencer(1L);
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    SnowflakeParquetWriter snowflakeParquetWriter =
        new SnowflakeParquetWriter(
            stream,
            schema,
            new HashMap<>(),
            "CHANNEL",
            1000,
            enableIcebergStreaming ? Optional.of(1) : Optional.empty(),
            Constants.BdecParquetCompression.GZIP,
            enableIcebergStreaming
                ? ParquetProperties.WriterVersion.PARQUET_2_0
                : ParquetProperties.WriterVersion.PARQUET_1_0,
            enableIcebergStreaming);
    snowflakeParquetWriter.writeRow(Collections.singletonList("1"));
    channelData.setVectors(
        new ParquetChunkData(
            Collections.singletonList(Collections.singletonList("A")), new HashMap<>()));
    channelData.setColumnEps(new HashMap<>());
    channelData.setRowCount(metadataRowCount);
    channelData.setMinMaxInsertTimeInMs(new Pair<>(2L, 3L));

    channelData
        .getColumnEps()
        .putIfAbsent(
            columnName,
            enableIcebergStreaming
                ? new RowBufferStats(
                    columnName,
                    null,
                    1,
                    1,
                    Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType())
                        .id(1)
                        .named("test"),
                    enableIcebergStreaming,
                    enableIcebergStreaming)
                : new RowBufferStats(columnName, null, 1, null, null, false, false));
    channelData.setChannelContext(
        new ChannelFlushContext("channel1", "DB", "SCHEMA", "TABLE", 1L, "enc", 1L));
    return Collections.singletonList(channelData);
  }

  private static MessageType createSchema(String columnName) {
    ParquetTypeInfo c1 =
        ParquetTypeGenerator.generateColumnParquetTypeInfo(createTestTextColumn(columnName), 1);
    return new MessageType("InMemory", Collections.singletonList(c1.getParquetType()));
  }

  private static ColumnMetadata createTestTextColumn(String name) {
    ColumnMetadata colChar = new ColumnMetadata();
    colChar.setOrdinal(1);
    colChar.setName(name);
    colChar.setPhysicalType("LOB");
    colChar.setNullable(true);
    colChar.setLogicalType("TEXT");
    colChar.setByteLength(14);
    colChar.setLength(11);
    colChar.setScale(0);
    return colChar;
  }

  static class InMemoryInputFile implements InputFile {
    private final byte[] data;

    public InMemoryInputFile(byte[] data) {
      this.data = data;
    }

    @Override
    public long getLength() {
      return data.length;
    }

    @Override
    public SeekableInputStream newStream() {
      return new InMemorySeekableInputStream(new InMemoryByteArrayInputStream(data));
    }
  }

  private static class InMemorySeekableInputStream extends DelegatingSeekableInputStream {
    private final InMemoryByteArrayInputStream stream;

    public InMemorySeekableInputStream(InMemoryByteArrayInputStream stream) {
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

  private static class InMemoryByteArrayInputStream extends ByteArrayInputStream {
    public InMemoryByteArrayInputStream(byte[] buf) {
      super(buf);
    }

    long getPos() {
      return pos;
    }

    void seek(long newPos) {
      pos = (int) newPos;
    }
  }
}
