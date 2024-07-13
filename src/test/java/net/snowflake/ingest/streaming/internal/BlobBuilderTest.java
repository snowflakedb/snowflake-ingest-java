package net.snowflake.ingest.streaming.internal;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import net.snowflake.ingest.utils.Constants;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.Pair;
import net.snowflake.ingest.utils.SFException;
import org.apache.parquet.hadoop.BdecParquetWriter;
import org.apache.parquet.schema.MessageType;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class BlobBuilderTest {

  @Test
  public void testSerializationErrors() throws Exception {
    // Construction succeeds if both data and metadata contain 1 row
    BlobBuilder.constructBlobAndMetadata(
        "a.bdec",
        Collections.singletonList(createChannelDataPerTable(1, false)),
        Constants.BdecVersion.THREE);
    BlobBuilder.constructBlobAndMetadata(
        "a.bdec",
        Collections.singletonList(createChannelDataPerTable(1, true)),
        Constants.BdecVersion.THREE);

    // Construction fails if metadata contains 0 rows and data 1 row
    try {
      BlobBuilder.constructBlobAndMetadata(
          "a.bdec",
          Collections.singletonList(createChannelDataPerTable(0, false)),
          Constants.BdecVersion.THREE);
      Assert.fail("Should not pass enableParquetInternalBuffering=false");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.INTERNAL_ERROR.getMessageCode(), e.getVendorCode());
      Assert.assertTrue(e.getMessage().contains("serializeFromJavaObjects"));
      Assert.assertTrue(e.getMessage().contains("parquetTotalRowsInFooter=1"));
      Assert.assertTrue(e.getMessage().contains("totalMetadataRowCount=0"));
      Assert.assertTrue(e.getMessage().contains("parquetTotalRowsWritten=1"));
      Assert.assertTrue(e.getMessage().contains("perChannelRowCountsInMetadata=0"));
      Assert.assertTrue(e.getMessage().contains("perBlockRowCountsInFooter=1"));
      Assert.assertTrue(e.getMessage().contains("channelsCountInMetadata=1"));
      Assert.assertTrue(e.getMessage().contains("countOfSerializedJavaObjects=1"));
    }

    try {
      BlobBuilder.constructBlobAndMetadata(
          "a.bdec",
          Collections.singletonList(createChannelDataPerTable(0, true)),
          Constants.BdecVersion.THREE);
      Assert.fail("Should not pass enableParquetInternalBuffering=true");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.INTERNAL_ERROR.getMessageCode(), e.getVendorCode());
      Assert.assertTrue(e.getMessage().contains("serializeFromParquetWriteBuffers"));
      Assert.assertTrue(e.getMessage().contains("parquetTotalRowsInFooter=1"));
      Assert.assertTrue(e.getMessage().contains("totalMetadataRowCount=0"));
      Assert.assertTrue(e.getMessage().contains("parquetTotalRowsWritten=1"));
      Assert.assertTrue(e.getMessage().contains("perChannelRowCountsInMetadata=0"));
      Assert.assertTrue(e.getMessage().contains("perBlockRowCountsInFooter=1"));
      Assert.assertTrue(e.getMessage().contains("channelsCountInMetadata=1"));
      Assert.assertTrue(e.getMessage().contains("countOfSerializedJavaObjects=-1"));
    }
  }

  /**
   * Creates a channel data configurable number of rows in metadata and 1 physical row (using both
   * with and without internal buffering optimization)
   */
  private List<ChannelData<ParquetChunkData>> createChannelDataPerTable(
      int metadataRowCount, boolean enableParquetInternalBuffering) throws IOException {
    String columnName = "C1";
    ChannelData<ParquetChunkData> channelData = Mockito.spy(new ChannelData<>());
    MessageType schema = createSchema(columnName);
    Mockito.doReturn(
            new ParquetFlusher(
                schema,
                enableParquetInternalBuffering,
                100L,
                Constants.BdecParquetCompression.GZIP))
        .when(channelData)
        .createFlusher();

    channelData.setRowSequencer(1L);
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    BdecParquetWriter bdecParquetWriter =
        new BdecParquetWriter(
            stream,
            schema,
            new HashMap<>(),
            "CHANNEL",
            1000,
            Constants.BdecParquetCompression.GZIP);
    bdecParquetWriter.writeRow(Collections.singletonList("1"));
    channelData.setVectors(
        new ParquetChunkData(
            Collections.singletonList(Collections.singletonList("A")),
            bdecParquetWriter,
            stream,
            new HashMap<>()));
    channelData.setColumnEps(new HashMap<>());
    channelData.setRowCount(metadataRowCount);
    channelData.setMinMaxInsertTimeInMs(new Pair<>(2L, 3L));

    channelData.getColumnEps().putIfAbsent(columnName, new RowBufferStats(columnName, null, 1));
    channelData.setChannelContext(
        new ChannelFlushContext("channel1", "DB", "SCHEMA", "TABLE", 1L, "enc", 1L));
    return Collections.singletonList(channelData);
  }

  private static MessageType createSchema(String columnName) {
    ParquetTypeGenerator.ParquetTypeInfo c1 =
        ParquetTypeGenerator.generateColumnParquetTypeInfo(createTestTextColumn(columnName), 1);
    return new MessageType("bdec", Collections.singletonList(c1.getParquetType()));
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
}
