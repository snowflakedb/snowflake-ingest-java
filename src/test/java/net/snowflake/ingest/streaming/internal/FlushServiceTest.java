package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.utils.Constants.BLOB_CHECKSUM_SIZE_IN_BYTES;
import static net.snowflake.ingest.utils.Constants.BLOB_CHUNK_METADATA_LENGTH_SIZE_IN_BYTES;
import static net.snowflake.ingest.utils.Constants.BLOB_EXTENSION_TYPE;
import static net.snowflake.ingest.utils.Constants.BLOB_FILE_SIZE_SIZE_IN_BYTES;
import static net.snowflake.ingest.utils.Constants.BLOB_FORMAT_VERSION;
import static net.snowflake.ingest.utils.Constants.BLOB_NO_HEADER;
import static net.snowflake.ingest.utils.Constants.BLOB_TAG_SIZE_IN_BYTES;
import static net.snowflake.ingest.utils.Constants.BLOB_VERSION_SIZE_IN_BYTES;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import net.snowflake.client.jdbc.SnowflakeConnectionV1;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;
import net.snowflake.ingest.utils.Utils;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.util.Text;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*", "javax.management.*"})
@RunWith(PowerMockRunner.class)
@PrepareForTest({
  Utils.class,
})
public class FlushServiceTest {
  private SnowflakeStreamingIngestClientInternal client;
  private ChannelCache channelCache;
  private SnowflakeConnectionV1 conn;
  private SnowflakeStreamingIngestChannelInternal channel1;
  private SnowflakeStreamingIngestChannelInternal channel2;
  private SnowflakeStreamingIngestChannelInternal channel3;
  private StreamingIngestStage stage;

  private final BufferAllocator allocator = new RootAllocator();

  @Before
  public void setup() {
    client = Mockito.mock(SnowflakeStreamingIngestClientInternal.class);
    stage = Mockito.mock(StreamingIngestStage.class);
    channelCache = new ChannelCache();
    conn = Mockito.mock(SnowflakeConnectionV1.class);
    channel1 =
        new SnowflakeStreamingIngestChannelInternal(
            "channel1", "db1", "schema1", "table1", "offset1", 0L, 0L, client, true);

    channel2 =
        new SnowflakeStreamingIngestChannelInternal(
            "channel2", "db1", "schema1", "table1", "offset2", 10L, 100L, client, true);

    channel3 =
        new SnowflakeStreamingIngestChannelInternal(
            "channel3", "db2", "schema1", "table2", "offset3", 0L, 0L, client, true);
  }

  @Test
  @Ignore // TODO: SNOW-414124 Put test back after EP server fix
  public void testGetFilePath() {
    FlushService flushService = new FlushService(client, channelCache, stage, false);
    Calendar calendar = Calendar.getInstance();
    String clientPrefix = "honk";
    String outputString = flushService.getFilePath(calendar, clientPrefix);
    Path outputPath = Paths.get(outputString);
    Assert.assertEquals(clientPrefix, outputPath.getParent().getFileName().toString());
    Assert.assertEquals(
        Integer.toString(calendar.get(Calendar.HOUR_OF_DAY)),
        outputPath.getParent().getParent().getFileName().toString());
    Assert.assertEquals(
        Integer.toString(calendar.get(Calendar.DAY_OF_MONTH)),
        outputPath.getParent().getParent().getParent().getFileName().toString());
    Assert.assertEquals(
        Integer.toString(calendar.get(Calendar.MONTH)),
        outputPath.getParent().getParent().getParent().getParent().getFileName().toString());
    Assert.assertEquals(
        Integer.toString(calendar.get(Calendar.YEAR)),
        outputPath.getParent().getParent().getParent().getParent().getParent().toString());
  }

  @Test
  public void testFlush() throws Exception {
    FlushService flushService = Mockito.spy(new FlushService(client, channelCache, stage, false));

    // Nothing to flush
    flushService.flush(false);
    Mockito.verify(flushService, Mockito.times(0)).distributeFlushTasks();

    // Force = true flushes
    flushService.flush(true);
    Mockito.verify(flushService).distributeFlushTasks();
    Mockito.verify(flushService, Mockito.times(1)).distributeFlushTasks();

    // isNeedFlush = true flushes
    flushService.isNeedFlush = true;
    flushService.flush(false);
    Mockito.verify(flushService, Mockito.times(2)).distributeFlushTasks();
    Assert.assertFalse(flushService.isNeedFlush);

    // lastFlushTime causes flush
    flushService.lastFlushTime = 0;
    flushService.flush(false);
    Mockito.verify(flushService, Mockito.times(3)).distributeFlushTasks();
    Assert.assertTrue(flushService.lastFlushTime > 0);
  }

  @Test
  public void testBuildAndUpload() throws Exception {
    FlushService flushService = Mockito.spy(new FlushService(client, channelCache, stage, true));

    List<List<ChannelData>> blobData = new ArrayList<>();
    List<ChannelData> chunkData = new ArrayList<>();

    // Construct fields
    ChannelData channel1Data = new ChannelData();
    ChannelData channel2Data = new ChannelData();

    FieldVector vector1 = new VarCharVector("vector1", allocator);
    FieldVector vector2 = new IntVector("vector2", allocator);
    FieldVector vector3 = new VarCharVector("vector3", allocator);
    FieldVector vector4 = new IntVector("vector4", allocator);
    List<FieldVector> vectorList1 = new ArrayList<>();
    vectorList1.add(vector1);
    vectorList1.add(vector2);
    List<FieldVector> vectorList2 = new ArrayList<>();
    vectorList2.add(vector3);
    vectorList2.add(vector4);

    channel1Data.setVectors(vectorList1);
    channel2Data.setVectors(vectorList2);

    Map<String, RowBufferStats> eps1 = new HashMap<>();
    Map<String, RowBufferStats> eps2 = new HashMap<>();

    RowBufferStats stats1 = new RowBufferStats();
    RowBufferStats stats2 = new RowBufferStats();

    eps1.put("one", stats1);
    eps2.put("one", stats2);

    stats1.addIntValue(new BigInteger("10"));
    stats1.addIntValue(new BigInteger("15"));
    stats2.addIntValue(new BigInteger("11"));
    stats2.addIntValue(new BigInteger("13"));
    stats2.addIntValue(new BigInteger("17"));

    channel1Data.setColumnEps(eps1);
    channel2Data.setColumnEps(eps2);

    chunkData.add(channel1Data);
    chunkData.add(channel2Data);
    blobData.add(chunkData);

    // Populate test row data
    ((VarCharVector) vector1).setSafe(0, new Text("alice"));
    ((VarCharVector) vector1).setSafe(1, new Text("bob"));
    ((IntVector) vector2).setSafe(0, 11);
    ((IntVector) vector2).setSafe(1, 22);

    ((VarCharVector) vector3).setSafe(0, new Text("toby"));
    ((IntVector) vector4).setNull(0);

    vector1.setValueCount(2);
    vector2.setValueCount(2);
    vector3.setValueCount(1);
    vector4.setValueCount(1);

    channel1Data.setRowSequencer(0L);
    channel1Data.setOffsetToken("offset1");
    channel1Data.setRowCount(2);
    channel1Data.setBufferSize(100);
    channel1Data.setChannel(channel1);

    channel2Data.setRowSequencer(10L);
    channel2Data.setOffsetToken("offset2");
    channel2Data.setRowCount(1);
    channel2Data.setBufferSize(100);
    channel2Data.setChannel(channel2);

    BlobMetadata blobMetadata = flushService.buildAndUpload("file_name", blobData);

    EpInfo expectedChunkEpInfo =
        ArrowRowBuffer.buildEpInfoFromStats(3, ChannelData.getCombinedColumnStatsMap(eps1, eps2));

    ChannelMetadata expectedChannel1Metadata =
        ChannelMetadata.builder()
            .setOwningChannel(channel1)
            .setRowSequencer(1L)
            .setOffsetToken("offset1")
            .build();
    ChannelMetadata expectedChannel2Metadata =
        ChannelMetadata.builder()
            .setOwningChannel(channel2)
            .setRowSequencer(10L)
            .setOffsetToken("offset2")
            .build();
    ChunkMetadata expectedChunkMetadata =
        ChunkMetadata.builder()
            .setOwningTable(channel1)
            .setChunkStartOffset(0L)
            .setChunkLength(248)
            .setChannelList(Lists.newArrayList(expectedChannel1Metadata, expectedChannel2Metadata))
            .setChunkMD5("md5")
            .setEpInfo(expectedChunkEpInfo)
            .build();

    // Check FlushService.upload called with correct arguments
    final ArgumentCaptor<String> nameCaptor = ArgumentCaptor.forClass(String.class);
    final ArgumentCaptor<byte[]> blobCaptor = ArgumentCaptor.forClass(byte[].class);
    final ArgumentCaptor<List<ChunkMetadata>> metadataCaptor = ArgumentCaptor.forClass(List.class);

    Mockito.verify(flushService)
        .upload(nameCaptor.capture(), blobCaptor.capture(), metadataCaptor.capture());
    Assert.assertEquals("file_name", nameCaptor.getValue());

    ChunkMetadata metadataResult = metadataCaptor.getValue().get(0);
    List<ChannelMetadata> channelMetadataResult = metadataResult.getChannels();

    Assert.assertEquals(BlobBuilder.computeMD5(blobCaptor.getValue()), blobMetadata.getMD5());

    Assert.assertEquals(
        expectedChunkEpInfo.getRowCount(), metadataResult.getEpInfo().getRowCount());
    Assert.assertEquals(
        expectedChunkEpInfo.getColumnEps().keySet().size(),
        metadataResult.getEpInfo().getColumnEps().keySet().size());
    Assert.assertEquals(
        expectedChunkEpInfo.getColumnEps().get("one"),
        metadataResult.getEpInfo().getColumnEps().get("one"));

    Assert.assertEquals(expectedChunkMetadata.getDBName(), metadataResult.getDBName());
    Assert.assertEquals(expectedChunkMetadata.getSchemaName(), metadataResult.getSchemaName());
    Assert.assertEquals(expectedChunkMetadata.getTableName(), metadataResult.getTableName());
    Assert.assertEquals(2, metadataResult.getChannels().size()); // Two channels on the table

    Assert.assertEquals("channel1", channelMetadataResult.get(0).getChannelName());
    Assert.assertEquals("offset1", channelMetadataResult.get(0).getOffsetToken());
    Assert.assertEquals(0L, (long) channelMetadataResult.get(0).getRowSequencer());
    Assert.assertEquals(0L, (long) channelMetadataResult.get(0).getClientSequencer());

    Assert.assertEquals("channel2", channelMetadataResult.get(1).getChannelName());
    Assert.assertEquals("offset2", channelMetadataResult.get(1).getOffsetToken());
    Assert.assertEquals(10L, (long) channelMetadataResult.get(1).getRowSequencer());
    Assert.assertEquals(10L, (long) channelMetadataResult.get(1).getClientSequencer());

    String md5 =
        BlobBuilder.computeMD5(
            Arrays.copyOfRange(
                blobCaptor.getValue(),
                (int) metadataResult.getChunkStartOffset().longValue(),
                (int) (metadataResult.getChunkStartOffset() + metadataResult.getChunkLength())));
    Assert.assertEquals(md5, metadataResult.getChunkMD5());

    try {
      // Close allocator to make sure no memory leak
      allocator.close();
    } catch (Exception e) {
      Assert.fail(String.format("Allocator close failure. Caused by %s", e.getMessage()));
    }
  }

  @Test
  public void testBuildErrors() throws Exception {
    // build should error if we try to group channels for different tables together
    FlushService flushService = Mockito.spy(new FlushService(client, channelCache, stage, true));

    List<List<ChannelData>> channelData = new ArrayList<>();
    List<ChannelData> channelData1 = new ArrayList<>();

    // Construct fields
    ChannelData data1 = new ChannelData();
    ChannelData data2 = new ChannelData();

    FieldVector vector1 = new VarCharVector("vector1", allocator);
    FieldVector vector3 = new IntVector("vector3", allocator);
    vector1.allocateNew();
    vector3.allocateNew();
    List<FieldVector> vectorList1 = new ArrayList<>();
    vectorList1.add(vector1);
    List<FieldVector> vectorList2 = new ArrayList<>();
    vectorList1.add(vector3);

    data1.setVectors(vectorList1);
    data2.setVectors(vectorList2);

    channelData1.add(data1);
    channelData1.add(data2);
    channelData.add(channelData1);

    // Populate test row data
    ((VarCharVector) vector1).setSafe(0, new Text("alice"));
    ((IntVector) vector3).setSafe(0, 111);

    vector1.setValueCount(2);
    vector3.setValueCount(3);

    data1.setRowSequencer(0L);
    data1.setOffsetToken("offset1");
    data1.setRowCount(2);
    data1.setBufferSize(100);
    data1.setChannel(channel1);

    data2.setRowSequencer(10L);
    data2.setOffsetToken("offset3");
    data2.setRowCount(1);
    data2.setBufferSize(100);
    data2.setChannel(channel3);

    try {
      flushService.buildAndUpload("file_name", channelData);
      Assert.fail("Expected SFException");
    } catch (SFException err) {
      Assert.assertEquals(ErrorCode.INVALID_DATA_IN_CHUNK.getMessageCode(), err.getVendorCode());
    }
  }

  @Test
  public void testBlobBuilder() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    List<ChunkMetadata> chunksMetadataList = new ArrayList<>();
    List<byte[]> chunksDataList = new ArrayList<>();
    long checksum = 100;
    byte[] data = "fake data".getBytes(StandardCharsets.UTF_8);
    int dataSize = data.length;

    chunksDataList.add(data);

    Map<String, RowBufferStats> eps1 = new HashMap<>();

    RowBufferStats stats1 = new RowBufferStats();

    eps1.put("one", stats1);

    stats1.addIntValue(new BigInteger("10"));
    stats1.addIntValue(new BigInteger("15"));
    EpInfo epInfo = ArrowRowBuffer.buildEpInfoFromStats(2, eps1);

    ChannelMetadata channelMetadata =
        ChannelMetadata.builder()
            .setOwningChannel(channel1)
            .setRowSequencer(0L)
            .setOffsetToken("offset1")
            .build();
    ChunkMetadata chunkMetadata =
        ChunkMetadata.builder()
            .setOwningTable(channel1)
            .setChunkStartOffset(0L)
            .setChunkLength(dataSize)
            .setChannelList(Lists.newArrayList(channelMetadata))
            .setChunkMD5("md5")
            .setEpInfo(epInfo)
            .build();

    chunksMetadataList.add(chunkMetadata);

    byte[] blob = BlobBuilder.build(chunksMetadataList, chunksDataList, checksum, dataSize);

    // Read the blob byte array back to valid the behavior
    InputStream input = new ByteArrayInputStream(blob);
    int offset = 0;
    if (!BLOB_NO_HEADER) {
      Assert.assertEquals(
          BLOB_EXTENSION_TYPE,
          new String(
              Arrays.copyOfRange(blob, offset, offset += BLOB_TAG_SIZE_IN_BYTES),
              StandardCharsets.UTF_8));
      Assert.assertEquals(
          BLOB_FORMAT_VERSION,
          Arrays.copyOfRange(blob, offset, offset += BLOB_VERSION_SIZE_IN_BYTES)[0]);
      long totalSize =
          ByteBuffer.wrap(Arrays.copyOfRange(blob, offset, offset += BLOB_FILE_SIZE_SIZE_IN_BYTES))
              .getLong();
      Assert.assertEquals(
          checksum,
          ByteBuffer.wrap(Arrays.copyOfRange(blob, offset, offset += BLOB_CHECKSUM_SIZE_IN_BYTES))
              .getLong());
      int chunkMetadataSize =
          ByteBuffer.wrap(
                  Arrays.copyOfRange(
                      blob, offset, offset += BLOB_CHUNK_METADATA_LENGTH_SIZE_IN_BYTES))
              .getInt();
      Assert.assertTrue(chunkMetadataSize < totalSize);
      String parsedChunkMetadataList =
          new String(
              Arrays.copyOfRange(blob, offset, offset += chunkMetadataSize),
              StandardCharsets.UTF_8);
      Map<String, Object> map =
          mapper.readValue(
              parsedChunkMetadataList.substring(1, parsedChunkMetadataList.length() - 1),
              Map.class);
      Assert.assertEquals(chunkMetadata.getTableName(), map.get("table"));
      Assert.assertEquals(chunkMetadata.getSchemaName(), map.get("schema"));
      Assert.assertEquals(chunkMetadata.getDBName(), map.get("database"));
      Assert.assertEquals(
          Long.toString(chunkMetadata.getChunkStartOffset() - offset),
          map.get("chunk_start_offset").toString());
      Assert.assertEquals(chunkMetadata.getChunkMD5(), map.get("chunk_md5"));
      Assert.assertEquals(2, ((LinkedHashMap) map.get("eps")).get("rows"));
      Assert.assertNotNull(((LinkedHashMap) map.get("eps")).get("columns"));
      byte[] actualData = Arrays.copyOfRange(blob, offset, (int) totalSize);
      Assert.assertArrayEquals(data, actualData);
    } else {
      Assert.assertArrayEquals(data, blob);
    }
  }

  @Test
  public void testShutDown() throws Exception {
    FlushService flushService = new FlushService(client, channelCache, stage, false);

    Assert.assertFalse(flushService.buildUploadWorkers.isShutdown());
    Assert.assertFalse(flushService.registerWorker.isShutdown());
    Assert.assertFalse(flushService.flushWorker.isShutdown());

    flushService.shutdown();

    Assert.assertTrue(flushService.buildUploadWorkers.isShutdown());
    Assert.assertTrue(flushService.registerWorker.isShutdown());
    Assert.assertTrue(flushService.flushWorker.isShutdown());
  }
}
