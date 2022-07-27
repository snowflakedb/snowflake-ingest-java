package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.utils.Constants.BLOB_CHECKSUM_SIZE_IN_BYTES;
import static net.snowflake.ingest.utils.Constants.BLOB_CHUNK_METADATA_LENGTH_SIZE_IN_BYTES;
import static net.snowflake.ingest.utils.Constants.BLOB_EXTENSION_TYPE;
import static net.snowflake.ingest.utils.Constants.BLOB_FILE_SIZE_SIZE_IN_BYTES;
import static net.snowflake.ingest.utils.Constants.BLOB_NO_HEADER;
import static net.snowflake.ingest.utils.Constants.BLOB_TAG_SIZE_IN_BYTES;
import static net.snowflake.ingest.utils.Constants.BLOB_VERSION_SIZE_IN_BYTES;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Calendar;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import net.snowflake.client.jdbc.SnowflakeConnectionV1;
import net.snowflake.client.jdbc.internal.org.bouncycastle.jce.provider.BouncyCastleProvider;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.utils.Constants;
import net.snowflake.ingest.utils.Cryptor;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.ParameterProvider;
import net.snowflake.ingest.utils.SFException;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.util.Text;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class FlushServiceTest {
  private SnowflakeStreamingIngestClientInternal<VectorSchemaRoot> client;
  private ChannelCache<VectorSchemaRoot> channelCache;
  private SnowflakeConnectionV1 conn;
  private SnowflakeStreamingIngestChannelInternal<VectorSchemaRoot> channel1;
  private SnowflakeStreamingIngestChannelInternal<VectorSchemaRoot> channel2;
  private SnowflakeStreamingIngestChannelInternal<VectorSchemaRoot> channel3;
  private StreamingIngestStage stage;

  private final BufferAllocator allocator = new RootAllocator();

  @Before
  public void setup() {
    java.security.Security.addProvider(new BouncyCastleProvider());

    ParameterProvider parameterProvider = new ParameterProvider();
    client = Mockito.mock(SnowflakeStreamingIngestClientInternal.class);
    Mockito.when(client.getParameterProvider()).thenReturn(parameterProvider);
    stage = Mockito.mock(StreamingIngestStage.class);
    Mockito.when(stage.getClientPrefix()).thenReturn("client_prefix");
    channelCache = new ChannelCache<>();
    Mockito.when(client.getChannelCache()).thenReturn(channelCache);
    conn = Mockito.mock(SnowflakeConnectionV1.class);
    channel1 =
        new SnowflakeStreamingIngestChannelInternal<>(
            "channel1",
            "db1",
            "schema1",
            "table1",
            "offset1",
            0L,
            0L,
            client,
            "key",
            1234L,
            OpenChannelRequest.OnErrorOption.CONTINUE,
            true);

    channel2 =
        new SnowflakeStreamingIngestChannelInternal<>(
            "channel2",
            "db1",
            "schema1",
            "table1",
            "offset2",
            10L,
            100L,
            client,
            "key",
            1234L,
            OpenChannelRequest.OnErrorOption.CONTINUE,
            true);

    channel3 =
        new SnowflakeStreamingIngestChannelInternal<>(
            "channel3",
            "db2",
            "schema1",
            "table2",
            "offset3",
            0L,
            0L,
            client,
            "key",
            1234L,
            OpenChannelRequest.OnErrorOption.CONTINUE,
            true);
    channelCache.addChannel(channel1);
    channelCache.addChannel(channel2);
    channelCache.addChannel(channel3);
  }

  @Test
  public void testGetFilePath() {
    FlushService<VectorSchemaRoot> flushService =
        new FlushService<>(client, channelCache, stage, false);
    Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    String clientPrefix = "honk";
    String outputString = flushService.getFilePath(calendar, clientPrefix);
    Path outputPath = Paths.get(outputString);
    Assert.assertTrue(outputPath.getFileName().toString().contains(clientPrefix));
    Assert.assertTrue(
        calendar.get(Calendar.MINUTE)
                - Integer.parseInt(outputPath.getParent().getFileName().toString())
            <= 1);
    Assert.assertEquals(
        Integer.toString(calendar.get(Calendar.HOUR_OF_DAY)),
        outputPath.getParent().getParent().getFileName().toString());
    Assert.assertEquals(
        Integer.toString(calendar.get(Calendar.DAY_OF_MONTH)),
        outputPath.getParent().getParent().getParent().getFileName().toString());
    Assert.assertEquals(
        Integer.toString(calendar.get(Calendar.MONTH) + 1),
        outputPath.getParent().getParent().getParent().getParent().getFileName().toString());
    Assert.assertEquals(
        Integer.toString(calendar.get(Calendar.YEAR)),
        outputPath
            .getParent()
            .getParent()
            .getParent()
            .getParent()
            .getParent()
            .getFileName()
            .toString());
  }

  @Test
  public void testFlush() throws Exception {
    FlushService<VectorSchemaRoot> flushService =
        Mockito.spy(new FlushService<>(client, channelCache, stage, false));

    // Nothing to flush
    flushService.flush(false).get();
    Mockito.verify(flushService, Mockito.times(0)).distributeFlushTasks();

    // Force = true flushes
    flushService.flush(true).get();
    Mockito.verify(flushService).distributeFlushTasks();
    Mockito.verify(flushService, Mockito.times(1)).distributeFlushTasks();

    // isNeedFlush = true flushes
    flushService.isNeedFlush = true;
    flushService.flush(false).get();
    Mockito.verify(flushService, Mockito.times(2)).distributeFlushTasks();
    Assert.assertFalse(flushService.isNeedFlush);

    // lastFlushTime causes flush
    flushService.lastFlushTime = 0;
    flushService.flush(false).get();
    Mockito.verify(flushService, Mockito.times(3)).distributeFlushTasks();
    Assert.assertTrue(flushService.lastFlushTime > 0);
  }

  @Test
  public void testBuildAndUpload() throws Exception {
    FlushService<VectorSchemaRoot> flushService =
        Mockito.spy(new FlushService<>(client, channelCache, stage, true));

    List<List<ChannelData<VectorSchemaRoot>>> blobData = new ArrayList<>();
    List<ChannelData<VectorSchemaRoot>> chunkData = new ArrayList<>();

    // Construct fields
    ChannelData<VectorSchemaRoot> channel1Data = new ChannelData<>();
    ChannelData<VectorSchemaRoot> channel2Data = new ChannelData<>();

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

    VectorSchemaRoot vectorRoot1 = new VectorSchemaRoot(vectorList1);
    vectorRoot1.setRowCount(2);
    VectorSchemaRoot vectorRoot2 = new VectorSchemaRoot(vectorList2);
    vectorRoot2.setRowCount(1);

    channel1Data.setVectors(vectorRoot1);
    channel2Data.setVectors(vectorRoot2);

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
    channel1Data.setBufferSize(100);
    channel1Data.setChannel(channel1);
    channel1Data.setRowCount(2);

    channel2Data.setRowSequencer(10L);
    channel2Data.setOffsetToken("offset2");
    channel2Data.setBufferSize(100);
    channel2Data.setChannel(channel2);
    channel2Data.setRowCount(1);

    BlobMetadata blobMetadata = flushService.buildAndUpload("file_name", blobData);

    EpInfo expectedChunkEpInfo =
        AbstractRowBuffer.buildEpInfoFromStats(
            3, ChannelData.getCombinedColumnStatsMap(eps1, eps2));

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
            .setChannelList(Arrays.asList(expectedChannel1Metadata, expectedChannel2Metadata))
            .setChunkMD5("md5")
            .setEncryptionKeyId(1234L)
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
    FlushService<VectorSchemaRoot> flushService =
        Mockito.spy(new FlushService<>(client, channelCache, stage, true));

    List<List<ChannelData<VectorSchemaRoot>>> channelData = new ArrayList<>();
    List<ChannelData<VectorSchemaRoot>> channelData1 = new ArrayList<>();

    // Construct fields
    ChannelData<VectorSchemaRoot> data1 = new ChannelData<>();
    ChannelData<VectorSchemaRoot> data2 = new ChannelData<>();

    FieldVector vector1 = new VarCharVector("vector1", allocator);
    FieldVector vector3 = new IntVector("vector3", allocator);
    vector1.allocateNew();
    vector3.allocateNew();
    List<FieldVector> vectorList1 = new ArrayList<>();
    vectorList1.add(vector1);
    List<FieldVector> vectorList2 = new ArrayList<>();
    vectorList1.add(vector3);

    VectorSchemaRoot vectorRoot1 = new VectorSchemaRoot(vectorList1);
    vectorRoot1.setRowCount(2);
    VectorSchemaRoot vectorRoot2 = new VectorSchemaRoot(vectorList2);
    vectorRoot2.setRowCount(1);

    data1.setVectors(vectorRoot1);
    data2.setVectors(vectorRoot2);

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
    data1.setBufferSize(100);
    data1.setChannel(channel1);

    data2.setRowSequencer(10L);
    data2.setOffsetToken("offset3");
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
  public void testInvalidateChannels() throws Exception {
    // Create a new Client in order to not interfere with other tests
    SnowflakeStreamingIngestClientInternal<VectorSchemaRoot> client =
        Mockito.mock(SnowflakeStreamingIngestClientInternal.class);
    ParameterProvider parameterProvider = new ParameterProvider();
    ChannelCache<VectorSchemaRoot> channelCache = new ChannelCache<>();
    Mockito.when(client.getChannelCache()).thenReturn(channelCache);
    Mockito.when(client.getParameterProvider()).thenReturn(parameterProvider);
    SnowflakeStreamingIngestChannelInternal<VectorSchemaRoot> channel1 =
        new SnowflakeStreamingIngestChannelInternal<>(
            "channel1",
            "db1",
            "schema1",
            "table1",
            "offset1",
            0L,
            0L,
            client,
            "key",
            1234L,
            OpenChannelRequest.OnErrorOption.CONTINUE,
            true);

    SnowflakeStreamingIngestChannelInternal<VectorSchemaRoot> channel2 =
        new SnowflakeStreamingIngestChannelInternal<>(
            "channel2",
            "db1",
            "schema1",
            "table1",
            "offset2",
            10L,
            100L,
            client,
            "key",
            1234L,
            OpenChannelRequest.OnErrorOption.CONTINUE,
            true);

    channelCache.addChannel(channel1);
    channelCache.addChannel(channel2);

    List<List<ChannelData<VectorSchemaRoot>>> blobData = new ArrayList<>();
    List<ChannelData<VectorSchemaRoot>> innerData = new ArrayList<>();
    blobData.add(innerData);

    ChannelData<VectorSchemaRoot> channel1Data = new ChannelData<>();
    ChannelData<VectorSchemaRoot> channel2Data = new ChannelData<>();
    channel1Data.setChannel(channel1);
    channel2Data.setChannel(channel1);

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

    VectorSchemaRoot vectorRoot1 = new VectorSchemaRoot(vectorList1);
    VectorSchemaRoot vectorRoot2 = new VectorSchemaRoot(vectorList2);

    channel1Data.setVectors(vectorRoot1);
    channel2Data.setVectors(vectorRoot2);
    innerData.add(channel1Data);
    innerData.add(channel2Data);

    FlushService<VectorSchemaRoot> flushService =
        new FlushService<>(client, channelCache, stage, false);
    flushService.invalidateAllChannelsInBlob(blobData);

    Assert.assertFalse(channel1.isValid());
    Assert.assertTrue(channel2.isValid());
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
    EpInfo epInfo = AbstractRowBuffer.buildEpInfoFromStats(2, eps1);

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
            .setChannelList(Arrays.asList(channelMetadata))
            .setChunkMD5("md5")
            .setEncryptionKeyId(1234L)
            .setEpInfo(epInfo)
            .build();

    chunksMetadataList.add(chunkMetadata);

    final Constants.BdecVersion bdecVersion = ParameterProvider.BLOB_FORMAT_VERSION_DEFAULT;
    byte[] blob =
        BlobBuilder.build(chunksMetadataList, chunksDataList, checksum, dataSize, bdecVersion);

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
          bdecVersion, Arrays.copyOfRange(blob, offset, offset += BLOB_VERSION_SIZE_IN_BYTES)[0]);
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
    FlushService<VectorSchemaRoot> flushService =
        new FlushService<>(client, channelCache, stage, false);

    Assert.assertFalse(flushService.buildUploadWorkers.isShutdown());
    Assert.assertFalse(flushService.registerWorker.isShutdown());
    Assert.assertFalse(flushService.flushWorker.isShutdown());

    flushService.shutdown();

    Assert.assertTrue(flushService.buildUploadWorkers.isShutdown());
    Assert.assertTrue(flushService.registerWorker.isShutdown());
    Assert.assertTrue(flushService.flushWorker.isShutdown());
  }

  @Test
  public void testEncryptionDecryption()
      throws InvalidAlgorithmParameterException, NoSuchPaddingException, IllegalBlockSizeException,
          NoSuchAlgorithmException, BadPaddingException, InvalidKeyException {
    byte[] data = "testEncryptionDecryption".getBytes(StandardCharsets.UTF_8);
    String encryptionKey =
        Base64.getEncoder().encodeToString("encryption_key".getBytes(StandardCharsets.UTF_8));
    String diversifier = "2021/08/10/blob.bdec";

    byte[] encryptedData = Cryptor.encrypt(data, encryptionKey, diversifier, 0);
    byte[] decryptedData = Cryptor.decrypt(encryptedData, encryptionKey, diversifier, 0);

    Assert.assertArrayEquals(data, decryptedData);
  }
}
