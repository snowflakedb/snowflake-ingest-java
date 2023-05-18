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
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import net.snowflake.client.jdbc.internal.org.bouncycastle.jce.provider.BouncyCastleProvider;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.utils.Constants;
import net.snowflake.ingest.utils.Cryptor;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.ParameterProvider;
import net.snowflake.ingest.utils.SFException;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

@RunWith(Parameterized.class)
public class FlushServiceTest {
  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> testContextFactory() {
    return Arrays.asList(
        new Object[][] {{ArrowTestContext.createFactory()}, {ParquetTestContext.createFactory()}});
  }

  public FlushServiceTest(TestContextFactory<?> testContextFactory) {
    this.testContextFactory = testContextFactory;
  }

  private abstract static class TestContextFactory<T> {
    private final String name;

    TestContextFactory(String name) {
      this.name = name;
    }

    abstract TestContext<T> create();

    @Override
    public String toString() {
      return name;
    }
  }

  private abstract static class TestContext<T> implements AutoCloseable {
    SnowflakeStreamingIngestClientInternal<T> client;
    ChannelCache<T> channelCache;
    final Map<String, SnowflakeStreamingIngestChannelInternal<T>> channels = new HashMap<>();
    FlushService<T> flushService;
    StreamingIngestStage stage;
    ParameterProvider parameterProvider;

    final List<ChannelData<T>> channelData = new ArrayList<>();

    TestContext() {
      stage = Mockito.mock(StreamingIngestStage.class);
      Mockito.when(stage.getClientPrefix()).thenReturn("client_prefix");
      parameterProvider = new ParameterProvider();
      client = Mockito.mock(SnowflakeStreamingIngestClientInternal.class);
      Mockito.when(client.getParameterProvider()).thenReturn(parameterProvider);
      channelCache = new ChannelCache<>();
      Mockito.when(client.getChannelCache()).thenReturn(channelCache);
      flushService = Mockito.spy(new FlushService<>(client, channelCache, stage, false));
    }

    ChannelData<T> flushChannel(String name) {
      SnowflakeStreamingIngestChannelInternal<T> channel = channels.get(name);
      ChannelData<T> channelData = channel.getRowBuffer().flush(name + "_snowpipe_streaming.bdec");
      channelData.setChannelContext(channel.getChannelContext());
      this.channelData.add(channelData);
      return channelData;
    }

    BlobMetadata buildAndUpload() throws Exception {
      List<List<ChannelData<T>>> blobData = Collections.singletonList(channelData);
      return flushService.buildAndUpload("file_name", blobData);
    }

    abstract SnowflakeStreamingIngestChannelInternal<T> createChannel(
        String name,
        String dbName,
        String schemaName,
        String tableName,
        String offsetToken,
        Long channelSequencer,
        Long rowSequencer,
        String encryptionKey,
        Long encryptionKeyId,
        OpenChannelRequest.OnErrorOption onErrorOption,
        ZoneId defaultTimezone);

    ChannelBuilder channelBuilder(String name) {
      return new ChannelBuilder(name);
    }

    class ChannelBuilder {
      private final String name;
      private String dbName = "db1";
      private String schemaName = "schema1";
      private String tableName = "table1";
      private String offsetToken = "offset1";
      private Long channelSequencer = 0L;
      private Long rowSequencer = 0L;
      private String encryptionKey = "key";
      private Long encryptionKeyId = 0L;
      private OpenChannelRequest.OnErrorOption onErrorOption =
          OpenChannelRequest.OnErrorOption.CONTINUE;

      private ChannelBuilder(String name) {
        this.name = name;
      }

      ChannelBuilder setDBName(String dbName) {
        this.dbName = dbName;
        return this;
      }

      ChannelBuilder setSchemaName(String schemaName) {
        this.schemaName = schemaName;
        return this;
      }

      ChannelBuilder setTableName(String tableName) {
        this.tableName = tableName;
        return this;
      }

      ChannelBuilder setOffsetToken(String offsetToken) {
        this.offsetToken = offsetToken;
        return this;
      }

      ChannelBuilder setChannelSequencer(Long sequencer) {
        this.channelSequencer = sequencer;
        return this;
      }

      ChannelBuilder setRowSequencer(Long sequencer) {
        this.rowSequencer = sequencer;
        return this;
      }

      ChannelBuilder setEncryptionKey(String encryptionKey) {
        this.encryptionKey = encryptionKey;
        return this;
      }

      ChannelBuilder setEncryptionKeyId(Long encryptionKeyId) {
        this.encryptionKeyId = encryptionKeyId;
        return this;
      }

      SnowflakeStreamingIngestChannelInternal<T> buildAndAdd() {
        SnowflakeStreamingIngestChannelInternal<T> channel =
            createChannel(
                name,
                dbName,
                schemaName,
                tableName,
                offsetToken,
                channelSequencer,
                rowSequencer,
                encryptionKey,
                encryptionKeyId,
                onErrorOption,
                ZoneOffset.UTC);
        channels.put(name, channel);
        channelCache.addChannel(channel);
        return channel;
      }
    }
  }

  private static class RowSetBuilder {
    private final List<Map<String, Object>> rows = new ArrayList<>();

    private Map<String, Object> lastRow() {
      return rows.get(rows.size() - 1);
    }

    RowSetBuilder addColumn(String column, Object value) {
      lastRow().put(column, value);
      return this;
    }

    RowSetBuilder newRow() {
      if (rows.isEmpty() || !lastRow().isEmpty()) {
        rows.add(new HashMap<>());
      }
      return this;
    }

    List<Map<String, Object>> build() {
      return rows;
    }

    static RowSetBuilder newBuilder() {
      return new RowSetBuilder().newRow();
    }
  }

  private static class ArrowTestContext extends TestContext<VectorSchemaRoot> {
    private final BufferAllocator allocator = new RootAllocator();

    SnowflakeStreamingIngestChannelInternal<VectorSchemaRoot> createChannel(
        String name,
        String dbName,
        String schemaName,
        String tableName,
        String offsetToken,
        Long channelSequencer,
        Long rowSequencer,
        String encryptionKey,
        Long encryptionKeyId,
        OpenChannelRequest.OnErrorOption onErrorOption,
        ZoneId defaultTimezone) {
      return new SnowflakeStreamingIngestChannelInternal<>(
          name,
          dbName,
          schemaName,
          tableName,
          offsetToken,
          channelSequencer,
          rowSequencer,
          client,
          encryptionKey,
          encryptionKeyId,
          onErrorOption,
          defaultTimezone,
          Constants.BdecVersion.ONE,
          allocator);
    }

    @Override
    public void close() {
      try {
        // Close allocator to make sure no memory leak
        allocator.close();
      } catch (Exception e) {
        Assert.fail(String.format("Allocator close failure. Caused by %s", e.getMessage()));
      }
    }

    static TestContextFactory<VectorSchemaRoot> createFactory() {
      return new TestContextFactory<VectorSchemaRoot>("Arrow") {
        @Override
        TestContext<VectorSchemaRoot> create() {
          return new ArrowTestContext();
        }
      };
    }
  }

  private static class ParquetTestContext extends TestContext<List<List<Object>>> {

    SnowflakeStreamingIngestChannelInternal<List<List<Object>>> createChannel(
        String name,
        String dbName,
        String schemaName,
        String tableName,
        String offsetToken,
        Long channelSequencer,
        Long rowSequencer,
        String encryptionKey,
        Long encryptionKeyId,
        OpenChannelRequest.OnErrorOption onErrorOption,
        ZoneId defaultTimezone) {
      return new SnowflakeStreamingIngestChannelInternal<>(
          name,
          dbName,
          schemaName,
          tableName,
          offsetToken,
          channelSequencer,
          rowSequencer,
          client,
          encryptionKey,
          encryptionKeyId,
          onErrorOption,
          defaultTimezone,
          Constants.BdecVersion.THREE,
          null);
    }

    @Override
    public void close() {}

    static TestContextFactory<List<List<Object>>> createFactory() {
      return new TestContextFactory<List<List<Object>>>("Parquet") {
        @Override
        TestContext<List<List<Object>>> create() {
          return new ParquetTestContext();
        }
      };
    }
  }

  TestContextFactory<?> testContextFactory;

  @Before
  public void setup() {
    java.security.Security.addProvider(new BouncyCastleProvider());
  }

  private SnowflakeStreamingIngestChannelInternal<?> addChannel1(TestContext<?> testContext) {
    return testContext
        .channelBuilder("channel1")
        .setDBName("db1")
        .setSchemaName("schema1")
        .setTableName("table1")
        .setOffsetToken("offset1")
        .setChannelSequencer(0L)
        .setRowSequencer(0L)
        .setEncryptionKey("key")
        .setEncryptionKeyId(1L)
        .buildAndAdd();
  }

  private SnowflakeStreamingIngestChannelInternal<?> addChannel2(TestContext<?> testContext) {
    return testContext
        .channelBuilder("channel2")
        .setDBName("db1")
        .setSchemaName("schema1")
        .setTableName("table1")
        .setOffsetToken("offset2")
        .setChannelSequencer(10L)
        .setRowSequencer(100L)
        .setEncryptionKey("key")
        .setEncryptionKeyId(1L)
        .buildAndAdd();
  }

  private SnowflakeStreamingIngestChannelInternal<?> addChannel3(TestContext<?> testContext) {
    return testContext
        .channelBuilder("channel3")
        .setDBName("db2")
        .setSchemaName("schema1")
        .setTableName("table2")
        .setOffsetToken("offset3")
        .setChannelSequencer(0L)
        .setRowSequencer(0L)
        .setEncryptionKey("key3")
        .setEncryptionKeyId(3L)
        .buildAndAdd();
  }

  private SnowflakeStreamingIngestChannelInternal<?> addChannel4(TestContext<?> testContext) {
    return testContext
        .channelBuilder("channel4")
        .setDBName("db1")
        .setSchemaName("schema1")
        .setTableName("table1")
        .setOffsetToken("offset2")
        .setChannelSequencer(10L)
        .setRowSequencer(100L)
        .setEncryptionKey("key4")
        .setEncryptionKeyId(4L)
        .buildAndAdd();
  }

  private static ColumnMetadata createTestIntegerColumn(String name) {
    ColumnMetadata colInt = new ColumnMetadata();
    colInt.setName(name);
    colInt.setPhysicalType("SB4");
    colInt.setNullable(true);
    colInt.setLogicalType("FIXED");
    colInt.setPrecision(2);
    colInt.setScale(0);
    return colInt;
  }

  private static ColumnMetadata createTestTextColumn(String name) {
    ColumnMetadata colChar = new ColumnMetadata();
    colChar.setName(name);
    colChar.setPhysicalType("LOB");
    colChar.setNullable(true);
    colChar.setLogicalType("TEXT");
    colChar.setByteLength(14);
    colChar.setLength(11);
    colChar.setScale(0);
    return colChar;
  }

  @Test
  public void testGetFilePath() {
    TestContext<?> testContext = testContextFactory.create();
    FlushService<?> flushService = testContext.flushService;
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
    TestContext<?> testContext = testContextFactory.create();
    FlushService<?> flushService = testContext.flushService;

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
  public void testBlobCreation() throws Exception {
    TestContext<?> testContext = testContextFactory.create();
    SnowflakeStreamingIngestChannelInternal<?> channel1 = addChannel1(testContext);
    SnowflakeStreamingIngestChannelInternal<?> channel2 = addChannel2(testContext);
    SnowflakeStreamingIngestChannelInternal<?> channel4 = addChannel4(testContext);
    String colName1 = "testBlobCreation1";
    String colName2 = "testBlobCreation2";

    List<ColumnMetadata> schema =
        Arrays.asList(createTestIntegerColumn(colName1), createTestTextColumn(colName2));
    channel1.getRowBuffer().setupSchema(schema);
    channel2.getRowBuffer().setupSchema(schema);
    channel4.getRowBuffer().setupSchema(schema);

    List<Map<String, Object>> rows1 =
        RowSetBuilder.newBuilder()
            .addColumn(colName1, 11)
            .addColumn(colName2, "bob")
            .newRow()
            .addColumn(colName1, 22)
            .addColumn(colName2, "bob")
            .build();

    channel1.insertRows(rows1, "offset1");
    channel2.insertRows(rows1, "offset2");
    channel4.insertRows(rows1, "offset4");

    FlushService<?> flushService = testContext.flushService;

    // Force = true flushes
    flushService.flush(true).get();
    Mockito.verify(flushService, Mockito.atLeast(2)).buildAndUpload(Mockito.any(), Mockito.any());
  }

  @Test
  public void testBlobSplitDueToDifferentSchema() throws Exception {
    TestContext<?> testContext = testContextFactory.create();
    SnowflakeStreamingIngestChannelInternal<?> channel1 = addChannel1(testContext);
    SnowflakeStreamingIngestChannelInternal<?> channel2 = addChannel2(testContext);
    String colName1 = "testBlobSplitDueToDifferentSchema1";
    String colName2 = "testBlobSplitDueToDifferentSchema2";
    String colName3 = "testBlobSplitDueToDifferentSchema3";

    List<ColumnMetadata> schema1 =
        Arrays.asList(createTestIntegerColumn(colName1), createTestTextColumn(colName2));
    List<ColumnMetadata> schema2 =
        Arrays.asList(
            createTestIntegerColumn(colName1),
            createTestTextColumn(colName2),
            createTestIntegerColumn(colName3));
    channel1.getRowBuffer().setupSchema(schema1);
    channel2.getRowBuffer().setupSchema(schema2);

    List<Map<String, Object>> rows1 =
        RowSetBuilder.newBuilder()
            .addColumn(colName1, 11)
            .addColumn(colName2, "bob")
            .newRow()
            .addColumn(colName1, 22)
            .addColumn(colName2, "bob")
            .build();

    List<Map<String, Object>> rows2 =
        RowSetBuilder.newBuilder()
            .addColumn(colName1, 11)
            .addColumn(colName2, "bob")
            .addColumn(colName3, 11)
            .newRow()
            .addColumn(colName1, 22)
            .addColumn(colName2, "bob")
            .addColumn(colName3, 22)
            .build();

    channel1.insertRows(rows1, "offset1");
    channel2.insertRows(rows2, "offset2");

    FlushService<?> flushService = testContext.flushService;

    // Force = true flushes
    flushService.flush(true).get();
    Mockito.verify(flushService, Mockito.atLeast(2)).buildAndUpload(Mockito.any(), Mockito.any());
  }

  @Test
  public void testBuildAndUpload() throws Exception {
    TestContext<?> testContext = testContextFactory.create();
    SnowflakeStreamingIngestChannelInternal<?> channel1 = addChannel1(testContext);
    SnowflakeStreamingIngestChannelInternal<?> channel2 = addChannel2(testContext);
    String colName1 = "testBuildAndUpload1";
    String colName2 = "testBuildAndUpload2";

    List<ColumnMetadata> schema =
        Arrays.asList(createTestIntegerColumn(colName1), createTestTextColumn(colName2));
    channel1.getRowBuffer().setupSchema(schema);
    channel2.getRowBuffer().setupSchema(schema);

    List<Map<String, Object>> rows1 =
        RowSetBuilder.newBuilder()
            .addColumn(colName1, 11)
            .addColumn(colName2, "bob")
            .newRow()
            .addColumn(colName1, 22)
            .addColumn(colName2, "bob")
            .build();
    List<Map<String, Object>> rows2 =
        RowSetBuilder.newBuilder().addColumn(colName1, null).addColumn(colName2, "toby").build();

    channel1.insertRows(rows1, "offset1");
    channel2.insertRows(rows2, "offset2");

    ChannelData<?> channel1Data = testContext.flushChannel(channel1.getName());
    ChannelData<?> channel2Data = testContext.flushChannel(channel2.getName());

    Map<String, RowBufferStats> eps1 = new HashMap<>();
    Map<String, RowBufferStats> eps2 = new HashMap<>();

    RowBufferStats stats1 = new RowBufferStats("COL1");
    RowBufferStats stats2 = new RowBufferStats("COL1");

    eps1.put("one", stats1);
    eps2.put("one", stats2);

    stats1.addIntValue(new BigInteger("10"));
    stats1.addIntValue(new BigInteger("15"));
    stats2.addIntValue(new BigInteger("11"));
    stats2.addIntValue(new BigInteger("13"));
    stats2.addIntValue(new BigInteger("17"));

    channel1Data.setColumnEps(eps1);
    channel2Data.setColumnEps(eps2);

    channel1Data.setRowSequencer(0L);
    channel1Data.setBufferSize(100);
    channel2Data.setRowSequencer(10L);
    channel2Data.setBufferSize(100);

    BlobMetadata blobMetadata = testContext.buildAndUpload();

    EpInfo expectedChunkEpInfo =
        AbstractRowBuffer.buildEpInfoFromStats(
            3, ChannelData.getCombinedColumnStatsMap(eps1, eps2));

    ChannelMetadata expectedChannel1Metadata =
        ChannelMetadata.builder()
            .setOwningChannelFromContext(channel1.getChannelContext())
            .setRowSequencer(1L)
            .setOffsetToken("offset1")
            .build();
    ChannelMetadata expectedChannel2Metadata =
        ChannelMetadata.builder()
            .setOwningChannelFromContext(channel2.getChannelContext())
            .setRowSequencer(10L)
            .setOffsetToken("offset2")
            .build();
    ChunkMetadata expectedChunkMetadata =
        ChunkMetadata.builder()
            .setOwningTableFromChannelContext(channel1.getChannelContext())
            .setChunkStartOffset(0L)
            .setChunkLength(248)
            .setChannelList(Arrays.asList(expectedChannel1Metadata, expectedChannel2Metadata))
            .setChunkMD5("md5")
            .setEncryptionKeyId(1234L)
            .setEpInfo(expectedChunkEpInfo)
            .setFirstInsertTimeInMs(1L)
            .setLastInsertTimeInMs(2L)
            .build();

    // Check FlushService.upload called with correct arguments
    final ArgumentCaptor<String> nameCaptor = ArgumentCaptor.forClass(String.class);
    final ArgumentCaptor<byte[]> blobCaptor = ArgumentCaptor.forClass(byte[].class);
    final ArgumentCaptor<List<ChunkMetadata>> metadataCaptor = ArgumentCaptor.forClass(List.class);

    Mockito.verify(testContext.flushService)
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

    testContext.close();
  }

  @Test
  public void testBuildErrors() throws Exception {
    TestContext<?> testContext = testContextFactory.create();
    SnowflakeStreamingIngestChannelInternal<?> channel1 = addChannel1(testContext);
    SnowflakeStreamingIngestChannelInternal<?> channel3 = addChannel3(testContext);
    String colName1 = "testBuildErrors1";
    String colName2 = "testBuildErrors2";

    List<ColumnMetadata> schema =
        Arrays.asList(createTestIntegerColumn(colName1), createTestTextColumn(colName2));
    channel1.getRowBuffer().setupSchema(schema);
    channel3.getRowBuffer().setupSchema(schema);

    List<Map<String, Object>> rows1 =
        RowSetBuilder.newBuilder().addColumn(colName1, 0).addColumn(colName2, "alice").build();
    List<Map<String, Object>> rows2 =
        RowSetBuilder.newBuilder().addColumn(colName1, 0).addColumn(colName2, 111).build();

    channel1.insertRows(rows1, "offset1");
    channel3.insertRows(rows2, "offset2");

    ChannelData<?> data1 = testContext.flushChannel(channel1.getName());
    ChannelData<?> data2 = testContext.flushChannel(channel3.getName());

    data1.setRowSequencer(0L);
    data1.setBufferSize(100);

    data2.setRowSequencer(10L);
    data2.setBufferSize(100);

    try {
      testContext.buildAndUpload();
      Assert.fail("Expected SFException");
    } catch (SFException err) {
      Assert.assertEquals(ErrorCode.INVALID_DATA_IN_CHUNK.getMessageCode(), err.getVendorCode());
    }
  }

  @Test
  public void testInvalidateChannels() {
    // Create a new Client in order to not interfere with other tests
    SnowflakeStreamingIngestClientInternal<StubChunkData> client =
        Mockito.mock(SnowflakeStreamingIngestClientInternal.class);
    ParameterProvider parameterProvider = new ParameterProvider();
    ChannelCache<StubChunkData> channelCache = new ChannelCache<>();
    Mockito.when(client.getChannelCache()).thenReturn(channelCache);
    Mockito.when(client.getParameterProvider()).thenReturn(parameterProvider);
    SnowflakeStreamingIngestChannelInternal<StubChunkData> channel1 =
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
            ZoneOffset.UTC);

    SnowflakeStreamingIngestChannelInternal<StubChunkData> channel2 =
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
            ZoneOffset.UTC);

    channelCache.addChannel(channel1);
    channelCache.addChannel(channel2);

    List<List<ChannelData<StubChunkData>>> blobData = new ArrayList<>();
    List<ChannelData<StubChunkData>> innerData = new ArrayList<>();
    blobData.add(innerData);

    ChannelData<StubChunkData> channel1Data = new ChannelData<>();
    ChannelData<StubChunkData> channel2Data = new ChannelData<>();
    channel1Data.setChannelContext(channel1.getChannelContext());
    channel2Data.setChannelContext(channel1.getChannelContext());

    channel1Data.setVectors(new StubChunkData());
    channel2Data.setVectors(new StubChunkData());
    innerData.add(channel1Data);
    innerData.add(channel2Data);

    StreamingIngestStage stage = Mockito.mock(StreamingIngestStage.class);
    Mockito.when(stage.getClientPrefix()).thenReturn("client_prefix");
    FlushService<StubChunkData> flushService =
        new FlushService<>(client, channelCache, stage, false);
    flushService.invalidateAllChannelsInBlob(blobData);

    Assert.assertFalse(channel1.isValid());
    Assert.assertTrue(channel2.isValid());
  }

  @Test
  public void testBlobBuilder() throws Exception {
    TestContext<?> testContext = testContextFactory.create();
    SnowflakeStreamingIngestChannelInternal<?> channel1 = addChannel1(testContext);

    ObjectMapper mapper = new ObjectMapper();
    List<ChunkMetadata> chunksMetadataList = new ArrayList<>();
    List<byte[]> chunksDataList = new ArrayList<>();
    long checksum = 100;
    byte[] data = "fake data".getBytes(StandardCharsets.UTF_8);
    int dataSize = data.length;

    chunksDataList.add(data);

    Map<String, RowBufferStats> eps1 = new HashMap<>();

    RowBufferStats stats1 = new RowBufferStats("COL1");

    eps1.put("one", stats1);

    stats1.addIntValue(new BigInteger("10"));
    stats1.addIntValue(new BigInteger("15"));
    EpInfo epInfo = AbstractRowBuffer.buildEpInfoFromStats(2, eps1);

    ChannelMetadata channelMetadata =
        ChannelMetadata.builder()
            .setOwningChannelFromContext(channel1.getChannelContext())
            .setRowSequencer(0L)
            .setOffsetToken("offset1")
            .build();
    ChunkMetadata chunkMetadata =
        ChunkMetadata.builder()
            .setOwningTableFromChannelContext(channel1.getChannelContext())
            .setChunkStartOffset(0L)
            .setChunkLength(dataSize)
            .setChannelList(Collections.singletonList(channelMetadata))
            .setChunkMD5("md5")
            .setEncryptionKeyId(1234L)
            .setEpInfo(epInfo)
            .setFirstInsertTimeInMs(1L)
            .setLastInsertTimeInMs(2L)
            .build();

    chunksMetadataList.add(chunkMetadata);

    final Constants.BdecVersion bdecVersion = ParameterProvider.BLOB_FORMAT_VERSION_DEFAULT;
    byte[] blob =
        BlobBuilder.buildBlob(chunksMetadataList, chunksDataList, checksum, dataSize, bdecVersion);

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
    TestContext<?> testContext = testContextFactory.create();
    FlushService<?> flushService = testContext.flushService;

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
