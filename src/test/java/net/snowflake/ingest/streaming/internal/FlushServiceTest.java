/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.utils.Constants.BLOB_CHECKSUM_SIZE_IN_BYTES;
import static net.snowflake.ingest.utils.Constants.BLOB_CHUNK_METADATA_LENGTH_SIZE_IN_BYTES;
import static net.snowflake.ingest.utils.Constants.BLOB_EXTENSION_TYPE;
import static net.snowflake.ingest.utils.Constants.BLOB_FILE_SIZE_SIZE_IN_BYTES;
import static net.snowflake.ingest.utils.Constants.BLOB_NO_HEADER;
import static net.snowflake.ingest.utils.Constants.BLOB_TAG_SIZE_IN_BYTES;
import static net.snowflake.ingest.utils.Constants.BLOB_VERSION_SIZE_IN_BYTES;
import static net.snowflake.ingest.utils.ParameterProvider.MAX_CHUNK_SIZE_IN_BYTES_DEFAULT;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.utils.Constants;
import net.snowflake.ingest.utils.Cryptor;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.ParameterProvider;
import net.snowflake.ingest.utils.SFException;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

@RunWith(Parameterized.class)
public class FlushServiceTest {

  @Parameterized.Parameters(name = "isIcebergMode: {0}")
  public static Object[] isIcebergMode() {
    return new Object[] {false, true};
  }

  @Parameterized.Parameter public static boolean isIcebergMode;

  public FlushServiceTest() {
    this.testContextFactory = ParquetTestContext.createFactory();
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
    IStorageManager storageManager;
    InternalStage storage;
    ExternalVolume extVolume;
    ParameterProvider parameterProvider;
    RegisterService registerService;

    final List<ChannelData<T>> channelData = new ArrayList<>();

    TestContext() {
      storage = Mockito.mock(InternalStage.class);
      extVolume = Mockito.mock(ExternalVolume.class);
      parameterProvider = new ParameterProvider(isIcebergMode);
      InternalParameterProvider internalParameterProvider =
          new InternalParameterProvider(isIcebergMode);
      client = Mockito.mock(SnowflakeStreamingIngestClientInternal.class);
      Mockito.when(client.getParameterProvider()).thenReturn(parameterProvider);
      Mockito.when(client.getInternalParameterProvider()).thenReturn(internalParameterProvider);
      storageManager =
          Mockito.spy(
              isIcebergMode
                  ? new ExternalVolumeManager(
                      true, "role", "client", MockSnowflakeServiceClient.create())
                  : new InternalStageManager(true, "role", "client", null));
      Mockito.doReturn(isIcebergMode ? extVolume : storage)
          .when(storageManager)
          .getStorage(ArgumentMatchers.any());
      Mockito.when(storageManager.getClientPrefix()).thenReturn("client_prefix");
      Mockito.when(client.getParameterProvider())
          .thenAnswer((Answer<ParameterProvider>) (i) -> parameterProvider);
      channelCache = new ChannelCache<>();
      Mockito.when(client.getChannelCache()).thenReturn(channelCache);
      registerService = Mockito.spy(new RegisterService(client, client.isTestMode()));
      flushService = Mockito.spy(new FlushService<>(client, channelCache, storageManager, true));
    }

    void setParameterOverride(Map<String, Object> parameterOverride) {
      this.parameterProvider = new ParameterProvider(parameterOverride, null, isIcebergMode);
    }

    ChannelData<T> flushChannel(String name) {
      SnowflakeStreamingIngestChannelInternal<T> channel = channels.get(name);
      ChannelData<T> channelData = channel.getRowBuffer().flush();
      channelData.setChannelContext(channel.getChannelContext());
      this.channelData.add(channelData);
      return channelData;
    }

    BlobMetadata buildAndUpload() throws Exception {
      List<List<ChannelData<T>>> blobData = Collections.singletonList(channelData);
      return flushService.buildAndUpload(
          BlobPath.fileNameWithoutToken("file_name"),
          blobData,
          blobData.get(0).get(0).getChannelContext().getFullyQualifiedTableName());
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
          null,
          isIcebergMode
              ? ParquetProperties.WriterVersion.PARQUET_2_0
              : ParquetProperties.WriterVersion.PARQUET_1_0);
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

  TestContextFactory<List<List<Object>>> testContextFactory;

  private SnowflakeStreamingIngestChannelInternal<List<List<Object>>> addChannel(
      TestContext<List<List<Object>>> testContext, int tableId, long encryptionKeyId) {
    return testContext
        .channelBuilder("channel" + UUID.randomUUID())
        .setDBName("db1")
        .setSchemaName("PUBLIC")
        .setTableName("table" + tableId)
        .setOffsetToken("offset1")
        .setChannelSequencer(0L)
        .setRowSequencer(0L)
        .setEncryptionKey("key")
        .setEncryptionKeyId(encryptionKeyId)
        .buildAndAdd();
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
    colInt.setOrdinal(1);
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

  private static ColumnMetadata createLargeTestTextColumn(String name) {
    ColumnMetadata colChar = new ColumnMetadata();
    colChar.setOrdinal(1);
    colChar.setName(name);
    colChar.setPhysicalType("LOB");
    colChar.setNullable(true);
    colChar.setLogicalType("TEXT");
    colChar.setByteLength(14000000);
    colChar.setLength(11000000);
    colChar.setScale(0);
    return colChar;
  }

  @Test
  public void testGetFilePath() {
    // SNOW-1490151 Iceberg testing gaps
    if (isIcebergMode) {
      // TODO: SNOW-1502887 Blob path generation for iceberg table
      return;
    }
    TestContext<?> testContext = testContextFactory.create();
    IStorageManager storageManager = testContext.storageManager;
    Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    String clientPrefix = "honk";
    String outputString =
        ((InternalStageManager<?>) storageManager).getNextFileName(calendar, clientPrefix);
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
  public void testInterleaveFlush() throws Exception {
    if (isIcebergMode) {
      // Interleaved blob is not supported in iceberg mode
      return;
    }
    int numChannels = 4;
    Long maxLastFlushTime = Long.MAX_VALUE - 1000L; // -1000L to avoid jitter overflow
    TestContext<List<List<Object>>> testContext = testContextFactory.create();
    testContext.setParameterOverride(
        Collections.singletonMap(
            ParameterProvider.MAX_CHUNKS_IN_BLOB, ParameterProvider.MAX_CHUNKS_IN_BLOB_DEFAULT));
    addChannel1(testContext);
    FlushService<?> flushService = testContext.flushService;
    ChannelCache<?> channelCache = testContext.channelCache;
    Mockito.when(flushService.isTestMode()).thenReturn(false);

    // Nothing to flush
    flushService.flush(false).get();
    Mockito.verify(flushService, Mockito.times(0)).distributeFlushTasks(Mockito.any());

    // Force = true flushes
    flushService.flush(true).get();
    Mockito.verify(flushService, Mockito.times(1)).distributeFlushTasks(Mockito.any());

    IntStream.range(0, numChannels)
        .forEach(
            i -> {
              addChannel(testContext, i, 1L);
              channelCache.setLastFlushTime(getFullyQualifiedTableName(i), maxLastFlushTime);
            });

    // isNeedFlush = true flushes
    flushService.setNeedFlush(getFullyQualifiedTableName(0));
    flushService.flush(false).get();
    Mockito.verify(flushService, Mockito.times(2)).distributeFlushTasks(Mockito.any());
    Assert.assertFalse(flushService.isNeedFlush);
    Assert.assertNotEquals(
        maxLastFlushTime, channelCache.getLastFlushTime(getFullyQualifiedTableName(0)));
    IntStream.range(0, numChannels)
        .forEach(
            i -> {
              Assert.assertFalse(channelCache.getNeedFlush(getFullyQualifiedTableName(i)));
              assertTimeDiffwithinThreshold(
                  channelCache.getLastFlushTime(getFullyQualifiedTableName(0)),
                  channelCache.getLastFlushTime(getFullyQualifiedTableName(i)),
                  1000L);
            });

    // lastFlushTime causes flush
    flushService.lastFlushTime = 0L;
    flushService.flush(false).get();
    Mockito.verify(flushService, Mockito.times(3)).distributeFlushTasks(Mockito.any());
    Assert.assertTrue(flushService.lastFlushTime > 0);
    Assert.assertNotEquals(
        maxLastFlushTime, channelCache.getLastFlushTime(getFullyQualifiedTableName(0)));
    IntStream.range(0, numChannels)
        .forEach(
            i -> {
              Assert.assertFalse(channelCache.getNeedFlush(getFullyQualifiedTableName(i)));
              assertTimeDiffwithinThreshold(
                  channelCache.getLastFlushTime(getFullyQualifiedTableName(0)),
                  channelCache.getLastFlushTime(getFullyQualifiedTableName(i)),
                  1000L);
            });
  }

  @Test
  public void testNonInterleaveFlush() throws ExecutionException, InterruptedException {
    int numChannels = 4;
    Long maxLastFlushTime = Long.MAX_VALUE - 1000L; // -1000L to avoid jitter overflow
    TestContext<List<List<Object>>> testContext = testContextFactory.create();
    FlushService<?> flushService = testContext.flushService;
    ChannelCache<?> channelCache = testContext.channelCache;
    Mockito.when(flushService.isTestMode()).thenReturn(false);
    testContext.setParameterOverride(
        Collections.singletonMap(ParameterProvider.MAX_CHUNKS_IN_BLOB, 1));

    // Test need flush
    IntStream.range(0, numChannels)
        .forEach(
            i -> {
              addChannel(testContext, i, 1L);
              channelCache.setLastFlushTime(getFullyQualifiedTableName(i), maxLastFlushTime);
              if (i % 2 == 0) {
                flushService.setNeedFlush(getFullyQualifiedTableName(i));
              }
            });
    flushService.flush(false).get();
    Mockito.verify(flushService, Mockito.times(1)).distributeFlushTasks(Mockito.any());
    IntStream.range(0, numChannels)
        .forEach(
            i -> {
              Assert.assertFalse(channelCache.getNeedFlush(getFullyQualifiedTableName(i)));
              if (i % 2 == 0) {
                Assert.assertNotEquals(
                    maxLastFlushTime, channelCache.getLastFlushTime(getFullyQualifiedTableName(i)));
              } else {
                assertTimeDiffwithinThreshold(
                    maxLastFlushTime,
                    channelCache.getLastFlushTime(getFullyQualifiedTableName(i)),
                    1000L);
              }
            });

    // Test time based flush
    IntStream.range(0, numChannels)
        .forEach(
            i -> {
              channelCache.setLastFlushTime(
                  getFullyQualifiedTableName(i), i % 2 == 0 ? 0L : maxLastFlushTime);
            });
    flushService.flush(false).get();
    Mockito.verify(flushService, Mockito.times(2)).distributeFlushTasks(Mockito.any());
    IntStream.range(0, numChannels)
        .forEach(
            i -> {
              Assert.assertFalse(channelCache.getNeedFlush(getFullyQualifiedTableName(i)));
              if (i % 2 == 0) {
                Assert.assertNotEquals(
                    0L, channelCache.getLastFlushTime(getFullyQualifiedTableName(i)).longValue());
              } else {
                assertTimeDiffwithinThreshold(
                    maxLastFlushTime,
                    channelCache.getLastFlushTime(getFullyQualifiedTableName(i)),
                    1000L);
              }
            });
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
    // SNOW-1490151 Iceberg testing gaps
    if (!isIcebergMode) {
      flushService.flush(true).get();
      Mockito.verify(flushService, Mockito.atLeast(2))
          .buildAndUpload(Mockito.any(), Mockito.any(), Mockito.any());
    }
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

    // SNOW-1490151 Iceberg testing gaps
    if (!isIcebergMode) {
      // Force = true flushes
      flushService.flush(true).get();
      Mockito.verify(flushService, Mockito.atLeast(2))
          .buildAndUpload(Mockito.any(), Mockito.any(), Mockito.any());
    }
  }

  @Test
  public void testBlobSplitDueToChunkSizeLimit() throws Exception {
    TestContext<?> testContext = testContextFactory.create();
    SnowflakeStreamingIngestChannelInternal<?> channel1 = addChannel1(testContext);
    SnowflakeStreamingIngestChannelInternal<?> channel2 = addChannel2(testContext);
    String colName1 = "testBlobSplitDueToChunkSizeLimit1";
    String colName2 = "testBlobSplitDueToChunkSizeLimit2";
    int rowSize = 10000000;
    String largeData = new String(new char[rowSize]);

    List<ColumnMetadata> schema =
        Arrays.asList(createTestIntegerColumn(colName1), createLargeTestTextColumn(colName2));
    channel1.getRowBuffer().setupSchema(schema);
    channel2.getRowBuffer().setupSchema(schema);

    RowSetBuilder builder = RowSetBuilder.newBuilder();
    RowSetBuilder.newBuilder().addColumn(colName1, 11).addColumn(colName2, largeData);

    for (int idx = 0; idx <= MAX_CHUNK_SIZE_IN_BYTES_DEFAULT / (2 * rowSize); idx++) {
      builder.addColumn(colName1, 11).addColumn(colName2, largeData).newRow();
    }

    List<Map<String, Object>> rows = builder.build();

    channel1.insertRows(rows, "offset1");
    channel2.insertRows(rows, "offset2");

    FlushService<?> flushService = testContext.flushService;

    // SNOW-1490151 Iceberg testing gaps
    if (!isIcebergMode) {
      // Force = true flushes
      flushService.flush(true).get();
      Mockito.verify(flushService, Mockito.times(2))
          .buildAndUpload(Mockito.any(), Mockito.any(), Mockito.any());
    }
  }

  @Test
  public void testBlobSplitDueToNumberOfChunks() throws Exception {
    // SNOW-1490151 Iceberg testing gaps
    if (isIcebergMode) {
      return;
    }

    for (int rowCount : Arrays.asList(0, 1, 30, 111, 159, 287, 1287, 1599, 4496)) {
      runTestBlobSplitDueToNumberOfChunks(rowCount);
    }
  }

  /**
   * Insert rows in batches of 3 into each table and assert that the expected number of blobs is
   * generated.
   *
   * @param numberOfRows How many rows to insert
   */
  public void runTestBlobSplitDueToNumberOfChunks(int numberOfRows) throws Exception {
    int channelsPerTable = 3;
    int expectedBlobs =
        (int)
            Math.ceil(
                (double) numberOfRows
                    / channelsPerTable
                    / (isIcebergMode
                        ? ParameterProvider.MAX_CHUNKS_IN_BLOB_ICEBERG_MODE_DEFAULT
                        : ParameterProvider.MAX_CHUNKS_IN_BLOB_DEFAULT));

    final TestContext<List<List<Object>>> testContext = testContextFactory.create();

    for (int i = 0; i < numberOfRows; i++) {
      SnowflakeStreamingIngestChannelInternal<List<List<Object>>> channel =
          addChannel(testContext, i / channelsPerTable, 1);
      channel.setupSchema(Collections.singletonList(createLargeTestTextColumn("C1")));
      channel.insertRow(Collections.singletonMap("C1", i), "");
    }

    FlushService<List<List<Object>>> flushService = testContext.flushService;
    flushService.flush(true).get();

    ArgumentCaptor<List<List<ChannelData<List<List<Object>>>>>> blobDataCaptor =
        ArgumentCaptor.forClass(List.class);
    Mockito.verify(flushService, Mockito.times(expectedBlobs))
        .buildAndUpload(Mockito.any(), blobDataCaptor.capture(), Mockito.any());

    // 1. list => blobs; 2. list => chunks; 3. list => channels; 4. list => rows, 5. list => columns
    List<List<List<ChannelData<List<List<Object>>>>>> allUploadedBlobs =
        blobDataCaptor.getAllValues();

    Assert.assertEquals(numberOfRows, getRows(allUploadedBlobs).size());
  }

  @Test
  public void testBlobSplitDueToNumberOfChunksWithLeftoverChannels() throws Exception {
    final TestContext<List<List<Object>>> testContext = testContextFactory.create();

    for (int i = 0; i < 99; i++) { // 19 simple chunks
      SnowflakeStreamingIngestChannelInternal<List<List<Object>>> channel =
          addChannel(testContext, i, 1);
      channel.setupSchema(Collections.singletonList(createLargeTestTextColumn("C1")));
      channel.insertRow(Collections.singletonMap("C1", i), "");
    }

    // 20th chunk would contain multiple channels, but there are some with different encryption key
    // ID, so they spill to a new blob
    SnowflakeStreamingIngestChannelInternal<List<List<Object>>> channel1 =
        addChannel(testContext, 99, 1);
    channel1.setupSchema(Collections.singletonList(createLargeTestTextColumn("C1")));
    channel1.insertRow(Collections.singletonMap("C1", 0), "");

    SnowflakeStreamingIngestChannelInternal<List<List<Object>>> channel2 =
        addChannel(testContext, 99, 2);
    channel2.setupSchema(Collections.singletonList(createLargeTestTextColumn("C1")));
    channel2.insertRow(Collections.singletonMap("C1", 0), "");

    SnowflakeStreamingIngestChannelInternal<List<List<Object>>> channel3 =
        addChannel(testContext, 99, 2);
    channel3.setupSchema(Collections.singletonList(createLargeTestTextColumn("C1")));
    channel3.insertRow(Collections.singletonMap("C1", 0), "");

    // SNOW-1490151 Iceberg testing gaps
    if (isIcebergMode) {
      return;
    }

    FlushService<List<List<Object>>> flushService = testContext.flushService;
    flushService.flush(true).get();

    ArgumentCaptor<List<List<ChannelData<List<List<Object>>>>>> blobDataCaptor =
        ArgumentCaptor.forClass(List.class);
    Mockito.verify(flushService, Mockito.atLeast(2))
        .buildAndUpload(Mockito.any(), blobDataCaptor.capture(), Mockito.any());

    // 1. list => blobs; 2. list => chunks; 3. list => channels; 4. list => rows, 5. list => columns
    List<List<List<ChannelData<List<List<Object>>>>>> allUploadedBlobs =
        blobDataCaptor.getAllValues();

    Assert.assertEquals(102, getRows(allUploadedBlobs).size());
  }

  private List<List<Object>> getRows(List<List<List<ChannelData<List<List<Object>>>>>> blobs) {
    List<List<Object>> result = new ArrayList<>();
    blobs.forEach(
        chunks ->
            chunks.forEach(
                channels ->
                    channels.forEach(
                        chunkData ->
                            result.addAll(((ParquetChunkData) chunkData.getVectors()).rows))));
    return result;
  }

  @Test
  public void testBuildAndUpload() throws Exception {
    long expectedBuildLatencyMs = 100;
    long expectedUploadLatencyMs = 200;

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

    RowBufferStats stats1 =
        new RowBufferStats(
            "COL1", Types.optional(PrimitiveType.PrimitiveTypeName.INT32).id(1).named("COL1"));
    RowBufferStats stats2 =
        new RowBufferStats(
            "COL1", Types.optional(PrimitiveType.PrimitiveTypeName.INT32).id(1).named("COL1"));

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

    // set client timers

    SnowflakeStreamingIngestClientInternal client = testContext.client;
    client.buildLatency = this.setupTimer(expectedBuildLatencyMs);
    client.uploadLatency = this.setupTimer(expectedUploadLatencyMs);
    client.uploadThroughput = Mockito.mock(Meter.class);
    client.blobSizeHistogram = Mockito.mock(Histogram.class);
    client.blobRowCountHistogram = Mockito.mock(Histogram.class);

    BlobMetadata blobMetadata = testContext.buildAndUpload();

    EpInfo expectedChunkEpInfo =
        AbstractRowBuffer.buildEpInfoFromStats(
            3, ChannelData.getCombinedColumnStatsMap(eps1, eps2), !isIcebergMode);

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
    final ArgumentCaptor<InternalStage> storageCaptor =
        ArgumentCaptor.forClass(InternalStage.class);
    final ArgumentCaptor<BlobPath> nameCaptor = ArgumentCaptor.forClass(BlobPath.class);
    final ArgumentCaptor<byte[]> blobCaptor = ArgumentCaptor.forClass(byte[].class);
    final ArgumentCaptor<List<ChunkMetadata>> metadataCaptor = ArgumentCaptor.forClass(List.class);

    Mockito.verify(testContext.flushService)
        .upload(
            storageCaptor.capture(),
            nameCaptor.capture(),
            blobCaptor.capture(),
            metadataCaptor.capture(),
            ArgumentMatchers.any());
    Assert.assertEquals("file_name", nameCaptor.getValue().fileName);

    ChunkMetadata metadataResult = metadataCaptor.getValue().get(0);
    List<ChannelMetadata> channelMetadataResult = metadataResult.getChannels();

    Assert.assertEquals(BlobBuilder.computeMD5(blobCaptor.getValue()), blobMetadata.getMD5());
    Assert.assertEquals(expectedBuildLatencyMs, blobMetadata.getBlobStats().getBuildDurationMs());
    Assert.assertEquals(expectedUploadLatencyMs, blobMetadata.getBlobStats().getUploadDurationMs());

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
    Assert.assertEquals("offset1", channelMetadataResult.get(0).getEndOffsetToken());
    Assert.assertEquals(0L, (long) channelMetadataResult.get(0).getRowSequencer());
    Assert.assertEquals(0L, (long) channelMetadataResult.get(0).getClientSequencer());

    Assert.assertEquals("channel2", channelMetadataResult.get(1).getChannelName());
    Assert.assertEquals("offset2", channelMetadataResult.get(1).getEndOffsetToken());
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
    ParameterProvider parameterProvider = new ParameterProvider(isIcebergMode);
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
            ZoneOffset.UTC,
            null /* offsetTokenVerificationFunction */,
            isIcebergMode
                ? ParquetProperties.WriterVersion.PARQUET_2_0
                : ParquetProperties.WriterVersion.PARQUET_1_0);

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
            ZoneOffset.UTC,
            null /* offsetTokenVerificationFunction */,
            isIcebergMode
                ? ParquetProperties.WriterVersion.PARQUET_2_0
                : ParquetProperties.WriterVersion.PARQUET_1_0);

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

    IStorageManager storageManager =
        Mockito.spy(new InternalStageManager<>(true, "role", "client", null));
    FlushService<StubChunkData> flushService =
        new FlushService<>(client, channelCache, storageManager, false);
    flushService.invalidateAllChannelsInBlob(blobData, "Invalidated by test");

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

    RowBufferStats stats1 =
        new RowBufferStats(
            "COL1", Types.optional(PrimitiveType.PrimitiveTypeName.INT32).id(1).named("COL1"));

    eps1.put("one", stats1);

    stats1.addIntValue(new BigInteger("10"));
    stats1.addIntValue(new BigInteger("15"));
    EpInfo epInfo = AbstractRowBuffer.buildEpInfoFromStats(2, eps1, !isIcebergMode);

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
            .setUncompressedChunkLength(dataSize * 2)
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
          bdecVersion.toByte(),
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
      Assert.assertEquals(chunkMetadata.getChunkLength(), map.get("chunk_length"));
      Assert.assertEquals(
          chunkMetadata.getUncompressedChunkLength(), map.get("chunk_length_uncompressed"));
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

  private Timer setupTimer(long expectedLatencyMs) {
    Timer.Context timerContext = Mockito.mock(Timer.Context.class);
    Mockito.when(timerContext.stop()).thenReturn(TimeUnit.MILLISECONDS.toNanos(expectedLatencyMs));
    Timer timer = Mockito.mock(Timer.class);
    Mockito.when(timer.time()).thenReturn(timerContext);

    return timer;
  }

  private String getFullyQualifiedTableName(int tableId) {
    return String.format("db1.PUBLIC.table%d", tableId);
  }

  private void assertTimeDiffwithinThreshold(Long time1, Long time2, long threshold) {
    Assert.assertTrue(Math.abs(time1 - time2) <= threshold);
  }
}
