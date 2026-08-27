package net.snowflake;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.OffsetTokenVerificationFunction;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.utils.Constants;
import net.snowflake.ingest.utils.ParameterProvider;
import net.snowflake.ingest.utils.SFException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class StandardIngestE2ETest {

  private static final boolean[] ICEBERG_MODES = {false, true};

  private IngestTestUtils ingestTestUtils;

  @After
  public void tearDown() throws Exception {
    if (ingestTestUtils != null) {
      ingestTestUtils.close();
    }
  }

  @Test
  public void basicTest() throws Exception {
    ingestTestUtils = new IngestTestUtils("standard_ingest");
    ingestTestUtils.runBasicTest();
  }

  @Test
  @Ignore("Takes too long to run")
  public void longRunningTest() throws Exception {
    ingestTestUtils = new IngestTestUtils("standard_ingest");
    ingestTestUtils.runLongRunningTest(Duration.of(80, ChronoUnit.MINUTES));
  }

  @Test
  public void testParquetReadBackVerificationCatchesRowCountMismatch() throws Exception {
    for (boolean iceberg : ICEBERG_MODES) {
      Object buffer = createTestBuffer(iceberg);
      setupTextColumn(buffer, 14, 11);
      loadData(buffer, Collections.singletonMap("C1", "hello"));

      Object data = flushWithContext(buffer);
      Object flusher = createFlusher(buffer);
      Object result = serialize(flusher, data, "rowcount_test.bdec");

      try {
        verifyReadBack(flusher, chunkData(result), rowCount(data) + 1);
        Assert.fail("Expected SFException for row count mismatch");
      } catch (SFException e) {
        Assert.assertTrue(e.getMessage().contains("Row count mismatch"));
      }
    }
  }

  @Test
  public void testParquetReadBackVerificationCatchesCorruption() throws Exception {
    for (boolean iceberg : ICEBERG_MODES) {
      Object buffer = createTestBuffer(iceberg);
      setupTextColumn(buffer, 14, 11);
      loadData(buffer, Collections.singletonMap("C1", "hello"));

      Object data = flushWithContext(buffer);
      Object flusher = createFlusher(buffer);
      Object result = serialize(flusher, data, "corruption_test.bdec");

      // Midpoint on this 1-row blob is in the footer. Two-byte XOR is version-dependent
      // (4.4.4 vs 4.4.4-SNAPSHOT slides the hit onto a neighboring thrift field); 16 bytes
      // makes footer parse fail either way.
      byte[] bytes = chunkBytes(result);
      int midpoint = bytes.length / 2;
      int corruptUntil = Math.min(midpoint + 16, bytes.length);
      for (int i = midpoint; i < corruptUntil; i++) {
        bytes[i] ^= 0xFF;
      }
      ByteArrayOutputStream corrupted = new ByteArrayOutputStream();
      corrupted.write(bytes);

      try {
        verifyReadBack(flusher, corrupted, rowCount(data));
        Assert.fail("Expected SFException for corrupt parquet data");
      } catch (SFException e) {
        Assert.assertNotNull(e.getMessage());
      }
    }
  }

  @Test
  public void testParquetReadBackVerificationCatchesPageBodyCorruption() throws Exception {
    for (boolean iceberg : ICEBERG_MODES) {
      Object buffer = createTestBuffer(iceberg);
      setupTextColumn(buffer, 256, 256);
      String sentence = "The quick brown fox jumps over the lazy dog. ";
      List<Map<String, Object>> rows = new ArrayList<>();
      for (int i = 0; i < 32; i++) {
        rows.add(
            Collections.singletonMap(
                "C1",
                sentence
                    + java.util.UUID.nameUUIDFromBytes(
                        ("bdec-corruption-row-" + i).getBytes(StandardCharsets.UTF_8))));
      }
      InsertValidationResponse response =
          (InsertValidationResponse)
              invokeAccessible(
                  buffer,
                  "insertRows",
                  new Class<?>[] {Iterable.class, String.class, String.class},
                  rows,
                  "1",
                  "32");
      Assert.assertFalse(response.hasErrors());

      Object data = flushWithContext(buffer);
      Object flusher = createFlusher(buffer);
      Object result = serialize(flusher, data, "corruption_page_body_test.bdec");

      byte[] bytes = chunkBytes(result);
      int midpoint = bytes.length / 2;
      long pageOffset = firstDataPageOffset(bytes);
      Object pageHeader = readPageHeader(bytes, pageOffset);
      int headerSize = pageHeaderSize(bytes, pageOffset);
      int bodyStart = (int) pageOffset + headerSize;
      int compressedPageSize = (Integer) field(pageHeader, "compressed_page_size");
      int bodyEnd = bodyStart + compressedPageSize;
      Assert.assertTrue(
          "midpoint must land in the GZIP page, not the footer",
          midpoint >= bodyStart && midpoint + 1 < bodyEnd);
      int corruptUntil = Math.min(midpoint + 16, bodyEnd);
      for (int i = midpoint; i < corruptUntil; i++) {
        bytes[i] ^= 0xFF;
      }
      ByteArrayOutputStream corrupted = new ByteArrayOutputStream();
      corrupted.write(bytes);

      try {
        verifyReadBack(flusher, corrupted, rowCount(data));
        Assert.fail("Expected SFException for corrupt parquet page body");
      } catch (SFException e) {
        Assert.assertNotNull(e.getMessage());
      }
    }
  }

  @Test
  public void testParquetReadBackVerificationRetrySucceeds() throws Exception {
    for (boolean iceberg : ICEBERG_MODES) {
      Object buffer = createTestBuffer(iceberg);
      setupTextColumn(buffer, 14, 11);
      loadData(buffer, Collections.singletonMap("C1", "hello"));

      Object data = flushWithContext(buffer);
      Object flusher = newParquetFlusherWithVerification(buffer, iceberg);
      Object result = serialize(flusher, data, "retry_test.bdec");

      Assert.assertNotNull(result);
      Assert.assertEquals(1L, serializationRowCount(result));
    }
  }

  @Test
  public void testParquetReadBackVerificationRetrySucceedsMultipleRows() throws Exception {
    for (boolean iceberg : ICEBERG_MODES) {
      Object buffer = createTestBuffer(iceberg);
      setupTextColumn(buffer, 14, 11);
      loadData(buffer, Collections.singletonMap("C1", "row_one"));
      loadData(buffer, Collections.singletonMap("C1", "row_two"));
      loadData(buffer, Collections.singletonMap("C1", "row_three"));

      Object data = flushWithContext(buffer);
      Object flusher = newParquetFlusherWithVerification(buffer, iceberg);
      Object result = serialize(flusher, data, "retry_multirow_test.bdec");

      Assert.assertNotNull(result);
      Assert.assertEquals(3L, serializationRowCount(result));
    }
  }

  @Test
  public void testParquetPageHeaderNumValuesCorruptionCaughtByRowCountCheck() throws Exception {
    for (boolean iceberg : ICEBERG_MODES) {
      Object buffer = createTestBuffer(iceberg);
      setupTextColumn(buffer, 14, 11);
      loadData(buffer, Collections.singletonMap("C1", "hello"));

      Object data = flushWithContext(buffer);
      Object flusher = createFlusher(buffer);
      Object result = serialize(flusher, data, "num_values_corruption_test.bdec");

      byte[] bytes = chunkBytes(result);
      long pageOffset = firstDataPageOffset(bytes);
      Object pageHeader = readPageHeader(bytes, pageOffset);
      int originalHeaderSize = pageHeaderSize(bytes, pageOffset);

      Object v1 = field(pageHeader, "data_page_header");
      if (v1 != null) {
        setField(v1, "num_values", 0);
      } else {
        setField(field(pageHeader, "data_page_header_v2"), "num_values", 0);
      }

      ByteArrayOutputStream modifiedHeader = new ByteArrayOutputStream();
      writePageHeader(pageHeader, modifiedHeader);
      ByteArrayOutputStream patched = new ByteArrayOutputStream();
      patched.write(bytes, 0, (int) pageOffset);
      patched.write(modifiedHeader.toByteArray());
      patched.write(
          bytes,
          (int) pageOffset + originalHeaderSize,
          bytes.length - (int) pageOffset - originalHeaderSize);

      try {
        bdecVerify(patched.toByteArray(), rowCount(data));
        Assert.fail("Expected IOException for num_values corruption");
      } catch (Exception e) {
        Assert.assertNotNull(e.getMessage());
      }
    }
  }

  private static Object createTestBuffer(boolean iceberg) throws Exception {
    Class<?> writerVersionClz = parquetClass("column.ParquetProperties$WriterVersion");
    @SuppressWarnings({"unchecked", "rawtypes"})
    Object writerVersion =
        Enum.valueOf((Class<Enum>) writerVersionClz, iceberg ? "PARQUET_2_0" : "PARQUET_1_0");

    Class<?> runtimeStateClz =
        Class.forName("net.snowflake.ingest.streaming.internal.ChannelRuntimeState");
    Constructor<?> rsCtor =
        runtimeStateClz.getDeclaredConstructor(String.class, long.class, boolean.class);
    rsCtor.setAccessible(true);
    Object runtimeState = rsCtor.newInstance("0", 0L, true);

    Class<?> cbpClz =
        Class.forName("net.snowflake.ingest.streaming.internal.ClientBufferParameters");
    Object cbp =
        cbpClz
            .getMethod(
                "test_createClientBufferParameters",
                long.class,
                long.class,
                Constants.BdecParquetCompression.class,
                boolean.class,
                Optional.class,
                boolean.class,
                boolean.class,
                boolean.class)
            .invoke(
                null,
                ParameterProvider.MAX_CHUNK_SIZE_IN_BYTES_DEFAULT,
                ParameterProvider.MAX_ALLOWED_ROW_SIZE_IN_BYTES_DEFAULT,
                Constants.BdecParquetCompression.GZIP,
                ParameterProvider.ENABLE_NEW_JSON_PARSING_LOGIC_DEFAULT,
                iceberg ? Optional.of(1) : Optional.empty(),
                iceberg,
                false,
                iceberg);

    Class<?> arb = Class.forName("net.snowflake.ingest.streaming.internal.AbstractRowBuffer");
    Method create =
        arb.getDeclaredMethod(
            "createRowBuffer",
            OpenChannelRequest.OnErrorOption.class,
            ZoneId.class,
            Constants.BdecVersion.class,
            String.class,
            Consumer.class,
            runtimeStateClz,
            cbpClz,
            OffsetTokenVerificationFunction.class,
            writerVersionClz,
            Class.forName("net.snowflake.ingest.connection.TelemetryService"));
    create.setAccessible(true);
    return create.invoke(
        null,
        OpenChannelRequest.OnErrorOption.CONTINUE,
        ZoneOffset.UTC,
        Constants.BdecVersion.THREE,
        "test.buffer",
        (Consumer<Float>) rs -> {},
        runtimeState,
        cbp,
        null,
        writerVersion,
        null);
  }

  private static void setupTextColumn(Object buffer, int byteLength, int length) throws Exception {
    Object col = newInternal("net.snowflake.ingest.streaming.internal.ColumnMetadata");
    invoke(col, "setOrdinal", new Class<?>[] {Integer.class}, 1);
    invoke(col, "setName", new Class<?>[] {String.class}, "C1");
    invoke(col, "setPhysicalType", new Class<?>[] {String.class}, "LOB");
    invoke(col, "setNullable", new Class<?>[] {boolean.class}, true);
    invoke(col, "setLogicalType", new Class<?>[] {String.class}, "TEXT");
    invoke(col, "setByteLength", new Class<?>[] {Integer.class}, byteLength);
    invoke(col, "setLength", new Class<?>[] {Integer.class}, length);
    invoke(col, "setScale", new Class<?>[] {Integer.class}, 0);
    Method setup = buffer.getClass().getMethod("setupSchema", List.class);
    setup.setAccessible(true);
    setup.invoke(buffer, Collections.singletonList(col));
  }

  private static void loadData(Object buffer, Map<String, Object> row) throws Exception {
    InsertValidationResponse response =
        (InsertValidationResponse)
            invokeAccessible(
                buffer,
                "insertRows",
                new Class<?>[] {Iterable.class, String.class, String.class},
                Collections.singletonList(row),
                "1",
                "1");
    Assert.assertFalse(response.hasErrors());
  }

  private static Object flushWithContext(Object buffer) throws Exception {
    Object data =
        invokeAccessible(buffer, "flush", new Class<?>[] {});
    Object ctx =
        ctor(
                "net.snowflake.ingest.streaming.internal.ChannelFlushContext",
                new Class<?>[] {
                  String.class,
                  String.class,
                  String.class,
                  String.class,
                  Long.class,
                  String.class,
                  Long.class
                })
            .newInstance("name", "db", "schema", "table", 1L, "key", 0L);
    Method setCtx =
        data.getClass()
            .getMethod(
                "setChannelContext",
                Class.forName("net.snowflake.ingest.streaming.internal.ChannelFlushContext"));
    setCtx.setAccessible(true);
    setCtx.invoke(data, ctx);
    return data;
  }

  private static Object createFlusher(Object buffer) throws Exception {
    return invokeAccessible(buffer, "createFlusher", new Class<?>[] {});
  }

  private static Object newParquetFlusherWithVerification(Object buffer, boolean iceberg)
      throws Exception {
    Field schemaField = buffer.getClass().getDeclaredField("schema");
    schemaField.setAccessible(true);
    Object schema = schemaField.get(buffer);
    Class<?> writerVersionClz = parquetClass("column.ParquetProperties$WriterVersion");
    @SuppressWarnings({"unchecked", "rawtypes"})
    Object writerVersion =
        Enum.valueOf((Class<Enum>) writerVersionClz, iceberg ? "PARQUET_2_0" : "PARQUET_1_0");
    Class<?> flusherClz = Class.forName("net.snowflake.ingest.streaming.internal.ParquetFlusher");
    Constructor<?> ctor =
        flusherClz.getConstructor(
            parquetClass("schema.MessageType"),
            long.class,
            Optional.class,
            Constants.BdecParquetCompression.class,
            writerVersionClz,
            boolean.class,
            boolean.class,
            boolean.class);
    return ctor.newInstance(
        schema,
        ParameterProvider.MAX_CHUNK_SIZE_IN_BYTES_DEFAULT,
        iceberg ? Optional.of(1) : Optional.empty(),
        Constants.BdecParquetCompression.GZIP,
        writerVersion,
        iceberg,
        iceberg,
        true);
  }

  private static Object serialize(Object flusher, Object data, String filePath) throws Exception {
    Class<?> overrides =
        Class.forName("net.snowflake.ingest.streaming.internal.FileMetadataTestingOverrides");
    Object none = overrides.getMethod("none").invoke(null);
    try {
      return flusher
          .getClass()
          .getMethod("serialize", List.class, String.class, long.class, overrides)
          .invoke(flusher, Collections.singletonList(data), filePath, 0L, none);
    } catch (InvocationTargetException e) {
      throw unwrap(e);
    }
  }

  private static void verifyReadBack(Object flusher, ByteArrayOutputStream data, long expected)
      throws Exception {
    Method verify =
        Class.forName("net.snowflake.ingest.streaming.internal.ParquetFlusher")
            .getDeclaredMethod("verifyReadBack", ByteArrayOutputStream.class, long.class);
    verify.setAccessible(true);
    try {
      verify.invoke(flusher, data, expected);
    } catch (InvocationTargetException e) {
      throw unwrap(e);
    }
  }

  private static void bdecVerify(byte[] bytes, long expectedRowCount) throws Exception {
    Method verify = parquetClass("hadoop.BdecParquetReader").getMethod("verify", byte[].class, long.class);
    try {
      verify.invoke(null, bytes, expectedRowCount);
    } catch (InvocationTargetException e) {
      throw unwrap(e);
    }
  }

  private static ByteArrayOutputStream chunkData(Object result) throws Exception {
    Field chunkData = result.getClass().getDeclaredField("chunkData");
    chunkData.setAccessible(true);
    return (ByteArrayOutputStream) chunkData.get(result);
  }

  private static byte[] chunkBytes(Object result) throws Exception {
    return chunkData(result).toByteArray();
  }

  private static long serializationRowCount(Object result) throws Exception {
    Field rowCount = result.getClass().getDeclaredField("rowCount");
    rowCount.setAccessible(true);
    return (Long) rowCount.get(result);
  }

  private static long rowCount(Object data) throws Exception {
    Method getRowCount = data.getClass().getDeclaredMethod("getRowCount");
    getRowCount.setAccessible(true);
    return ((Integer) getRowCount.invoke(data)).longValue();
  }

  private static long firstDataPageOffset(byte[] bytes) throws Exception {
    Class<?> readerClz = parquetClass("hadoop.ParquetFileReader");
    Class<?> bdecReader = parquetClass("hadoop.BdecParquetReader");
    Class<?> inputFileClz = null;
    for (Class<?> nested : bdecReader.getDeclaredClasses()) {
      if (nested.getSimpleName().equals("BdecInputFile")) {
        inputFileClz = nested;
        break;
      }
    }
    Object inputFile = inputFileClz.getConstructor(byte[].class).newInstance(new Object[] {bytes});
    Method open = readerClz.getMethod("open", parquetClass("io.InputFile"));
    AutoCloseable pfr = (AutoCloseable) open.invoke(null, inputFile);
    try {
    Object footer = invokeAccessible(pfr, "getFooter", new Class<?>[] {});
    List<?> blocks = (List<?>) invokeAccessible(footer, "getBlocks", new Class<?>[] {});
    List<?> columns = (List<?>) invokeAccessible(blocks.get(0), "getColumns", new Class<?>[] {});
    return (Long) invokeAccessible(columns.get(0), "getFirstDataPageOffset", new Class<?>[] {});
    } finally {
      pfr.close();
    }
  }

  private static Object readPageHeader(byte[] bytes, long pageOffset) throws Exception {
    ByteArrayInputStream headerIn =
        new ByteArrayInputStream(bytes, (int) pageOffset, bytes.length - (int) pageOffset);
    Method read =
        parquetClass("format.Util").getMethod("readPageHeader", InputStream.class);
    return read.invoke(null, headerIn);
  }

  private static int pageHeaderSize(byte[] bytes, long pageOffset) throws Exception {
    ByteArrayInputStream headerIn =
        new ByteArrayInputStream(bytes, (int) pageOffset, bytes.length - (int) pageOffset);
    parquetClass("format.Util").getMethod("readPageHeader", InputStream.class).invoke(null, headerIn);
    return (bytes.length - (int) pageOffset) - headerIn.available();
  }

  private static void writePageHeader(Object pageHeader, OutputStream out) throws Exception {
    parquetClass("format.Util")
        .getMethod("writePageHeader", parquetClass("format.PageHeader"), OutputStream.class)
        .invoke(null, pageHeader, out);
  }

  private static Class<?> parquetClass(String relative) throws ClassNotFoundException {
    try {
      return Class.forName("org.apache.parquet." + relative);
    } catch (ClassNotFoundException e) {
      return Class.forName("net.snowflake.ingest.internal.org.apache.parquet." + relative);
    }
  }

  private static Object newInternal(String className) throws Exception {
    Constructor<?> ctor = Class.forName(className).getDeclaredConstructor();
    ctor.setAccessible(true);
    return ctor.newInstance();
  }

  private static Constructor<?> ctor(String className, Class<?>[] types) throws Exception {
    Constructor<?> ctor = Class.forName(className).getDeclaredConstructor(types);
    ctor.setAccessible(true);
    return ctor;
  }

  private static Object invokeAccessible(Object target, String method, Class<?>[] types, Object... args)
      throws Exception {
    Method m = target.getClass().getMethod(method, types);
    m.setAccessible(true);
    try {
      return m.invoke(target, args);
    } catch (InvocationTargetException e) {
      throw unwrap(e);
    }
  }

  private static void invoke(Object target, String method, Class<?>[] types, Object... args)
      throws Exception {
    Method m = target.getClass().getDeclaredMethod(method, types);
    m.setAccessible(true);
    m.invoke(target, args);
  }

  private static Object field(Object target, String name) throws Exception {
    Field f = target.getClass().getField(name);
    return f.get(target);
  }

  private static void setField(Object target, String name, Object value) throws Exception {
    Field f = target.getClass().getField(name);
    f.set(target, value);
  }

  private static Exception unwrap(InvocationTargetException e) throws Exception {
    Throwable cause = e.getCause();
    if (cause instanceof Exception) {
      return (Exception) cause;
    }
    if (cause instanceof Error) {
      throw (Error) cause;
    }
    throw e;
  }
}
