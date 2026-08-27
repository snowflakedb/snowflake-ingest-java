/*
 * Copyright (c) 2026 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import static java.time.ZoneOffset.UTC;
import static net.snowflake.ingest.utils.ParameterProvider.ENABLE_NEW_JSON_PARSING_LOGIC_DEFAULT;
import static net.snowflake.ingest.utils.ParameterProvider.MAX_ALLOWED_ROW_SIZE_IN_BYTES_DEFAULT;
import static net.snowflake.ingest.utils.ParameterProvider.MAX_CHUNK_SIZE_IN_BYTES_DEFAULT;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import net.snowflake.ingest.internal.org.apache.parquet.column.ParquetProperties;
import net.snowflake.ingest.internal.org.apache.parquet.format.PageHeader;
import net.snowflake.ingest.internal.org.apache.parquet.format.Util;
import net.snowflake.ingest.internal.org.apache.parquet.hadoop.BdecParquetReader;
import net.snowflake.ingest.internal.org.apache.parquet.hadoop.ParquetFileReader;
import net.snowflake.ingest.internal.org.apache.parquet.schema.MessageType;
import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.utils.Constants;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Copies of the parquet readback verification tests from {@code RowBufferTest}, compiled against
 * the shaded SDK (relocated parquet/hadoop packages). Runs only for {@code test_type=shaded}.
 */
@RunWith(Parameterized.class)
public class RowBufferReadbackVerificationTest {
  @Parameterized.Parameters(name = "enableIcebergStreaming: {0}")
  public static Object[] enableIcebergStreaming() {
    return new Object[] {false, true};
  }

  @Parameterized.Parameter public static boolean enableIcebergStreaming;

  private AbstractRowBuffer<?> createTestBuffer(OpenChannelRequest.OnErrorOption onErrorOption) {
    ChannelRuntimeState initialState = new ChannelRuntimeState("0", 0L, true);
    return AbstractRowBuffer.createRowBuffer(
        onErrorOption,
        UTC,
        Constants.BdecVersion.THREE,
        "test.buffer",
        rs -> {},
        initialState,
        ClientBufferParameters.test_createClientBufferParameters(
            MAX_CHUNK_SIZE_IN_BYTES_DEFAULT,
            MAX_ALLOWED_ROW_SIZE_IN_BYTES_DEFAULT,
            Constants.BdecParquetCompression.GZIP,
            ENABLE_NEW_JSON_PARSING_LOGIC_DEFAULT,
            enableIcebergStreaming ? Optional.of(1) : Optional.empty(),
            enableIcebergStreaming,
            InternalParameterProvider.ENABLE_DISTINCT_VALUES_COUNT_DEFAULT,
            enableIcebergStreaming),
        null,
        enableIcebergStreaming
            ? ParquetProperties.WriterVersion.PARQUET_2_0
            : ParquetProperties.WriterVersion.PARQUET_1_0,
        null);
  }

  private static ParquetRowBuffer loadData(
      final ParquetRowBuffer bufferToLoad, final Map<String, Object> data) {
    final List<Map<String, Object>> validRows = new ArrayList<>();
    validRows.add(data);

    final InsertValidationResponse nResponse = bufferToLoad.insertRows(validRows, "1", "1");
    Assert.assertFalse(nResponse.hasErrors());
    return bufferToLoad;
  }

  private static ColumnMetadata textColumn(int byteLength, int length) {
    ColumnMetadata col = new ColumnMetadata();
    col.setOrdinal(1);
    col.setName("C1");
    col.setPhysicalType("LOB");
    col.setNullable(true);
    col.setLogicalType("TEXT");
    col.setByteLength(byteLength);
    col.setLength(length);
    col.setScale(0);
    return col;
  }

  @Test
  public void testParquetReadBackVerificationCatchesRowCountMismatch() throws IOException {
    ParquetRowBuffer buffer =
        (ParquetRowBuffer) createTestBuffer(OpenChannelRequest.OnErrorOption.CONTINUE);
    buffer.setupSchema(Collections.singletonList(textColumn(14, 11)));
    loadData(buffer, Collections.singletonMap("C1", "hello"));

    ChannelData<ParquetChunkData> data = buffer.flush();
    data.setChannelContext(new ChannelFlushContext("name", "db", "schema", "table", 1L, "key", 0L));

    ParquetFlusher flusher = (ParquetFlusher) buffer.createFlusher();
    Flusher.SerializationResult result =
        flusher.serialize(
            Collections.singletonList(data),
            "rowcount_test.bdec",
            0,
            FileMetadataTestingOverrides.none());

    try {
      flusher.verifyReadBack(result.chunkData, data.getRowCount() + 1);
      Assert.fail("Expected SFException for row count mismatch");
    } catch (SFException e) {
      Assert.assertTrue(e.getMessage().contains("Row count mismatch"));
    }
  }

  @Test
  public void testParquetReadBackVerificationCatchesCorruption() throws IOException {
    ParquetRowBuffer buffer =
        (ParquetRowBuffer) createTestBuffer(OpenChannelRequest.OnErrorOption.CONTINUE);
    buffer.setupSchema(Collections.singletonList(textColumn(14, 11)));
    loadData(buffer, Collections.singletonMap("C1", "hello"));

    ChannelData<ParquetChunkData> data = buffer.flush();
    data.setChannelContext(new ChannelFlushContext("name", "db", "schema", "table", 1L, "key", 0L));

    ParquetFlusher flusher = (ParquetFlusher) buffer.createFlusher();
    Flusher.SerializationResult result =
        flusher.serialize(
            Collections.singletonList(data),
            "corruption_test.bdec",
            0,
            FileMetadataTestingOverrides.none());

    // Midpoint on this 1-row blob is in the footer. Two-byte XOR is version-dependent
    // (4.4.4 vs 4.4.4-SNAPSHOT slides the hit onto a neighboring thrift field); 16 bytes
    // makes footer parse fail either way.
    byte[] bytes = result.chunkData.toByteArray();
    int midpoint = bytes.length / 2;
    int corruptUntil = Math.min(midpoint + 16, bytes.length);
    for (int i = midpoint; i < corruptUntil; i++) {
      bytes[i] ^= 0xFF;
    }
    ByteArrayOutputStream corrupted = new ByteArrayOutputStream();
    corrupted.write(bytes);

    try {
      flusher.verifyReadBack(corrupted, data.getRowCount());
      Assert.fail("Expected SFException for corrupt parquet data");
    } catch (SFException e) {
      Assert.assertNotNull(e.getMessage());
    }
  }

  @Test
  public void testParquetReadBackVerificationCatchesPageBodyCorruption() throws IOException {
    ParquetRowBuffer buffer =
        (ParquetRowBuffer) createTestBuffer(OpenChannelRequest.OnErrorOption.CONTINUE);
    buffer.setupSchema(Collections.singletonList(textColumn(256, 256)));
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
    Assert.assertFalse(buffer.insertRows(rows, "1", "32").hasErrors());

    ChannelData<ParquetChunkData> data = buffer.flush();
    data.setChannelContext(new ChannelFlushContext("name", "db", "schema", "table", 1L, "key", 0L));

    ParquetFlusher flusher = (ParquetFlusher) buffer.createFlusher();
    Flusher.SerializationResult result =
        flusher.serialize(
            Collections.singletonList(data),
            "corruption_page_body_test.bdec",
            0,
            FileMetadataTestingOverrides.none());

    byte[] bytes = result.chunkData.toByteArray();
    int midpoint = bytes.length / 2;
    long pageOffset;
    try (ParquetFileReader pfr =
        ParquetFileReader.open(new BdecParquetReader.BdecInputFile(bytes))) {
      pageOffset = pfr.getFooter().getBlocks().get(0).getColumns().get(0).getFirstDataPageOffset();
    }
    ByteArrayInputStream headerIn =
        new ByteArrayInputStream(bytes, (int) pageOffset, bytes.length - (int) pageOffset);
    PageHeader pageHeader = Util.readPageHeader(headerIn);
    int headerSize = (bytes.length - (int) pageOffset) - headerIn.available();
    int bodyStart = (int) pageOffset + headerSize;
    int bodyEnd = bodyStart + pageHeader.compressed_page_size;
    Assert.assertTrue(
        "midpoint must land in the GZIP page, not the footer",
        midpoint >= bodyStart && midpoint + 1 < bodyEnd);
    // Iceberg v2 can still decode after a two-byte flip; smash a short window.
    int corruptUntil = Math.min(midpoint + 16, bodyEnd);
    for (int i = midpoint; i < corruptUntil; i++) {
      bytes[i] ^= 0xFF;
    }
    ByteArrayOutputStream corrupted = new ByteArrayOutputStream();
    corrupted.write(bytes);

    try {
      flusher.verifyReadBack(corrupted, data.getRowCount());
      Assert.fail("Expected SFException for corrupt parquet page body");
    } catch (SFException e) {
      Assert.assertNotNull(e.getMessage());
    }
  }

  @Test
  public void testParquetReadBackVerificationRetrySucceeds() throws Exception {
    ParquetRowBuffer buffer =
        (ParquetRowBuffer) createTestBuffer(OpenChannelRequest.OnErrorOption.CONTINUE);
    buffer.setupSchema(Collections.singletonList(textColumn(14, 11)));
    loadData(buffer, Collections.singletonMap("C1", "hello"));

    ChannelData<ParquetChunkData> data = buffer.flush();
    data.setChannelContext(new ChannelFlushContext("name", "db", "schema", "table", 1L, "key", 0L));

    // Extract the schema that was built by setupSchema so the flusher uses an identical MessageType
    java.lang.reflect.Field schemaField = ParquetRowBuffer.class.getDeclaredField("schema");
    schemaField.setAccessible(true);
    MessageType parquetSchema = (MessageType) schemaField.get(buffer);

    // Subclass ParquetFlusher with readback enabled; verifyReadBack throws on the first call only
    int[] callCount = {0};
    ParquetFlusher flusher =
        new ParquetFlusher(
            parquetSchema,
            MAX_CHUNK_SIZE_IN_BYTES_DEFAULT,
            enableIcebergStreaming ? Optional.of(1) : Optional.empty(),
            Constants.BdecParquetCompression.GZIP,
            enableIcebergStreaming
                ? ParquetProperties.WriterVersion.PARQUET_2_0
                : ParquetProperties.WriterVersion.PARQUET_1_0,
            enableIcebergStreaming,
            enableIcebergStreaming,
            true /* enableParquetReadbackVerification */) {
          @Override
          void verifyReadBack(ByteArrayOutputStream mergedData, long expectedRowCount) {
            if (callCount[0]++ == 0) {
              throw new SFException(ErrorCode.INTERNAL_ERROR, "simulated transient corruption");
            }
          }
        };

    // Serialization succeeds because the retry on attempt 2 passes verification
    Flusher.SerializationResult result =
        flusher.serialize(
            Collections.singletonList(data),
            "retry_test.bdec",
            0,
            FileMetadataTestingOverrides.none());

    Assert.assertNotNull(result);
    Assert.assertEquals(1L, result.rowCount);
    Assert.assertEquals(2, callCount[0]); // attempt 1 threw, attempt 2 succeeded
  }

  @Test
  public void testParquetReadBackVerificationRetrySucceedsMultipleRows() throws Exception {
    ParquetRowBuffer buffer =
        (ParquetRowBuffer) createTestBuffer(OpenChannelRequest.OnErrorOption.CONTINUE);
    buffer.setupSchema(Collections.singletonList(textColumn(14, 11)));
    loadData(buffer, Collections.singletonMap("C1", "row_one"));
    loadData(buffer, Collections.singletonMap("C1", "row_two"));
    loadData(buffer, Collections.singletonMap("C1", "row_three"));

    ChannelData<ParquetChunkData> data = buffer.flush();
    data.setChannelContext(new ChannelFlushContext("name", "db", "schema", "table", 1L, "key", 0L));

    java.lang.reflect.Field schemaField = ParquetRowBuffer.class.getDeclaredField("schema");
    schemaField.setAccessible(true);
    MessageType parquetSchema = (MessageType) schemaField.get(buffer);

    int[] callCount = {0};
    ParquetFlusher flusher =
        new ParquetFlusher(
            parquetSchema,
            MAX_CHUNK_SIZE_IN_BYTES_DEFAULT,
            enableIcebergStreaming ? Optional.of(1) : Optional.empty(),
            Constants.BdecParquetCompression.GZIP,
            enableIcebergStreaming
                ? ParquetProperties.WriterVersion.PARQUET_2_0
                : ParquetProperties.WriterVersion.PARQUET_1_0,
            enableIcebergStreaming,
            enableIcebergStreaming,
            true /* enableParquetReadbackVerification */) {
          @Override
          void verifyReadBack(ByteArrayOutputStream mergedData, long expectedRowCount) {
            if (callCount[0]++ == 0) {
              throw new SFException(ErrorCode.INTERNAL_ERROR, "simulated transient corruption");
            }
          }
        };

    Flusher.SerializationResult result =
        flusher.serialize(
            Collections.singletonList(data),
            "retry_multirow_test.bdec",
            0,
            FileMetadataTestingOverrides.none());

    Assert.assertNotNull(result);
    Assert.assertEquals(3L, result.rowCount);
    Assert.assertEquals(2, callCount[0]);
  }

  @Test
  public void testParquetPageHeaderNumValuesCorruptionCaughtByRowCountCheck() throws Exception {
    ParquetRowBuffer buffer =
        (ParquetRowBuffer) createTestBuffer(OpenChannelRequest.OnErrorOption.CONTINUE);
    buffer.setupSchema(Collections.singletonList(textColumn(14, 11)));
    loadData(buffer, Collections.singletonMap("C1", "hello"));

    ChannelData<ParquetChunkData> data = buffer.flush();
    data.setChannelContext(new ChannelFlushContext("name", "db", "schema", "table", 1L, "key", 0L));

    ParquetFlusher flusher = (ParquetFlusher) buffer.createFlusher();
    Flusher.SerializationResult result =
        flusher.serialize(
            Collections.singletonList(data),
            "num_values_corruption_test.bdec",
            0,
            FileMetadataTestingOverrides.none());

    byte[] bytes = result.chunkData.toByteArray();

    // Find the byte offset of the first data page header
    long pageOffset;
    try (ParquetFileReader pfr =
        ParquetFileReader.open(new BdecParquetReader.BdecInputFile(bytes))) {
      pageOffset = pfr.getFooter().getBlocks().get(0).getColumns().get(0).getFirstDataPageOffset();
    }

    // Deserialize the Thrift-compact page header
    ByteArrayInputStream headerIn =
        new ByteArrayInputStream(bytes, (int) pageOffset, bytes.length - (int) pageOffset);
    PageHeader pageHeader = Util.readPageHeader(headerIn);
    int originalHeaderSize = (bytes.length - (int) pageOffset) - headerIn.available();

    // Simulate SNOW-3903306: bit-flip corrupts num_values to 0 in the page header.
    // Parquet v1 pages use data_page_header; Parquet v2 (Iceberg) uses data_page_header_v2.
    if (pageHeader.data_page_header != null) {
      pageHeader.data_page_header.num_values = 0;
    } else {
      pageHeader.data_page_header_v2.num_values = 0;
    }

    // Re-serialize the header with corrupted num_values and reassemble the file
    ByteArrayOutputStream modifiedHeader = new ByteArrayOutputStream();
    Util.writePageHeader(pageHeader, modifiedHeader);
    ByteArrayOutputStream patched = new ByteArrayOutputStream();
    patched.write(bytes, 0, (int) pageOffset);
    patched.write(modifiedHeader.toByteArray());
    patched.write(
        bytes,
        (int) pageOffset + originalHeaderSize,
        bytes.length - (int) pageOffset - originalHeaderSize);

    // verify() catches the corruption — the reader encounters an error processing the
    // corrupted page header and throws IOException before or during row count validation
    try {
      BdecParquetReader.verify(patched.toByteArray(), data.getRowCount());
      Assert.fail("Expected IOException for num_values corruption");
    } catch (IOException e) {
      Assert.assertNotNull(e.getMessage());
    }
  }
}
