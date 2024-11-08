/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import static java.time.ZoneOffset.UTC;
import static net.snowflake.ingest.utils.Constants.EP_NV_UNKNOWN;
import static net.snowflake.ingest.utils.ParameterProvider.ENABLE_NEW_JSON_PARSING_LOGIC_DEFAULT;
import static net.snowflake.ingest.utils.ParameterProvider.MAX_ALLOWED_ROW_SIZE_IN_BYTES_DEFAULT;
import static net.snowflake.ingest.utils.ParameterProvider.MAX_CHUNK_SIZE_IN_BYTES_DEFAULT;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import net.snowflake.ingest.connection.RequestBuilder;
import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.utils.Constants;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.BdecParquetReader;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class RowBufferTest {
  @Parameterized.Parameters(name = "enableIcebergStreaming: {0}")
  public static Object[] enableIcebergStreaming() {
    return new Object[] {false, true};
  }

  @Parameterized.Parameter public static boolean enableIcebergStreaming;

  private AbstractRowBuffer<?> rowBufferOnErrorContinue;
  private AbstractRowBuffer<?> rowBufferOnErrorAbort;
  private AbstractRowBuffer<?> rowBufferOnErrorSkipBatch;

  @Before
  public void setupRowBuffer() {
    this.rowBufferOnErrorContinue = createTestBuffer(OpenChannelRequest.OnErrorOption.CONTINUE);
    this.rowBufferOnErrorAbort = createTestBuffer(OpenChannelRequest.OnErrorOption.ABORT);
    this.rowBufferOnErrorSkipBatch = createTestBuffer(OpenChannelRequest.OnErrorOption.SKIP_BATCH);
    List<ColumnMetadata> schema = createSchema();
    this.rowBufferOnErrorContinue.setupSchema(schema);
    this.rowBufferOnErrorAbort.setupSchema(schema);
    this.rowBufferOnErrorSkipBatch.setupSchema(schema);
  }

  static List<ColumnMetadata> createSchema() {
    ColumnMetadata colTinyIntCase = new ColumnMetadata();
    colTinyIntCase.setName("\"colTinyInt\"");
    colTinyIntCase.setPhysicalType("SB1");
    colTinyIntCase.setNullable(true);
    colTinyIntCase.setLogicalType("FIXED");
    colTinyIntCase.setPrecision(2);
    colTinyIntCase.setScale(0);

    ColumnMetadata colTinyInt = new ColumnMetadata();
    colTinyInt.setName("COLTINYINT");
    colTinyInt.setPhysicalType("SB1");
    colTinyInt.setNullable(true);
    colTinyInt.setLogicalType("FIXED");
    colTinyInt.setPrecision(1);
    colTinyInt.setScale(0);

    ColumnMetadata colSmallInt = new ColumnMetadata();
    colSmallInt.setName("COLSMALLINT");
    colSmallInt.setPhysicalType("SB2");
    colSmallInt.setNullable(true);
    colSmallInt.setLogicalType("FIXED");
    colSmallInt.setPrecision(2);
    colSmallInt.setScale(0);

    ColumnMetadata colInt = new ColumnMetadata();
    colInt.setName("COLINT");
    colInt.setPhysicalType("SB4");
    colInt.setNullable(true);
    colInt.setLogicalType("FIXED");
    colInt.setPrecision(2);
    colInt.setScale(0);

    ColumnMetadata colBigInt = new ColumnMetadata();
    colBigInt.setName("COLBIGINT");
    colBigInt.setPhysicalType("SB8");
    colBigInt.setNullable(true);
    colBigInt.setLogicalType("FIXED");
    colBigInt.setPrecision(2);
    colBigInt.setScale(0);

    ColumnMetadata colDecimal = new ColumnMetadata();
    colDecimal.setName("COLDECIMAL");
    colDecimal.setPhysicalType("SB16");
    colDecimal.setNullable(true);
    colDecimal.setLogicalType("FIXED");
    colDecimal.setPrecision(38);
    colDecimal.setScale(2);

    ColumnMetadata colChar = new ColumnMetadata();
    colChar.setName("COLCHAR");
    colChar.setPhysicalType("LOB");
    colChar.setNullable(true);
    colChar.setLogicalType("TEXT");
    colChar.setByteLength(14);
    colChar.setLength(11);
    colChar.setScale(0);

    if (enableIcebergStreaming) {
      colTinyIntCase.setSourceIcebergDataType("\"decimal(2,0)\"");
      colTinyInt.setSourceIcebergDataType("\"decimal(1,0)\"");
      colSmallInt.setSourceIcebergDataType("\"decimal(2,0)\"");
      colInt.setSourceIcebergDataType("\"int\"");
      colBigInt.setSourceIcebergDataType("\"long\"");
      colDecimal.setSourceIcebergDataType("\"decimal(38,2)\"");
      colChar.setSourceIcebergDataType("\"string\"");
    }

    List<ColumnMetadata> columns =
        Arrays.asList(
            colTinyIntCase, colTinyInt, colSmallInt, colInt, colBigInt, colDecimal, colChar);
    for (int i = 0; i < columns.size(); i++) {
      columns.get(i).setOrdinal(i + 1);
    }
    return columns;
  }

  static List<ColumnMetadata> createStrcuturedDataTypeSchema() {
    ColumnMetadata colObject = new ColumnMetadata();
    colObject.setName("COLOBJECT");
    colObject.setPhysicalType("LOB");
    colObject.setNullable(true);
    colObject.setLogicalType("OBJECT");
    colObject.setSourceIcebergDataType(
        "{\n"
            + "    \"type\": \"struct\",\n"
            + "    \"fields\":\n"
            + "    [\n"
            + "        {\n"
            + "            \"id\": 4,\n"
            + "            \"name\": \"a\",\n"
            + "            \"required\": false,\n"
            + "            \"type\": \"int\"\n"
            + "        },\n"
            + "        {\n"
            + "            \"id\": 5,\n"
            + "            \"name\": \"b\",\n"
            + "            \"required\": false,\n"
            + "            \"type\": \"string\"\n"
            + "        }\n"
            + "    ]\n"
            + "}");

    ColumnMetadata colMap = new ColumnMetadata();
    colMap.setName("COLMAP");
    colMap.setPhysicalType("LOB");
    colMap.setNullable(true);
    colMap.setLogicalType("MAP");
    colMap.setSourceIcebergDataType(
        "{\n"
            + "    \"type\": \"map\",\n"
            + "    \"key-id\": 6,\n"
            + "    \"key\": \"string\",\n"
            + "    \"value-id\": 7,\n"
            + "    \"value\": \"boolean\",\n"
            + "    \"value-required\": false\n"
            + "}");

    ColumnMetadata colArray = new ColumnMetadata();
    colArray.setName("COLARRAY");
    colArray.setPhysicalType("LOB");
    colArray.setNullable(true);
    colArray.setLogicalType("ARRAY");
    colArray.setSourceIcebergDataType(
        "{\n"
            + "    \"type\": \"list\",\n"
            + "    \"element-id\": 8,\n"
            + "    \"element\": \"long\",\n"
            + "    \"element-required\": false\n"
            + "}");

    List<ColumnMetadata> columns = Arrays.asList(colObject, colMap, colArray);
    for (int i = 0; i < columns.size(); i++) {
      columns.get(i).setOrdinal(i + 1);
    }
    return columns;
  }

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
            enableIcebergStreaming,
            enableIcebergStreaming),
        null,
        enableIcebergStreaming
            ? ParquetProperties.WriterVersion.PARQUET_2_0
            : ParquetProperties.WriterVersion.PARQUET_1_0,
        null);
  }

  @Test
  public void testCollatedColumnsAreRejected() {
    ColumnMetadata collatedColumn = new ColumnMetadata();
    collatedColumn.setName("COLCHAR");
    collatedColumn.setPhysicalType("LOB");
    collatedColumn.setNullable(true);
    collatedColumn.setLogicalType("TEXT");
    collatedColumn.setByteLength(14);
    collatedColumn.setLength(11);
    collatedColumn.setScale(0);
    collatedColumn.setCollation("en-ci");
    try {
      this.rowBufferOnErrorAbort.setupSchema(Collections.singletonList(collatedColumn));
      fail("Collated columns are not supported");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.UNSUPPORTED_DATA_TYPE.getMessageCode(), e.getVendorCode());
    }
  }

  @Test
  public void buildFieldErrorStates() {
    // Nonsense Type
    ColumnMetadata testCol = new ColumnMetadata();
    testCol.setName("testCol");
    testCol.setPhysicalType("Failure");
    testCol.setNullable(true);
    testCol.setLogicalType("FIXED");
    testCol.setByteLength(14);
    testCol.setLength(11);
    testCol.setScale(0);
    testCol.setPrecision(4);
    try {
      this.rowBufferOnErrorContinue.setupSchema(Collections.singletonList(testCol));
      fail("Expected error");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.UNKNOWN_DATA_TYPE.getMessageCode(), e.getVendorCode());
    }

    // Fixed LOB
    testCol = new ColumnMetadata();
    testCol.setName("COL1");
    testCol.setPhysicalType("LOB");
    testCol.setLogicalType("FIXED");
    try {
      this.rowBufferOnErrorContinue.setupSchema(Collections.singletonList(testCol));
      fail("Expected error");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.UNKNOWN_DATA_TYPE.getMessageCode(), e.getVendorCode());
    }

    // TIMESTAMP_NTZ SB2
    testCol = new ColumnMetadata();
    testCol.setName("COL1");
    testCol.setPhysicalType("SB2");
    testCol.setLogicalType("TIMESTAMP_NTZ");
    try {
      this.rowBufferOnErrorContinue.setupSchema(Collections.singletonList(testCol));
      fail("Expected error");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.UNKNOWN_DATA_TYPE.getMessageCode(), e.getVendorCode());
    }

    // TIMESTAMP_TZ SB1
    testCol = new ColumnMetadata();
    testCol.setName("COL1");
    testCol.setPhysicalType("SB1");
    testCol.setLogicalType("TIMESTAMP_TZ");
    try {
      this.rowBufferOnErrorContinue.setupSchema(Collections.singletonList(testCol));
      fail("Expected error");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.UNKNOWN_DATA_TYPE.getMessageCode(), e.getVendorCode());
    }

    // TIME SB16
    testCol = new ColumnMetadata();
    testCol.setName("COL1");
    testCol.setPhysicalType("SB16");
    testCol.setLogicalType("TIME");
    try {
      this.rowBufferOnErrorContinue.setupSchema(Collections.singletonList(testCol));
      fail("Expected error");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.UNKNOWN_DATA_TYPE.getMessageCode(), e.getVendorCode());
    }
  }

  @Test
  public void testReset() {
    RowBufferStats stats =
        this.rowBufferOnErrorContinue.statsMap.get(enableIcebergStreaming ? "1" : "COLCHAR");
    stats.addIntValue(BigInteger.valueOf(1));
    Assert.assertEquals(BigInteger.valueOf(1), stats.getCurrentMaxIntValue());
    Assert.assertNull(stats.getCollationDefinitionString());
    this.rowBufferOnErrorContinue.reset();
    RowBufferStats resetStats =
        this.rowBufferOnErrorContinue.statsMap.get(enableIcebergStreaming ? "1" : "COLCHAR");
    Assert.assertNotNull(resetStats);
    Assert.assertNull(resetStats.getCurrentMaxIntValue());
    Assert.assertNull(resetStats.getCollationDefinitionString());
  }

  @Test
  public void testInvalidLogicalType() {
    ColumnMetadata colInvalidLogical = new ColumnMetadata();
    colInvalidLogical.setName("COLINVALIDLOGICAL");
    colInvalidLogical.setPhysicalType("SB1");
    colInvalidLogical.setNullable(false);
    colInvalidLogical.setLogicalType("INVALID");
    colInvalidLogical.setByteLength(14);
    colInvalidLogical.setLength(11);
    colInvalidLogical.setScale(0);

    try {
      this.rowBufferOnErrorContinue.setupSchema(Collections.singletonList(colInvalidLogical));
      fail("Setup should fail if invalid column metadata is provided");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.UNKNOWN_DATA_TYPE.getMessageCode(), e.getVendorCode());
      // Do nothing
    }
  }

  @Test
  public void testInvalidPhysicalType() {
    ColumnMetadata colInvalidPhysical = new ColumnMetadata();
    colInvalidPhysical.setName("COLINVALIDPHYSICAL");
    colInvalidPhysical.setPhysicalType("INVALID");
    colInvalidPhysical.setNullable(false);
    colInvalidPhysical.setLogicalType("FIXED");
    colInvalidPhysical.setByteLength(14);
    colInvalidPhysical.setLength(11);
    colInvalidPhysical.setScale(0);

    try {
      this.rowBufferOnErrorContinue.setupSchema(Collections.singletonList(colInvalidPhysical));
      fail("Setup should fail if invalid column metadata is provided");
    } catch (SFException e) {
      Assert.assertEquals(e.getVendorCode(), ErrorCode.UNKNOWN_DATA_TYPE.getMessageCode());
    }
  }

  @Test
  public void testStringLength() {
    /* Iceberg cannot specify max length of string */
    if (!enableIcebergStreaming) {
      testStringLengthHelper(this.rowBufferOnErrorContinue);
      testStringLengthHelper(this.rowBufferOnErrorAbort);
      testStringLengthHelper(this.rowBufferOnErrorSkipBatch);
    }
  }

  @Test
  public void testRowIndexWithMultipleRowsWithError() {
    testRowIndexWithMultipleRowsWithErrorHelper(this.rowBufferOnErrorContinue);
    testRowIndexWithMultipleRowsWithErrorHelper(this.rowBufferOnErrorSkipBatch);
  }

  public void testRowIndexWithMultipleRowsWithErrorHelper(AbstractRowBuffer<?> rowBuffer) {
    List<Map<String, Object>> rows = new ArrayList<>();
    Map<String, Object> row = new HashMap<>();

    // row with interleaved good and bad data
    row.put("colInt", 3);
    rows.add(row);

    row = new HashMap<>();
    row.put("colChar", StringUtils.repeat('1', 16777217)); // too big
    rows.add(row);

    row = new HashMap<>();
    row.put("colInt", 3);
    rows.add(row);

    row = new HashMap<>();
    row.put("colChar", StringUtils.repeat('1', 16777217)); // too big
    rows.add(row);

    InsertValidationResponse response = rowBuffer.insertRows(rows, null, null);
    Assert.assertTrue(response.hasErrors());

    Assert.assertEquals(2, response.getErrorRowCount());

    // second row out of the rows we sent was having bad data.
    // so InsertError corresponds to second row.
    Assert.assertEquals(1, response.getInsertErrors().get(0).getRowIndex());
    Assert.assertEquals(3, response.getInsertErrors().get(1).getRowIndex());

    Assert.assertNotNull(response.getInsertErrors().get(0).getException());
    Assert.assertNotNull(response.getInsertErrors().get(1).getException());

    Assert.assertEquals(
        response.getInsertErrors().get(0).getException().getVendorCode(),
        ErrorCode.INVALID_VALUE_ROW.getMessageCode());
    Assert.assertEquals(
        response.getInsertErrors().get(1).getException().getVendorCode(),
        ErrorCode.INVALID_VALUE_ROW.getMessageCode());

    Assert.assertTrue(
        response
            .getInsertErrors()
            .get(0)
            .getException()
            .getMessage()
            .equalsIgnoreCase(
                "The given row cannot be converted to the internal format due to invalid value:"
                    + " Value cannot be ingested into Snowflake column COLCHAR of type STRING,"
                    + " rowIndex:1, reason: String too long: length=16777217 bytes"
                    + " maxLength=16777216 bytes"));
    Assert.assertTrue(
        response
            .getInsertErrors()
            .get(1)
            .getException()
            .getMessage()
            .equalsIgnoreCase(
                "The given row cannot be converted to the internal format due to invalid value:"
                    + " Value cannot be ingested into Snowflake column COLCHAR of type STRING,"
                    + " rowIndex:3, reason: String too long: length=16777217 bytes"
                    + " maxLength=16777216 bytes"));
  }

  private void testStringLengthHelper(AbstractRowBuffer<?> rowBuffer) {
    Map<String, Object> row = new HashMap<>();
    row.put("colTinyInt", (byte) 1);
    row.put("\"colTinyInt\"", (byte) 1);
    row.put("colSmallInt", (short) 2);
    row.put("colInt", 3);
    row.put("colBigInt", 4L);
    row.put("colDecimal", 1.23);
    row.put("colChar", "1234567890"); // still fits

    InsertValidationResponse response =
        rowBuffer.insertRows(Collections.singletonList(row), null, null);
    Assert.assertFalse(response.hasErrors());

    row.put("colTinyInt", (byte) 1);
    row.put("\"colTinyInt\"", (byte) 1);
    row.put("colSmallInt", (short) 2);
    row.put("colInt", 3);
    row.put("colBigInt", 4L);
    row.put("colDecimal", 1.23);
    row.put("colChar", "1111111111111111111111"); // too big

    if (rowBuffer.onErrorOption == OpenChannelRequest.OnErrorOption.CONTINUE) {
      response = rowBuffer.insertRows(Collections.singletonList(row), null, null);
      Assert.assertTrue(response.hasErrors());
      Assert.assertEquals(1, response.getErrorRowCount());
      Assert.assertEquals(
          ErrorCode.INVALID_VALUE_ROW.getMessageCode(),
          response.getInsertErrors().get(0).getException().getVendorCode());
      Assert.assertTrue(response.getInsertErrors().get(0).getMessage().contains("String too long"));
    } else {
      try {
        rowBuffer.insertRows(Collections.singletonList(row), null, null);
      } catch (SFException e) {
        Assert.assertEquals(ErrorCode.INVALID_VALUE_ROW.getMessageCode(), e.getVendorCode());
      }
    }
  }

  @Test
  public void testInsertRow() {
    testInsertRowHelper(this.rowBufferOnErrorContinue);
    testInsertRowHelper(this.rowBufferOnErrorAbort);
    testInsertRowHelper(this.rowBufferOnErrorSkipBatch);
  }

  private void testInsertRowHelper(AbstractRowBuffer<?> rowBuffer) {
    Map<String, Object> row = new HashMap<>();
    row.put("colTinyInt", (byte) 1);
    row.put("\"colTinyInt\"", (byte) 1);
    row.put("colSmallInt", (short) 2);
    row.put("colInt", 3);
    row.put("colBigInt", 4L);
    row.put("colDecimal", 1.23);
    row.put("colChar", "2");

    InsertValidationResponse response =
        rowBuffer.insertRows(Collections.singletonList(row), null, null);
    Assert.assertFalse(response.hasErrors());
  }

  @Test
  public void testNullInsertRow() {
    testInsertNullRowHelper(this.rowBufferOnErrorContinue);
    testInsertNullRowHelper(this.rowBufferOnErrorAbort);
    testInsertNullRowHelper(this.rowBufferOnErrorSkipBatch);
  }

  private void testInsertNullRowHelper(AbstractRowBuffer<?> rowBuffer) {
    Map<String, Object> row = new HashMap<>();
    row.put("colTinyInt", null);
    row.put("\"colTinyInt\"", null);
    row.put("colSmallInt", null);
    row.put("colInt", null);
    row.put("colBigInt", null);
    row.put("colDecimal", null);
    row.put("colChar", null);

    InsertValidationResponse response =
        rowBuffer.insertRows(Collections.singletonList(row), null, null);
    Assert.assertFalse(response.hasErrors());
  }

  @Test
  public void testInsertRows() {
    testInsertRowsHelper(this.rowBufferOnErrorContinue);
    testInsertRowsHelper(this.rowBufferOnErrorAbort);
    testInsertRowsHelper(this.rowBufferOnErrorSkipBatch);
  }

  private void testInsertRowsHelper(AbstractRowBuffer<?> rowBuffer) {
    Map<String, Object> row1 = new HashMap<>();
    row1.put("colTinyInt", (byte) 1);
    row1.put("\"colTinyInt\"", (byte) 1);
    row1.put("colSmallInt", (short) 2);
    row1.put("colInt", 3);
    row1.put("colBigInt", 4L);
    row1.put("colDecimal", 1.23);
    row1.put("colChar", "2");

    Map<String, Object> row2 = new HashMap<>();
    row2.put("colTinyInt", (byte) 1);
    row2.put("\"colTinyInt\"", (byte) 1);
    row2.put("colSmallInt", (short) 2);
    row2.put("colInt", 3);
    row2.put("colBigInt", 4L);
    row2.put("colDecimal", 2.34);
    row2.put("colChar", "3");

    InsertValidationResponse response = rowBuffer.insertRows(Arrays.asList(row1, row2), null, null);
    Assert.assertFalse(response.hasErrors());
  }

  @Test
  public void testFlush() {
    testFlushHelper(this.rowBufferOnErrorAbort);
    testFlushHelper(this.rowBufferOnErrorContinue);
    testFlushHelper(this.rowBufferOnErrorSkipBatch);
  }

  private void testFlushHelper(AbstractRowBuffer<?> rowBuffer) {
    String startOffsetToken = "1";
    String endOffsetToken = "2";
    Map<String, Object> row1 = new HashMap<>();
    row1.put("colTinyInt", (byte) 1);
    row1.put("\"colTinyInt\"", (byte) 1);
    row1.put("colSmallInt", (short) 2);
    row1.put("colInt", 3);
    row1.put("colBigInt", 4L);
    row1.put("colDecimal", 1.23);
    row1.put("colChar", "2");

    Map<String, Object> row2 = new HashMap<>();
    row2.put("colTinyInt", (byte) 1);
    row2.put("\"colTinyInt\"", (byte) 1);
    row2.put("colSmallInt", (short) 2);
    row2.put("colInt", 3);
    row2.put("colBigInt", 4L);
    row2.put("colDecimal", 2.34);
    row2.put("colChar", "3");

    InsertValidationResponse response =
        rowBuffer.insertRows(Arrays.asList(row1, row2), startOffsetToken, endOffsetToken);
    Assert.assertFalse(response.hasErrors());
    float bufferSize = rowBuffer.getSize();

    final String filename = "2022/7/13/16/56/testFlushHelper_streaming.bdec";
    ChannelData<?> data = rowBuffer.flush();
    Assert.assertEquals(2, data.getRowCount());
    Assert.assertEquals((Long) 1L, data.getRowSequencer());
    Assert.assertEquals(startOffsetToken, data.getStartOffsetToken());
    Assert.assertEquals(endOffsetToken, data.getEndOffsetToken());
    Assert.assertEquals(bufferSize, data.getBufferSize(), 0);
  }

  @Test
  public void testDoubleQuotesColumnName() {
    testDoubleQuotesColumnNameHelper(OpenChannelRequest.OnErrorOption.ABORT);
    testDoubleQuotesColumnNameHelper(OpenChannelRequest.OnErrorOption.CONTINUE);
    testDoubleQuotesColumnNameHelper(OpenChannelRequest.OnErrorOption.SKIP_BATCH);
  }

  private void testDoubleQuotesColumnNameHelper(OpenChannelRequest.OnErrorOption onErrorOption) {
    AbstractRowBuffer<?> innerBuffer = createTestBuffer(onErrorOption);

    ColumnMetadata colDoubleQuotes = new ColumnMetadata();
    colDoubleQuotes.setOrdinal(1);
    colDoubleQuotes.setName("\"colDoubleQuotes\"");
    colDoubleQuotes.setPhysicalType("SB16");
    colDoubleQuotes.setNullable(true);
    colDoubleQuotes.setLogicalType("FIXED");
    colDoubleQuotes.setPrecision(38);
    colDoubleQuotes.setScale(0);

    innerBuffer.setupSchema(Collections.singletonList(colDoubleQuotes));

    Map<String, Object> row = new HashMap<>();
    row.put("\"colDoubleQuotes\"", 1);

    InsertValidationResponse response =
        innerBuffer.insertRows(Collections.singletonList(row), null, null);
    Assert.assertFalse(response.hasErrors());
  }

  @Test
  public void testBuildEpInfoFromStats() {
    Map<String, RowBufferStats> colStats = new HashMap<>();

    RowBufferStats stats1 =
        new RowBufferStats(
            "intColumn",
            Types.optional(PrimitiveType.PrimitiveTypeName.INT32).id(1).named("intColumn"),
            enableIcebergStreaming,
            enableIcebergStreaming);
    stats1.addIntValue(BigInteger.valueOf(2));
    stats1.addIntValue(BigInteger.valueOf(10));
    stats1.addIntValue(BigInteger.valueOf(1));

    RowBufferStats stats2 =
        new RowBufferStats(
            "strColumn",
            Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).id(2).named("strColumn"),
            enableIcebergStreaming,
            enableIcebergStreaming);
    stats2.addStrValue("alice");
    stats2.addStrValue("bob");
    stats2.incCurrentNullCount();

    colStats.put("intColumn", stats1);
    colStats.put("strColumn", stats2);

    EpInfo result =
        AbstractRowBuffer.buildEpInfoFromStats(
            2, colStats, !enableIcebergStreaming, enableIcebergStreaming);
    Map<String, FileColumnProperties> columnResults = result.getColumnEps();
    Assert.assertEquals(2, columnResults.keySet().size());

    FileColumnProperties strColumnResult = columnResults.get("strColumn");
    Assert.assertEquals(enableIcebergStreaming ? 2 : -1, strColumnResult.getDistinctValues());
    Assert.assertEquals(
        Hex.encodeHexString("alice".getBytes(StandardCharsets.UTF_8)),
        strColumnResult.getMinStrValue());
    Assert.assertEquals(
        Hex.encodeHexString("bob".getBytes(StandardCharsets.UTF_8)),
        strColumnResult.getMaxStrValue());
    Assert.assertEquals(1, strColumnResult.getNullCount());

    FileColumnProperties intColumnResult = columnResults.get("intColumn");
    Assert.assertEquals(enableIcebergStreaming ? 3 : -1, intColumnResult.getDistinctValues());
    Assert.assertEquals(BigInteger.valueOf(1), intColumnResult.getMinIntValue());
    Assert.assertEquals(BigInteger.valueOf(10), intColumnResult.getMaxIntValue());
    Assert.assertEquals(0, intColumnResult.getNullCount());
  }

  @Test
  public void testBuildEpInfoFromNullColumnStats() {
    final String intColName = "intCol";
    final String realColName = "realCol";
    Map<String, RowBufferStats> colStats = new HashMap<>();

    RowBufferStats stats1 =
        new RowBufferStats(
            intColName,
            Types.optional(PrimitiveType.PrimitiveTypeName.INT32).id(1).named(intColName),
            enableIcebergStreaming,
            enableIcebergStreaming);
    RowBufferStats stats2 =
        new RowBufferStats(
            realColName,
            Types.optional(PrimitiveType.PrimitiveTypeName.DOUBLE).id(2).named(realColName),
            enableIcebergStreaming,
            enableIcebergStreaming);
    stats1.incCurrentNullCount();
    stats2.incCurrentNullCount();

    colStats.put(intColName, stats1);
    colStats.put(realColName, stats2);

    EpInfo result =
        AbstractRowBuffer.buildEpInfoFromStats(
            2, colStats, !enableIcebergStreaming, enableIcebergStreaming);
    Map<String, FileColumnProperties> columnResults = result.getColumnEps();
    Assert.assertEquals(2, columnResults.keySet().size());

    FileColumnProperties intColumnResult = columnResults.get(intColName);
    Assert.assertEquals(enableIcebergStreaming ? 0 : -1, intColumnResult.getDistinctValues());
    Assert.assertEquals(
        FileColumnProperties.DEFAULT_MIN_MAX_INT_VAL_FOR_EP, intColumnResult.getMinIntValue());
    Assert.assertEquals(
        FileColumnProperties.DEFAULT_MIN_MAX_INT_VAL_FOR_EP, intColumnResult.getMaxIntValue());
    Assert.assertEquals(1, intColumnResult.getNullCount());
    Assert.assertEquals(0, intColumnResult.getMaxLength());

    FileColumnProperties realColumnResult = columnResults.get(realColName);
    Assert.assertEquals(enableIcebergStreaming ? 0 : -1, intColumnResult.getDistinctValues());
    Assert.assertEquals(
        FileColumnProperties.DEFAULT_MIN_MAX_REAL_VAL_FOR_EP, realColumnResult.getMinRealValue());
    Assert.assertEquals(
        FileColumnProperties.DEFAULT_MIN_MAX_REAL_VAL_FOR_EP, realColumnResult.getMaxRealValue());
    Assert.assertEquals(1, realColumnResult.getNullCount());
    Assert.assertEquals(0, realColumnResult.getMaxLength());
  }

  @Test
  public void testInvalidEPInfo() {
    Map<String, RowBufferStats> colStats = new HashMap<>();

    RowBufferStats stats1 =
        new RowBufferStats(
            "intColumn",
            Types.optional(PrimitiveType.PrimitiveTypeName.INT32).id(1).named("intColumn"),
            enableIcebergStreaming,
            enableIcebergStreaming);
    stats1.addIntValue(BigInteger.valueOf(2));
    stats1.addIntValue(BigInteger.valueOf(10));
    stats1.addIntValue(BigInteger.valueOf(1));

    RowBufferStats stats2 =
        new RowBufferStats(
            "strColumn",
            Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).id(2).named("strColumn"),
            enableIcebergStreaming,
            enableIcebergStreaming);
    stats2.addStrValue("alice");
    stats2.incCurrentNullCount();
    stats2.incCurrentNullCount();

    colStats.put("intColumn", stats1);
    colStats.put("strColumn", stats2);

    try {
      AbstractRowBuffer.buildEpInfoFromStats(
          1, colStats, !enableIcebergStreaming, enableIcebergStreaming);
      fail("should fail when row count is smaller than null count.");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.INTERNAL_ERROR.getMessageCode(), e.getVendorCode());
    }
  }

  @Test
  public void testE2E() {
    testE2EHelper(this.rowBufferOnErrorAbort);
    testE2EHelper(this.rowBufferOnErrorContinue);
    testE2EHelper(this.rowBufferOnErrorSkipBatch);
  }

  private void testE2EHelper(AbstractRowBuffer<?> rowBuffer) {
    Map<String, Object> row1 = new HashMap<>();
    row1.put("\"colTinyInt\"", (byte) 10);
    row1.put("colTinyInt", (byte) 1);
    row1.put("colSmallInt", (short) 2);
    row1.put("colInt", 3);
    row1.put("colBigInt", 4L);
    row1.put("colDecimal", 4);
    row1.put("colChar", "2");

    InsertValidationResponse response =
        rowBuffer.insertRows(Collections.singletonList(row1), null, null);
    Assert.assertFalse(response.hasErrors());

    Assert.assertEquals((byte) 10, rowBuffer.getVectorValueAt("colTinyInt", 0));
    Assert.assertEquals((byte) 1, rowBuffer.getVectorValueAt("COLTINYINT", 0));
    Assert.assertEquals((short) 2, rowBuffer.getVectorValueAt("COLSMALLINT", 0));
    Assert.assertEquals(3, rowBuffer.getVectorValueAt("COLINT", 0));
    Assert.assertEquals(4L, rowBuffer.getVectorValueAt("COLBIGINT", 0));
    Assert.assertEquals(new BigDecimal("4.00"), rowBuffer.getVectorValueAt("COLDECIMAL", 0));
    Assert.assertEquals("2", rowBuffer.getVectorValueAt("COLCHAR", 0));
  }

  @Test
  public void testE2ETimestampErrors() {
    testE2ETimestampErrorsHelper(this.rowBufferOnErrorAbort);
    testE2ETimestampErrorsHelper(this.rowBufferOnErrorContinue);
    testE2ETimestampErrorsHelper(this.rowBufferOnErrorSkipBatch);
  }

  private void testE2ETimestampErrorsHelper(AbstractRowBuffer<?> innerBuffer) {

    ColumnMetadata colTimestampLtzSB16 = new ColumnMetadata();
    colTimestampLtzSB16.setOrdinal(1);
    colTimestampLtzSB16.setName("COLTIMESTAMPLTZ_SB16");
    colTimestampLtzSB16.setPhysicalType("SB16");
    colTimestampLtzSB16.setNullable(false);
    colTimestampLtzSB16.setLogicalType("TIMESTAMP_LTZ");
    colTimestampLtzSB16.setScale(6);

    innerBuffer.setupSchema(Collections.singletonList(colTimestampLtzSB16));

    Map<String, Object> row = new HashMap<>();
    row.put("COLTIMESTAMPLTZ_SB8", "1621899220");
    row.put("COLTIMESTAMPLTZ_SB16", "1621899220.1234567");

    if (innerBuffer.onErrorOption == OpenChannelRequest.OnErrorOption.CONTINUE) {
      InsertValidationResponse response =
          innerBuffer.insertRows(Collections.singletonList(row), null, null);
      Assert.assertTrue(response.hasErrors());
      Assert.assertEquals(
          ErrorCode.INVALID_FORMAT_ROW.getMessageCode(),
          response.getInsertErrors().get(0).getException().getVendorCode());
    } else {
      try {
        innerBuffer.insertRows(Collections.singletonList(row), null, null);
      } catch (SFException e) {
        Assert.assertEquals(ErrorCode.INVALID_FORMAT_ROW.getMessageCode(), e.getVendorCode());
      }
    }
  }

  @Test
  public void testStatsE2E() {
    testStatsE2EHelper(this.rowBufferOnErrorAbort);
    testStatsE2EHelper(this.rowBufferOnErrorContinue);
    testStatsE2EHelper(this.rowBufferOnErrorSkipBatch);
  }

  private void testStatsE2EHelper(AbstractRowBuffer<?> rowBuffer) {
    Map<String, Object> row1 = new HashMap<>();
    row1.put("\"colTinyInt\"", (byte) 10);
    row1.put("colTinyInt", (byte) 1);
    row1.put("colSmallInt", (short) 2);
    row1.put("colInt", 3);
    row1.put("colBigInt", 4L);
    row1.put("colDecimal", 5);
    row1.put("colChar", "2");

    Map<String, Object> row2 = new HashMap<>();
    row2.put("\"colTinyInt\"", (byte) 11);
    row2.put("colTinyInt", (byte) 1);
    row2.put("colSmallInt", (short) 3);
    row2.put("colInt", null);
    row2.put("colBigInt", 40L);
    row2.put("colDecimal", 4);
    row2.put("colChar", "alice");

    final String filename = "testStatsE2EHelper_streaming.bdec";
    InsertValidationResponse response = rowBuffer.insertRows(Arrays.asList(row1, row2), null, null);
    Assert.assertFalse(response.hasErrors());
    ChannelData<?> result = rowBuffer.flush();
    Map<String, RowBufferStats> columnEpStats = result.getColumnEps();

    Assert.assertEquals(
        BigInteger.valueOf(11),
        columnEpStats.get(enableIcebergStreaming ? "1" : "colTinyInt").getCurrentMaxIntValue());
    Assert.assertEquals(
        BigInteger.valueOf(10),
        columnEpStats.get(enableIcebergStreaming ? "1" : "colTinyInt").getCurrentMinIntValue());
    Assert.assertEquals(
        0, columnEpStats.get(enableIcebergStreaming ? "1" : "colTinyInt").getCurrentNullCount());
    Assert.assertEquals(
        enableIcebergStreaming ? 2 : -1,
        columnEpStats.get(enableIcebergStreaming ? "1" : "colTinyInt").getDistinctValues());

    Assert.assertEquals(
        BigInteger.valueOf(1),
        columnEpStats.get(enableIcebergStreaming ? "2" : "COLTINYINT").getCurrentMaxIntValue());
    Assert.assertEquals(
        BigInteger.valueOf(1),
        columnEpStats.get(enableIcebergStreaming ? "2" : "COLTINYINT").getCurrentMinIntValue());
    Assert.assertEquals(
        0, columnEpStats.get(enableIcebergStreaming ? "2" : "COLTINYINT").getCurrentNullCount());
    Assert.assertEquals(
        enableIcebergStreaming ? 1 : -1,
        columnEpStats.get(enableIcebergStreaming ? "2" : "COLTINYINT").getDistinctValues());

    Assert.assertEquals(
        BigInteger.valueOf(3),
        columnEpStats.get(enableIcebergStreaming ? "3" : "COLSMALLINT").getCurrentMaxIntValue());
    Assert.assertEquals(
        BigInteger.valueOf(2),
        columnEpStats.get(enableIcebergStreaming ? "3" : "COLSMALLINT").getCurrentMinIntValue());
    Assert.assertEquals(
        0, columnEpStats.get(enableIcebergStreaming ? "3" : "COLSMALLINT").getCurrentNullCount());
    Assert.assertEquals(
        enableIcebergStreaming ? 2 : -1,
        columnEpStats.get(enableIcebergStreaming ? "3" : "COLSMALLINT").getDistinctValues());

    Assert.assertEquals(
        BigInteger.valueOf(3),
        columnEpStats.get(enableIcebergStreaming ? "4" : "COLINT").getCurrentMaxIntValue());
    Assert.assertEquals(
        BigInteger.valueOf(3),
        columnEpStats.get(enableIcebergStreaming ? "4" : "COLINT").getCurrentMinIntValue());
    Assert.assertEquals(
        1L, columnEpStats.get(enableIcebergStreaming ? "4" : "COLINT").getCurrentNullCount());
    Assert.assertEquals(
        enableIcebergStreaming ? 1 : -1,
        columnEpStats.get(enableIcebergStreaming ? "4" : "COLINT").getDistinctValues());

    Assert.assertEquals(
        BigInteger.valueOf(40),
        columnEpStats.get(enableIcebergStreaming ? "5" : "COLBIGINT").getCurrentMaxIntValue());
    Assert.assertEquals(
        BigInteger.valueOf(4),
        columnEpStats.get(enableIcebergStreaming ? "5" : "COLBIGINT").getCurrentMinIntValue());
    Assert.assertEquals(
        0, columnEpStats.get(enableIcebergStreaming ? "5" : "COLBIGINT").getCurrentNullCount());
    Assert.assertEquals(
        enableIcebergStreaming ? 2 : -1,
        columnEpStats.get(enableIcebergStreaming ? "5" : "COLBIGINT").getDistinctValues());

    Assert.assertArrayEquals(
        "2".getBytes(StandardCharsets.UTF_8),
        columnEpStats.get(enableIcebergStreaming ? "7" : "COLCHAR").getCurrentMinStrValue());
    Assert.assertArrayEquals(
        "alice".getBytes(StandardCharsets.UTF_8),
        columnEpStats.get(enableIcebergStreaming ? "7" : "COLCHAR").getCurrentMaxStrValue());
    Assert.assertEquals(
        0, columnEpStats.get(enableIcebergStreaming ? "7" : "COLCHAR").getCurrentNullCount());
    Assert.assertEquals(
        enableIcebergStreaming ? 2 : -1,
        columnEpStats.get(enableIcebergStreaming ? "7" : "COLCHAR").getDistinctValues());

    // Confirm we reset
    ChannelData<?> resetResults = rowBuffer.flush();
    Assert.assertNull(resetResults);
  }

  @Test
  public void testStatsE2ETimestamp() {
    testStatsE2ETimestampHelper(OpenChannelRequest.OnErrorOption.ABORT);
    testStatsE2ETimestampHelper(OpenChannelRequest.OnErrorOption.CONTINUE);
    testStatsE2ETimestampHelper(OpenChannelRequest.OnErrorOption.SKIP_BATCH);
  }

  private void testStatsE2ETimestampHelper(OpenChannelRequest.OnErrorOption onErrorOption) {
    AbstractRowBuffer<?> innerBuffer = createTestBuffer(onErrorOption);

    ColumnMetadata colTimestampLtzSB8 = new ColumnMetadata();
    colTimestampLtzSB8.setOrdinal(1);
    colTimestampLtzSB8.setName("COLTIMESTAMPLTZ_SB8");
    colTimestampLtzSB8.setPhysicalType("SB8");
    colTimestampLtzSB8.setNullable(true);
    colTimestampLtzSB8.setLogicalType("TIMESTAMP_LTZ");
    colTimestampLtzSB8.setScale(0);

    ColumnMetadata colTimestampLtzSB16 = new ColumnMetadata();
    colTimestampLtzSB16.setOrdinal(2);
    colTimestampLtzSB16.setName("COLTIMESTAMPLTZ_SB16");
    colTimestampLtzSB16.setPhysicalType("SB16");
    colTimestampLtzSB16.setNullable(true);
    colTimestampLtzSB16.setLogicalType("TIMESTAMP_LTZ");
    colTimestampLtzSB16.setScale(9);

    ColumnMetadata colTimestampLtzSB16Scale6 = new ColumnMetadata();
    colTimestampLtzSB16Scale6.setOrdinal(3);
    colTimestampLtzSB16Scale6.setName("COLTIMESTAMPLTZ_SB16_SCALE6");
    colTimestampLtzSB16Scale6.setPhysicalType("SB16");
    colTimestampLtzSB16Scale6.setNullable(true);
    colTimestampLtzSB16Scale6.setLogicalType("TIMESTAMP_LTZ");
    colTimestampLtzSB16Scale6.setScale(6);

    if (enableIcebergStreaming) {
      colTimestampLtzSB8.setSourceIcebergDataType("\"timestamptz\"");
      colTimestampLtzSB16.setSourceIcebergDataType("\"timestamptz\"");
      colTimestampLtzSB16Scale6.setSourceIcebergDataType("\"timestamptz\"");
    }

    innerBuffer.setupSchema(
        Arrays.asList(colTimestampLtzSB8, colTimestampLtzSB16, colTimestampLtzSB16Scale6));

    Map<String, Object> row1 = new HashMap<>();
    row1.put("COLTIMESTAMPLTZ_SB8", "1621899220");
    row1.put("COLTIMESTAMPLTZ_SB16", "1621899220123456789");
    row1.put("COLTIMESTAMPLTZ_SB16_SCALE6", "1621899220123456");

    Map<String, Object> row2 = new HashMap<>();
    row2.put("COLTIMESTAMPLTZ_SB8", "1621899221");
    row2.put("COLTIMESTAMPLTZ_SB16", "1621899220223456789");
    row2.put("COLTIMESTAMPLTZ_SB16_SCALE6", "1621899220123457");

    Map<String, Object> row3 = new HashMap<>();
    row3.put("COLTIMESTAMPLTZ_SB8", null);
    row3.put("COLTIMESTAMPLTZ_SB16", null);
    row3.put("COLTIMESTAMPLTZ_SB16_SCALE6", null);

    InsertValidationResponse response =
        innerBuffer.insertRows(Arrays.asList(row1, row2, row3), null, null);
    Assert.assertFalse(response.hasErrors());
    ChannelData<?> result = innerBuffer.flush();
    Assert.assertEquals(3, result.getRowCount());

    Assert.assertEquals(
        BigInteger.valueOf(1621899220 * (enableIcebergStreaming ? 1000000L : 1)),
        result
            .getColumnEps()
            .get(enableIcebergStreaming ? "1" : "COLTIMESTAMPLTZ_SB8")
            .getCurrentMinIntValue());
    Assert.assertEquals(
        BigInteger.valueOf(1621899221 * (enableIcebergStreaming ? 1000000L : 1)),
        result
            .getColumnEps()
            .get(enableIcebergStreaming ? "1" : "COLTIMESTAMPLTZ_SB8")
            .getCurrentMaxIntValue());

    /* Iceberg only supports microsecond precision for TIMESTAMP_LTZ */
    if (!enableIcebergStreaming) {
      Assert.assertEquals(
          new BigInteger("1621899220123456789"),
          result.getColumnEps().get("COLTIMESTAMPLTZ_SB16").getCurrentMinIntValue());
      Assert.assertEquals(
          new BigInteger("1621899220223456789"),
          result.getColumnEps().get("COLTIMESTAMPLTZ_SB16").getCurrentMaxIntValue());
      Assert.assertEquals(
          1, result.getColumnEps().get("COLTIMESTAMPLTZ_SB16").getCurrentNullCount());
    }

    Assert.assertEquals(
        new BigInteger("1621899220123456"),
        result
            .getColumnEps()
            .get(enableIcebergStreaming ? "3" : "COLTIMESTAMPLTZ_SB16_SCALE6")
            .getCurrentMinIntValue());
    Assert.assertEquals(
        new BigInteger("1621899220123457"),
        result
            .getColumnEps()
            .get(enableIcebergStreaming ? "3" : "COLTIMESTAMPLTZ_SB16_SCALE6")
            .getCurrentMaxIntValue());

    Assert.assertEquals(
        1,
        result
            .getColumnEps()
            .get(enableIcebergStreaming ? "1" : "COLTIMESTAMPLTZ_SB8")
            .getCurrentNullCount());
    Assert.assertEquals(
        1,
        result
            .getColumnEps()
            .get(enableIcebergStreaming ? "1" : "COLTIMESTAMPLTZ_SB16_SCALE6")
            .getCurrentNullCount());
  }

  @Test
  public void testE2EDate() {
    testE2EDateHelper(OpenChannelRequest.OnErrorOption.ABORT);
    testE2EDateHelper(OpenChannelRequest.OnErrorOption.CONTINUE);
    testE2EDateHelper(OpenChannelRequest.OnErrorOption.SKIP_BATCH);
  }

  private void testE2EDateHelper(OpenChannelRequest.OnErrorOption onErrorOption) {
    AbstractRowBuffer<?> innerBuffer = createTestBuffer(onErrorOption);

    ColumnMetadata colDate = new ColumnMetadata();
    colDate.setOrdinal(1);
    colDate.setName("COLDATE");
    colDate.setPhysicalType("SB8");
    colDate.setNullable(true);
    colDate.setLogicalType("DATE");
    colDate.setScale(0);

    innerBuffer.setupSchema(Collections.singletonList(colDate));

    Map<String, Object> row1 = new HashMap<>();
    row1.put("COLDATE", String.valueOf(18772 * 24 * 60 * 60 * 1000L + 1));

    Map<String, Object> row2 = new HashMap<>();
    row2.put("COLDATE", String.valueOf(18773 * 24 * 60 * 60 * 1000L + 1));

    Map<String, Object> row3 = new HashMap<>();
    row3.put("COLDATE", null);

    InsertValidationResponse response =
        innerBuffer.insertRows(Arrays.asList(row1, row2, row3), null, null);
    Assert.assertFalse(response.hasErrors());

    // Check data was inserted into the buffer correctly
    Assert.assertEquals(18772, innerBuffer.getVectorValueAt("COLDATE", 0));
    Assert.assertEquals(18773, innerBuffer.getVectorValueAt("COLDATE", 1));
    Assert.assertNull(innerBuffer.getVectorValueAt("COLDATE", 2));

    // Check stats generation
    ChannelData<?> result = innerBuffer.flush();
    Assert.assertEquals(3, result.getRowCount());

    Assert.assertEquals(
        BigInteger.valueOf(18772),
        result
            .getColumnEps()
            .get(enableIcebergStreaming ? "1" : "COLDATE")
            .getCurrentMinIntValue());
    Assert.assertEquals(
        BigInteger.valueOf(18773),
        result
            .getColumnEps()
            .get(enableIcebergStreaming ? "1" : "COLDATE")
            .getCurrentMaxIntValue());

    Assert.assertEquals(
        1,
        result.getColumnEps().get(enableIcebergStreaming ? "1" : "COLDATE").getCurrentNullCount());
  }

  @Test
  public void testE2ETime() {
    testE2ETimeHelper(OpenChannelRequest.OnErrorOption.ABORT);
    testE2ETimeHelper(OpenChannelRequest.OnErrorOption.CONTINUE);
    testE2ETimeHelper(OpenChannelRequest.OnErrorOption.SKIP_BATCH);
  }

  private void testE2ETimeHelper(OpenChannelRequest.OnErrorOption onErrorOption) {
    AbstractRowBuffer<?> innerBuffer = createTestBuffer(onErrorOption);

    ColumnMetadata colTimeSB4 = new ColumnMetadata();
    colTimeSB4.setOrdinal(1);
    colTimeSB4.setName("COLTIMESB4");
    colTimeSB4.setPhysicalType("SB4");
    colTimeSB4.setNullable(true);
    colTimeSB4.setLogicalType("TIME");
    colTimeSB4.setScale(0);

    ColumnMetadata colTimeSB8 = new ColumnMetadata();
    colTimeSB8.setOrdinal(2);
    colTimeSB8.setName("COLTIMESB8");
    colTimeSB8.setPhysicalType("SB8");
    colTimeSB8.setNullable(true);
    colTimeSB8.setLogicalType("TIME");
    colTimeSB8.setScale(3);

    if (enableIcebergStreaming) {
      colTimeSB4.setSourceIcebergDataType("\"time\"");
      colTimeSB8.setSourceIcebergDataType("\"time\"");
    }

    innerBuffer.setupSchema(Arrays.asList(colTimeSB4, colTimeSB8));

    Map<String, Object> row1 = new HashMap<>();
    row1.put("COLTIMESB4", "10:00:00");
    row1.put("COLTIMESB8", "10:00:00.123");

    Map<String, Object> row2 = new HashMap<>();
    row2.put("COLTIMESB4", "11:15:00.000");
    row2.put("COLTIMESB8", "11:15:00.456");

    Map<String, Object> row3 = new HashMap<>();
    row3.put("COLTIMESB4", null);
    row3.put("COLTIMESB8", null);

    InsertValidationResponse response =
        innerBuffer.insertRows(Arrays.asList(row1, row2, row3), null, null);
    Assert.assertFalse(response.hasErrors());

    // Check data was inserted into the buffer correctly
    if (enableIcebergStreaming) {
      Assert.assertEquals(10 * 60 * 60 * 1000000L, innerBuffer.getVectorValueAt("COLTIMESB4", 0));
      Assert.assertEquals(
          (11 * 60 * 60 + 15 * 60) * 1000000L, innerBuffer.getVectorValueAt("COLTIMESB4", 1));
      Assert.assertEquals(
          (10 * 60 * 60 * 1000L + 123) * 1000L, innerBuffer.getVectorValueAt("COLTIMESB8", 0));
      Assert.assertEquals(
          (11 * 60 * 60 * 1000L + 15 * 60 * 1000 + 456) * 1000L,
          innerBuffer.getVectorValueAt("COLTIMESB8", 1));
    } else {
      Assert.assertEquals(10 * 60 * 60, innerBuffer.getVectorValueAt("COLTIMESB4", 0));
      Assert.assertEquals(11 * 60 * 60 + 15 * 60, innerBuffer.getVectorValueAt("COLTIMESB4", 1));
      Assert.assertEquals(
          10 * 60 * 60 * 1000L + 123, innerBuffer.getVectorValueAt("COLTIMESB8", 0));
      Assert.assertEquals(
          11 * 60 * 60 * 1000L + 15 * 60 * 1000 + 456,
          innerBuffer.getVectorValueAt("COLTIMESB8", 1));
    }

    Assert.assertNull(innerBuffer.getVectorValueAt("COLTIMESB4", 2));
    Assert.assertNull(innerBuffer.getVectorValueAt("COLTIMESB8", 2));

    // Check stats generation
    ChannelData<?> result = innerBuffer.flush();
    Assert.assertEquals(3, result.getRowCount());

    if (enableIcebergStreaming) {
      Assert.assertEquals(
          BigInteger.valueOf(10 * 60 * 60 * 1000000L),
          result
              .getColumnEps()
              .get(enableIcebergStreaming ? "1" : "COLTIMESB4")
              .getCurrentMinIntValue());
      Assert.assertEquals(
          BigInteger.valueOf((11 * 60 * 60 + 15 * 60) * 1000000L),
          result
              .getColumnEps()
              .get(enableIcebergStreaming ? "1" : "COLTIMESB4")
              .getCurrentMaxIntValue());
      Assert.assertEquals(
          1,
          result
              .getColumnEps()
              .get(enableIcebergStreaming ? "1" : "COLTIMESB4")
              .getCurrentNullCount());

      Assert.assertEquals(
          BigInteger.valueOf((10 * 60 * 60 * 1000L + 123) * 1000L),
          result
              .getColumnEps()
              .get(enableIcebergStreaming ? "2" : "COLTIMESB8")
              .getCurrentMinIntValue());
      Assert.assertEquals(
          BigInteger.valueOf((11 * 60 * 60 * 1000L + 15 * 60 * 1000 + 456) * 1000L),
          result
              .getColumnEps()
              .get(enableIcebergStreaming ? "2" : "COLTIMESB8")
              .getCurrentMaxIntValue());
      Assert.assertEquals(
          1,
          result
              .getColumnEps()
              .get(enableIcebergStreaming ? "2" : "COLTIMESB8")
              .getCurrentNullCount());
    } else {
      Assert.assertEquals(
          BigInteger.valueOf(10 * 60 * 60),
          result
              .getColumnEps()
              .get(enableIcebergStreaming ? "1" : "COLTIMESB4")
              .getCurrentMinIntValue());
      Assert.assertEquals(
          BigInteger.valueOf(11 * 60 * 60 + 15 * 60),
          result
              .getColumnEps()
              .get(enableIcebergStreaming ? "1" : "COLTIMESB4")
              .getCurrentMaxIntValue());
      Assert.assertEquals(
          1,
          result
              .getColumnEps()
              .get(enableIcebergStreaming ? "1" : "COLTIMESB4")
              .getCurrentNullCount());

      Assert.assertEquals(
          BigInteger.valueOf(10 * 60 * 60 * 1000L + 123),
          result
              .getColumnEps()
              .get(enableIcebergStreaming ? "2" : "COLTIMESB8")
              .getCurrentMinIntValue());
      Assert.assertEquals(
          BigInteger.valueOf(11 * 60 * 60 * 1000L + 15 * 60 * 1000 + 456),
          result
              .getColumnEps()
              .get(enableIcebergStreaming ? "2" : "COLTIMESB8")
              .getCurrentMaxIntValue());
      Assert.assertEquals(
          1,
          result
              .getColumnEps()
              .get(enableIcebergStreaming ? "2" : "COLTIMESB8")
              .getCurrentNullCount());
    }
  }

  @Test
  public void testMaxInsertRowsBatchSize() {
    testMaxInsertRowsBatchSizeHelper(OpenChannelRequest.OnErrorOption.ABORT);
    testMaxInsertRowsBatchSizeHelper(OpenChannelRequest.OnErrorOption.SKIP_BATCH);
  }

  private void testMaxInsertRowsBatchSizeHelper(OpenChannelRequest.OnErrorOption onErrorOption) {
    AbstractRowBuffer<?> innerBuffer = createTestBuffer(onErrorOption);
    ColumnMetadata colBinary = new ColumnMetadata();
    colBinary.setOrdinal(1);
    colBinary.setName("COLBINARY");
    colBinary.setPhysicalType("LOB");
    colBinary.setNullable(true);
    colBinary.setLogicalType("BINARY");
    colBinary.setLength(8 * 1024 * 1024);
    colBinary.setByteLength(8 * 1024 * 1024);
    if (enableIcebergStreaming) {
      colBinary.setSourceIcebergDataType("\"binary\"");
    }

    byte[] arr = new byte[8 * 1024 * 1024];
    innerBuffer.setupSchema(Collections.singletonList(colBinary));
    List<Map<String, Object>> rows = new ArrayList<>();
    for (int i = 0; i < 15; i++) {
      rows.add(Collections.singletonMap("COLBINARY", arr));
    }

    // Insert rows should succeed
    rows.add(Collections.singletonMap("COLBINARY", arr));
    innerBuffer.insertRows(rows, "", "");
  }

  @Test
  public void testNullableCheck() {
    testNullableCheckHelper(OpenChannelRequest.OnErrorOption.ABORT);
    testNullableCheckHelper(OpenChannelRequest.OnErrorOption.CONTINUE);
    testNullableCheckHelper(OpenChannelRequest.OnErrorOption.SKIP_BATCH);
  }

  private void testNullableCheckHelper(OpenChannelRequest.OnErrorOption onErrorOption) {
    AbstractRowBuffer<?> innerBuffer = createTestBuffer(onErrorOption);

    ColumnMetadata colBoolean = new ColumnMetadata();
    colBoolean.setOrdinal(1);
    colBoolean.setName("COLBOOLEAN");
    colBoolean.setPhysicalType("SB1");
    colBoolean.setNullable(false);
    colBoolean.setLogicalType("BOOLEAN");
    colBoolean.setScale(0);

    innerBuffer.setupSchema(Collections.singletonList(colBoolean));
    Map<String, Object> row = new HashMap<>();
    row.put("COLBOOLEAN", true);

    InsertValidationResponse response =
        innerBuffer.insertRows(Collections.singletonList(row), "1", "1");
    Assert.assertFalse(response.hasErrors());

    row.put("COLBOOLEAN", null);
    if (innerBuffer.onErrorOption == OpenChannelRequest.OnErrorOption.CONTINUE) {
      response = innerBuffer.insertRows(Collections.singletonList(row), "1", "1");
      Assert.assertTrue(response.hasErrors());
      Assert.assertEquals(
          ErrorCode.INVALID_FORMAT_ROW.getMessageCode(),
          response.getInsertErrors().get(0).getException().getVendorCode());
    } else {
      try {
        innerBuffer.insertRows(Collections.singletonList(row), "1", "1");
      } catch (SFException e) {
        Assert.assertEquals(ErrorCode.INVALID_FORMAT_ROW.getMessageCode(), e.getVendorCode());
      }
    }
  }

  @Test
  public void testMissingColumnCheck() {
    testMissingColumnCheckHelper(OpenChannelRequest.OnErrorOption.ABORT);
    testMissingColumnCheckHelper(OpenChannelRequest.OnErrorOption.CONTINUE);
    testMissingColumnCheckHelper(OpenChannelRequest.OnErrorOption.SKIP_BATCH);
  }

  private void testMissingColumnCheckHelper(OpenChannelRequest.OnErrorOption onErrorOption) {
    AbstractRowBuffer<?> innerBuffer = createTestBuffer(onErrorOption);

    ColumnMetadata colBoolean = new ColumnMetadata();
    colBoolean.setOrdinal(1);
    colBoolean.setName("COLBOOLEAN");
    colBoolean.setPhysicalType("SB1");
    colBoolean.setNullable(false);
    colBoolean.setLogicalType("BOOLEAN");
    colBoolean.setScale(0);

    ColumnMetadata colBoolean2 = new ColumnMetadata();
    colBoolean2.setOrdinal(2);
    colBoolean2.setName("COLBOOLEAN2");
    colBoolean2.setPhysicalType("SB1");
    colBoolean2.setNullable(true);
    colBoolean2.setLogicalType("BOOLEAN");
    colBoolean2.setScale(0);

    innerBuffer.setupSchema(Arrays.asList(colBoolean, colBoolean2));
    Map<String, Object> row = new HashMap<>();
    row.put("COLBOOLEAN", true);

    InsertValidationResponse response =
        innerBuffer.insertRows(Collections.singletonList(row), "1", "1");
    Assert.assertFalse(response.hasErrors());

    Map<String, Object> row2 = new HashMap<>();
    row2.put("COLBOOLEAN2", true);
    if (innerBuffer.onErrorOption == OpenChannelRequest.OnErrorOption.CONTINUE) {
      response = innerBuffer.insertRows(Collections.singletonList(row2), "1", "2");
      Assert.assertTrue(response.hasErrors());
      InsertValidationResponse.InsertError error = response.getInsertErrors().get(0);
      Assert.assertEquals(
          ErrorCode.INVALID_FORMAT_ROW.getMessageCode(), error.getException().getVendorCode());
      Assert.assertEquals(
          Collections.singletonList("COLBOOLEAN"), error.getMissingNotNullColNames());
    } else {
      try {
        innerBuffer.insertRows(Collections.singletonList(row2), "1", "2");
      } catch (SFException e) {
        Assert.assertEquals(ErrorCode.INVALID_FORMAT_ROW.getMessageCode(), e.getVendorCode());
      }
    }
  }

  @Test
  public void testExtraColumnsCheck() {
    AbstractRowBuffer<?> innerBuffer = createTestBuffer(OpenChannelRequest.OnErrorOption.CONTINUE);

    ColumnMetadata colBoolean = new ColumnMetadata();
    colBoolean.setOrdinal(1);
    colBoolean.setName("COLBOOLEAN1");
    colBoolean.setPhysicalType("SB1");
    colBoolean.setNullable(false);
    colBoolean.setLogicalType("BOOLEAN");
    colBoolean.setScale(0);

    innerBuffer.setupSchema(Collections.singletonList(colBoolean));
    Map<String, Object> row = new HashMap<>();
    row.put("COLBOOLEAN1", true);
    row.put("COLBOOLEAN2", true);
    row.put("COLBOOLEAN3", true);

    InsertValidationResponse response =
        innerBuffer.insertRows(Collections.singletonList(row), "1", "1");
    Assert.assertTrue(response.hasErrors());
    InsertValidationResponse.InsertError error = response.getInsertErrors().get(0);
    Assert.assertEquals(
        ErrorCode.INVALID_FORMAT_ROW.getMessageCode(), error.getException().getVendorCode());
    Assert.assertEquals(Arrays.asList("COLBOOLEAN3", "COLBOOLEAN2"), error.getExtraColNames());
  }

  @Test
  public void testFailureHalfwayThroughColumnProcessing() {
    doTestFailureHalfwayThroughColumnProcessing(OpenChannelRequest.OnErrorOption.CONTINUE);
    doTestFailureHalfwayThroughColumnProcessing(OpenChannelRequest.OnErrorOption.ABORT);
    doTestFailureHalfwayThroughColumnProcessing(OpenChannelRequest.OnErrorOption.SKIP_BATCH);
  }

  private void doTestFailureHalfwayThroughColumnProcessing(
      OpenChannelRequest.OnErrorOption onErrorOption) {
    AbstractRowBuffer<?> innerBuffer = createTestBuffer(onErrorOption);

    ColumnMetadata colVarchar1 = new ColumnMetadata();
    colVarchar1.setOrdinal(1);
    colVarchar1.setName("COLVARCHAR1");
    colVarchar1.setPhysicalType("LOB");
    colVarchar1.setNullable(true);
    colVarchar1.setLogicalType("TEXT");
    colVarchar1.setLength(1000);

    ColumnMetadata colVarchar2 = new ColumnMetadata();
    colVarchar2.setOrdinal(2);
    colVarchar2.setName("COLVARCHAR2");
    colVarchar2.setPhysicalType("LOB");
    colVarchar2.setNullable(true);
    colVarchar2.setLogicalType("TEXT");
    colVarchar2.setLength(1000);

    ColumnMetadata colBoolean = new ColumnMetadata();
    colBoolean.setOrdinal(3);
    colBoolean.setName("COLBOOLEAN1");
    colBoolean.setPhysicalType("SB1");
    colBoolean.setNullable(true);
    colBoolean.setLogicalType("BOOLEAN");
    colBoolean.setScale(0);

    innerBuffer.setupSchema(Arrays.asList(colVarchar1, colVarchar2, colBoolean));

    LinkedHashMap<String, Object> row1 = new LinkedHashMap<>();
    row1.put("COLVARCHAR1", null);
    row1.put("COLVARCHAR2", "X");
    row1.put("COLBOOLEAN1", "falze"); // will fail validation

    LinkedHashMap<String, Object> row2 = new LinkedHashMap<>();
    row2.put("COLVARCHAR1", "A");
    row2.put("COLVARCHAR2", null);
    row2.put("COLBOOLEAN1", "falze"); // will fail validation

    LinkedHashMap<String, Object> row3 = new LinkedHashMap<>();
    row3.put("COLVARCHAR1", "c");
    row3.put("COLVARCHAR2", "d");
    row3.put("COLBOOLEAN1", "true");

    for (Map<String, Object> row : Arrays.asList(row1, row2, row3)) {
      try {
        innerBuffer.insertRows(Collections.singletonList(row), "", "");
      } catch (Exception ignored) {
        // we ignore exceptions, for ABORT option there will be some, but we don't care in this test
      }
    }

    ChannelData<?> channelData = innerBuffer.flush();
    RowBufferStats statsCol1 =
        channelData.getColumnEps().get(enableIcebergStreaming ? "1" : "COLVARCHAR1");
    RowBufferStats statsCol2 =
        channelData.getColumnEps().get(enableIcebergStreaming ? "2" : "COLVARCHAR2");
    RowBufferStats statsCol3 =
        channelData.getColumnEps().get(enableIcebergStreaming ? "3" : "COLBOOLEAN1");
    Assert.assertEquals(1, channelData.getRowCount());
    Assert.assertEquals(0, statsCol1.getCurrentNullCount());
    Assert.assertEquals(0, statsCol2.getCurrentNullCount());
    Assert.assertEquals(0, statsCol3.getCurrentNullCount());
    Assert.assertArrayEquals(
        "c".getBytes(StandardCharsets.UTF_8), statsCol1.getCurrentMinStrValue());
    Assert.assertArrayEquals(
        "c".getBytes(StandardCharsets.UTF_8), statsCol1.getCurrentMaxStrValue());
    Assert.assertArrayEquals(
        "d".getBytes(StandardCharsets.UTF_8), statsCol2.getCurrentMinStrValue());
    Assert.assertArrayEquals(
        "d".getBytes(StandardCharsets.UTF_8), statsCol2.getCurrentMaxStrValue());
    Assert.assertEquals(BigInteger.ONE, statsCol3.getCurrentMinIntValue());
    Assert.assertEquals(BigInteger.ONE, statsCol3.getCurrentMaxIntValue());
  }

  @Test
  public void testE2EBoolean() {
    testE2EBooleanHelper(OpenChannelRequest.OnErrorOption.ABORT);
    testE2EBooleanHelper(OpenChannelRequest.OnErrorOption.CONTINUE);
    testE2EBooleanHelper(OpenChannelRequest.OnErrorOption.SKIP_BATCH);
  }

  private void testE2EBooleanHelper(OpenChannelRequest.OnErrorOption onErrorOption) {
    AbstractRowBuffer<?> innerBuffer = createTestBuffer(onErrorOption);

    ColumnMetadata colBoolean = new ColumnMetadata();
    colBoolean.setOrdinal(1);
    colBoolean.setName("COLBOOLEAN");
    colBoolean.setPhysicalType("SB1");
    colBoolean.setNullable(true);
    colBoolean.setLogicalType("BOOLEAN");
    colBoolean.setScale(0);
    if (enableIcebergStreaming) {
      colBoolean.setSourceIcebergDataType("\"boolean\"");
    }

    innerBuffer.setupSchema(Collections.singletonList(colBoolean));

    Map<String, Object> row1 = new HashMap<>();
    row1.put("COLBOOLEAN", true);

    Map<String, Object> row2 = new HashMap<>();
    row2.put("COLBOOLEAN", false);

    Map<String, Object> row3 = new HashMap<>();
    row3.put("COLBOOLEAN", null);

    // innerBuffer.insertRows(Collections.singletonList(row1));
    InsertValidationResponse response =
        innerBuffer.insertRows(Arrays.asList(row1, row2, row3), null, null);
    Assert.assertFalse(response.hasErrors());

    // Check data was inserted into the buffer correctly
    Assert.assertEquals(true, innerBuffer.getVectorValueAt("COLBOOLEAN", 0));
    Assert.assertEquals(false, innerBuffer.getVectorValueAt("COLBOOLEAN", 1));
    Assert.assertNull(innerBuffer.getVectorValueAt("COLBOOLEAN", 2));

    // Check stats generation
    ChannelData<?> result = innerBuffer.flush();
    Assert.assertEquals(3, result.getRowCount());

    Assert.assertEquals(
        BigInteger.valueOf(0),
        result
            .getColumnEps()
            .get(enableIcebergStreaming ? "1" : "COLBOOLEAN")
            .getCurrentMinIntValue());
    Assert.assertEquals(
        BigInteger.valueOf(1),
        result
            .getColumnEps()
            .get(enableIcebergStreaming ? "1" : "COLBOOLEAN")
            .getCurrentMaxIntValue());
    Assert.assertEquals(
        1,
        result
            .getColumnEps()
            .get(enableIcebergStreaming ? "1" : "COLBOOLEAN")
            .getCurrentNullCount());
  }

  @Test
  public void testE2EBinary() {
    testE2EBinaryHelper(OpenChannelRequest.OnErrorOption.ABORT);
    testE2EBinaryHelper(OpenChannelRequest.OnErrorOption.CONTINUE);
    testE2EBinaryHelper(OpenChannelRequest.OnErrorOption.SKIP_BATCH);
  }

  private void testE2EBinaryHelper(OpenChannelRequest.OnErrorOption onErrorOption) {
    AbstractRowBuffer<?> innerBuffer = createTestBuffer(onErrorOption);

    ColumnMetadata colBinary = new ColumnMetadata();
    colBinary.setOrdinal(1);
    colBinary.setName("COLBINARY");
    colBinary.setPhysicalType("LOB");
    colBinary.setNullable(true);
    colBinary.setLogicalType("BINARY");
    colBinary.setLength(32);
    colBinary.setByteLength(256);
    colBinary.setScale(0);
    if (enableIcebergStreaming) {
      colBinary.setSourceIcebergDataType("\"binary\"");
    }

    innerBuffer.setupSchema(Collections.singletonList(colBinary));

    Map<String, Object> row1 = new HashMap<>();
    row1.put("COLBINARY", "Hello World".getBytes(StandardCharsets.UTF_8));

    Map<String, Object> row2 = new HashMap<>();
    row2.put("COLBINARY", "Honk Honk".getBytes(StandardCharsets.UTF_8));

    Map<String, Object> row3 = new HashMap<>();
    row3.put("COLBINARY", null);

    InsertValidationResponse response =
        innerBuffer.insertRows(Arrays.asList(row1, row2, row3), null, null);
    Assert.assertFalse(response.hasErrors());

    // Check data was inserted into the buffer correctly
    Assert.assertEquals(
        "Hello World",
        new String((byte[]) innerBuffer.getVectorValueAt("COLBINARY", 0), StandardCharsets.UTF_8));
    Assert.assertEquals(
        "Honk Honk",
        new String((byte[]) innerBuffer.getVectorValueAt("COLBINARY", 1), StandardCharsets.UTF_8));
    Assert.assertNull(innerBuffer.getVectorValueAt("COLBINARY", 2));

    // Check stats generation
    ChannelData<?> result = innerBuffer.flush();

    Assert.assertEquals(3, result.getRowCount());
    Assert.assertEquals(
        11L,
        result
            .getColumnEps()
            .get(enableIcebergStreaming ? "1" : "COLBINARY")
            .getCurrentMaxLength());
    Assert.assertArrayEquals(
        "Hello World".getBytes(StandardCharsets.UTF_8),
        result
            .getColumnEps()
            .get(enableIcebergStreaming ? "1" : "COLBINARY")
            .getCurrentMinStrValue());
    Assert.assertArrayEquals(
        "Honk Honk".getBytes(StandardCharsets.UTF_8),
        result
            .getColumnEps()
            .get(enableIcebergStreaming ? "1" : "COLBINARY")
            .getCurrentMaxStrValue());
    Assert.assertEquals(
        1,
        result
            .getColumnEps()
            .get(enableIcebergStreaming ? "1" : "COLBINARY")
            .getCurrentNullCount());
  }

  @Test
  public void testE2EReal() {
    testE2ERealHelper(OpenChannelRequest.OnErrorOption.ABORT);
    testE2ERealHelper(OpenChannelRequest.OnErrorOption.CONTINUE);
    testE2ERealHelper(OpenChannelRequest.OnErrorOption.SKIP_BATCH);
  }

  private void testE2ERealHelper(OpenChannelRequest.OnErrorOption onErrorOption) {
    AbstractRowBuffer<?> innerBuffer = createTestBuffer(onErrorOption);

    ColumnMetadata colReal = new ColumnMetadata();
    colReal.setOrdinal(1);
    colReal.setName("COLREAL");
    colReal.setPhysicalType("SB16");
    colReal.setNullable(true);
    colReal.setLogicalType("REAL");
    colReal.setScale(0);

    innerBuffer.setupSchema(Collections.singletonList(colReal));

    Map<String, Object> row1 = new HashMap<>();
    row1.put("COLREAL", 123.456);

    Map<String, Object> row2 = new HashMap<>();
    row2.put("COLREAL", 123.4567);

    Map<String, Object> row3 = new HashMap<>();
    row3.put("COLREAL", null);

    InsertValidationResponse response =
        innerBuffer.insertRows(Arrays.asList(row1, row2, row3), null, null);
    Assert.assertFalse(response.hasErrors());

    // Check data was inserted into the buffer correctly
    Assert.assertEquals(123.456, innerBuffer.getVectorValueAt("COLREAL", 0));
    Assert.assertEquals(123.4567, innerBuffer.getVectorValueAt("COLREAL", 1));
    Assert.assertNull(innerBuffer.getVectorValueAt("COLREAL", 2));

    // Check stats generation
    ChannelData<?> result = innerBuffer.flush();

    Assert.assertEquals(3, result.getRowCount());
    Assert.assertEquals(
        Double.valueOf(123.456),
        result
            .getColumnEps()
            .get(enableIcebergStreaming ? "1" : "COLREAL")
            .getCurrentMinRealValue());
    Assert.assertEquals(
        Double.valueOf(123.4567),
        result
            .getColumnEps()
            .get(enableIcebergStreaming ? "1" : "COLREAL")
            .getCurrentMaxRealValue());
    Assert.assertEquals(
        1,
        result.getColumnEps().get(enableIcebergStreaming ? "1" : "COLREAL").getCurrentNullCount());
  }

  @Test
  public void testOnErrorAbortFailures() {
    AbstractRowBuffer<?> innerBuffer = createTestBuffer(OpenChannelRequest.OnErrorOption.ABORT);

    ColumnMetadata colDecimal = new ColumnMetadata();
    colDecimal.setOrdinal(1);
    colDecimal.setName("COLDECIMAL");
    colDecimal.setPhysicalType("SB16");
    colDecimal.setNullable(true);
    colDecimal.setLogicalType("FIXED");
    colDecimal.setPrecision(38);
    colDecimal.setScale(0);

    innerBuffer.setupSchema(Collections.singletonList(colDecimal));
    Map<String, Object> row = new HashMap<>();
    row.put("COLDECIMAL", 1);

    InsertValidationResponse response =
        innerBuffer.insertRows(Collections.singletonList(row), "1", "1");
    Assert.assertFalse(response.hasErrors());

    Assert.assertEquals(1, innerBuffer.bufferedRowCount);
    Assert.assertEquals(0, innerBuffer.getTempRowCount());
    Assert.assertEquals(
        1,
        innerBuffer
            .statsMap
            .get(enableIcebergStreaming ? "1" : "COLDECIMAL")
            .getCurrentMaxIntValue()
            .intValue());
    Assert.assertEquals(
        1,
        innerBuffer
            .statsMap
            .get(enableIcebergStreaming ? "1" : "COLDECIMAL")
            .getCurrentMinIntValue()
            .intValue());
    Assert.assertNull(
        innerBuffer
            .tempStatsMap
            .get(enableIcebergStreaming ? "1" : "COLDECIMAL")
            .getCurrentMaxIntValue());
    Assert.assertNull(
        innerBuffer
            .tempStatsMap
            .get(enableIcebergStreaming ? "1" : "COLDECIMAL")
            .getCurrentMinIntValue());

    Map<String, Object> row2 = new HashMap<>();
    row2.put("COLDECIMAL", 2);
    response = innerBuffer.insertRows(Collections.singletonList(row2), "1", "2");
    Assert.assertFalse(response.hasErrors());

    Assert.assertEquals(2, innerBuffer.bufferedRowCount);
    Assert.assertEquals(0, innerBuffer.getTempRowCount());
    Assert.assertEquals(
        2,
        innerBuffer
            .statsMap
            .get(enableIcebergStreaming ? "1" : "COLDECIMAL")
            .getCurrentMaxIntValue()
            .intValue());
    Assert.assertEquals(
        1,
        innerBuffer
            .statsMap
            .get(enableIcebergStreaming ? "1" : "COLDECIMAL")
            .getCurrentMinIntValue()
            .intValue());
    Assert.assertNull(
        innerBuffer
            .tempStatsMap
            .get(enableIcebergStreaming ? "1" : "COLDECIMAL")
            .getCurrentMaxIntValue());
    Assert.assertNull(
        innerBuffer
            .tempStatsMap
            .get(enableIcebergStreaming ? "1" : "COLDECIMAL")
            .getCurrentMinIntValue());

    Map<String, Object> row3 = new HashMap<>();
    row3.put("COLDECIMAL", true);
    try {
      innerBuffer.insertRows(Collections.singletonList(row3), "1", "3");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.INVALID_FORMAT_ROW.getMessageCode(), e.getVendorCode());
    }

    Assert.assertEquals(2, innerBuffer.bufferedRowCount);
    Assert.assertEquals(0, innerBuffer.getTempRowCount());
    Assert.assertEquals(
        2,
        innerBuffer
            .statsMap
            .get(enableIcebergStreaming ? "1" : "COLDECIMAL")
            .getCurrentMaxIntValue()
            .intValue());
    Assert.assertEquals(
        1,
        innerBuffer
            .statsMap
            .get(enableIcebergStreaming ? "1" : "COLDECIMAL")
            .getCurrentMinIntValue()
            .intValue());
    Assert.assertNull(
        innerBuffer
            .tempStatsMap
            .get(enableIcebergStreaming ? "1" : "COLDECIMAL")
            .getCurrentMaxIntValue());
    Assert.assertNull(
        innerBuffer
            .tempStatsMap
            .get(enableIcebergStreaming ? "1" : "COLDECIMAL")
            .getCurrentMinIntValue());

    row3.put("COLDECIMAL", 3);
    response = innerBuffer.insertRows(Collections.singletonList(row3), "1", "3");
    Assert.assertFalse(response.hasErrors());
    Assert.assertEquals(3, innerBuffer.bufferedRowCount);
    Assert.assertEquals(0, innerBuffer.getTempRowCount());
    Assert.assertEquals(
        3,
        innerBuffer
            .statsMap
            .get(enableIcebergStreaming ? "1" : "COLDECIMAL")
            .getCurrentMaxIntValue()
            .intValue());
    Assert.assertEquals(
        1,
        innerBuffer
            .statsMap
            .get(enableIcebergStreaming ? "1" : "COLDECIMAL")
            .getCurrentMinIntValue()
            .intValue());
    Assert.assertNull(
        innerBuffer
            .tempStatsMap
            .get(enableIcebergStreaming ? "1" : "COLDECIMAL")
            .getCurrentMaxIntValue());
    Assert.assertNull(
        innerBuffer
            .tempStatsMap
            .get(enableIcebergStreaming ? "1" : "COLDECIMAL")
            .getCurrentMinIntValue());

    ChannelData<?> data = innerBuffer.flush();
    Assert.assertEquals(3, data.getRowCount());
    Assert.assertEquals(0, innerBuffer.bufferedRowCount);
  }

  @Test
  public void testOnErrorAbortSkipBatch() {
    AbstractRowBuffer<?> innerBuffer =
        createTestBuffer(OpenChannelRequest.OnErrorOption.SKIP_BATCH);

    ColumnMetadata colDecimal = new ColumnMetadata();
    colDecimal.setOrdinal(1);
    colDecimal.setName("COLDECIMAL");
    colDecimal.setPhysicalType("SB16");
    colDecimal.setNullable(true);
    colDecimal.setLogicalType("FIXED");
    colDecimal.setPrecision(38);
    colDecimal.setScale(0);

    innerBuffer.setupSchema(Collections.singletonList(colDecimal));
    Map<String, Object> row = new HashMap<>();
    row.put("COLDECIMAL", 1);

    InsertValidationResponse response =
        innerBuffer.insertRows(Collections.singletonList(row), "1", "1");
    Assert.assertFalse(response.hasErrors());

    Assert.assertEquals(1, innerBuffer.bufferedRowCount);
    Assert.assertEquals(0, innerBuffer.getTempRowCount());
    Assert.assertEquals(
        1,
        innerBuffer
            .statsMap
            .get(enableIcebergStreaming ? "1" : "COLDECIMAL")
            .getCurrentMaxIntValue()
            .intValue());
    Assert.assertEquals(
        1,
        innerBuffer
            .statsMap
            .get(enableIcebergStreaming ? "1" : "COLDECIMAL")
            .getCurrentMinIntValue()
            .intValue());
    Assert.assertNull(
        innerBuffer
            .tempStatsMap
            .get(enableIcebergStreaming ? "1" : "COLDECIMAL")
            .getCurrentMaxIntValue());
    Assert.assertNull(
        innerBuffer
            .tempStatsMap
            .get(enableIcebergStreaming ? "1" : "COLDECIMAL")
            .getCurrentMinIntValue());

    Map<String, Object> row2 = new HashMap<>();
    row2.put("COLDECIMAL", 2);
    Map<String, Object> row3 = new HashMap<>();
    row3.put("COLDECIMAL", true);

    response = innerBuffer.insertRows(Arrays.asList(row2, row3), "1", "3");
    Assert.assertTrue(response.hasErrors());

    Assert.assertEquals(1, innerBuffer.bufferedRowCount);
    Assert.assertEquals(0, innerBuffer.getTempRowCount());
    Assert.assertEquals(
        1,
        innerBuffer
            .statsMap
            .get(enableIcebergStreaming ? "1" : "COLDECIMAL")
            .getCurrentMaxIntValue()
            .intValue());
    Assert.assertEquals(
        1,
        innerBuffer
            .statsMap
            .get(enableIcebergStreaming ? "1" : "COLDECIMAL")
            .getCurrentMinIntValue()
            .intValue());
    Assert.assertNull(
        innerBuffer
            .tempStatsMap
            .get(enableIcebergStreaming ? "1" : "COLDECIMAL")
            .getCurrentMaxIntValue());
    Assert.assertNull(
        innerBuffer
            .tempStatsMap
            .get(enableIcebergStreaming ? "1" : "COLDECIMAL")
            .getCurrentMinIntValue());

    Assert.assertEquals(1, innerBuffer.bufferedRowCount);
    Assert.assertEquals(0, innerBuffer.getTempRowCount());
    Assert.assertEquals(
        1,
        innerBuffer
            .statsMap
            .get(enableIcebergStreaming ? "1" : "COLDECIMAL")
            .getCurrentMaxIntValue()
            .intValue());
    Assert.assertEquals(
        1,
        innerBuffer
            .statsMap
            .get(enableIcebergStreaming ? "1" : "COLDECIMAL")
            .getCurrentMinIntValue()
            .intValue());
    Assert.assertNull(
        innerBuffer
            .tempStatsMap
            .get(enableIcebergStreaming ? "1" : "COLDECIMAL")
            .getCurrentMaxIntValue());
    Assert.assertNull(
        innerBuffer
            .tempStatsMap
            .get(enableIcebergStreaming ? "1" : "COLDECIMAL")
            .getCurrentMinIntValue());

    row3.put("COLDECIMAL", 3);
    response = innerBuffer.insertRows(Arrays.asList(row2, row3), "1", "3");
    Assert.assertFalse(response.hasErrors());
    Assert.assertEquals(3, innerBuffer.bufferedRowCount);
    Assert.assertEquals(0, innerBuffer.getTempRowCount());
    Assert.assertEquals(
        3,
        innerBuffer
            .statsMap
            .get(enableIcebergStreaming ? "1" : "COLDECIMAL")
            .getCurrentMaxIntValue()
            .intValue());
    Assert.assertEquals(
        1,
        innerBuffer
            .statsMap
            .get(enableIcebergStreaming ? "1" : "COLDECIMAL")
            .getCurrentMinIntValue()
            .intValue());
    Assert.assertNull(
        innerBuffer
            .tempStatsMap
            .get(enableIcebergStreaming ? "1" : "COLDECIMAL")
            .getCurrentMaxIntValue());
    Assert.assertNull(
        innerBuffer
            .tempStatsMap
            .get(enableIcebergStreaming ? "1" : "COLDECIMAL")
            .getCurrentMinIntValue());

    ChannelData<?> data = innerBuffer.flush();
    Assert.assertEquals(3, data.getRowCount());
    Assert.assertEquals(0, innerBuffer.bufferedRowCount);
  }

  @Test
  public void testE2EVariant() {
    if (!enableIcebergStreaming) {
      testE2EVariantHelper(OpenChannelRequest.OnErrorOption.ABORT);
      testE2EVariantHelper(OpenChannelRequest.OnErrorOption.CONTINUE);
      testE2EVariantHelper(OpenChannelRequest.OnErrorOption.SKIP_BATCH);
    }
  }

  private void testE2EVariantHelper(OpenChannelRequest.OnErrorOption onErrorOption) {
    AbstractRowBuffer<?> innerBuffer = createTestBuffer(onErrorOption);

    ColumnMetadata colVariant = new ColumnMetadata();
    colVariant.setOrdinal(1);
    colVariant.setName("COLVARIANT");
    colVariant.setPhysicalType("LOB");
    colVariant.setNullable(true);
    colVariant.setLogicalType("VARIANT");

    innerBuffer.setupSchema(Collections.singletonList(colVariant));

    Map<String, Object> row1 = new HashMap<>();
    row1.put("COLVARIANT", null);

    Map<String, Object> row2 = new HashMap<>();
    row2.put("COLVARIANT", "");

    Map<String, Object> row3 = new HashMap<>();
    row3.put("COLVARIANT", "null");

    Map<String, Object> row4 = new HashMap<>();
    row4.put("COLVARIANT", "{\"key\":1}");

    Map<String, Object> row5 = new HashMap<>();
    row5.put("COLVARIANT", 3);

    InsertValidationResponse response =
        innerBuffer.insertRows(Arrays.asList(row1, row2, row3, row4, row5), null, null);
    Assert.assertFalse(response.hasErrors());

    // Check data was inserted into the buffer correctly
    Assert.assertNull(null, innerBuffer.getVectorValueAt("COLVARIANT", 0));
    Assert.assertNull(null, innerBuffer.getVectorValueAt("COLVARIANT", 1));
    Assert.assertEquals("null", innerBuffer.getVectorValueAt("COLVARIANT", 2));
    Assert.assertEquals("{\"key\":1}", innerBuffer.getVectorValueAt("COLVARIANT", 3));
    Assert.assertEquals("3", innerBuffer.getVectorValueAt("COLVARIANT", 4));

    // Check stats generation
    ChannelData<?> result = innerBuffer.flush();
    Assert.assertEquals(5, result.getRowCount());
    Assert.assertEquals(2, result.getColumnEps().get("COLVARIANT").getCurrentNullCount());
  }

  @Test
  public void testE2EObject() {
    if (!enableIcebergStreaming) {
      testE2EObjectHelper(OpenChannelRequest.OnErrorOption.ABORT);
      testE2EObjectHelper(OpenChannelRequest.OnErrorOption.CONTINUE);
      testE2EObjectHelper(OpenChannelRequest.OnErrorOption.SKIP_BATCH);
    }
  }

  private void testE2EObjectHelper(OpenChannelRequest.OnErrorOption onErrorOption) {
    AbstractRowBuffer<?> innerBuffer = createTestBuffer(onErrorOption);

    ColumnMetadata colObject = new ColumnMetadata();
    colObject.setOrdinal(1);
    colObject.setName("COLOBJECT");
    colObject.setPhysicalType("LOB");
    colObject.setNullable(true);
    colObject.setLogicalType("OBJECT");

    innerBuffer.setupSchema(Collections.singletonList(colObject));

    Map<String, Object> row1 = new HashMap<>();
    row1.put("COLOBJECT", "{\"key\":1}");

    InsertValidationResponse response = innerBuffer.insertRows(Arrays.asList(row1), null, null);
    Assert.assertFalse(response.hasErrors());

    // Check data was inserted into the buffer correctly
    Assert.assertEquals("{\"key\":1}", innerBuffer.getVectorValueAt("COLOBJECT", 0));

    // Check stats generation
    ChannelData<?> result = innerBuffer.flush();
    Assert.assertEquals(1, result.getRowCount());
  }

  @Test
  public void testE2EArray() {
    if (!enableIcebergStreaming) {
      testE2EArrayHelper(OpenChannelRequest.OnErrorOption.ABORT);
      testE2EArrayHelper(OpenChannelRequest.OnErrorOption.CONTINUE);
      testE2EArrayHelper(OpenChannelRequest.OnErrorOption.SKIP_BATCH);
    }
  }

  private void testE2EArrayHelper(OpenChannelRequest.OnErrorOption onErrorOption) {
    AbstractRowBuffer<?> innerBuffer = createTestBuffer(onErrorOption);

    ColumnMetadata colObject = new ColumnMetadata();
    colObject.setOrdinal(1);
    colObject.setName("COLARRAY");
    colObject.setPhysicalType("LOB");
    colObject.setNullable(true);
    colObject.setLogicalType("ARRAY");

    innerBuffer.setupSchema(Collections.singletonList(colObject));

    Map<String, Object> row1 = new HashMap<>();
    row1.put("COLARRAY", null);

    Map<String, Object> row2 = new HashMap<>();
    row2.put("COLARRAY", "");

    Map<String, Object> row3 = new HashMap<>();
    row3.put("COLARRAY", "null");

    Map<String, Object> row4 = new HashMap<>();
    row4.put("COLARRAY", "{\"key\":1}");

    Map<String, Object> row5 = new HashMap<>();
    row5.put("COLARRAY", Arrays.asList(1, 2, 3));

    InsertValidationResponse response =
        innerBuffer.insertRows(Arrays.asList(row1, row2, row3, row4, row5), null, null);
    Assert.assertFalse(response.hasErrors());

    // Check data was inserted into the buffer correctly
    Assert.assertNull(innerBuffer.getVectorValueAt("COLARRAY", 0));
    Assert.assertEquals("[null]", innerBuffer.getVectorValueAt("COLARRAY", 1));
    Assert.assertEquals("[null]", innerBuffer.getVectorValueAt("COLARRAY", 2));
    Assert.assertEquals("[{\"key\":1}]", innerBuffer.getVectorValueAt("COLARRAY", 3));
    Assert.assertEquals("[1,2,3]", innerBuffer.getVectorValueAt("COLARRAY", 4));

    // Check stats generation
    ChannelData<?> result = innerBuffer.flush();
    Assert.assertEquals(5, result.getRowCount());
  }

  @Test
  public void testOnErrorAbortRowsWithError() {
    AbstractRowBuffer<?> innerBufferOnErrorContinue =
        createTestBuffer(OpenChannelRequest.OnErrorOption.CONTINUE);
    AbstractRowBuffer<?> innerBufferOnErrorAbort =
        createTestBuffer(OpenChannelRequest.OnErrorOption.ABORT);
    AbstractRowBuffer<?> innerBufferOnErrorSkipBatch =
        createTestBuffer(OpenChannelRequest.OnErrorOption.SKIP_BATCH);

    ColumnMetadata colChar = new ColumnMetadata();
    colChar.setOrdinal(1);
    colChar.setName("COLCHAR");
    colChar.setPhysicalType("LOB");
    colChar.setNullable(true);
    colChar.setLogicalType("TEXT");
    colChar.setByteLength(14);
    colChar.setLength(11);
    colChar.setScale(0);

    innerBufferOnErrorContinue.setupSchema(Collections.singletonList(colChar));
    innerBufferOnErrorAbort.setupSchema(Collections.singletonList(colChar));

    // insert one valid row
    List<Map<String, Object>> validRows = new ArrayList<>();
    validRows.add(Collections.singletonMap("colChar", "a"));

    InsertValidationResponse response = innerBufferOnErrorContinue.insertRows(validRows, "1", "1");
    Assert.assertFalse(response.hasErrors());
    response = innerBufferOnErrorAbort.insertRows(validRows, "1", "1");
    Assert.assertFalse(response.hasErrors());

    // insert one valid and one invalid row
    List<Map<String, Object>> mixedRows = new ArrayList<>();
    mixedRows.add(Collections.singletonMap("colChar", "b"));
    mixedRows.add(
        Collections.singletonMap("colChar", StringUtils.repeat('1', 16777217))); // too big

    response = innerBufferOnErrorContinue.insertRows(mixedRows, "1", "3");
    Assert.assertTrue(response.hasErrors());

    Assert.assertThrows(
        SFException.class, () -> innerBufferOnErrorAbort.insertRows(mixedRows, "1", "3"));

    List<List<Object>> snapshotContinueParquet =
        ((ParquetChunkData) innerBufferOnErrorContinue.getSnapshot().get()).rows;
    if (enableIcebergStreaming) {
      // Convert every object to string for iceberg mode
      snapshotContinueParquet =
          snapshotContinueParquet.stream()
              .map(
                  row ->
                      row.stream()
                          .map(
                              obj -> {
                                if (obj instanceof byte[]) {
                                  return new String((byte[]) obj, StandardCharsets.UTF_8);
                                }
                                return obj;
                              })
                          .collect(Collectors.toList()))
              .collect(Collectors.toList());
    }
    // validRows and only the good row from mixedRows are in the buffer
    Assert.assertEquals(2, snapshotContinueParquet.size());
    Assert.assertEquals(Arrays.asList("a"), snapshotContinueParquet.get(0));
    Assert.assertEquals(Arrays.asList("b"), snapshotContinueParquet.get(1));

    List<List<Object>> snapshotAbortParquet =
        ((ParquetChunkData) innerBufferOnErrorAbort.getSnapshot().get()).rows;
    if (enableIcebergStreaming) {
      // Convert every object to string for iceberg mode
      snapshotAbortParquet =
          snapshotAbortParquet.stream()
              .map(
                  row ->
                      row.stream()
                          .map(
                              obj -> {
                                if (obj instanceof byte[]) {
                                  return new String((byte[]) obj, StandardCharsets.UTF_8);
                                }
                                return obj;
                              })
                          .collect(Collectors.toList()))
              .collect(Collectors.toList());
    }
    // only validRows and none of the mixedRows are in the buffer
    Assert.assertEquals(1, snapshotAbortParquet.size());
    Assert.assertEquals(Arrays.asList("a"), snapshotAbortParquet.get(0));
  }

  @Test
  public void testParquetChunkMetadataCreationIsThreadSafe() throws InterruptedException {
    final ParquetRowBuffer bufferUnderTest =
        (ParquetRowBuffer) createTestBuffer(OpenChannelRequest.OnErrorOption.CONTINUE);

    final int columnOrdinal = 1;
    final ColumnMetadata colChar1 = new ColumnMetadata();
    colChar1.setOrdinal(columnOrdinal);
    colChar1.setName("COLCHAR");
    colChar1.setPhysicalType("LOB");
    colChar1.setNullable(true);
    colChar1.setLogicalType("TEXT");
    colChar1.setByteLength(14);
    colChar1.setLength(11);
    colChar1.setScale(0);

    final ColumnMetadata colChar2 = new ColumnMetadata();
    colChar2.setOrdinal(columnOrdinal);
    colChar2.setName("COLCHAR");
    colChar2.setPhysicalType("SB1");
    colChar2.setNullable(true);
    colChar2.setLogicalType("TEXT");
    colChar2.setByteLength(14);
    colChar2.setLength(11);
    colChar2.setScale(0);

    bufferUnderTest.setupSchema(Collections.singletonList(colChar1));

    loadData(bufferUnderTest, Collections.singletonMap("colChar", "a"));

    final CountDownLatch latch = new CountDownLatch(1);
    final AtomicReference<ChannelData<ParquetChunkData>> firstFlushResult = new AtomicReference<>();
    final Thread t =
        getThreadThatWaitsForLockReleaseAndFlushes(bufferUnderTest, latch, firstFlushResult);
    t.start();

    final ChannelData<ParquetChunkData> secondFlushResult = bufferUnderTest.flush();
    bufferUnderTest.setupSchema(Collections.singletonList(colChar2));

    latch.countDown();
    t.join();

    // The logical and physical types should be different
    Assert.assertNotEquals(
        getColumnType(firstFlushResult.get(), columnOrdinal),
        getColumnType(secondFlushResult, columnOrdinal));
  }

  @Test
  public void testParquetFileNameMetadata() throws IOException {
    String filePath = "testParquetFileNameMetadata.bdec";
    final ParquetRowBuffer bufferUnderTest =
        (ParquetRowBuffer) createTestBuffer(OpenChannelRequest.OnErrorOption.CONTINUE);

    final ColumnMetadata colChar = new ColumnMetadata();
    colChar.setOrdinal(1);
    colChar.setName("COLCHAR");
    colChar.setPhysicalType("LOB");
    colChar.setNullable(true);
    colChar.setLogicalType("TEXT");
    colChar.setByteLength(14);
    colChar.setLength(11);
    colChar.setScale(0);

    bufferUnderTest.setupSchema(Collections.singletonList(colChar));
    loadData(bufferUnderTest, Collections.singletonMap("colChar", "a"));
    ChannelData<ParquetChunkData> data = bufferUnderTest.flush();
    data.setChannelContext(new ChannelFlushContext("name", "db", "schema", "table", 1L, "key", 0L));

    ParquetFlusher flusher = (ParquetFlusher) bufferUnderTest.createFlusher();
    {
      Flusher.SerializationResult result =
          flusher.serialize(Collections.singletonList(data), filePath, 0);

      BdecParquetReader reader = new BdecParquetReader(result.chunkData.toByteArray());
      Assert.assertEquals(
          "testParquetFileNameMetadata.bdec",
          reader
              .getKeyValueMetadata()
              .get(
                  enableIcebergStreaming
                      ? Constants.ASSIGNED_FULL_FILE_NAME_KEY
                      : Constants.PRIMARY_FILE_ID_KEY));
      Assert.assertEquals(
          RequestBuilder.DEFAULT_VERSION,
          reader.getKeyValueMetadata().get(Constants.SDK_VERSION_KEY));
    }
    {
      try {
        Flusher.SerializationResult result =
            flusher.serialize(Collections.singletonList(data), filePath, 13);
        if (enableIcebergStreaming) {
          Assert.fail(
              "Should have thrown an exception because iceberg streams do not support offsets");
        }

        BdecParquetReader reader = new BdecParquetReader(result.chunkData.toByteArray());
        Assert.assertEquals(
            "testParquetFileNameMetadata_13.bdec",
            reader
                .getKeyValueMetadata()
                .get(
                    enableIcebergStreaming
                        ? Constants.ASSIGNED_FULL_FILE_NAME_KEY
                        : Constants.PRIMARY_FILE_ID_KEY));
        Assert.assertEquals(
            RequestBuilder.DEFAULT_VERSION,
            reader.getKeyValueMetadata().get(Constants.SDK_VERSION_KEY));
      } catch (IllegalStateException ex) {
        if (!enableIcebergStreaming) {
          throw ex;
        }
      }
    }
  }

  @Test
  public void testStructuredStatsE2E() {
    if (!enableIcebergStreaming) return;
    testStructuredStatsE2EHelper(createTestBuffer(OpenChannelRequest.OnErrorOption.CONTINUE));
    testStructuredStatsE2EHelper(createTestBuffer(OpenChannelRequest.OnErrorOption.ABORT));
    testStructuredStatsE2EHelper(createTestBuffer(OpenChannelRequest.OnErrorOption.SKIP_BATCH));
  }

  private void testStructuredStatsE2EHelper(AbstractRowBuffer<?> rowBuffer) {
    rowBuffer.setupSchema(createStrcuturedDataTypeSchema());
    Map<String, Object> row1 = new HashMap<>();
    row1.put(
        "COLOBJECT",
        new HashMap<String, Object>() {
          {
            put("a", 1);
            put("b", "string1");
          }
        });
    row1.put(
        "COLMAP",
        new HashMap<String, Boolean>() {
          {
            put("key1", true);
            put("key2", true);
          }
        });
    row1.put("COLARRAY", Arrays.asList(1, 1, 1));

    Map<String, Object> row2 = new HashMap<>();
    row2.put(
        "COLOBJECT",
        new HashMap<String, Object>() {
          {
            put("a", 2);
            put("b", null);
          }
        });
    row2.put("COLMAP", null);
    row2.put("COLARRAY", Arrays.asList(1, null));

    InsertValidationResponse response = rowBuffer.insertRows(Arrays.asList(row1, row2), null, null);
    Assert.assertFalse(response.hasErrors());
    ChannelData<?> result = rowBuffer.flush();
    Map<String, RowBufferStats> columnEpStats = result.getColumnEps();

    assertThat(
            columnEpStats.get(enableIcebergStreaming ? "4" : "COLOBJECT.a").getCurrentMinIntValue())
        .isEqualTo(BigInteger.valueOf(1));
    assertThat(
            columnEpStats.get(enableIcebergStreaming ? "4" : "COLOBJECT.a").getCurrentMaxIntValue())
        .isEqualTo(BigInteger.valueOf(2));
    assertThat(
            columnEpStats.get(enableIcebergStreaming ? "4" : "COLOBJECT.a").getCurrentNullCount())
        .isEqualTo(0);
    assertThat(columnEpStats.get(enableIcebergStreaming ? "4" : "COLOBJECT.a").getDistinctValues())
        .isEqualTo(2);
    assertThat(columnEpStats.get(enableIcebergStreaming ? "4" : "COLOBJECT.a").getNumberOfValues())
        .isEqualTo(EP_NV_UNKNOWN);

    assertThat(
            columnEpStats.get(enableIcebergStreaming ? "5" : "COLOBJECT.b").getCurrentMinStrValue())
        .isEqualTo("string1".getBytes(StandardCharsets.UTF_8));
    assertThat(
            columnEpStats.get(enableIcebergStreaming ? "5" : "COLOBJECT.b").getCurrentMaxStrValue())
        .isEqualTo("string1".getBytes(StandardCharsets.UTF_8));
    assertThat(
            columnEpStats.get(enableIcebergStreaming ? "5" : "COLOBJECT.b").getCurrentNullCount())
        .isEqualTo(1);
    assertThat(columnEpStats.get(enableIcebergStreaming ? "5" : "COLOBJECT.b").getDistinctValues())
        .isEqualTo(1);
    assertThat(columnEpStats.get(enableIcebergStreaming ? "5" : "COLOBJECT.b").getNumberOfValues())
        .isEqualTo(EP_NV_UNKNOWN);

    assertThat(
            columnEpStats
                .get(enableIcebergStreaming ? "6" : "COLMAP.key_value.key")
                .getCurrentMinStrValue())
        .isEqualTo("key1".getBytes(StandardCharsets.UTF_8));
    assertThat(
            columnEpStats
                .get(enableIcebergStreaming ? "6" : "COLMAP.key_value.key")
                .getCurrentMaxStrValue())
        .isEqualTo("key2".getBytes(StandardCharsets.UTF_8));
    assertThat(
            columnEpStats
                .get(enableIcebergStreaming ? "6" : "COLMAP.key_value.key")
                .getCurrentNullCount())
        .isEqualTo(1);
    assertThat(
            columnEpStats
                .get(enableIcebergStreaming ? "6" : "COLMAP.key_value.key")
                .getDistinctValues())
        .isEqualTo(2);
    assertThat(
            columnEpStats
                .get(enableIcebergStreaming ? "6" : "COLMAP.key_value.key")
                .getNumberOfValues())
        .isEqualTo(3);

    assertThat(
            columnEpStats
                .get(enableIcebergStreaming ? "7" : "COLMAP.key_value.value")
                .getCurrentMinIntValue())
        .isEqualTo(BigInteger.ONE);
    assertThat(
            columnEpStats
                .get(enableIcebergStreaming ? "7" : "COLMAP.key_value.value")
                .getCurrentMaxIntValue())
        .isEqualTo(BigInteger.ONE);
    assertThat(
            columnEpStats
                .get(enableIcebergStreaming ? "7" : "COLMAP.key_value.value")
                .getCurrentNullCount())
        .isEqualTo(1);
    assertThat(
            columnEpStats
                .get(enableIcebergStreaming ? "7" : "COLMAP.key_value.value")
                .getDistinctValues())
        .isEqualTo(1);
    assertThat(
            columnEpStats
                .get(enableIcebergStreaming ? "7" : "COLMAP.key_value.value")
                .getNumberOfValues())
        .isEqualTo(3);

    assertThat(
            columnEpStats
                .get(enableIcebergStreaming ? "8" : "COLARRAY.list.element")
                .getCurrentMinIntValue())
        .isEqualTo(BigInteger.ONE);
    assertThat(
            columnEpStats
                .get(enableIcebergStreaming ? "8" : "COLARRAY.list.element")
                .getCurrentMaxIntValue())
        .isEqualTo(BigInteger.ONE);
    assertThat(
            columnEpStats
                .get(enableIcebergStreaming ? "8" : "COLARRAY.list.element")
                .getCurrentNullCount())
        .isEqualTo(1);
    assertThat(
            columnEpStats
                .get(enableIcebergStreaming ? "8" : "COLARRAY.list.element")
                .getDistinctValues())
        .isEqualTo(1);
    assertThat(
            columnEpStats
                .get(enableIcebergStreaming ? "8" : "COLARRAY.list.element")
                .getNumberOfValues())
        .isEqualTo(5);
  }

  private static Thread getThreadThatWaitsForLockReleaseAndFlushes(
      final ParquetRowBuffer bufferUnderTest,
      final CountDownLatch latch,
      final AtomicReference<ChannelData<ParquetChunkData>> flushResult) {
    return new Thread(
        () -> {
          try {
            latch.await();
          } catch (InterruptedException e) {
            fail("Thread was unexpectedly interrupted");
          }

          final ChannelData<ParquetChunkData> flush =
              loadData(bufferUnderTest, Collections.singletonMap("colChar", "b")).flush();
          flushResult.set(flush);
        });
  }

  private static ParquetRowBuffer loadData(
      final ParquetRowBuffer bufferToLoad, final Map<String, Object> data) {
    final List<Map<String, Object>> validRows = new ArrayList<>();
    validRows.add(data);

    final InsertValidationResponse nResponse = bufferToLoad.insertRows(validRows, "1", "1");
    Assert.assertFalse(nResponse.hasErrors());
    return bufferToLoad;
  }

  private static String getColumnType(final ChannelData<ParquetChunkData> chunkData, int columnId) {
    return chunkData.getVectors().metadata.get(Integer.toString(columnId));
  }
}
