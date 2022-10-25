package net.snowflake.ingest.streaming.internal;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.utils.Constants;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;
import org.apache.arrow.memory.RootAllocator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class RowBufferTest {
  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> bdecVersion() {
    return Arrays.asList(
        new Object[][] {
          {"Arrow", Constants.BdecVersion.ONE},
          {"Parquet", Constants.BdecVersion.THREE}
        });
  }

  private final Constants.BdecVersion bdecVersion;
  private AbstractRowBuffer<?> rowBufferOnErrorContinue;
  private AbstractRowBuffer<?> rowBufferOnErrorAbort;

  public RowBufferTest(@SuppressWarnings("unused") String name, Constants.BdecVersion bdecVersion) {
    this.bdecVersion = bdecVersion;
  }

  @Before
  public void setupRowBuffer() {
    this.rowBufferOnErrorContinue = createTestBuffer(OpenChannelRequest.OnErrorOption.CONTINUE);
    this.rowBufferOnErrorAbort = createTestBuffer(OpenChannelRequest.OnErrorOption.ABORT);
    List<ColumnMetadata> schema = createSchema();
    this.rowBufferOnErrorContinue.setupSchema(schema);
    this.rowBufferOnErrorAbort.setupSchema(schema);
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
    colChar.setCollation("en-ci");

    return Arrays.asList(
        colTinyIntCase, colTinyInt, colSmallInt, colInt, colBigInt, colDecimal, colChar);
  }

  private AbstractRowBuffer<?> createTestBuffer(OpenChannelRequest.OnErrorOption onErrorOption) {
    return (AbstractRowBuffer<?>)
        new SnowflakeStreamingIngestChannelInternal<>(
                "channel",
                "db",
                "schema",
                "table",
                "0",
                0L,
                0L,
                new SnowflakeStreamingIngestClientInternal<>("client"),
                "key",
                1234L,
                onErrorOption,
                bdecVersion,
                new RootAllocator())
            .getRowBuffer();
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
      Assert.fail("Expected error");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.UNKNOWN_DATA_TYPE.getMessageCode(), e.getVendorCode());
    }

    // Fixed LOB
    testCol = new ColumnMetadata();
    testCol.setPhysicalType("LOB");
    testCol.setLogicalType("FIXED");
    try {
      this.rowBufferOnErrorContinue.setupSchema(Collections.singletonList(testCol));
      Assert.fail("Expected error");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.UNKNOWN_DATA_TYPE.getMessageCode(), e.getVendorCode());
    }

    // TIMESTAMP_NTZ SB2
    testCol = new ColumnMetadata();
    testCol.setPhysicalType("SB2");
    testCol.setLogicalType("TIMESTAMP_NTZ");
    try {
      this.rowBufferOnErrorContinue.setupSchema(Collections.singletonList(testCol));
      Assert.fail("Expected error");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.UNKNOWN_DATA_TYPE.getMessageCode(), e.getVendorCode());
    }

    // TIMESTAMP_TZ SB1
    testCol = new ColumnMetadata();
    testCol.setPhysicalType("SB1");
    testCol.setLogicalType("TIMESTAMP_TZ");
    try {
      this.rowBufferOnErrorContinue.setupSchema(Collections.singletonList(testCol));
      Assert.fail("Expected error");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.UNKNOWN_DATA_TYPE.getMessageCode(), e.getVendorCode());
    }

    // TIME SB16
    testCol = new ColumnMetadata();
    testCol.setPhysicalType("SB16");
    testCol.setLogicalType("TIME");
    try {
      this.rowBufferOnErrorContinue.setupSchema(Collections.singletonList(testCol));
      Assert.fail("Expected error");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.UNKNOWN_DATA_TYPE.getMessageCode(), e.getVendorCode());
    }
  }

  @Test
  public void testReset() {
    RowBufferStats stats = this.rowBufferOnErrorContinue.statsMap.get("COLCHAR");
    stats.addIntValue(BigInteger.valueOf(1));
    Assert.assertEquals(BigInteger.valueOf(1), stats.getCurrentMaxIntValue());
    Assert.assertEquals("en-ci", stats.getCollationDefinitionString());
    this.rowBufferOnErrorContinue.reset();
    RowBufferStats resetStats = this.rowBufferOnErrorContinue.statsMap.get("COLCHAR");
    Assert.assertNotNull(resetStats);
    Assert.assertNull(resetStats.getCurrentMaxIntValue());
    Assert.assertEquals("en-ci", resetStats.getCollationDefinitionString());
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
      Assert.fail("Setup should fail if invalid column metadata is provided");
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
      Assert.fail("Setup should fail if invalid column metadata is provided");
    } catch (SFException e) {
      Assert.assertEquals(e.getVendorCode(), ErrorCode.UNKNOWN_DATA_TYPE.getMessageCode());
    }
  }

  @Test
  public void testStringLength() {
    testStringLengthHelper(this.rowBufferOnErrorContinue);
    testStringLengthHelper(this.rowBufferOnErrorAbort);
  }

  @Test
  public void testRowIndexWithMultipleRowsWithErrorr() {
    List<Map<String, Object>> rows = new ArrayList<>();
    Map<String, Object> row = new HashMap<>();

    // row with good data
    row.put("colInt", 3);
    rows.add(row);

    row = new HashMap<>();
    row.put("colChar", "1111111111111111111111"); // too big

    // lets add a row with bad data
    rows.add(row);

    InsertValidationResponse response = this.rowBufferOnErrorContinue.insertRows(rows, null);
    Assert.assertTrue(response.hasErrors());

    Assert.assertEquals(1, response.getErrorRowCount());

    // second row out of the rows we sent was having bad data.
    // so InsertError corresponds to second row.
    Assert.assertEquals(1, response.getInsertErrors().get(0).getRowIndex());
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

    InsertValidationResponse response = rowBuffer.insertRows(Collections.singletonList(row), null);
    Assert.assertFalse(response.hasErrors());

    row.put("colTinyInt", (byte) 1);
    row.put("\"colTinyInt\"", (byte) 1);
    row.put("colSmallInt", (short) 2);
    row.put("colInt", 3);
    row.put("colBigInt", 4L);
    row.put("colDecimal", 1.23);
    row.put("colChar", "1111111111111111111111"); // too big

    if (rowBuffer.owningChannel.getOnErrorOption() == OpenChannelRequest.OnErrorOption.CONTINUE) {
      response = rowBuffer.insertRows(Collections.singletonList(row), null);
      Assert.assertTrue(response.hasErrors());
      Assert.assertEquals(1, response.getErrorRowCount());
      Assert.assertEquals(
          ErrorCode.INVALID_ROW.getMessageCode(),
          response.getInsertErrors().get(0).getException().getVendorCode());
      Assert.assertTrue(response.getInsertErrors().get(0).getMessage().contains("String too long"));
    } else {
      try {
        rowBuffer.insertRows(Collections.singletonList(row), null);
      } catch (SFException e) {
        Assert.assertEquals(ErrorCode.INVALID_ROW.getMessageCode(), e.getVendorCode());
      }
    }
  }

  @Test
  public void testInsertRow() {
    testInsertRowHelper(this.rowBufferOnErrorContinue);
    testInsertRowHelper(this.rowBufferOnErrorAbort);
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

    InsertValidationResponse response = rowBuffer.insertRows(Collections.singletonList(row), null);
    Assert.assertFalse(response.hasErrors());
  }

  @Test
  public void testInsertRows() {
    testInsertRowsHelper(this.rowBufferOnErrorContinue);
    testInsertRowsHelper(this.rowBufferOnErrorAbort);
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

    InsertValidationResponse response = rowBuffer.insertRows(Arrays.asList(row1, row2), null);
    Assert.assertFalse(response.hasErrors());
  }

  @Test
  public void testClose() {
    this.rowBufferOnErrorContinue.close("testClose");
    Map<String, Object> row = new HashMap<>();
    row.put("colTinyInt", (byte) 1);
    row.put("colSmallInt", (short) 2);
    row.put("colInt", 3);
    row.put("colBigInt", 4L);
    row.put("colDecimal", 1.23);
    row.put("colChar", "2");

    try {
      this.rowBufferOnErrorContinue.insertRows(Collections.singletonList(row), null);
      Assert.fail("Insert should fail after buffer is closed");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.INTERNAL_ERROR.getMessageCode(), e.getVendorCode());
    }
  }

  @Test
  public void testFlush() {
    testFlushHelper(this.rowBufferOnErrorAbort);
    testFlushHelper(this.rowBufferOnErrorContinue);
  }

  private void testFlushHelper(AbstractRowBuffer<?> rowBuffer) {
    String offsetToken = "1";
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
        rowBuffer.insertRows(Arrays.asList(row1, row2), offsetToken);
    Assert.assertFalse(response.hasErrors());
    float bufferSize = rowBuffer.getSize();

    ChannelData<?> data = rowBuffer.flush();
    Assert.assertEquals(2, data.getRowCount());
    Assert.assertEquals((Long) 1L, data.getRowSequencer());
    Assert.assertEquals(offsetToken, data.getOffsetToken());
    Assert.assertEquals(bufferSize, data.getBufferSize(), 0);
  }

  @Test
  public void testDoubleQuotesColumnName() {
    testDoubleQuotesColumnNameHelper(OpenChannelRequest.OnErrorOption.ABORT);
    testDoubleQuotesColumnNameHelper(OpenChannelRequest.OnErrorOption.CONTINUE);
  }

  private void testDoubleQuotesColumnNameHelper(OpenChannelRequest.OnErrorOption onErrorOption) {
    AbstractRowBuffer<?> innerBuffer = createTestBuffer(onErrorOption);

    ColumnMetadata colDoubleQuotes = new ColumnMetadata();
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
        innerBuffer.insertRows(Collections.singletonList(row), null);
    Assert.assertFalse(response.hasErrors());
  }

  @Test
  public void testBuildEpInfoFromStats() {
    Map<String, RowBufferStats> colStats = new HashMap<>();

    RowBufferStats stats1 = new RowBufferStats();
    stats1.addIntValue(BigInteger.valueOf(2));
    stats1.addIntValue(BigInteger.valueOf(10));
    stats1.addIntValue(BigInteger.valueOf(1));

    RowBufferStats stats2 = new RowBufferStats();
    stats2.addStrValue("alice");
    stats2.addStrValue("bob");
    stats2.incCurrentNullCount();

    colStats.put("intColumn", stats1);
    colStats.put("strColumn", stats2);

    EpInfo result = AbstractRowBuffer.buildEpInfoFromStats(2, colStats);
    Map<String, FileColumnProperties> columnResults = result.getColumnEps();
    Assert.assertEquals(2, columnResults.keySet().size());

    FileColumnProperties strColumnResult = columnResults.get("strColumn");
    Assert.assertEquals(-1, strColumnResult.getDistinctValues());
    Assert.assertEquals("alice", strColumnResult.getMinStrValue());
    Assert.assertEquals("bob", strColumnResult.getMaxStrValue());
    Assert.assertEquals(1, strColumnResult.getNullCount());

    FileColumnProperties intColumnResult = columnResults.get("intColumn");
    Assert.assertEquals(-1, intColumnResult.getDistinctValues());
    Assert.assertEquals(BigInteger.valueOf(1), intColumnResult.getMinIntValue());
    Assert.assertEquals(BigInteger.valueOf(10), intColumnResult.getMaxIntValue());
    Assert.assertEquals(0, intColumnResult.getNullCount());
  }

  @Test
  public void testBuildEpInfoFromNullColumnStats() {
    final String intColName = "intCol";
    final String realColName = "realCol";
    Map<String, RowBufferStats> colStats = new HashMap<>();

    RowBufferStats stats = new RowBufferStats();
    stats.incCurrentNullCount();

    colStats.put(intColName, stats);
    colStats.put(realColName, stats);

    EpInfo result = AbstractRowBuffer.buildEpInfoFromStats(2, colStats);
    Map<String, FileColumnProperties> columnResults = result.getColumnEps();
    Assert.assertEquals(2, columnResults.keySet().size());

    FileColumnProperties intColumnResult = columnResults.get(intColName);
    Assert.assertEquals(-1, intColumnResult.getDistinctValues());
    Assert.assertEquals(
        FileColumnProperties.DEFAULT_MIN_MAX_INT_VAL_FOR_EP, intColumnResult.getMinIntValue());
    Assert.assertEquals(
        FileColumnProperties.DEFAULT_MIN_MAX_INT_VAL_FOR_EP, intColumnResult.getMaxIntValue());
    Assert.assertEquals(1, intColumnResult.getNullCount());
    Assert.assertEquals(0, intColumnResult.getMaxLength());

    FileColumnProperties realColumnResult = columnResults.get(realColName);
    Assert.assertEquals(-1, intColumnResult.getDistinctValues());
    Assert.assertEquals(
        FileColumnProperties.DEFAULT_MIN_MAX_REAL_VAL_FOR_EP, realColumnResult.getMinRealValue());
    Assert.assertEquals(
        FileColumnProperties.DEFAULT_MIN_MAX_REAL_VAL_FOR_EP, realColumnResult.getMaxRealValue());
    Assert.assertEquals(1, realColumnResult.getNullCount());
    Assert.assertEquals(0, realColumnResult.getMaxLength());
  }

  @Test
  public void testE2E() {
    testE2EHelper(this.rowBufferOnErrorAbort);
    testE2EHelper(this.rowBufferOnErrorContinue);
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

    InsertValidationResponse response = rowBuffer.insertRows(Collections.singletonList(row1), null);
    Assert.assertFalse(response.hasErrors());

    Assert.assertEquals((byte) 10, rowBuffer.getVectorValueAt("\"colTinyInt\"", 0));
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
  }

  private void testE2ETimestampErrorsHelper(AbstractRowBuffer<?> innerBuffer) {

    ColumnMetadata colTimestampLtzSB16 = new ColumnMetadata();
    colTimestampLtzSB16.setName("COLTIMESTAMPLTZ_SB16");
    colTimestampLtzSB16.setPhysicalType("SB16");
    colTimestampLtzSB16.setNullable(false);
    colTimestampLtzSB16.setLogicalType("TIMESTAMP_LTZ");
    colTimestampLtzSB16.setScale(6);

    innerBuffer.setupSchema(Collections.singletonList(colTimestampLtzSB16));

    Map<String, Object> row = new HashMap<>();
    row.put("COLTIMESTAMPLTZ_SB8", "1621899220");
    row.put("COLTIMESTAMPLTZ_SB16", "1621899220.1234567");

    if (innerBuffer.owningChannel.getOnErrorOption() == OpenChannelRequest.OnErrorOption.CONTINUE) {
      InsertValidationResponse response =
          innerBuffer.insertRows(Collections.singletonList(row), null);
      Assert.assertTrue(response.hasErrors());
      Assert.assertEquals(
          ErrorCode.INVALID_ROW.getMessageCode(),
          response.getInsertErrors().get(0).getException().getVendorCode());
    } else {
      try {
        innerBuffer.insertRows(Collections.singletonList(row), null);
      } catch (SFException e) {
        Assert.assertEquals(ErrorCode.INVALID_ROW.getMessageCode(), e.getVendorCode());
      }
    }
  }

  @Test
  public void testStatsE2E() {
    testStatsE2EHelper(this.rowBufferOnErrorAbort);
    testStatsE2EHelper(this.rowBufferOnErrorContinue);
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

    InsertValidationResponse response = rowBuffer.insertRows(Arrays.asList(row1, row2), null);
    Assert.assertFalse(response.hasErrors());
    ChannelData<?> result = rowBuffer.flush();
    Map<String, RowBufferStats> columnEpStats = result.getColumnEps();

    Assert.assertEquals(
        BigInteger.valueOf(11), columnEpStats.get("\"colTinyInt\"").getCurrentMaxIntValue());
    Assert.assertEquals(
        BigInteger.valueOf(10), columnEpStats.get("\"colTinyInt\"").getCurrentMinIntValue());
    Assert.assertEquals(0, columnEpStats.get("\"colTinyInt\"").getCurrentNullCount());
    Assert.assertEquals(-1, columnEpStats.get("\"colTinyInt\"").getDistinctValues());

    Assert.assertEquals(
        BigInteger.valueOf(1), columnEpStats.get("COLTINYINT").getCurrentMaxIntValue());
    Assert.assertEquals(
        BigInteger.valueOf(1), columnEpStats.get("COLTINYINT").getCurrentMinIntValue());
    Assert.assertEquals(0, columnEpStats.get("COLTINYINT").getCurrentNullCount());
    Assert.assertEquals(-1, columnEpStats.get("COLTINYINT").getDistinctValues());

    Assert.assertEquals(
        BigInteger.valueOf(3), columnEpStats.get("COLSMALLINT").getCurrentMaxIntValue());
    Assert.assertEquals(
        BigInteger.valueOf(2), columnEpStats.get("COLSMALLINT").getCurrentMinIntValue());
    Assert.assertEquals(0, columnEpStats.get("COLSMALLINT").getCurrentNullCount());
    Assert.assertEquals(-1, columnEpStats.get("COLSMALLINT").getDistinctValues());

    Assert.assertEquals(BigInteger.valueOf(3), columnEpStats.get("COLINT").getCurrentMaxIntValue());
    Assert.assertEquals(BigInteger.valueOf(3), columnEpStats.get("COLINT").getCurrentMinIntValue());
    Assert.assertEquals(1L, columnEpStats.get("COLINT").getCurrentNullCount());
    Assert.assertEquals(-1, columnEpStats.get("COLINT").getDistinctValues());

    Assert.assertEquals(
        BigInteger.valueOf(40), columnEpStats.get("COLBIGINT").getCurrentMaxIntValue());
    Assert.assertEquals(
        BigInteger.valueOf(4), columnEpStats.get("COLBIGINT").getCurrentMinIntValue());
    Assert.assertEquals(0, columnEpStats.get("COLBIGINT").getCurrentNullCount());
    Assert.assertEquals(-1, columnEpStats.get("COLBIGINT").getDistinctValues());

    Assert.assertEquals("alice", columnEpStats.get("COLCHAR").getCurrentMaxColStrValue());
    Assert.assertEquals("2", columnEpStats.get("COLCHAR").getCurrentMinColStrValue());
    Assert.assertEquals(0, columnEpStats.get("COLCHAR").getCurrentNullCount());
    Assert.assertEquals(-1, columnEpStats.get("COLCHAR").getDistinctValues());

    // Confirm we reset
    ChannelData<?> resetResults = rowBuffer.flush();
    Assert.assertNull(resetResults);
  }

  @Test
  public void testStatsE2ETimestamp() {
    testStatsE2ETimestampHelper(OpenChannelRequest.OnErrorOption.ABORT);
    testStatsE2ETimestampHelper(OpenChannelRequest.OnErrorOption.CONTINUE);
  }

  private void testStatsE2ETimestampHelper(OpenChannelRequest.OnErrorOption onErrorOption) {
    AbstractRowBuffer<?> innerBuffer = createTestBuffer(onErrorOption);

    ColumnMetadata colTimestampLtzSB8 = new ColumnMetadata();
    colTimestampLtzSB8.setName("COLTIMESTAMPLTZ_SB8");
    colTimestampLtzSB8.setPhysicalType("SB8");
    colTimestampLtzSB8.setNullable(true);
    colTimestampLtzSB8.setLogicalType("TIMESTAMP_LTZ");
    colTimestampLtzSB8.setScale(0);

    ColumnMetadata colTimestampLtzSB16 = new ColumnMetadata();
    colTimestampLtzSB16.setName("COLTIMESTAMPLTZ_SB16");
    colTimestampLtzSB16.setPhysicalType("SB16");
    colTimestampLtzSB16.setNullable(true);
    colTimestampLtzSB16.setLogicalType("TIMESTAMP_LTZ");
    colTimestampLtzSB16.setScale(9);

    ColumnMetadata colTimestampLtzSB16Scale6 = new ColumnMetadata();
    colTimestampLtzSB16Scale6.setName("COLTIMESTAMPLTZ_SB16_SCALE6");
    colTimestampLtzSB16Scale6.setPhysicalType("SB16");
    colTimestampLtzSB16Scale6.setNullable(true);
    colTimestampLtzSB16Scale6.setLogicalType("TIMESTAMP_LTZ");
    colTimestampLtzSB16Scale6.setScale(6);

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
        innerBuffer.insertRows(Arrays.asList(row1, row2, row3), null);
    Assert.assertFalse(response.hasErrors());
    ChannelData<?> result = innerBuffer.flush();
    Assert.assertEquals(3, result.getRowCount());

    Assert.assertEquals(
        BigInteger.valueOf(1621899220),
        result.getColumnEps().get("COLTIMESTAMPLTZ_SB8").getCurrentMinIntValue());
    Assert.assertEquals(
        BigInteger.valueOf(1621899221),
        result.getColumnEps().get("COLTIMESTAMPLTZ_SB8").getCurrentMaxIntValue());

    Assert.assertEquals(
        new BigInteger("1621899220123456789"),
        result.getColumnEps().get("COLTIMESTAMPLTZ_SB16").getCurrentMinIntValue());
    Assert.assertEquals(
        new BigInteger("1621899220223456789"),
        result.getColumnEps().get("COLTIMESTAMPLTZ_SB16").getCurrentMaxIntValue());

    Assert.assertEquals(
        new BigInteger("1621899220123456"),
        result.getColumnEps().get("COLTIMESTAMPLTZ_SB16_SCALE6").getCurrentMinIntValue());
    Assert.assertEquals(
        new BigInteger("1621899220123457"),
        result.getColumnEps().get("COLTIMESTAMPLTZ_SB16_SCALE6").getCurrentMaxIntValue());

    Assert.assertEquals(1, result.getColumnEps().get("COLTIMESTAMPLTZ_SB8").getCurrentNullCount());
    Assert.assertEquals(1, result.getColumnEps().get("COLTIMESTAMPLTZ_SB16").getCurrentNullCount());
    Assert.assertEquals(
        1, result.getColumnEps().get("COLTIMESTAMPLTZ_SB16_SCALE6").getCurrentNullCount());
  }

  @Test
  public void testE2EDate() {
    testE2EDateHelper(OpenChannelRequest.OnErrorOption.ABORT);
    testE2EDateHelper(OpenChannelRequest.OnErrorOption.CONTINUE);
  }

  private void testE2EDateHelper(OpenChannelRequest.OnErrorOption onErrorOption) {
    AbstractRowBuffer<?> innerBuffer = createTestBuffer(onErrorOption);

    ColumnMetadata colDate = new ColumnMetadata();
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
        innerBuffer.insertRows(Arrays.asList(row1, row2, row3), null);
    Assert.assertFalse(response.hasErrors());

    // Check data was inserted into the buffer correctly
    Assert.assertEquals(18772, innerBuffer.getVectorValueAt("COLDATE", 0));
    Assert.assertEquals(18773, innerBuffer.getVectorValueAt("COLDATE", 1));
    Assert.assertNull(innerBuffer.getVectorValueAt("COLDATE", 2));

    // Check stats generation
    ChannelData<?> result = innerBuffer.flush();
    Assert.assertEquals(3, result.getRowCount());

    Assert.assertEquals(
        BigInteger.valueOf(18772), result.getColumnEps().get("COLDATE").getCurrentMinIntValue());
    Assert.assertEquals(
        BigInteger.valueOf(18773), result.getColumnEps().get("COLDATE").getCurrentMaxIntValue());

    Assert.assertEquals(1, result.getColumnEps().get("COLDATE").getCurrentNullCount());
  }

  @Test
  public void testE2ETime() {
    testE2ETimeHelper(OpenChannelRequest.OnErrorOption.ABORT);
    testE2ETimeHelper(OpenChannelRequest.OnErrorOption.CONTINUE);
  }

  private void testE2ETimeHelper(OpenChannelRequest.OnErrorOption onErrorOption) {
    AbstractRowBuffer<?> innerBuffer = createTestBuffer(onErrorOption);

    ColumnMetadata colTimeSB4 = new ColumnMetadata();
    colTimeSB4.setName("COLTIMESB4");
    colTimeSB4.setPhysicalType("SB4");
    colTimeSB4.setNullable(true);
    colTimeSB4.setLogicalType("TIME");
    colTimeSB4.setScale(0);

    ColumnMetadata colTimeSB8 = new ColumnMetadata();
    colTimeSB8.setName("COLTIMESB8");
    colTimeSB8.setPhysicalType("SB8");
    colTimeSB8.setNullable(true);
    colTimeSB8.setLogicalType("TIME");
    colTimeSB8.setScale(3);

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
        innerBuffer.insertRows(Arrays.asList(row1, row2, row3), null);
    Assert.assertFalse(response.hasErrors());

    // Check data was inserted into the buffer correctly
    Assert.assertEquals(10 * 60 * 60, innerBuffer.getVectorValueAt("COLTIMESB4", 0));
    Assert.assertEquals(11 * 60 * 60 + 15 * 60, innerBuffer.getVectorValueAt("COLTIMESB4", 1));
    Assert.assertNull(innerBuffer.getVectorValueAt("COLTIMESB4", 2));

    Assert.assertEquals(10 * 60 * 60 * 1000L + 123, innerBuffer.getVectorValueAt("COLTIMESB8", 0));
    Assert.assertEquals(
        11 * 60 * 60 * 1000L + 15 * 60 * 1000 + 456, innerBuffer.getVectorValueAt("COLTIMESB8", 1));
    Assert.assertNull(innerBuffer.getVectorValueAt("COLTIMESB8", 2));

    // Check stats generation
    ChannelData<?> result = innerBuffer.flush();
    Assert.assertEquals(3, result.getRowCount());

    Assert.assertEquals(
        BigInteger.valueOf(10 * 60 * 60),
        result.getColumnEps().get("COLTIMESB4").getCurrentMinIntValue());
    Assert.assertEquals(
        BigInteger.valueOf(11 * 60 * 60 + 15 * 60),
        result.getColumnEps().get("COLTIMESB4").getCurrentMaxIntValue());
    Assert.assertEquals(1, result.getColumnEps().get("COLTIMESB4").getCurrentNullCount());

    Assert.assertEquals(
        BigInteger.valueOf(10 * 60 * 60 * 1000L + 123),
        result.getColumnEps().get("COLTIMESB8").getCurrentMinIntValue());
    Assert.assertEquals(
        BigInteger.valueOf(11 * 60 * 60 * 1000L + 15 * 60 * 1000 + 456),
        result.getColumnEps().get("COLTIMESB8").getCurrentMaxIntValue());
    Assert.assertEquals(1, result.getColumnEps().get("COLTIMESB8").getCurrentNullCount());
  }

  @Test
  public void testNullableCheck() {
    testNullableCheckHelper(OpenChannelRequest.OnErrorOption.ABORT);
    testNullableCheckHelper(OpenChannelRequest.OnErrorOption.CONTINUE);
  }

  private void testNullableCheckHelper(OpenChannelRequest.OnErrorOption onErrorOption) {
    AbstractRowBuffer<?> innerBuffer = createTestBuffer(onErrorOption);

    ColumnMetadata colBoolean = new ColumnMetadata();
    colBoolean.setName("COLBOOLEAN");
    colBoolean.setPhysicalType("SB1");
    colBoolean.setNullable(false);
    colBoolean.setLogicalType("BOOLEAN");
    colBoolean.setScale(0);

    innerBuffer.setupSchema(Collections.singletonList(colBoolean));
    Map<String, Object> row = new HashMap<>();
    row.put("COLBOOLEAN", true);

    InsertValidationResponse response = innerBuffer.insertRows(Collections.singletonList(row), "1");
    Assert.assertFalse(response.hasErrors());

    row.put("COLBOOLEAN", null);
    if (innerBuffer.owningChannel.getOnErrorOption() == OpenChannelRequest.OnErrorOption.CONTINUE) {
      response = innerBuffer.insertRows(Collections.singletonList(row), "1");
      Assert.assertTrue(response.hasErrors());
      Assert.assertEquals(
          ErrorCode.INVALID_ROW.getMessageCode(),
          response.getInsertErrors().get(0).getException().getVendorCode());
    } else {
      try {
        innerBuffer.insertRows(Collections.singletonList(row), "1");
      } catch (SFException e) {
        Assert.assertEquals(ErrorCode.INVALID_ROW.getMessageCode(), e.getVendorCode());
      }
    }
  }

  @Test
  public void testMissingColumnCheck() {
    testMissingColumnCheckHelper(OpenChannelRequest.OnErrorOption.ABORT);
    testMissingColumnCheckHelper(OpenChannelRequest.OnErrorOption.CONTINUE);
  }

  private void testMissingColumnCheckHelper(OpenChannelRequest.OnErrorOption onErrorOption) {
    AbstractRowBuffer<?> innerBuffer = createTestBuffer(onErrorOption);

    ColumnMetadata colBoolean = new ColumnMetadata();
    colBoolean.setName("COLBOOLEAN");
    colBoolean.setPhysicalType("SB1");
    colBoolean.setNullable(false);
    colBoolean.setLogicalType("BOOLEAN");
    colBoolean.setScale(0);

    ColumnMetadata colBoolean2 = new ColumnMetadata();
    colBoolean2.setName("COLBOOLEAN2");
    colBoolean2.setPhysicalType("SB1");
    colBoolean2.setNullable(true);
    colBoolean2.setLogicalType("BOOLEAN");
    colBoolean2.setScale(0);

    innerBuffer.setupSchema(Arrays.asList(colBoolean, colBoolean2));
    Map<String, Object> row = new HashMap<>();
    row.put("COLBOOLEAN", true);

    InsertValidationResponse response = innerBuffer.insertRows(Collections.singletonList(row), "1");
    Assert.assertFalse(response.hasErrors());

    Map<String, Object> row2 = new HashMap<>();
    row2.put("COLBOOLEAN2", true);
    if (innerBuffer.owningChannel.getOnErrorOption() == OpenChannelRequest.OnErrorOption.CONTINUE) {
      response = innerBuffer.insertRows(Collections.singletonList(row2), "2");
      Assert.assertTrue(response.hasErrors());
      InsertValidationResponse.InsertError error = response.getInsertErrors().get(0);
      Assert.assertEquals(
          ErrorCode.INVALID_ROW.getMessageCode(), error.getException().getVendorCode());
      Assert.assertEquals(
          Collections.singletonList("COLBOOLEAN"), error.getMissingNotNullColNames());
    } else {
      try {
        innerBuffer.insertRows(Collections.singletonList(row2), "2");
      } catch (SFException e) {
        Assert.assertEquals(ErrorCode.INVALID_ROW.getMessageCode(), e.getVendorCode());
      }
    }
  }

  @Test
  public void testExtraColumnsCheck() {
    AbstractRowBuffer<?> innerBuffer = createTestBuffer(OpenChannelRequest.OnErrorOption.CONTINUE);

    ColumnMetadata colBoolean = new ColumnMetadata();
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

    InsertValidationResponse response = innerBuffer.insertRows(Collections.singletonList(row), "1");
    Assert.assertTrue(response.hasErrors());
    InsertValidationResponse.InsertError error = response.getInsertErrors().get(0);
    Assert.assertEquals(
        ErrorCode.INVALID_ROW.getMessageCode(), error.getException().getVendorCode());
    Assert.assertEquals(Arrays.asList("COLBOOLEAN3", "COLBOOLEAN2"), error.getExtraColNames());
  }

  @Test
  public void testE2EBoolean() {
    testE2EBooleanHelper(OpenChannelRequest.OnErrorOption.ABORT);
    testE2EBooleanHelper(OpenChannelRequest.OnErrorOption.CONTINUE);
  }

  private void testE2EBooleanHelper(OpenChannelRequest.OnErrorOption onErrorOption) {
    AbstractRowBuffer<?> innerBuffer = createTestBuffer(onErrorOption);

    ColumnMetadata colBoolean = new ColumnMetadata();
    colBoolean.setName("COLBOOLEAN");
    colBoolean.setPhysicalType("SB1");
    colBoolean.setNullable(true);
    colBoolean.setLogicalType("BOOLEAN");
    colBoolean.setScale(0);

    innerBuffer.setupSchema(Collections.singletonList(colBoolean));

    Map<String, Object> row1 = new HashMap<>();
    row1.put("COLBOOLEAN", true);

    Map<String, Object> row2 = new HashMap<>();
    row2.put("COLBOOLEAN", false);

    Map<String, Object> row3 = new HashMap<>();
    row3.put("COLBOOLEAN", null);

    // innerBuffer.insertRows(Collections.singletonList(row1));
    InsertValidationResponse response =
        innerBuffer.insertRows(Arrays.asList(row1, row2, row3), null);
    Assert.assertFalse(response.hasErrors());

    // Check data was inserted into the buffer correctly
    Assert.assertEquals(true, innerBuffer.getVectorValueAt("COLBOOLEAN", 0));
    Assert.assertEquals(false, innerBuffer.getVectorValueAt("COLBOOLEAN", 1));
    Assert.assertNull(innerBuffer.getVectorValueAt("COLBOOLEAN", 2));

    // Check stats generation
    ChannelData<?> result = innerBuffer.flush();
    Assert.assertEquals(3, result.getRowCount());

    Assert.assertEquals(
        BigInteger.valueOf(0), result.getColumnEps().get("COLBOOLEAN").getCurrentMinIntValue());
    Assert.assertEquals(
        BigInteger.valueOf(1), result.getColumnEps().get("COLBOOLEAN").getCurrentMaxIntValue());
    Assert.assertEquals(1, result.getColumnEps().get("COLBOOLEAN").getCurrentNullCount());
  }

  @Test
  public void testE2EBinary() {
    testE2EBinaryHelper(OpenChannelRequest.OnErrorOption.ABORT);
    testE2EBinaryHelper(OpenChannelRequest.OnErrorOption.CONTINUE);
  }

  private void testE2EBinaryHelper(OpenChannelRequest.OnErrorOption onErrorOption) {
    AbstractRowBuffer<?> innerBuffer = createTestBuffer(onErrorOption);

    ColumnMetadata colBinary = new ColumnMetadata();
    colBinary.setName("COLBINARY");
    colBinary.setPhysicalType("LOB");
    colBinary.setNullable(true);
    colBinary.setLogicalType("BINARY");
    colBinary.setLength(32);
    colBinary.setScale(0);

    innerBuffer.setupSchema(Collections.singletonList(colBinary));

    Map<String, Object> row1 = new HashMap<>();
    row1.put("COLBINARY", "Hello World".getBytes(StandardCharsets.UTF_8));

    Map<String, Object> row2 = new HashMap<>();
    row2.put("COLBINARY", "Honk Honk".getBytes(StandardCharsets.UTF_8));

    Map<String, Object> row3 = new HashMap<>();
    row3.put("COLBINARY", null);

    InsertValidationResponse response =
        innerBuffer.insertRows(Arrays.asList(row1, row2, row3), null);
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
    Assert.assertEquals(11L, result.getColumnEps().get("COLBINARY").getCurrentMaxLength());
    Assert.assertEquals(
        "Hello World", result.getColumnEps().get("COLBINARY").getCurrentMinColStrValue());
    Assert.assertEquals(
        "Honk Honk", result.getColumnEps().get("COLBINARY").getCurrentMaxColStrValue());
    Assert.assertEquals(1, result.getColumnEps().get("COLBINARY").getCurrentNullCount());
  }

  @Test
  public void testE2EReal() {
    testE2ERealHelper(OpenChannelRequest.OnErrorOption.ABORT);
    testE2ERealHelper(OpenChannelRequest.OnErrorOption.CONTINUE);
  }

  private void testE2ERealHelper(OpenChannelRequest.OnErrorOption onErrorOption) {
    AbstractRowBuffer<?> innerBuffer = createTestBuffer(onErrorOption);

    ColumnMetadata colReal = new ColumnMetadata();
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
        innerBuffer.insertRows(Arrays.asList(row1, row2, row3), null);
    Assert.assertFalse(response.hasErrors());

    // Check data was inserted into the buffer correctly
    Assert.assertEquals(123.456, innerBuffer.getVectorValueAt("COLREAL", 0));
    Assert.assertEquals(123.4567, innerBuffer.getVectorValueAt("COLREAL", 1));
    Assert.assertNull(innerBuffer.getVectorValueAt("COLREAL", 2));

    // Check stats generation
    ChannelData<?> result = innerBuffer.flush();

    Assert.assertEquals(3, result.getRowCount());
    Assert.assertEquals(
        Double.valueOf(123.456), result.getColumnEps().get("COLREAL").getCurrentMinRealValue());
    Assert.assertEquals(
        Double.valueOf(123.4567), result.getColumnEps().get("COLREAL").getCurrentMaxRealValue());
    Assert.assertEquals(1, result.getColumnEps().get("COLREAL").getCurrentNullCount());
  }

  @Test
  public void testOnErrorAbortFailures() {
    AbstractRowBuffer<?> innerBuffer = createTestBuffer(OpenChannelRequest.OnErrorOption.ABORT);

    ColumnMetadata colDecimal = new ColumnMetadata();
    colDecimal.setName("COLDECIMAL");
    colDecimal.setPhysicalType("SB16");
    colDecimal.setNullable(true);
    colDecimal.setLogicalType("FIXED");
    colDecimal.setPrecision(38);
    colDecimal.setScale(0);

    innerBuffer.setupSchema(Collections.singletonList(colDecimal));
    Map<String, Object> row = new HashMap<>();
    row.put("COLDECIMAL", 1);

    InsertValidationResponse response = innerBuffer.insertRows(Collections.singletonList(row), "1");
    Assert.assertFalse(response.hasErrors());

    Assert.assertEquals(1, innerBuffer.rowCount);
    Assert.assertEquals(0, innerBuffer.getTempRowCount());
    Assert.assertEquals(
        1, innerBuffer.statsMap.get("COLDECIMAL").getCurrentMaxIntValue().intValue());
    Assert.assertEquals(
        1, innerBuffer.statsMap.get("COLDECIMAL").getCurrentMinIntValue().intValue());
    Assert.assertNull(innerBuffer.tempStatsMap.get("COLDECIMAL").getCurrentMaxIntValue());
    Assert.assertNull(innerBuffer.tempStatsMap.get("COLDECIMAL").getCurrentMinIntValue());

    Map<String, Object> row2 = new HashMap<>();
    row2.put("COLDECIMAL", 2);
    response = innerBuffer.insertRows(Collections.singletonList(row2), "2");
    Assert.assertFalse(response.hasErrors());

    Assert.assertEquals(2, innerBuffer.rowCount);
    Assert.assertEquals(0, innerBuffer.getTempRowCount());
    Assert.assertEquals(
        2, innerBuffer.statsMap.get("COLDECIMAL").getCurrentMaxIntValue().intValue());
    Assert.assertEquals(
        1, innerBuffer.statsMap.get("COLDECIMAL").getCurrentMinIntValue().intValue());
    Assert.assertNull(innerBuffer.tempStatsMap.get("COLDECIMAL").getCurrentMaxIntValue());
    Assert.assertNull(innerBuffer.tempStatsMap.get("COLDECIMAL").getCurrentMinIntValue());

    Map<String, Object> row3 = new HashMap<>();
    row3.put("COLDECIMAL", true);
    try {
      innerBuffer.insertRows(Collections.singletonList(row3), "3");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.INVALID_ROW.getMessageCode(), e.getVendorCode());
    }

    Assert.assertEquals(2, innerBuffer.rowCount);
    Assert.assertEquals(0, innerBuffer.getTempRowCount());
    Assert.assertEquals(
        2, innerBuffer.statsMap.get("COLDECIMAL").getCurrentMaxIntValue().intValue());
    Assert.assertEquals(
        1, innerBuffer.statsMap.get("COLDECIMAL").getCurrentMinIntValue().intValue());
    Assert.assertNull(innerBuffer.tempStatsMap.get("COLDECIMAL").getCurrentMaxIntValue());
    Assert.assertNull(innerBuffer.tempStatsMap.get("COLDECIMAL").getCurrentMinIntValue());

    row3.put("COLDECIMAL", 3);
    response = innerBuffer.insertRows(Collections.singletonList(row3), "3");
    Assert.assertFalse(response.hasErrors());
    Assert.assertEquals(3, innerBuffer.rowCount);
    Assert.assertEquals(0, innerBuffer.getTempRowCount());
    Assert.assertEquals(
        3, innerBuffer.statsMap.get("COLDECIMAL").getCurrentMaxIntValue().intValue());
    Assert.assertEquals(
        1, innerBuffer.statsMap.get("COLDECIMAL").getCurrentMinIntValue().intValue());
    Assert.assertNull(innerBuffer.tempStatsMap.get("COLDECIMAL").getCurrentMaxIntValue());
    Assert.assertNull(innerBuffer.tempStatsMap.get("COLDECIMAL").getCurrentMinIntValue());

    ChannelData<?> data = innerBuffer.flush();
    Assert.assertEquals(3, data.getRowCount());
    Assert.assertEquals(0, innerBuffer.rowCount);
  }
}
