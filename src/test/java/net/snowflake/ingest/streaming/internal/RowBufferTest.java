package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.streaming.internal.ArrowRowBuffer.DECIMAL_BIT_WIDTH;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.Logging;
import net.snowflake.ingest.utils.SFException;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.Text;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class RowBufferTest {
  private static final Logging logger = new Logging(RegisterService.class);

  private ArrowRowBuffer rowBufferOnErrorContinue;
  private SnowflakeStreamingIngestChannelInternal channelOnErrorContinue;

  private ArrowRowBuffer rowBufferOnErrorAbort;
  private SnowflakeStreamingIngestChannelInternal channelOnErrorAbort;

  @Before
  public void setupRowBuffer() {
    // Create row buffer
    SnowflakeStreamingIngestClientInternal client =
        new SnowflakeStreamingIngestClientInternal("client");
    this.channelOnErrorContinue =
        new SnowflakeStreamingIngestChannelInternal(
            "channel",
            "db",
            "schema",
            "table",
            "0",
            0L,
            0L,
            client,
            "key",
            1234L,
            OpenChannelRequest.OnErrorOption.CONTINUE,
            true);
    this.rowBufferOnErrorContinue = new ArrowRowBuffer(this.channelOnErrorContinue);

    this.channelOnErrorAbort =
        new SnowflakeStreamingIngestChannelInternal(
            "channel",
            "db",
            "schema",
            "table",
            "0",
            0L,
            0L,
            client,
            "key",
            1234L,
            OpenChannelRequest.OnErrorOption.ABORT,
            true);
    this.rowBufferOnErrorAbort = new ArrowRowBuffer(this.channelOnErrorAbort);

    ColumnMetadata colTinyIntCase = new ColumnMetadata();
    colTinyIntCase.setName("colTinyInt");
    colTinyIntCase.setPhysicalType("SB1");
    colTinyIntCase.setNullable(true);
    colTinyIntCase.setLogicalType("FIXED");
    colTinyIntCase.setScale(0);

    ColumnMetadata colTinyInt = new ColumnMetadata();
    colTinyInt.setName("COLTINYINT");
    colTinyInt.setPhysicalType("SB1");
    colTinyInt.setNullable(true);
    colTinyInt.setLogicalType("FIXED");
    colTinyInt.setScale(0);

    ColumnMetadata colSmallInt = new ColumnMetadata();
    colSmallInt.setName("COLSMALLINT");
    colSmallInt.setPhysicalType("SB2");
    colSmallInt.setNullable(true);
    colSmallInt.setLogicalType("FIXED");
    colSmallInt.setScale(0);

    ColumnMetadata colInt = new ColumnMetadata();
    colInt.setName("COLINT");
    colInt.setPhysicalType("SB4");
    colInt.setNullable(true);
    colInt.setLogicalType("FIXED");
    colInt.setScale(0);

    ColumnMetadata colBigInt = new ColumnMetadata();
    colBigInt.setName("COLBIGINT");
    colBigInt.setPhysicalType("SB8");
    colBigInt.setNullable(true);
    colBigInt.setLogicalType("FIXED");
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

    // Setup column fields and vectors
    this.rowBufferOnErrorContinue.setupSchema(
        Arrays.asList(
            colTinyIntCase, colTinyInt, colSmallInt, colInt, colBigInt, colDecimal, colChar));
    this.rowBufferOnErrorAbort.setupSchema(
        Arrays.asList(
            colTinyIntCase, colTinyInt, colSmallInt, colInt, colBigInt, colDecimal, colChar));
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
      Field result = this.rowBufferOnErrorContinue.buildField(testCol);
      Assert.fail("Expected error");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.UNKNOWN_DATA_TYPE.getMessageCode(), e.getVendorCode());
    }

    // Fixed LOB
    testCol = new ColumnMetadata();
    testCol.setPhysicalType("LOB");
    testCol.setLogicalType("FIXED");
    try {
      Field result = this.rowBufferOnErrorContinue.buildField(testCol);
      Assert.fail("Expected error");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.UNKNOWN_DATA_TYPE.getMessageCode(), e.getVendorCode());
    }

    // TIMESTAMP_NTZ SB2
    testCol = new ColumnMetadata();
    testCol.setPhysicalType("SB2");
    testCol.setLogicalType("TIMESTAMP_NTZ");
    try {
      Field result = this.rowBufferOnErrorContinue.buildField(testCol);
      Assert.fail("Expected error");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.UNKNOWN_DATA_TYPE.getMessageCode(), e.getVendorCode());
    }

    // TIMESTAMP_TZ SB1
    testCol = new ColumnMetadata();
    testCol.setPhysicalType("SB1");
    testCol.setLogicalType("TIMESTAMP_TZ");
    try {
      Field result = this.rowBufferOnErrorContinue.buildField(testCol);
      Assert.fail("Expected error");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.UNKNOWN_DATA_TYPE.getMessageCode(), e.getVendorCode());
    }

    // TIME SB16
    testCol = new ColumnMetadata();
    testCol.setPhysicalType("SB16");
    testCol.setLogicalType("TIME");
    try {
      Field result = this.rowBufferOnErrorContinue.buildField(testCol);
      Assert.fail("Expected error");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.UNKNOWN_DATA_TYPE.getMessageCode(), e.getVendorCode());
    }
  }

  @Test
  public void buildFieldFixedSB1() {
    // FIXED, SB1
    ColumnMetadata testCol = new ColumnMetadata();
    testCol.setName("testCol");
    testCol.setPhysicalType("SB1");
    testCol.setNullable(true);
    testCol.setLogicalType("FIXED");
    testCol.setByteLength(14);
    testCol.setLength(11);
    testCol.setScale(0);
    testCol.setPrecision(4);
    Field result = this.rowBufferOnErrorContinue.buildField(testCol);

    Assert.assertEquals("testCol", result.getName());
    Assert.assertEquals(result.getFieldType().getType(), Types.MinorType.TINYINT.getType());
    Assert.assertEquals(result.getFieldType().getMetadata().get("physicalType"), "SB1");
    Assert.assertEquals(result.getFieldType().getMetadata().get("scale"), "0");
    Assert.assertEquals(result.getFieldType().getMetadata().get("logicalType"), "FIXED");
    Assert.assertEquals(result.getFieldType().getMetadata().get("nullable"), "true");
    Assert.assertTrue(result.getFieldType().isNullable());
    Assert.assertEquals(result.getChildren().size(), 0);
  }

  @Test
  public void buildFieldFixedSB2() {
    ColumnMetadata testCol = new ColumnMetadata();
    testCol.setName("testCol");
    testCol.setPhysicalType("SB2");
    testCol.setNullable(false);
    testCol.setLogicalType("FIXED");
    testCol.setByteLength(14);
    testCol.setLength(11);
    testCol.setScale(0);
    testCol.setPrecision(4);
    Field result = this.rowBufferOnErrorContinue.buildField(testCol);

    Assert.assertEquals("testCol", result.getName());
    Assert.assertEquals(result.getFieldType().getType(), Types.MinorType.SMALLINT.getType());
    Assert.assertEquals(result.getFieldType().getMetadata().get("physicalType"), "SB2");
    Assert.assertEquals(result.getFieldType().getMetadata().get("scale"), "0");
    Assert.assertEquals(result.getFieldType().getMetadata().get("logicalType"), "FIXED");
    Assert.assertEquals(result.getFieldType().getMetadata().get("nullable"), "false");
    Assert.assertFalse(result.getFieldType().isNullable());
    Assert.assertEquals(result.getChildren().size(), 0);
  }

  @Test
  public void buildFieldFixedSB4() {
    ColumnMetadata testCol = new ColumnMetadata();
    testCol.setName("testCol");
    testCol.setPhysicalType("SB4");
    testCol.setNullable(true);
    testCol.setLogicalType("FIXED");
    testCol.setByteLength(14);
    testCol.setLength(11);
    testCol.setScale(0);
    testCol.setPrecision(4);
    Field result = this.rowBufferOnErrorContinue.buildField(testCol);

    Assert.assertEquals("testCol", result.getName());
    Assert.assertEquals(result.getFieldType().getType(), Types.MinorType.INT.getType());
    Assert.assertEquals(result.getFieldType().getMetadata().get("physicalType"), "SB4");
    Assert.assertEquals(result.getFieldType().getMetadata().get("scale"), "0");
    Assert.assertEquals(result.getFieldType().getMetadata().get("logicalType"), "FIXED");
    Assert.assertTrue(result.getFieldType().isNullable());
    Assert.assertEquals(result.getChildren().size(), 0);
  }

  @Test
  public void buildFieldFixedSB8() {
    ColumnMetadata testCol = new ColumnMetadata();
    testCol.setName("testCol");
    testCol.setPhysicalType("SB8");
    testCol.setNullable(true);
    testCol.setLogicalType("FIXED");
    testCol.setByteLength(14);
    testCol.setLength(11);
    testCol.setScale(0);
    testCol.setPrecision(4);
    Field result = this.rowBufferOnErrorContinue.buildField(testCol);

    Assert.assertEquals("testCol", result.getName());
    Assert.assertEquals(result.getFieldType().getType(), Types.MinorType.BIGINT.getType());
    Assert.assertEquals(result.getFieldType().getMetadata().get("physicalType"), "SB8");
    Assert.assertEquals(result.getFieldType().getMetadata().get("scale"), "0");
    Assert.assertEquals(result.getFieldType().getMetadata().get("logicalType"), "FIXED");
    Assert.assertTrue(result.getFieldType().isNullable());
    Assert.assertEquals(result.getChildren().size(), 0);
  }

  @Test
  public void buildFieldFixedSB16() {
    ColumnMetadata testCol = new ColumnMetadata();
    testCol.setName("testCol");
    testCol.setPhysicalType("SB16");
    testCol.setNullable(true);
    testCol.setLogicalType("FIXED");
    testCol.setByteLength(14);
    testCol.setLength(11);
    testCol.setScale(0);
    testCol.setPrecision(4);
    Field result = this.rowBufferOnErrorContinue.buildField(testCol);

    ArrowType expectedType =
        new ArrowType.Decimal(testCol.getPrecision(), testCol.getScale(), DECIMAL_BIT_WIDTH);

    Assert.assertEquals("testCol", result.getName());
    Assert.assertEquals(result.getFieldType().getType(), expectedType);
    Assert.assertEquals(result.getFieldType().getMetadata().get("physicalType"), "SB16");
    Assert.assertEquals(result.getFieldType().getMetadata().get("scale"), "0");
    Assert.assertEquals(result.getFieldType().getMetadata().get("logicalType"), "FIXED");
    Assert.assertTrue(result.getFieldType().isNullable());
    Assert.assertEquals(result.getChildren().size(), 0);
  }

  @Test
  public void buildFieldLobVariant() {
    ColumnMetadata testCol = new ColumnMetadata();
    testCol.setName("testCol");
    testCol.setPhysicalType("LOB");
    testCol.setNullable(true);
    testCol.setLogicalType("VARIANT");
    testCol.setByteLength(14);
    testCol.setLength(11);
    testCol.setScale(0);
    testCol.setPrecision(4);
    Field result = this.rowBufferOnErrorContinue.buildField(testCol);

    Assert.assertEquals("testCol", result.getName());
    Assert.assertEquals(result.getFieldType().getType(), Types.MinorType.VARCHAR.getType());
    Assert.assertEquals(result.getFieldType().getMetadata().get("physicalType"), "LOB");
    Assert.assertEquals(result.getFieldType().getMetadata().get("scale"), "0");
    Assert.assertEquals(result.getFieldType().getMetadata().get("logicalType"), "VARIANT");
    Assert.assertTrue(result.getFieldType().isNullable());
    Assert.assertEquals(result.getChildren().size(), 0);
  }

  @Test
  public void buildFieldTimestampNtzSB8() {
    ColumnMetadata testCol = new ColumnMetadata();
    testCol.setName("testCol");
    testCol.setPhysicalType("SB8");
    testCol.setNullable(true);
    testCol.setLogicalType("TIMESTAMP_NTZ");
    testCol.setByteLength(14);
    testCol.setLength(11);
    testCol.setScale(0);
    testCol.setPrecision(4);
    Field result = this.rowBufferOnErrorContinue.buildField(testCol);

    Assert.assertEquals("testCol", result.getName());
    Assert.assertEquals(result.getFieldType().getType(), Types.MinorType.BIGINT.getType());
    Assert.assertEquals(result.getFieldType().getMetadata().get("physicalType"), "SB8");
    Assert.assertEquals(result.getFieldType().getMetadata().get("scale"), "0");
    Assert.assertEquals(result.getFieldType().getMetadata().get("logicalType"), "TIMESTAMP_NTZ");
    Assert.assertTrue(result.getFieldType().isNullable());
    Assert.assertEquals(result.getChildren().size(), 0);
  }

  @Test
  public void buildFieldTimestampNtzSB16() {
    ColumnMetadata testCol = new ColumnMetadata();
    testCol.setName("testCol");
    testCol.setPhysicalType("SB16");
    testCol.setNullable(true);
    testCol.setLogicalType("TIMESTAMP_NTZ");
    testCol.setByteLength(14);
    testCol.setLength(11);
    testCol.setScale(0);
    testCol.setPrecision(4);
    Field result = this.rowBufferOnErrorContinue.buildField(testCol);

    Assert.assertEquals("testCol", result.getName());
    Assert.assertEquals(result.getFieldType().getType(), Types.MinorType.STRUCT.getType());
    Assert.assertEquals(result.getFieldType().getMetadata().get("physicalType"), "SB16");
    Assert.assertEquals(result.getFieldType().getMetadata().get("scale"), "0");
    Assert.assertEquals(result.getFieldType().getMetadata().get("logicalType"), "TIMESTAMP_NTZ");
    Assert.assertTrue(result.getFieldType().isNullable());
    Assert.assertEquals(result.getChildren().size(), 2);
    Assert.assertEquals(
        result.getChildren().get(0).getFieldType().getType(), Types.MinorType.BIGINT.getType());
    Assert.assertEquals(
        result.getChildren().get(1).getFieldType().getType(), Types.MinorType.INT.getType());
  }

  @Test
  public void buildFieldTimestampTzSB8() throws Exception {
    ColumnMetadata testCol = new ColumnMetadata();
    testCol.setName("testCol");
    testCol.setPhysicalType("SB8");
    testCol.setNullable(true);
    testCol.setLogicalType("TIMESTAMP_TZ");
    testCol.setByteLength(14);
    testCol.setLength(11);
    testCol.setScale(0);
    testCol.setPrecision(4);
    Field result = this.rowBufferOnErrorContinue.buildField(testCol);

    Assert.assertEquals("testCol", result.getName());
    Assert.assertEquals(result.getFieldType().getType(), Types.MinorType.STRUCT.getType());
    Assert.assertEquals(result.getFieldType().getMetadata().get("physicalType"), "SB8");
    Assert.assertEquals(result.getFieldType().getMetadata().get("scale"), "0");
    Assert.assertEquals(result.getFieldType().getMetadata().get("logicalType"), "TIMESTAMP_TZ");
    Assert.assertTrue(result.getFieldType().isNullable());
    Assert.assertEquals(result.getChildren().size(), 2);
    Assert.assertEquals(
        result.getChildren().get(0).getFieldType().getType(), Types.MinorType.BIGINT.getType());
    Assert.assertEquals(
        result.getChildren().get(1).getFieldType().getType(), Types.MinorType.INT.getType());
  }

  @Test
  public void buildFieldTimestampTzSB16() throws Exception {
    ColumnMetadata testCol = new ColumnMetadata();
    testCol.setName("testCol");
    testCol.setPhysicalType("SB16");
    testCol.setNullable(true);
    testCol.setLogicalType("TIMESTAMP_TZ");
    testCol.setByteLength(14);
    testCol.setLength(11);
    testCol.setScale(0);
    testCol.setPrecision(4);
    Field result = this.rowBufferOnErrorContinue.buildField(testCol);

    Assert.assertEquals("testCol", result.getName());
    Assert.assertEquals(result.getFieldType().getType(), Types.MinorType.STRUCT.getType());
    Assert.assertEquals(result.getFieldType().getMetadata().get("physicalType"), "SB16");
    Assert.assertEquals(result.getFieldType().getMetadata().get("scale"), "0");
    Assert.assertEquals(result.getFieldType().getMetadata().get("logicalType"), "TIMESTAMP_TZ");
    Assert.assertTrue(result.getFieldType().isNullable());
    Assert.assertEquals(result.getChildren().size(), 3);
    Assert.assertEquals(
        result.getChildren().get(0).getFieldType().getType(), Types.MinorType.BIGINT.getType());
    Assert.assertEquals(
        result.getChildren().get(1).getFieldType().getType(), Types.MinorType.INT.getType());
    Assert.assertEquals(
        result.getChildren().get(2).getFieldType().getType(), Types.MinorType.INT.getType());
  }

  @Test
  public void buildFieldTimestampDate() {
    ColumnMetadata testCol = new ColumnMetadata();
    testCol.setName("testCol");
    testCol.setPhysicalType("SB8");
    testCol.setNullable(true);
    testCol.setLogicalType("DATE");
    testCol.setByteLength(14);
    testCol.setLength(11);
    testCol.setScale(0);
    testCol.setPrecision(4);
    Field result = this.rowBufferOnErrorContinue.buildField(testCol);

    Assert.assertEquals("testCol", result.getName());
    Assert.assertEquals(result.getFieldType().getType(), Types.MinorType.DATEDAY.getType());
    Assert.assertEquals(result.getFieldType().getMetadata().get("physicalType"), "SB8");
    Assert.assertEquals(result.getFieldType().getMetadata().get("scale"), "0");
    Assert.assertEquals(result.getFieldType().getMetadata().get("logicalType"), "DATE");
    Assert.assertTrue(result.getFieldType().isNullable());
    Assert.assertEquals(result.getChildren().size(), 0);
  }

  @Test
  public void buildFieldTimeSB4() {
    ColumnMetadata testCol = new ColumnMetadata();
    testCol.setName("testCol");
    testCol.setPhysicalType("SB4");
    testCol.setNullable(true);
    testCol.setLogicalType("TIME");
    testCol.setByteLength(14);
    testCol.setLength(11);
    testCol.setScale(0);
    testCol.setPrecision(4);
    Field result = this.rowBufferOnErrorContinue.buildField(testCol);

    Assert.assertEquals("testCol", result.getName());
    Assert.assertEquals(result.getFieldType().getType(), Types.MinorType.INT.getType());
    Assert.assertEquals(result.getFieldType().getMetadata().get("physicalType"), "SB4");
    Assert.assertEquals(result.getFieldType().getMetadata().get("scale"), "0");
    Assert.assertEquals(result.getFieldType().getMetadata().get("logicalType"), "TIME");
    Assert.assertTrue(result.getFieldType().isNullable());
    Assert.assertEquals(result.getChildren().size(), 0);
  }

  @Test
  public void buildFieldTimeSB8() {
    ColumnMetadata testCol = new ColumnMetadata();
    testCol.setName("testCol");
    testCol.setPhysicalType("SB8");
    testCol.setNullable(true);
    testCol.setLogicalType("TIME");
    testCol.setByteLength(14);
    testCol.setLength(11);
    testCol.setScale(0);
    testCol.setPrecision(4);
    Field result = this.rowBufferOnErrorContinue.buildField(testCol);

    Assert.assertEquals("testCol", result.getName());
    Assert.assertEquals(result.getFieldType().getType(), Types.MinorType.BIGINT.getType());
    Assert.assertEquals(result.getFieldType().getMetadata().get("physicalType"), "SB8");
    Assert.assertEquals(result.getFieldType().getMetadata().get("scale"), "0");
    Assert.assertEquals(result.getFieldType().getMetadata().get("logicalType"), "TIME");
    Assert.assertTrue(result.getFieldType().isNullable());
    Assert.assertEquals(result.getChildren().size(), 0);
  }

  @Test
  public void buildFieldBoolean() {
    ColumnMetadata testCol = new ColumnMetadata();
    testCol.setName("testCol");
    testCol.setPhysicalType("BINARY");
    testCol.setNullable(true);
    testCol.setLogicalType("BOOLEAN");
    testCol.setByteLength(14);
    testCol.setLength(11);
    testCol.setScale(0);
    testCol.setPrecision(4);
    Field result = this.rowBufferOnErrorContinue.buildField(testCol);

    Assert.assertEquals("testCol", result.getName());
    Assert.assertEquals(result.getFieldType().getType(), Types.MinorType.BIT.getType());
    Assert.assertEquals(result.getFieldType().getMetadata().get("physicalType"), "BINARY");
    Assert.assertEquals(result.getFieldType().getMetadata().get("scale"), "0");
    Assert.assertEquals(result.getFieldType().getMetadata().get("logicalType"), "BOOLEAN");
    Assert.assertTrue(result.getFieldType().isNullable());
    Assert.assertEquals(result.getChildren().size(), 0);
  }

  @Test
  public void buildFieldRealSB16() {
    ColumnMetadata testCol = new ColumnMetadata();
    testCol.setName("testCol");
    testCol.setPhysicalType("SB16");
    testCol.setNullable(true);
    testCol.setLogicalType("REAL");
    testCol.setByteLength(14);
    testCol.setLength(11);
    testCol.setScale(0);
    testCol.setPrecision(4);
    Field result = this.rowBufferOnErrorContinue.buildField(testCol);

    Assert.assertEquals("testCol", result.getName());
    Assert.assertEquals(result.getFieldType().getType(), Types.MinorType.FLOAT8.getType());
    Assert.assertEquals(result.getFieldType().getMetadata().get("physicalType"), "SB16");
    Assert.assertEquals(result.getFieldType().getMetadata().get("scale"), "0");
    Assert.assertEquals(result.getFieldType().getMetadata().get("logicalType"), "REAL");
    Assert.assertTrue(result.getFieldType().isNullable());
    Assert.assertEquals(result.getChildren().size(), 0);
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

  private void testStringLengthHelper(ArrowRowBuffer rowBuffer) {
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

  private void testInsertRowHelper(ArrowRowBuffer rowBuffer) {
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

  private void testInsertRowsHelper(ArrowRowBuffer rowBuffer) {
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
    this.rowBufferOnErrorContinue.close();
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

  private void testFlushHelper(ArrowRowBuffer rowBuffer) {
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

    ChannelData data = rowBuffer.flush();
    Assert.assertEquals(2, data.getRowCount());
    Assert.assertEquals((Long) 1L, data.getRowSequencer());
    Assert.assertEquals(7, data.getVectors().getFieldVectors().size());
    Assert.assertEquals(offsetToken, data.getOffsetToken());
    Assert.assertEquals(bufferSize, data.getBufferSize(), 0);
  }

  @Test
  public void testDoubleQuotesColumnName() {
    testDoubleQuotesColumnNameHelper(this.channelOnErrorAbort);
    testDoubleQuotesColumnNameHelper(this.channelOnErrorContinue);
  }

  private void testDoubleQuotesColumnNameHelper(SnowflakeStreamingIngestChannelInternal channel) {
    ArrowRowBuffer innerBuffer = new ArrowRowBuffer(channel);

    ColumnMetadata colDoubleQuotes = new ColumnMetadata();
    colDoubleQuotes.setName("colDoubleQuotes");
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

    EpInfo result = ArrowRowBuffer.buildEpInfoFromStats(2, colStats);
    Map<String, FileColumnProperties> columnResults = result.getColumnEps();
    Assert.assertEquals(2, columnResults.keySet().size());

    FileColumnProperties strColumnResult = columnResults.get("strColumn");
    Assert.assertEquals(-1, strColumnResult.getDistinctValues());
    Assert.assertEquals("alice", strColumnResult.getMinStrValue());
    Assert.assertEquals("bob", strColumnResult.getMaxStrValue());
    Assert.assertEquals(1, strColumnResult.getNullCount());
    logger.logDebug("strColumnResult={}", strColumnResult.toString());

    FileColumnProperties intColumnResult = columnResults.get("intColumn");
    Assert.assertEquals(-1, intColumnResult.getDistinctValues());
    Assert.assertEquals(BigInteger.valueOf(1), intColumnResult.getMinIntValue());
    Assert.assertEquals(BigInteger.valueOf(10), intColumnResult.getMaxIntValue());
    Assert.assertEquals(0, intColumnResult.getNullCount());
    logger.logDebug("intColumnResult={}", strColumnResult.toString());
  }

  @Test
  public void testArrowE2E() {
    testArrowE2EHelper(this.rowBufferOnErrorAbort);
    testArrowE2EHelper(this.rowBufferOnErrorContinue);
  }

  private void testArrowE2EHelper(ArrowRowBuffer rowBuffer) {
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

    Assert.assertEquals((byte) 10, rowBuffer.vectorsRoot.getVector("colTinyInt").getObject(0));
    Assert.assertEquals((byte) 1, rowBuffer.vectorsRoot.getVector("COLTINYINT").getObject(0));
    Assert.assertEquals((short) 2, rowBuffer.vectorsRoot.getVector("COLSMALLINT").getObject(0));
    Assert.assertEquals(3, rowBuffer.vectorsRoot.getVector("COLINT").getObject(0));
    Assert.assertEquals(4L, rowBuffer.vectorsRoot.getVector("COLBIGINT").getObject(0));
    Assert.assertEquals(
        new BigDecimal("4.00"), rowBuffer.vectorsRoot.getVector("COLDECIMAL").getObject(0));
    Assert.assertEquals(new Text("2"), rowBuffer.vectorsRoot.getVector("COLCHAR").getObject(0));
  }

  @Test
  public void testArrowE2ETimestampLTZ() {
    testArrowE2ETimestampLTZHelper(this.channelOnErrorContinue);
    testArrowE2ETimestampLTZHelper(this.channelOnErrorAbort);
  }

  private void testArrowE2ETimestampLTZHelper(SnowflakeStreamingIngestChannelInternal channel) {
    ArrowRowBuffer innerBuffer = new ArrowRowBuffer(channel);

    ColumnMetadata colTimestampLtzSB8 = new ColumnMetadata();
    colTimestampLtzSB8.setName("COLTIMESTAMPLTZ_SB8");
    colTimestampLtzSB8.setPhysicalType("SB8");
    colTimestampLtzSB8.setNullable(false);
    colTimestampLtzSB8.setLogicalType("TIMESTAMP_LTZ");
    colTimestampLtzSB8.setScale(0);

    ColumnMetadata colTimestampLtzSB16 = new ColumnMetadata();
    colTimestampLtzSB16.setName("COLTIMESTAMPLTZ_SB16");
    colTimestampLtzSB16.setPhysicalType("SB16");
    colTimestampLtzSB16.setNullable(false);
    colTimestampLtzSB16.setLogicalType("TIMESTAMP_LTZ");
    colTimestampLtzSB16.setScale(9);

    innerBuffer.setupSchema(Arrays.asList(colTimestampLtzSB8, colTimestampLtzSB16));

    Map<String, Object> row = new HashMap<>();
    row.put("COLTIMESTAMPLTZ_SB8", "1621899220");
    row.put("COLTIMESTAMPLTZ_SB16", new BigDecimal("1621899220.123456789"));

    InsertValidationResponse response =
        innerBuffer.insertRows(Collections.singletonList(row), null);
    Assert.assertFalse(response.hasErrors());
    Assert.assertEquals(
        1621899220l, innerBuffer.vectorsRoot.getVector("COLTIMESTAMPLTZ_SB8").getObject(0));
    Assert.assertEquals(
        "epoch",
        innerBuffer
            .vectorsRoot
            .getVector("COLTIMESTAMPLTZ_SB16")
            .getChildrenFromFields()
            .get(0)
            .getName());
    Assert.assertEquals(
        1621899220l,
        innerBuffer
            .vectorsRoot
            .getVector("COLTIMESTAMPLTZ_SB16")
            .getChildrenFromFields()
            .get(0)
            .getObject(0));
    Assert.assertEquals(
        "fraction",
        innerBuffer
            .vectorsRoot
            .getVector("COLTIMESTAMPLTZ_SB16")
            .getChildrenFromFields()
            .get(1)
            .getName());
    Assert.assertEquals(
        123456789,
        innerBuffer
            .vectorsRoot
            .getVector("COLTIMESTAMPLTZ_SB16")
            .getChildrenFromFields()
            .get(1)
            .getObject(0));
  }

  @Test
  public void testArrowE2ETimestampTZ() {
    testArrowE2ETimestampTZHelper(this.channelOnErrorContinue);
    testArrowE2ETimestampTZHelper(this.channelOnErrorAbort);
  }

  private void testArrowE2ETimestampTZHelper(SnowflakeStreamingIngestChannelInternal channel) {
    ArrowRowBuffer innerBuffer = new ArrowRowBuffer(channel);

    ColumnMetadata colTimestampTzSB8 = new ColumnMetadata();
    colTimestampTzSB8.setName("COLTIMESTAMPTZ_SB8");
    colTimestampTzSB8.setPhysicalType("SB8");
    colTimestampTzSB8.setNullable(false);
    colTimestampTzSB8.setLogicalType("TIMESTAMP_TZ");
    colTimestampTzSB8.setScale(0);

    ColumnMetadata colTimestampTzSB16 = new ColumnMetadata();
    colTimestampTzSB16.setName("COLTIMESTAMPTZ_SB16");
    colTimestampTzSB16.setPhysicalType("SB16");
    colTimestampTzSB16.setNullable(false);
    colTimestampTzSB16.setLogicalType("TIMESTAMP_TZ");
    colTimestampTzSB16.setScale(9);

    innerBuffer.setupSchema(Arrays.asList(colTimestampTzSB8, colTimestampTzSB16));

    Map<String, Object> row = new HashMap<>();
    row.put("COLTIMESTAMPTZ_SB8", "2021-01-01 01:00:00 +0100");
    row.put("COLTIMESTAMPTZ_SB16", "2021-01-01 10:00:00.123456789 +1000");

    InsertValidationResponse response =
        innerBuffer.insertRows(Collections.singletonList(row), null);
    Assert.assertFalse(response.hasErrors());

    // SB8
    Assert.assertEquals(
        "epoch",
        innerBuffer
            .vectorsRoot
            .getVector("COLTIMESTAMPTZ_SB8")
            .getChildrenFromFields()
            .get(0)
            .getName());
    Assert.assertEquals(
        1609459200l,
        innerBuffer
            .vectorsRoot
            .getVector("COLTIMESTAMPTZ_SB8")
            .getChildrenFromFields()
            .get(0)
            .getObject(0));

    Assert.assertEquals(
        "timezone",
        innerBuffer
            .vectorsRoot
            .getVector("COLTIMESTAMPTZ_SB8")
            .getChildrenFromFields()
            .get(1)
            .getName());
    Assert.assertEquals(
        1500,
        innerBuffer
            .vectorsRoot
            .getVector("COLTIMESTAMPTZ_SB8")
            .getChildrenFromFields()
            .get(1)
            .getObject(0));

    // SB16
    Assert.assertEquals(
        "epoch",
        innerBuffer
            .vectorsRoot
            .getVector("COLTIMESTAMPTZ_SB16")
            .getChildrenFromFields()
            .get(0)
            .getName());
    Assert.assertEquals(
        1609459200l,
        innerBuffer
            .vectorsRoot
            .getVector("COLTIMESTAMPTZ_SB16")
            .getChildrenFromFields()
            .get(0)
            .getObject(0));

    Assert.assertEquals(
        "fraction",
        innerBuffer
            .vectorsRoot
            .getVector("COLTIMESTAMPTZ_SB16")
            .getChildrenFromFields()
            .get(1)
            .getName());
    Assert.assertEquals(
        123456789,
        innerBuffer
            .vectorsRoot
            .getVector("COLTIMESTAMPTZ_SB16")
            .getChildrenFromFields()
            .get(1)
            .getObject(0));

    Assert.assertEquals(
        "timezone",
        innerBuffer
            .vectorsRoot
            .getVector("COLTIMESTAMPTZ_SB16")
            .getChildrenFromFields()
            .get(2)
            .getName());
    Assert.assertEquals(
        2040,
        innerBuffer
            .vectorsRoot
            .getVector("COLTIMESTAMPTZ_SB16")
            .getChildrenFromFields()
            .get(2)
            .getObject(0));
  }

  @Test
  public void testArrowE2ETimestampErrors() {
    testArrowE2ETimestampErrorsHelper(this.channelOnErrorAbort);
    testArrowE2ETimestampErrorsHelper(this.channelOnErrorContinue);
  }

  private void testArrowE2ETimestampErrorsHelper(SnowflakeStreamingIngestChannelInternal channel) {
    ArrowRowBuffer innerBuffer = new ArrowRowBuffer(channel);

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

    if (channel.getOnErrorOption() == OpenChannelRequest.OnErrorOption.CONTINUE) {
      InsertValidationResponse response =
          innerBuffer.insertRows(Collections.singletonList(row), null);
      Assert.assertTrue(response.hasErrors());
      Assert.assertEquals(
          ErrorCode.INVALID_ROW.getMessageCode(),
          response.getInsertErrors().get(0).getException().getVendorCode());
    } else {
      try {
        InsertValidationResponse response =
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

  private void testStatsE2EHelper(ArrowRowBuffer rowBuffer) {
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
    ChannelData result = rowBuffer.flush();
    Map<String, RowBufferStats> columnEpStats = result.getColumnEps();

    Assert.assertEquals(
        BigInteger.valueOf(11), columnEpStats.get("colTinyInt").getCurrentMaxIntValue());
    Assert.assertEquals(
        BigInteger.valueOf(10), columnEpStats.get("colTinyInt").getCurrentMinIntValue());
    Assert.assertEquals(0, columnEpStats.get("colTinyInt").getCurrentNullCount());
    Assert.assertEquals(-1, columnEpStats.get("colTinyInt").getDistinctValues());

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

    Assert.assertEquals("alice", columnEpStats.get("COLCHAR").getCurrentMaxStrValue());
    Assert.assertEquals("2", columnEpStats.get("COLCHAR").getCurrentMinStrValue());
    Assert.assertEquals(0, columnEpStats.get("COLCHAR").getCurrentNullCount());
    Assert.assertEquals(-1, columnEpStats.get("COLCHAR").getDistinctValues());

    // Confirm we reset
    ChannelData resetResults = rowBuffer.flush();
    Assert.assertNull(resetResults);
  }

  @Test
  public void testStatsE2ETimestamp() {
    testStatsE2ETimestampHelper(this.channelOnErrorAbort);
    testStatsE2ETimestampHelper(this.channelOnErrorContinue);
  }

  private void testStatsE2ETimestampHelper(SnowflakeStreamingIngestChannelInternal channel) {
    ArrowRowBuffer innerBuffer = new ArrowRowBuffer(channel);

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
    row1.put("COLTIMESTAMPLTZ_SB16", "1621899220.123456789");
    row1.put("COLTIMESTAMPLTZ_SB16_SCALE6", "1621899220.123456");

    Map<String, Object> row2 = new HashMap<>();
    row2.put("COLTIMESTAMPLTZ_SB8", "1621899221");
    row2.put("COLTIMESTAMPLTZ_SB16", "1621899220.12345679");
    row2.put("COLTIMESTAMPLTZ_SB16_SCALE6", "1621899220.123457");

    Map<String, Object> row3 = new HashMap<>();
    row3.put("COLTIMESTAMPLTZ_SB8", null);
    row3.put("COLTIMESTAMPLTZ_SB16", null);
    row3.put("COLTIMESTAMPLTZ_SB16_SCALE6", null);

    InsertValidationResponse response =
        innerBuffer.insertRows(Arrays.asList(row1, row2, row3), null);
    Assert.assertFalse(response.hasErrors());
    ChannelData result = innerBuffer.flush();
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
        new BigInteger("1621899220123456790"),
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
    testE2EDateHelper(this.channelOnErrorContinue);
    testE2EDateHelper(this.channelOnErrorAbort);
  }

  private void testE2EDateHelper(SnowflakeStreamingIngestChannelInternal channel) {
    ArrowRowBuffer innerBuffer = new ArrowRowBuffer(channel);

    ColumnMetadata colDate = new ColumnMetadata();
    colDate.setName("COLDATE");
    colDate.setPhysicalType("SB8");
    colDate.setNullable(true);
    colDate.setLogicalType("DATE");
    colDate.setScale(0);

    innerBuffer.setupSchema(Collections.singletonList(colDate));

    Map<String, Object> row1 = new HashMap<>();
    row1.put("COLDATE", "18772");

    Map<String, Object> row2 = new HashMap<>();
    row2.put("COLDATE", "18773");

    Map<String, Object> row3 = new HashMap<>();
    row3.put("COLDATE", null);

    InsertValidationResponse response =
        innerBuffer.insertRows(Arrays.asList(row1, row2, row3), null);
    Assert.assertFalse(response.hasErrors());

    // Check data was inserted into Arrow correctly
    Assert.assertEquals(18772, innerBuffer.vectorsRoot.getVector("COLDATE").getObject(0));
    Assert.assertEquals(18773, innerBuffer.vectorsRoot.getVector("COLDATE").getObject(1));
    Assert.assertNull(innerBuffer.vectorsRoot.getVector("COLDATE").getObject(2));

    // Check stats generation
    ChannelData result = innerBuffer.flush();
    Assert.assertEquals(3, result.getRowCount());

    Assert.assertEquals(
        BigInteger.valueOf(18772), result.getColumnEps().get("COLDATE").getCurrentMinIntValue());
    Assert.assertEquals(
        BigInteger.valueOf(18773), result.getColumnEps().get("COLDATE").getCurrentMaxIntValue());

    Assert.assertEquals(1, result.getColumnEps().get("COLDATE").getCurrentNullCount());
  }

  @Test
  public void testE2ETime() {
    testE2ETimeHelper(this.channelOnErrorAbort);
    testE2ETimeHelper(this.channelOnErrorContinue);
  }

  private void testE2ETimeHelper(SnowflakeStreamingIngestChannelInternal channel) {
    ArrowRowBuffer innerBuffer = new ArrowRowBuffer(channel);

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
    row1.put("COLTIMESB4", "43200");
    row1.put("COLTIMESB8", "44200.123");

    Map<String, Object> row2 = new HashMap<>();
    row2.put("COLTIMESB4", "43260");
    row2.put("COLTIMESB8", "44201");

    Map<String, Object> row3 = new HashMap<>();
    row3.put("COLTIMESB4", null);
    row3.put("COLTIMESB8", null);

    InsertValidationResponse response =
        innerBuffer.insertRows(Arrays.asList(row1, row2, row3), null);
    Assert.assertFalse(response.hasErrors());

    // Check data was inserted into Arrow correctly
    Assert.assertEquals(43200, innerBuffer.vectorsRoot.getVector("COLTIMESB4").getObject(0));
    Assert.assertEquals(43260, innerBuffer.vectorsRoot.getVector("COLTIMESB4").getObject(1));
    Assert.assertNull(innerBuffer.vectorsRoot.getVector("COLTIMESB4").getObject(2));

    Assert.assertEquals(44200123l, innerBuffer.vectorsRoot.getVector("COLTIMESB8").getObject(0));
    Assert.assertEquals(44201000l, innerBuffer.vectorsRoot.getVector("COLTIMESB8").getObject(1));
    Assert.assertNull(innerBuffer.vectorsRoot.getVector("COLTIMESB8").getObject(2));

    // Check stats generation
    ChannelData result = innerBuffer.flush();
    Assert.assertEquals(3, result.getRowCount());

    Assert.assertEquals(
        BigInteger.valueOf(43200), result.getColumnEps().get("COLTIMESB4").getCurrentMinIntValue());
    Assert.assertEquals(
        BigInteger.valueOf(43260), result.getColumnEps().get("COLTIMESB4").getCurrentMaxIntValue());
    Assert.assertEquals(1, result.getColumnEps().get("COLTIMESB4").getCurrentNullCount());

    Assert.assertEquals(
        BigInteger.valueOf(44200123),
        result.getColumnEps().get("COLTIMESB8").getCurrentMinIntValue());
    Assert.assertEquals(
        BigInteger.valueOf(44201000),
        result.getColumnEps().get("COLTIMESB8").getCurrentMaxIntValue());
    Assert.assertEquals(1, result.getColumnEps().get("COLTIMESB8").getCurrentNullCount());
  }

  @Test
  public void testNullableCheck() {
    testNullableCheckHelper(this.channelOnErrorContinue);
    testNullableCheckHelper(this.channelOnErrorAbort);
  }

  private void testNullableCheckHelper(SnowflakeStreamingIngestChannelInternal channel) {
    ArrowRowBuffer innerBuffer = new ArrowRowBuffer(channel);

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
    ;

    row.put("COLBOOLEAN", null);
    if (channel.getOnErrorOption() == OpenChannelRequest.OnErrorOption.CONTINUE) {
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
    testMissingColumnCheckHelper(this.channelOnErrorContinue);
    testMissingColumnCheckHelper(this.channelOnErrorAbort);
  }

  private void testMissingColumnCheckHelper(SnowflakeStreamingIngestChannelInternal channel) {
    ArrowRowBuffer innerBuffer = new ArrowRowBuffer(channel);

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
    if (channel.getOnErrorOption() == OpenChannelRequest.OnErrorOption.CONTINUE) {
      response = innerBuffer.insertRows(Collections.singletonList(row2), "2");
      Assert.assertTrue(response.hasErrors());
      Assert.assertEquals(
          ErrorCode.INVALID_ROW.getMessageCode(),
          response.getInsertErrors().get(0).getException().getVendorCode());
    } else {
      try {
        innerBuffer.insertRows(Collections.singletonList(row2), "2");
      } catch (SFException e) {
        Assert.assertEquals(ErrorCode.INVALID_ROW.getMessageCode(), e.getVendorCode());
      }
    }
  }

  @Test
  public void testE2EBoolean() {
    testE2EBooleanHelper(this.channelOnErrorContinue);
    testE2EBooleanHelper(this.channelOnErrorAbort);
  }

  private void testE2EBooleanHelper(SnowflakeStreamingIngestChannelInternal channel) {
    ArrowRowBuffer innerBuffer = new ArrowRowBuffer(channel);

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

    // Check data was inserted into Arrow correctly
    Assert.assertEquals(true, innerBuffer.vectorsRoot.getVector("COLBOOLEAN").getObject(0));
    Assert.assertEquals(false, innerBuffer.vectorsRoot.getVector("COLBOOLEAN").getObject(1));
    Assert.assertNull(innerBuffer.vectorsRoot.getVector("COLBOOLEAN").getObject(2));

    // Check stats generation
    ChannelData result = innerBuffer.flush();
    Assert.assertEquals(3, result.getRowCount());

    Assert.assertEquals(
        BigInteger.valueOf(0), result.getColumnEps().get("COLBOOLEAN").getCurrentMinIntValue());
    Assert.assertEquals(
        BigInteger.valueOf(1), result.getColumnEps().get("COLBOOLEAN").getCurrentMaxIntValue());
    Assert.assertEquals(1, result.getColumnEps().get("COLBOOLEAN").getCurrentNullCount());
  }

  @Test
  public void testE2EBinary() {
    testE2EBinaryHelper(this.channelOnErrorAbort);
    testE2EBinaryHelper(this.channelOnErrorContinue);
  }

  private void testE2EBinaryHelper(SnowflakeStreamingIngestChannelInternal channel) {
    ArrowRowBuffer innerBuffer = new ArrowRowBuffer(channel);

    ColumnMetadata colBinary = new ColumnMetadata();
    colBinary.setName("COLBINARY");
    colBinary.setPhysicalType("LOB");
    colBinary.setNullable(true);
    colBinary.setLogicalType("BINARY");
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

    // Check data was inserted into Arrow correctly
    Assert.assertEquals(
        "Hello World",
        new String(
            (byte[]) innerBuffer.vectorsRoot.getVector("COLBINARY").getObject(0),
            StandardCharsets.UTF_8));
    Assert.assertEquals(
        "Honk Honk",
        new String(
            (byte[]) innerBuffer.vectorsRoot.getVector("COLBINARY").getObject(1),
            StandardCharsets.UTF_8));
    Assert.assertNull(innerBuffer.vectorsRoot.getVector("COLBINARY").getObject(2));

    // Check stats generation
    ChannelData result = innerBuffer.flush();

    Assert.assertEquals(3, result.getRowCount());
    Assert.assertEquals(11L, result.getColumnEps().get("COLBINARY").getCurrentMaxLength());
    Assert.assertEquals(1, result.getColumnEps().get("COLBINARY").getCurrentNullCount());
  }

  @Test
  public void testE2EReal() {
    testE2ERealHelper(this.channelOnErrorContinue);
    testE2ERealHelper(this.channelOnErrorAbort);
  }

  private void testE2ERealHelper(SnowflakeStreamingIngestChannelInternal channel) {
    ArrowRowBuffer innerBuffer = new ArrowRowBuffer(channel);

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

    // Check data was inserted into Arrow correctly
    Assert.assertEquals(123.456, innerBuffer.vectorsRoot.getVector("COLREAL").getObject(0));
    Assert.assertEquals(123.4567, innerBuffer.vectorsRoot.getVector("COLREAL").getObject(1));
    Assert.assertNull(innerBuffer.vectorsRoot.getVector("COLREAL").getObject(2));

    // Check stats generation
    ChannelData result = innerBuffer.flush();

    Assert.assertEquals(3, result.getRowCount());
    Assert.assertEquals(
        Double.valueOf(123.456), result.getColumnEps().get("COLREAL").getCurrentMinRealValue());
    Assert.assertEquals(
        Double.valueOf(123.4567), result.getColumnEps().get("COLREAL").getCurrentMaxRealValue());
    Assert.assertEquals(1, result.getColumnEps().get("COLREAL").getCurrentNullCount());
  }

  @Test
  public void testOnErrorAbortFailures() {
    ArrowRowBuffer innerBuffer = new ArrowRowBuffer(this.channelOnErrorAbort);

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
    Assert.assertEquals(0, innerBuffer.tempVectorsRoot.getRowCount());
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
    Assert.assertEquals(0, innerBuffer.tempVectorsRoot.getRowCount());
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
    Assert.assertEquals(0, innerBuffer.tempVectorsRoot.getRowCount());
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
    Assert.assertEquals(0, innerBuffer.tempVectorsRoot.getRowCount());
    Assert.assertEquals(
        3, innerBuffer.statsMap.get("COLDECIMAL").getCurrentMaxIntValue().intValue());
    Assert.assertEquals(
        1, innerBuffer.statsMap.get("COLDECIMAL").getCurrentMinIntValue().intValue());
    Assert.assertNull(innerBuffer.tempStatsMap.get("COLDECIMAL").getCurrentMaxIntValue());
    Assert.assertNull(innerBuffer.tempStatsMap.get("COLDECIMAL").getCurrentMinIntValue());

    ChannelData data = innerBuffer.flush();
    Assert.assertEquals(3, data.getVectors().getRowCount());
    Assert.assertEquals(0, innerBuffer.rowCount);
  }
}
