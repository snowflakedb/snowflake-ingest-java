package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.streaming.internal.ArrowRowBuffer.DECIMAL_BIT_WIDTH;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.Text;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class RowBufferTest {
  private ArrowRowBuffer rowBuffer;
  private SnowflakeStreamingIngestChannelInternal channel;

  @Before
  public void setupRowBuffer() {
    // Create row buffer
    channel =
        new SnowflakeStreamingIngestChannelInternal(
            "channel", "db", "schema", "table", "0", 0L, 0L, null, true);
    this.rowBuffer = new ArrowRowBuffer(channel);

    ColumnMetadata colTinyIntCase = new ColumnMetadata();
    colTinyIntCase.setName("colTinyInt");
    colTinyIntCase.setPhysicalType("SB1");
    colTinyIntCase.setNullable(false);
    colTinyIntCase.setLogicalType("FIXED");
    colTinyIntCase.setScale(0);

    ColumnMetadata colTinyInt = new ColumnMetadata();
    colTinyInt.setName("COLTINYINT");
    colTinyInt.setPhysicalType("SB1");
    colTinyInt.setNullable(false);
    colTinyInt.setLogicalType("FIXED");
    colTinyInt.setScale(0);

    ColumnMetadata colSmallInt = new ColumnMetadata();
    colSmallInt.setName("COLSMALLINT");
    colSmallInt.setPhysicalType("SB2");
    colSmallInt.setNullable(false);
    colSmallInt.setLogicalType("FIXED");
    colSmallInt.setScale(0);

    ColumnMetadata colInt = new ColumnMetadata();
    colInt.setName("COLINT");
    colInt.setPhysicalType("SB4");
    colInt.setNullable(false);
    colInt.setLogicalType("FIXED");
    colInt.setScale(0);

    ColumnMetadata colBigInt = new ColumnMetadata();
    colBigInt.setName("COLBIGINT");
    colBigInt.setPhysicalType("SB8");
    colBigInt.setNullable(false);
    colBigInt.setLogicalType("FIXED");
    colBigInt.setScale(0);

    ColumnMetadata colDecimal = new ColumnMetadata();
    colDecimal.setName("COLDECIMAL");
    colDecimal.setPhysicalType("SB16");
    colDecimal.setNullable(false);
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

    // Setup column fields and vectors
    this.rowBuffer.setupSchema(
        Arrays.asList(
            colTinyIntCase, colTinyInt, colSmallInt, colInt, colBigInt, colDecimal, colChar));
  }

  @Test
  public void buildFieldErrorStates() throws Exception {
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
      Field result = this.rowBuffer.buildField(testCol);
      Assert.fail("Expected error");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.UNKNOWN_DATA_TYPE.getMessageCode(), e.getVendorCode());
    }

    // Fixed LOB
    testCol = new ColumnMetadata();
    testCol.setPhysicalType("LOB");
    testCol.setLogicalType("FIXED");
    try {
      Field result = this.rowBuffer.buildField(testCol);
      Assert.fail("Expected error");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.UNKNOWN_DATA_TYPE.getMessageCode(), e.getVendorCode());
    }

    // TIMESTAMP_NTZ SB1
    testCol = new ColumnMetadata();
    testCol.setPhysicalType("SB1");
    testCol.setLogicalType("TIMESTAMP_NTZ");
    try {
      Field result = this.rowBuffer.buildField(testCol);
      Assert.fail("Expected error");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.UNKNOWN_DATA_TYPE.getMessageCode(), e.getVendorCode());
    }

    // TODO: timestamp_tz not currently supported in convertRowToArrow
    // TIMESTAMP_TZ SB1
    //    testCol = new ColumnMetadata();
    //    testCol.setPhysicalType("SB1");
    //    testCol.setLogicalType("TIMESTAMP_TZ");
    //    try {
    //      Field result = this.rowBuffer.buildField(testCol);
    //      Assert.fail("Expected error");
    //    } catch (SFException e) {
    //      Assert.assertEquals(ErrorCode.UNKNOWN_DATA_TYPE.getMessageCode(), e.getVendorCode());
    //    }

    // TIME SB16
    testCol = new ColumnMetadata();
    testCol.setPhysicalType("SB16");
    testCol.setLogicalType("TIME");
    try {
      Field result = this.rowBuffer.buildField(testCol);
      Assert.fail("Expected error");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.UNKNOWN_DATA_TYPE.getMessageCode(), e.getVendorCode());
    }
  }

  @Test
  public void buildFieldFixedSB1() throws Exception {
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
    Field result = this.rowBuffer.buildField(testCol);

    Assert.assertEquals("testCol", result.getName());
    Assert.assertEquals(result.getFieldType().getType(), Types.MinorType.TINYINT.getType());
    Assert.assertEquals(result.getFieldType().getMetadata().get("physicalType"), "SB1");
    Assert.assertEquals(result.getFieldType().getMetadata().get("scale"), "0");
    Assert.assertEquals(result.getFieldType().getMetadata().get("logicalType"), "FIXED");
    Assert.assertTrue(result.getFieldType().isNullable());
    Assert.assertEquals(result.getChildren().size(), 0);
  }

  @Test
  public void buildFieldFixedSB2() throws Exception {
    ColumnMetadata testCol = new ColumnMetadata();
    testCol.setName("testCol");
    testCol.setPhysicalType("SB2");
    testCol.setNullable(true);
    testCol.setLogicalType("FIXED");
    testCol.setByteLength(14);
    testCol.setLength(11);
    testCol.setScale(0);
    testCol.setPrecision(4);
    Field result = this.rowBuffer.buildField(testCol);

    Assert.assertEquals("testCol", result.getName());
    Assert.assertEquals(result.getFieldType().getType(), Types.MinorType.SMALLINT.getType());
    Assert.assertEquals(result.getFieldType().getMetadata().get("physicalType"), "SB2");
    Assert.assertEquals(result.getFieldType().getMetadata().get("scale"), "0");
    Assert.assertEquals(result.getFieldType().getMetadata().get("logicalType"), "FIXED");
    Assert.assertTrue(result.getFieldType().isNullable());
    Assert.assertEquals(result.getChildren().size(), 0);
  }

  @Test
  public void buildFieldFixedSB4() throws Exception {
    ColumnMetadata testCol = new ColumnMetadata();
    testCol.setName("testCol");
    testCol.setPhysicalType("SB4");
    testCol.setNullable(true);
    testCol.setLogicalType("FIXED");
    testCol.setByteLength(14);
    testCol.setLength(11);
    testCol.setScale(0);
    testCol.setPrecision(4);
    Field result = this.rowBuffer.buildField(testCol);

    Assert.assertEquals("testCol", result.getName());
    Assert.assertEquals(result.getFieldType().getType(), Types.MinorType.INT.getType());
    Assert.assertEquals(result.getFieldType().getMetadata().get("physicalType"), "SB4");
    Assert.assertEquals(result.getFieldType().getMetadata().get("scale"), "0");
    Assert.assertEquals(result.getFieldType().getMetadata().get("logicalType"), "FIXED");
    Assert.assertTrue(result.getFieldType().isNullable());
    Assert.assertEquals(result.getChildren().size(), 0);
  }

  @Test
  public void buildFieldFixedSB8() throws Exception {
    ColumnMetadata testCol = new ColumnMetadata();
    testCol.setName("testCol");
    testCol.setPhysicalType("SB8");
    testCol.setNullable(true);
    testCol.setLogicalType("FIXED");
    testCol.setByteLength(14);
    testCol.setLength(11);
    testCol.setScale(0);
    testCol.setPrecision(4);
    Field result = this.rowBuffer.buildField(testCol);

    Assert.assertEquals("testCol", result.getName());
    Assert.assertEquals(result.getFieldType().getType(), Types.MinorType.BIGINT.getType());
    Assert.assertEquals(result.getFieldType().getMetadata().get("physicalType"), "SB8");
    Assert.assertEquals(result.getFieldType().getMetadata().get("scale"), "0");
    Assert.assertEquals(result.getFieldType().getMetadata().get("logicalType"), "FIXED");
    Assert.assertTrue(result.getFieldType().isNullable());
    Assert.assertEquals(result.getChildren().size(), 0);
  }

  @Test
  public void buildFieldFixedSB16() throws Exception {
    ColumnMetadata testCol = new ColumnMetadata();
    testCol.setName("testCol");
    testCol.setPhysicalType("SB16");
    testCol.setNullable(true);
    testCol.setLogicalType("FIXED");
    testCol.setByteLength(14);
    testCol.setLength(11);
    testCol.setScale(0);
    testCol.setPrecision(4);
    Field result = this.rowBuffer.buildField(testCol);

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
  public void buildFieldLobVariant() throws Exception {
    ColumnMetadata testCol = new ColumnMetadata();
    testCol.setName("testCol");
    testCol.setPhysicalType("LOB");
    testCol.setNullable(true);
    testCol.setLogicalType("VARIANT");
    testCol.setByteLength(14);
    testCol.setLength(11);
    testCol.setScale(0);
    testCol.setPrecision(4);
    Field result = this.rowBuffer.buildField(testCol);

    Assert.assertEquals("testCol", result.getName());
    Assert.assertEquals(result.getFieldType().getType(), Types.MinorType.VARCHAR.getType());
    Assert.assertEquals(result.getFieldType().getMetadata().get("physicalType"), "LOB");
    Assert.assertEquals(result.getFieldType().getMetadata().get("scale"), "0");
    Assert.assertEquals(result.getFieldType().getMetadata().get("logicalType"), "VARIANT");
    Assert.assertTrue(result.getFieldType().isNullable());
    Assert.assertEquals(result.getChildren().size(), 0);
  }

  @Test
  public void buildFieldTimestampNtzSB8() throws Exception {
    ColumnMetadata testCol = new ColumnMetadata();
    testCol.setName("testCol");
    testCol.setPhysicalType("SB8");
    testCol.setNullable(true);
    testCol.setLogicalType("TIMESTAMP_NTZ");
    testCol.setByteLength(14);
    testCol.setLength(11);
    testCol.setScale(0);
    testCol.setPrecision(4);
    Field result = this.rowBuffer.buildField(testCol);

    Assert.assertEquals("testCol", result.getName());
    Assert.assertEquals(result.getFieldType().getType(), Types.MinorType.BIGINT.getType());
    Assert.assertEquals(result.getFieldType().getMetadata().get("physicalType"), "SB8");
    Assert.assertEquals(result.getFieldType().getMetadata().get("scale"), "0");
    Assert.assertEquals(result.getFieldType().getMetadata().get("logicalType"), "TIMESTAMP_NTZ");
    Assert.assertTrue(result.getFieldType().isNullable());
    Assert.assertEquals(result.getChildren().size(), 0);
  }

  @Test
  public void buildFieldTimestampNtzSB16() throws Exception {
    ColumnMetadata testCol = new ColumnMetadata();
    testCol.setName("testCol");
    testCol.setPhysicalType("SB16");
    testCol.setNullable(true);
    testCol.setLogicalType("TIMESTAMP_NTZ");
    testCol.setByteLength(14);
    testCol.setLength(11);
    testCol.setScale(0);
    testCol.setPrecision(4);
    Field result = this.rowBuffer.buildField(testCol);

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

  // TODO: timetamp_tz not currently supported in convertRowToArrow
  //  @Test
  //  public void buildFieldTimestampTzSB8() throws Exception {
  //    ColumnMetadata testCol = new ColumnMetadata();
  //    testCol.setName("testCol");
  //    testCol.setPhysicalType("SB8");
  //    testCol.setNullable(true);
  //    testCol.setLogicalType("TIMESTAMP_TZ");
  //    testCol.setByteLength(14);
  //    testCol.setLength(11);
  //    testCol.setScale(0);
  //    testCol.setPrecision(4);
  //    Field result = this.rowBuffer.buildField(testCol);
  //
  //    Assert.assertEquals("testCol", result.getName());
  //    Assert.assertEquals(result.getFieldType().getType(), Types.MinorType.STRUCT.getType());
  //    Assert.assertEquals(result.getFieldType().getMetadata().get("physicalType"), "SB8");
  //    Assert.assertEquals(result.getFieldType().getMetadata().get("scale"), "0");
  //    Assert.assertEquals(result.getFieldType().getMetadata().get("logicalType"), "TIMESTAMP_TZ");
  //    Assert.assertTrue(result.getFieldType().isNullable());
  //    Assert.assertEquals(result.getChildren().size(), 2);
  //    Assert.assertEquals(
  //        result.getChildren().get(0).getFieldType().getType(), Types.MinorType.BIGINT.getType());
  //    Assert.assertEquals(
  //        result.getChildren().get(1).getFieldType().getType(), Types.MinorType.INT.getType());
  //  }
  //
  //  @Test
  //  public void buildFieldTimestampTzSB16() throws Exception {
  //    ColumnMetadata testCol = new ColumnMetadata();
  //    testCol.setName("testCol");
  //    testCol.setPhysicalType("SB16");
  //    testCol.setNullable(true);
  //    testCol.setLogicalType("TIMESTAMP_TZ");
  //    testCol.setByteLength(14);
  //    testCol.setLength(11);
  //    testCol.setScale(0);
  //    testCol.setPrecision(4);
  //    Field result = this.rowBuffer.buildField(testCol);
  //
  //    Assert.assertEquals("testCol", result.getName());
  //    Assert.assertEquals(result.getFieldType().getType(), Types.MinorType.STRUCT.getType());
  //    Assert.assertEquals(result.getFieldType().getMetadata().get("physicalType"), "SB16");
  //    Assert.assertEquals(result.getFieldType().getMetadata().get("scale"), "0");
  //    Assert.assertEquals(result.getFieldType().getMetadata().get("logicalType"), "TIMESTAMP_TZ");
  //    Assert.assertTrue(result.getFieldType().isNullable());
  //    Assert.assertEquals(result.getChildren().size(), 3);
  //    Assert.assertEquals(
  //        result.getChildren().get(0).getFieldType().getType(), Types.MinorType.BIGINT.getType());
  //    Assert.assertEquals(
  //        result.getChildren().get(1).getFieldType().getType(), Types.MinorType.INT.getType());
  //    Assert.assertEquals(
  //        result.getChildren().get(2).getFieldType().getType(), Types.MinorType.INT.getType());
  //  }

  @Test
  public void buildFieldTimestampDate() throws Exception {
    ColumnMetadata testCol = new ColumnMetadata();
    testCol.setName("testCol");
    testCol.setPhysicalType("SB8");
    testCol.setNullable(true);
    testCol.setLogicalType("DATE");
    testCol.setByteLength(14);
    testCol.setLength(11);
    testCol.setScale(0);
    testCol.setPrecision(4);
    Field result = this.rowBuffer.buildField(testCol);

    Assert.assertEquals("testCol", result.getName());
    Assert.assertEquals(result.getFieldType().getType(), Types.MinorType.DATEDAY.getType());
    Assert.assertEquals(result.getFieldType().getMetadata().get("physicalType"), "SB8");
    Assert.assertEquals(result.getFieldType().getMetadata().get("scale"), "0");
    Assert.assertEquals(result.getFieldType().getMetadata().get("logicalType"), "DATE");
    Assert.assertTrue(result.getFieldType().isNullable());
    Assert.assertEquals(result.getChildren().size(), 0);
  }

  @Test
  public void buildFieldTimeSB4() throws Exception {
    ColumnMetadata testCol = new ColumnMetadata();
    testCol.setName("testCol");
    testCol.setPhysicalType("SB4");
    testCol.setNullable(true);
    testCol.setLogicalType("TIME");
    testCol.setByteLength(14);
    testCol.setLength(11);
    testCol.setScale(0);
    testCol.setPrecision(4);
    Field result = this.rowBuffer.buildField(testCol);

    Assert.assertEquals("testCol", result.getName());
    Assert.assertEquals(result.getFieldType().getType(), Types.MinorType.INT.getType());
    Assert.assertEquals(result.getFieldType().getMetadata().get("physicalType"), "SB4");
    Assert.assertEquals(result.getFieldType().getMetadata().get("scale"), "0");
    Assert.assertEquals(result.getFieldType().getMetadata().get("logicalType"), "TIME");
    Assert.assertTrue(result.getFieldType().isNullable());
    Assert.assertEquals(result.getChildren().size(), 0);
  }

  @Test
  public void buildFieldTimeSB8() throws Exception {
    ColumnMetadata testCol = new ColumnMetadata();
    testCol.setName("testCol");
    testCol.setPhysicalType("SB8");
    testCol.setNullable(true);
    testCol.setLogicalType("TIME");
    testCol.setByteLength(14);
    testCol.setLength(11);
    testCol.setScale(0);
    testCol.setPrecision(4);
    Field result = this.rowBuffer.buildField(testCol);

    Assert.assertEquals("testCol", result.getName());
    Assert.assertEquals(result.getFieldType().getType(), Types.MinorType.BIGINT.getType());
    Assert.assertEquals(result.getFieldType().getMetadata().get("physicalType"), "SB8");
    Assert.assertEquals(result.getFieldType().getMetadata().get("scale"), "0");
    Assert.assertEquals(result.getFieldType().getMetadata().get("logicalType"), "TIME");
    Assert.assertTrue(result.getFieldType().isNullable());
    Assert.assertEquals(result.getChildren().size(), 0);
  }

  @Test
  public void buildFieldBoolean() throws Exception {
    ColumnMetadata testCol = new ColumnMetadata();
    testCol.setName("testCol");
    testCol.setPhysicalType("BINARY");
    testCol.setNullable(true);
    testCol.setLogicalType("BOOLEAN");
    testCol.setByteLength(14);
    testCol.setLength(11);
    testCol.setScale(0);
    testCol.setPrecision(4);
    Field result = this.rowBuffer.buildField(testCol);

    Assert.assertEquals("testCol", result.getName());
    Assert.assertEquals(result.getFieldType().getType(), Types.MinorType.BIT.getType());
    Assert.assertEquals(result.getFieldType().getMetadata().get("physicalType"), "BINARY");
    Assert.assertEquals(result.getFieldType().getMetadata().get("scale"), "0");
    Assert.assertEquals(result.getFieldType().getMetadata().get("logicalType"), "BOOLEAN");
    Assert.assertTrue(result.getFieldType().isNullable());
    Assert.assertEquals(result.getChildren().size(), 0);
  }

  @Test
  public void buildFieldRealSB16() throws Exception {
    ColumnMetadata testCol = new ColumnMetadata();
    testCol.setName("testCol");
    testCol.setPhysicalType("SB16");
    testCol.setNullable(true);
    testCol.setLogicalType("REAL");
    testCol.setByteLength(14);
    testCol.setLength(11);
    testCol.setScale(0);
    testCol.setPrecision(4);
    Field result = this.rowBuffer.buildField(testCol);

    Assert.assertEquals("testCol", result.getName());
    Assert.assertEquals(result.getFieldType().getType(), Types.MinorType.FLOAT8.getType());
    Assert.assertEquals(result.getFieldType().getMetadata().get("physicalType"), "SB16");
    Assert.assertEquals(result.getFieldType().getMetadata().get("scale"), "0");
    Assert.assertEquals(result.getFieldType().getMetadata().get("logicalType"), "REAL");
    Assert.assertTrue(result.getFieldType().isNullable());
    Assert.assertEquals(result.getChildren().size(), 0);
  }

  @Test
  public void testReset() throws Exception {
    RowBufferStats stats = this.rowBuffer.statsMap.get("COLCHAR");
    stats.addIntValue(BigInteger.valueOf(1));
    Assert.assertEquals(BigInteger.valueOf(1), stats.getCurrentMaxIntValue());
    this.rowBuffer.reset();
    RowBufferStats resetStats = this.rowBuffer.statsMap.get("COLCHAR");
    Assert.assertNotNull(resetStats);
    Assert.assertNull(resetStats.getCurrentMaxIntValue());
  }

  @Test
  public void testInvalidLogicalType() throws Exception {
    ColumnMetadata colInvalidLogical = new ColumnMetadata();
    colInvalidLogical.setName("COLINVALIDLOGICAL");
    colInvalidLogical.setPhysicalType("SB1");
    colInvalidLogical.setNullable(false);
    colInvalidLogical.setLogicalType("INVALID");
    colInvalidLogical.setByteLength(14);
    colInvalidLogical.setLength(11);
    colInvalidLogical.setScale(0);

    try {
      this.rowBuffer.setupSchema(Arrays.asList(colInvalidLogical));
      Assert.fail("Setup should fail if invalid column metadata is provided");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.UNKNOWN_DATA_TYPE.getMessageCode(), e.getVendorCode());
      // Do nothing
    }
  }

  @Test
  public void testInvalidPhysicalType() throws Exception {
    ColumnMetadata colInvalidPhysical = new ColumnMetadata();
    colInvalidPhysical.setName("COLINVALIDPHYSICAL");
    colInvalidPhysical.setPhysicalType("INVALID");
    colInvalidPhysical.setNullable(false);
    colInvalidPhysical.setLogicalType("FIXED");
    colInvalidPhysical.setByteLength(14);
    colInvalidPhysical.setLength(11);
    colInvalidPhysical.setScale(0);

    try {
      this.rowBuffer.setupSchema(Arrays.asList(colInvalidPhysical));
      Assert.fail("Setup should fail if invalid column metadata is provided");
    } catch (SFException e) {
      Assert.assertEquals(e.getVendorCode(), ErrorCode.UNKNOWN_DATA_TYPE.getMessageCode());
    }
  }

  @Test
  @Ignore // TODO SNOW-348857: verify that null can't be inserted into non-nullable column
  public void testInsertNullToNotNullColumn() throws Exception {
    ColumnMetadata colNotNull = new ColumnMetadata();
    colNotNull.setName("COLNOTNULL");
    colNotNull.setPhysicalType("SB16");
    colNotNull.setNullable(false);
    colNotNull.setLogicalType("FIXED");
    colNotNull.setPrecision(38);
    colNotNull.setScale(0);

    this.rowBuffer.setupSchema(Arrays.asList(colNotNull));

    try {
      Map<String, Object> row = new HashMap<>();
      row.put("colInt", null);
      row.put("colDecimal", null);
      row.put("colChar", null);
      row.put("colNotNull", null);
      this.rowBuffer.insertRows(Collections.singletonList(row), null);
      Assert.fail("Insert null to non-nullable column should fail");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.INVALID_ROW.getMessageCode(), e.getVendorCode());
    }
  }

  @Test
  public void testInsertRow() throws Exception {
    Map<String, Object> row = new HashMap<>();
    row.put("colTinyInt", (byte) 1);
    row.put("colSmallInt", (short) 2);
    row.put("colInt", 3);
    row.put("colBigInt", 4L);
    row.put("colDecimal", 1.23);
    row.put("colChar", "2");

    try {
      this.rowBuffer.insertRows(Collections.singletonList(row), null);
    } catch (Exception e) {
      Assert.fail("Row buffer insert row failed");
    }
  }

  @Test
  public void testInsertRows() throws Exception {
    Map<String, Object> row1 = new HashMap<>();
    row1.put("colTinyInt", (byte) 1);
    row1.put("colSmallInt", (short) 2);
    row1.put("colInt", 3);
    row1.put("colBigInt", 4L);
    row1.put("colDecimal", 1.23);
    row1.put("colChar", "2");

    Map<String, Object> row2 = new HashMap<>();
    row2.put("colTinyInt", (byte) 1);
    row2.put("colSmallInt", (short) 2);
    row2.put("colInt", 3);
    row2.put("colBigInt", 4L);
    row2.put("colDecimal", 2.34);
    row2.put("colChar", "3");

    try {
      this.rowBuffer.insertRows(Arrays.asList(row1, row2), null);
    } catch (Exception e) {
      Assert.fail("Row buffer insert rows failed");
    }
  }

  @Test
  public void testInsertInvalidRow() throws Exception {
    Map<String, Object> row = new HashMap<>();
    row.put("colTinyInt", null);
    row.put("colSmallInt", null);
    row.put("colInt", null);
    row.put("colBigInt", null);
    row.put("colDecimal", 1.23456);
    row.put("colChar", null);

    try {
      this.rowBuffer.insertRows(Collections.singletonList(row), null);
      Assert.fail("Row buffer insert row should fail");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.INVALID_ROW.getMessageCode(), e.getVendorCode());
    }
  }

  @Test
  public void testClose() throws Exception {
    this.rowBuffer.close();
    Map<String, Object> row = new HashMap<>();
    row.put("colTinyInt", (byte) 1);
    row.put("colSmallInt", (short) 2);
    row.put("colInt", 3);
    row.put("colBigInt", 4L);
    row.put("colDecimal", 1.23);
    row.put("colChar", "2");
    try {
      this.rowBuffer.insertRows(Collections.singletonList(row), null);
      Assert.fail("Insert should fail after buffer is closed");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.INVALID_ROW.getMessageCode(), e.getVendorCode());
    }
  }

  @Test
  public void testFlush() throws Exception {
    String offsetToken = "1";
    Map<String, Object> row1 = new HashMap<>();
    row1.put("colTinyInt", (byte) 1);
    row1.put("colSmallInt", (short) 2);
    row1.put("colInt", 3);
    row1.put("colBigInt", 4L);
    row1.put("colDecimal", 1.23);
    row1.put("colChar", "2");

    Map<String, Object> row2 = new HashMap<>();
    row2.put("colTinyInt", (byte) 1);
    row2.put("colSmallInt", (short) 2);
    row2.put("colInt", 3);
    row2.put("colBigInt", 4L);
    row2.put("colDecimal", 2.34);
    row2.put("colChar", "3");

    this.rowBuffer.insertRows(Arrays.asList(row1, row2), offsetToken);
    float bufferSize = this.rowBuffer.getSize();

    ChannelData data = this.rowBuffer.flush();
    Assert.assertEquals(2, data.getRowCount());
    Assert.assertEquals((Long) 1L, data.getRowSequencer());
    Assert.assertEquals(7, data.getVectors().size());
    Assert.assertEquals(offsetToken, data.getOffsetToken());
    Assert.assertEquals(bufferSize, data.getBufferSize(), 0);
  }

  @Test
  public void testDoubleQuotesColumnName() throws Exception {
    ColumnMetadata colDoubleQuotes = new ColumnMetadata();
    colDoubleQuotes.setName("colDoubleQuotes");
    colDoubleQuotes.setPhysicalType("SB16");
    colDoubleQuotes.setNullable(false);
    colDoubleQuotes.setLogicalType("FIXED");
    colDoubleQuotes.setPrecision(38);
    colDoubleQuotes.setScale(0);

    this.rowBuffer.setupSchema(Arrays.asList(colDoubleQuotes));

    Map<String, Object> row = new HashMap<>();
    row.put("colTinyInt", (byte) 1);
    row.put("colSmallInt", (short) 2);
    row.put("colInt", 3);
    row.put("colBigInt", 4L);
    row.put("colDecimal", 1.23);
    row.put("colChar", "2");
    row.put("\"colDoubleQuotes\"", 1);

    try {
      this.rowBuffer.insertRows(Collections.singletonList(row), null);
    } catch (Exception e) {
      Assert.fail("Row buffer insert row failed");
    }
  }

  @Test
  public void testBuildEpInfoFromStats() throws Exception {
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
    Assert.assertEquals(2, strColumnResult.getDistinctValues());
    Assert.assertEquals("alice", strColumnResult.getMinStrValue());
    Assert.assertEquals("bob", strColumnResult.getMaxStrValue());
    Assert.assertEquals(1, strColumnResult.getNullCount());

    FileColumnProperties intColumnResult = columnResults.get("intColumn");
    Assert.assertEquals(3, intColumnResult.getDistinctValues());
    Assert.assertEquals(BigInteger.valueOf(1), intColumnResult.getMinIntValue());
    Assert.assertEquals(BigInteger.valueOf(10), intColumnResult.getMaxIntValue());
    Assert.assertEquals(0, intColumnResult.getNullCount());
  }

  @Test
  public void testArrowE2E() throws Exception {
    Map<String, Object> row1 = new HashMap<>();
    row1.put("\"colTinyInt\"", (byte) 10);
    row1.put("colTinyInt", (byte) 1);
    row1.put("colSmallInt", (short) 2);
    row1.put("colInt", 3);
    row1.put("colBigInt", 4L);
    row1.put("colChar", "2");

    this.rowBuffer.insertRows(Arrays.asList(row1), null);

    Assert.assertEquals((byte) 10, this.rowBuffer.vectors.get("colTinyInt").getObject(0));
    Assert.assertEquals((byte) 1, this.rowBuffer.vectors.get("COLTINYINT").getObject(0));
    Assert.assertEquals((short) 2, this.rowBuffer.vectors.get("COLSMALLINT").getObject(0));
    Assert.assertEquals(3, this.rowBuffer.vectors.get("COLINT").getObject(0));
    Assert.assertEquals(4L, this.rowBuffer.vectors.get("COLBIGINT").getObject(0));
    Assert.assertEquals(new Text("2"), this.rowBuffer.vectors.get("COLCHAR").getObject(0));
  }

  @Test
  public void testArrowE2ETimestampLTZ() throws Exception {
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

    innerBuffer.insertRows(Arrays.asList(row), null);
    Assert.assertEquals(1621899220l, innerBuffer.vectors.get("COLTIMESTAMPLTZ_SB8").getObject(0));
    Assert.assertEquals(
        "epoch",
        innerBuffer.vectors.get("COLTIMESTAMPLTZ_SB16").getChildrenFromFields().get(0).getName());
    Assert.assertEquals(
        1621899220l,
        innerBuffer
            .vectors
            .get("COLTIMESTAMPLTZ_SB16")
            .getChildrenFromFields()
            .get(0)
            .getObject(0));
    Assert.assertEquals(
        "fraction",
        innerBuffer.vectors.get("COLTIMESTAMPLTZ_SB16").getChildrenFromFields().get(1).getName());
    Assert.assertEquals(
        123456789,
        innerBuffer
            .vectors
            .get("COLTIMESTAMPLTZ_SB16")
            .getChildrenFromFields()
            .get(1)
            .getObject(0));
  }

  @Test
  public void testArrowE2ETimestampErrors() throws Exception {
    ArrowRowBuffer innerBuffer = new ArrowRowBuffer(channel);

    ColumnMetadata colTimestampLtzSB16 = new ColumnMetadata();
    colTimestampLtzSB16.setName("COLTIMESTAMPLTZ_SB16");
    colTimestampLtzSB16.setPhysicalType("SB16");
    colTimestampLtzSB16.setNullable(false);
    colTimestampLtzSB16.setLogicalType("TIMESTAMP_LTZ");
    colTimestampLtzSB16.setScale(6);

    innerBuffer.setupSchema(Arrays.asList(colTimestampLtzSB16));

    Map<String, Object> row = new HashMap<>();
    row.put("COLTIMESTAMPLTZ_SB8", "1621899220");
    row.put("COLTIMESTAMPLTZ_SB16", "1621899220.1234567");

    try {
      innerBuffer.insertRows(Arrays.asList(row), null);
      Assert.fail("Expected error from fraction scale mismatch");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.INVALID_ROW.getMessageCode(), e.getVendorCode());
    }
  }

  @Test
  public void testStatsE2E() throws Exception {
    Map<String, Object> row1 = new HashMap<>();
    row1.put("\"colTinyInt\"", (byte) 10);
    row1.put("colTinyInt", (byte) 1);
    row1.put("colSmallInt", (short) 2);
    row1.put("colInt", 3);
    row1.put("colBigInt", 4L);
    row1.put("colChar", "2");

    Map<String, Object> row2 = new HashMap<>();
    row2.put("\"colTinyInt\"", (byte) 11);
    row2.put("colTinyInt", (byte) 1);
    row2.put("colSmallInt", (short) 3);
    row2.put("colInt", null);
    row2.put("colBigInt", 40L);
    row2.put("colChar", "alice");

    this.rowBuffer.insertRows(Arrays.asList(row1, row2), null);
    ChannelData result = this.rowBuffer.flush();
    Map<String, RowBufferStats> columnEpStats = result.getColumnEps();

    Assert.assertEquals(
        BigInteger.valueOf(11), columnEpStats.get("colTinyInt").getCurrentMaxIntValue());
    Assert.assertEquals(
        BigInteger.valueOf(10), columnEpStats.get("colTinyInt").getCurrentMinIntValue());
    Assert.assertEquals(0, columnEpStats.get("colTinyInt").getCurrentNullCount());
    Assert.assertEquals(2L, columnEpStats.get("colTinyInt").getDistinctValues());

    Assert.assertEquals(
        BigInteger.valueOf(1), columnEpStats.get("COLTINYINT").getCurrentMaxIntValue());
    Assert.assertEquals(
        BigInteger.valueOf(1), columnEpStats.get("COLTINYINT").getCurrentMinIntValue());
    Assert.assertEquals(0, columnEpStats.get("COLTINYINT").getCurrentNullCount());
    Assert.assertEquals(2L, columnEpStats.get("COLTINYINT").getDistinctValues());

    Assert.assertEquals(
        BigInteger.valueOf(3), columnEpStats.get("COLSMALLINT").getCurrentMaxIntValue());
    Assert.assertEquals(
        BigInteger.valueOf(2), columnEpStats.get("COLSMALLINT").getCurrentMinIntValue());
    Assert.assertEquals(0, columnEpStats.get("COLSMALLINT").getCurrentNullCount());
    Assert.assertEquals(2L, columnEpStats.get("COLSMALLINT").getDistinctValues());

    Assert.assertEquals(BigInteger.valueOf(3), columnEpStats.get("COLINT").getCurrentMaxIntValue());
    Assert.assertEquals(BigInteger.valueOf(3), columnEpStats.get("COLINT").getCurrentMinIntValue());
    Assert.assertEquals(1L, columnEpStats.get("COLINT").getCurrentNullCount());
    Assert.assertEquals(1L, columnEpStats.get("COLINT").getDistinctValues());

    Assert.assertEquals(
        BigInteger.valueOf(40), columnEpStats.get("COLBIGINT").getCurrentMaxIntValue());
    Assert.assertEquals(
        BigInteger.valueOf(4), columnEpStats.get("COLBIGINT").getCurrentMinIntValue());
    Assert.assertEquals(0, columnEpStats.get("COLBIGINT").getCurrentNullCount());
    Assert.assertEquals(2L, columnEpStats.get("COLBIGINT").getDistinctValues());

    Assert.assertEquals("alice", columnEpStats.get("COLCHAR").getCurrentMaxStrValue());
    Assert.assertEquals("2", columnEpStats.get("COLCHAR").getCurrentMinStrValue());
    Assert.assertEquals(0, columnEpStats.get("COLCHAR").getCurrentNullCount());
    Assert.assertEquals(2L, columnEpStats.get("COLCHAR").getDistinctValues());

    // Confirm we reset
    ChannelData resetResults = this.rowBuffer.flush();
    Assert.assertNull(resetResults);
  }

  @Test
  public void testStatsE2ETimestamp() throws Exception {
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

    ColumnMetadata colTimestampLtzSB16Scale6 = new ColumnMetadata();
    colTimestampLtzSB16Scale6.setName("COLTIMESTAMPLTZ_SB16_SCALE6");
    colTimestampLtzSB16Scale6.setPhysicalType("SB16");
    colTimestampLtzSB16Scale6.setNullable(false);
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

    innerBuffer.insertRows(Arrays.asList(row1, row2, row3), null);
    ChannelData result = innerBuffer.flush();
    EpInfo resultInfo = result.getEpInfo();
    Assert.assertEquals(3, resultInfo.getRowCount());

    Assert.assertEquals(
        BigInteger.valueOf(1621899220),
        resultInfo.getColumnEps().get("COLTIMESTAMPLTZ_SB8").getMinIntValue());
    Assert.assertEquals(
        BigInteger.valueOf(1621899221),
        resultInfo.getColumnEps().get("COLTIMESTAMPLTZ_SB8").getMaxIntValue());

    Assert.assertEquals(
        new BigInteger("1621899220123456789"),
        resultInfo.getColumnEps().get("COLTIMESTAMPLTZ_SB16").getMinIntValue());
    Assert.assertEquals(
        new BigInteger("1621899220123456790"),
        resultInfo.getColumnEps().get("COLTIMESTAMPLTZ_SB16").getMaxIntValue());

    Assert.assertEquals(
        new BigInteger("1621899220123456"),
        resultInfo.getColumnEps().get("COLTIMESTAMPLTZ_SB16_SCALE6").getMinIntValue());
    Assert.assertEquals(
        new BigInteger("1621899220123457"),
        resultInfo.getColumnEps().get("COLTIMESTAMPLTZ_SB16_SCALE6").getMaxIntValue());

    Assert.assertEquals(1, resultInfo.getColumnEps().get("COLTIMESTAMPLTZ_SB8").getNullCount());
    Assert.assertEquals(1, resultInfo.getColumnEps().get("COLTIMESTAMPLTZ_SB16").getNullCount());
    Assert.assertEquals(
        1, resultInfo.getColumnEps().get("COLTIMESTAMPLTZ_SB16_SCALE6").getNullCount());
  }

  @Test
  public void testE2EDate() throws Exception {
    ArrowRowBuffer innerBuffer = new ArrowRowBuffer(channel);

    ColumnMetadata colDate = new ColumnMetadata();
    colDate.setName("COLDATE");
    colDate.setPhysicalType("SB8");
    colDate.setNullable(false);
    colDate.setLogicalType("DATE");
    colDate.setScale(0);

    innerBuffer.setupSchema(Arrays.asList(colDate));

    Map<String, Object> row1 = new HashMap<>();
    row1.put("COLDATE", "18772");

    Map<String, Object> row2 = new HashMap<>();
    row2.put("COLDATE", "18773");

    Map<String, Object> row3 = new HashMap<>();
    row3.put("COLDATE", null);

    innerBuffer.insertRows(Arrays.asList(row1, row2, row3), null);

    // Check data was inserted into Arrow correctly
    Assert.assertEquals(18772, innerBuffer.vectors.get("COLDATE").getObject(0));
    Assert.assertEquals(18773, innerBuffer.vectors.get("COLDATE").getObject(1));
    Assert.assertNull(innerBuffer.vectors.get("COLDATE").getObject(2));

    // Check stats generation
    ChannelData result = innerBuffer.flush();
    EpInfo resultInfo = result.getEpInfo();
    Assert.assertEquals(3, resultInfo.getRowCount());

    Assert.assertEquals(
        BigInteger.valueOf(18772), resultInfo.getColumnEps().get("COLDATE").getMinIntValue());
    Assert.assertEquals(
        BigInteger.valueOf(18773), resultInfo.getColumnEps().get("COLDATE").getMaxIntValue());

    Assert.assertEquals(1, resultInfo.getColumnEps().get("COLDATE").getNullCount());
  }

  @Test
  public void testE2ETime() throws Exception {
    ArrowRowBuffer innerBuffer = new ArrowRowBuffer(channel);

    ColumnMetadata colTimeSB4 = new ColumnMetadata();
    colTimeSB4.setName("COLTIMESB4");
    colTimeSB4.setPhysicalType("SB4");
    colTimeSB4.setNullable(false);
    colTimeSB4.setLogicalType("TIME");
    colTimeSB4.setScale(0);

    ColumnMetadata colTimeSB8 = new ColumnMetadata();
    colTimeSB8.setName("COLTIMESB8");
    colTimeSB8.setPhysicalType("SB8");
    colTimeSB8.setNullable(false);
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

    innerBuffer.insertRows(Arrays.asList(row1, row2, row3), null);

    // Check data was inserted into Arrow correctly
    Assert.assertEquals(43200, innerBuffer.vectors.get("COLTIMESB4").getObject(0));
    Assert.assertEquals(43260, innerBuffer.vectors.get("COLTIMESB4").getObject(1));
    Assert.assertNull(innerBuffer.vectors.get("COLTIMESB4").getObject(2));

    Assert.assertEquals(44200123l, innerBuffer.vectors.get("COLTIMESB8").getObject(0));
    Assert.assertEquals(44201000l, innerBuffer.vectors.get("COLTIMESB8").getObject(1));
    Assert.assertNull(innerBuffer.vectors.get("COLTIMESB8").getObject(2));

    // Check stats generation
    ChannelData result = innerBuffer.flush();
    EpInfo resultInfo = result.getEpInfo();
    Assert.assertEquals(3, resultInfo.getRowCount());

    Assert.assertEquals(
        BigInteger.valueOf(43200), resultInfo.getColumnEps().get("COLTIMESB4").getMinIntValue());
    Assert.assertEquals(
        BigInteger.valueOf(43260), resultInfo.getColumnEps().get("COLTIMESB4").getMaxIntValue());
    Assert.assertEquals(1, resultInfo.getColumnEps().get("COLTIMESB4").getNullCount());

    Assert.assertEquals(
        BigInteger.valueOf(44200123), resultInfo.getColumnEps().get("COLTIMESB8").getMinIntValue());
    Assert.assertEquals(
        BigInteger.valueOf(44201000), resultInfo.getColumnEps().get("COLTIMESB8").getMaxIntValue());
    Assert.assertEquals(1, resultInfo.getColumnEps().get("COLTIMESB8").getNullCount());
  }

  @Test
  public void testE2EBoolean() throws Exception {
    ArrowRowBuffer innerBuffer = new ArrowRowBuffer(channel);

    ColumnMetadata colBoolean = new ColumnMetadata();
    colBoolean.setName("COLBOOLEAN");
    colBoolean.setPhysicalType("SB1");
    colBoolean.setNullable(false);
    colBoolean.setLogicalType("BOOLEAN");
    colBoolean.setScale(0);

    innerBuffer.setupSchema(Arrays.asList(colBoolean));

    Map<String, Object> row1 = new HashMap<>();
    row1.put("COLBOOLEAN", true);

    Map<String, Object> row2 = new HashMap<>();
    row2.put("COLBOOLEAN", false);

    Map<String, Object> row3 = new HashMap<>();
    row3.put("COLBOOLEAN", null);

    innerBuffer.insertRows(Arrays.asList(row1, row2, row3), null);

    // Check data was inserted into Arrow correctly
    Assert.assertEquals(true, innerBuffer.vectors.get("COLBOOLEAN").getObject(0));
    Assert.assertEquals(false, innerBuffer.vectors.get("COLBOOLEAN").getObject(1));
    Assert.assertNull(innerBuffer.vectors.get("COLBOOLEAN").getObject(2));

    // Check stats generation
    ChannelData result = innerBuffer.flush();
    EpInfo resultInfo = result.getEpInfo();
    Assert.assertEquals(3, resultInfo.getRowCount());

    Assert.assertEquals(
        BigInteger.valueOf(0), resultInfo.getColumnEps().get("COLBOOLEAN").getMinIntValue());
    Assert.assertEquals(
        BigInteger.valueOf(1), resultInfo.getColumnEps().get("COLBOOLEAN").getMaxIntValue());
    Assert.assertEquals(1, resultInfo.getColumnEps().get("COLBOOLEAN").getNullCount());
  }

  @Test
  public void testE2EBinary() throws Exception {
    ArrowRowBuffer innerBuffer = new ArrowRowBuffer(channel);

    ColumnMetadata colBinary = new ColumnMetadata();
    colBinary.setName("COLBINARY");
    colBinary.setPhysicalType("LOB");
    colBinary.setNullable(false);
    colBinary.setLogicalType("BINARY");
    colBinary.setScale(0);

    innerBuffer.setupSchema(Arrays.asList(colBinary));

    Map<String, Object> row1 = new HashMap<>();
    row1.put("COLBINARY", "Hello World".getBytes(StandardCharsets.UTF_8));

    Map<String, Object> row2 = new HashMap<>();
    row2.put("COLBINARY", "Honk Honk".getBytes(StandardCharsets.UTF_8));

    Map<String, Object> row3 = new HashMap<>();
    row3.put("COLBINARY", null);

    innerBuffer.insertRows(Arrays.asList(row1, row2, row3), null);

    // Check data was inserted into Arrow correctly
    Assert.assertEquals(
        "Hello World",
        new String(
            (byte[]) innerBuffer.vectors.get("COLBINARY").getObject(0), StandardCharsets.UTF_8));
    Assert.assertEquals(
        "Honk Honk",
        new String(
            (byte[]) innerBuffer.vectors.get("COLBINARY").getObject(1), StandardCharsets.UTF_8));
    Assert.assertNull(innerBuffer.vectors.get("COLBINARY").getObject(2));

    // Check stats generation
    ChannelData result = innerBuffer.flush();
    EpInfo resultInfo = result.getEpInfo();

    Assert.assertEquals(3, resultInfo.getRowCount());
    Assert.assertEquals(11L, resultInfo.getColumnEps().get("COLBINARY").getMaxLength());
    Assert.assertEquals(1, resultInfo.getColumnEps().get("COLBINARY").getNullCount());
  }

  @Test
  public void testE2EReal() throws Exception {
    ArrowRowBuffer innerBuffer = new ArrowRowBuffer(channel);

    ColumnMetadata colReal = new ColumnMetadata();
    colReal.setName("COLREAL");
    colReal.setPhysicalType("SB16");
    colReal.setNullable(false);
    colReal.setLogicalType("REAL");
    colReal.setScale(0);

    innerBuffer.setupSchema(Arrays.asList(colReal));

    Map<String, Object> row1 = new HashMap<>();
    row1.put("COLREAL", 123.456);

    Map<String, Object> row2 = new HashMap<>();
    row2.put("COLREAL", 123.4567);

    Map<String, Object> row3 = new HashMap<>();
    row3.put("COLREAL", null);

    innerBuffer.insertRows(Arrays.asList(row1, row2, row3), null);

    // Check data was inserted into Arrow correctly
    Assert.assertEquals(123.456, innerBuffer.vectors.get("COLREAL").getObject(0));
    Assert.assertEquals(123.4567, innerBuffer.vectors.get("COLREAL").getObject(1));
    Assert.assertNull(innerBuffer.vectors.get("COLREAL").getObject(2));

    // Check stats generation
    ChannelData result = innerBuffer.flush();
    EpInfo resultInfo = result.getEpInfo();

    Assert.assertEquals(3, resultInfo.getRowCount());
    Assert.assertEquals(
        Double.valueOf(123.456), resultInfo.getColumnEps().get("COLREAL").getMinRealValue());
    Assert.assertEquals(
        Double.valueOf(123.4567), resultInfo.getColumnEps().get("COLREAL").getMaxRealValue());
    Assert.assertEquals(1, resultInfo.getColumnEps().get("COLREAL").getNullCount());
  }

  @Test
  public void testGetStringValue() throws Exception {
    Assert.assertEquals("123", rowBuffer.getStringValue("123"));
    Assert.assertEquals("123", rowBuffer.getStringValue(123));
    Assert.assertEquals("123", rowBuffer.getStringValue(new BigDecimal("123")));
    Assert.assertEquals("123", rowBuffer.getStringValue(new BigInteger("123")));
    Assert.assertEquals("123.0", rowBuffer.getStringValue(123f));
    Assert.assertEquals("123.0", rowBuffer.getStringValue(123d));
    Assert.assertEquals("123", rowBuffer.getStringValue(123l));
  }

  @Test
  public void testGetTimestampInScale() throws Exception {
    Assert.assertEquals(new BigInteger("123"), rowBuffer.getTimeInScale("123.000000000", 0));
    Assert.assertEquals(new BigInteger("12301"), rowBuffer.getTimeInScale("123.01", 2));
  }

  @Test
  public void testConvertStringToBoolean() throws Exception {
    Assert.assertEquals(true, rowBuffer.convertStringToBoolean("true"));
    Assert.assertEquals(true, rowBuffer.convertStringToBoolean("TRue"));
    Assert.assertEquals(true, rowBuffer.convertStringToBoolean("t"));
    Assert.assertEquals(true, rowBuffer.convertStringToBoolean("y"));
    Assert.assertEquals(true, rowBuffer.convertStringToBoolean("yes"));
    Assert.assertEquals(true, rowBuffer.convertStringToBoolean("1"));
    Assert.assertEquals(true, rowBuffer.convertStringToBoolean("on"));
    Assert.assertEquals(false, rowBuffer.convertStringToBoolean("off"));
    Assert.assertEquals(false, rowBuffer.convertStringToBoolean("honk"));
    Assert.assertEquals(false, rowBuffer.convertStringToBoolean("false"));
  }
}
