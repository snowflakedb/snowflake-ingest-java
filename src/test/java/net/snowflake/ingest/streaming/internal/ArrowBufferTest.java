package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.streaming.internal.ArrowRowBuffer.DECIMAL_BIT_WIDTH;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ArrowBufferTest {
  private ArrowRowBuffer rowBufferOnErrorContinue;

  @Before
  public void setupRowBuffer() {
    this.rowBufferOnErrorContinue = createTestBuffer(OpenChannelRequest.OnErrorOption.CONTINUE);
    this.rowBufferOnErrorContinue.setupSchema(RowBufferTest.createSchema());
  }

  ArrowRowBuffer createTestBuffer(OpenChannelRequest.OnErrorOption onErrorOption) {
    SnowflakeStreamingIngestClientInternal<VectorSchemaRoot> client =
        new SnowflakeStreamingIngestClientInternal<>("client");
    SnowflakeStreamingIngestChannelInternal<VectorSchemaRoot> channel =
        new SnowflakeStreamingIngestChannelInternal<>(
            "channel", "db", "schema", "table", "0", 0L, 0L, client, "key", 1234L, onErrorOption);
    return new ArrowRowBuffer(channel);
  }

  @Test
  public void testFieldNumberAfterFlush() {
    String offsetToken = "1";
    Map<String, Object> row = new HashMap<>();
    row.put("colTinyInt", (byte) 1);
    row.put("\"colTinyInt\"", (byte) 1);
    row.put("colSmallInt", (short) 2);
    row.put("colInt", 3);
    row.put("colBigInt", 4L);
    row.put("colDecimal", 1.23);
    row.put("colChar", "2");

    InsertValidationResponse response =
        rowBufferOnErrorContinue.insertRows(Collections.singletonList(row), offsetToken);
    Assert.assertFalse(response.hasErrors());

    ChannelData<VectorSchemaRoot> data = rowBufferOnErrorContinue.flush();
    Assert.assertEquals(7, data.getVectors().getFieldVectors().size());
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
  public void buildFieldTimestampTzSB8() {
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
  public void buildFieldTimestampTzSB16() {
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
  public void buildFieldDate() {
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
  public void testArrowE2ETimestampLTZ() {
    testArrowE2ETimestampLTZHelper(OpenChannelRequest.OnErrorOption.ABORT);
    testArrowE2ETimestampLTZHelper(OpenChannelRequest.OnErrorOption.CONTINUE);
  }

  private void testArrowE2ETimestampLTZHelper(OpenChannelRequest.OnErrorOption onErrorOption) {
    ArrowRowBuffer innerBuffer = createTestBuffer(onErrorOption);

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
    row.put("COLTIMESTAMPLTZ_SB16", "1621899220123456789");

    InsertValidationResponse response =
        innerBuffer.insertRows(Collections.singletonList(row), null);
    Assert.assertFalse(response.hasErrors());
    Assert.assertEquals(
        1621899220L, innerBuffer.vectorsRoot.getVector("COLTIMESTAMPLTZ_SB8").getObject(0));
    Assert.assertEquals(
        "epoch",
        innerBuffer
            .vectorsRoot
            .getVector("COLTIMESTAMPLTZ_SB16")
            .getChildrenFromFields()
            .get(0)
            .getName());
    Assert.assertEquals(
        1621899220L,
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
}
