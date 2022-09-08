package net.snowflake.ingest.streaming.internal;

import net.snowflake.ingest.streaming.OpenChannelRequest;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ParquetBufferTest {
  private ParquetRowBuffer rowBufferOnErrorContinue;

  @Before
  public void setupRowBuffer() {
    this.rowBufferOnErrorContinue = createTestBuffer(OpenChannelRequest.OnErrorOption.CONTINUE);

    // Setup column fields and vectors
    this.rowBufferOnErrorContinue.setupSchema(RowBufferTest.generateCols());
  }

  ParquetRowBuffer createTestBuffer(OpenChannelRequest.OnErrorOption onErrorOption) {
    SnowflakeStreamingIngestClientInternal<ParquetChunkData> client =
        new SnowflakeStreamingIngestClientInternal<>("client");
    SnowflakeStreamingIngestChannelInternal<ParquetChunkData> channel =
        new SnowflakeStreamingIngestChannelInternal<>(
            "channel", "db", "schema", "table", "0", 0L, 0L, client, "key", 1234L, onErrorOption);

    return new ParquetRowBuffer(channel);
  }

  @Test
  public void buildFieldFixedSB1() {
    // FIXED, SB1
    ColumnMetadata testCol = RowBufferTest.generateCol("SB1", "FIXED", true);
    Type result = this.rowBufferOnErrorContinue.getColumnParquetType(testCol, 0);

    Assert.assertEquals("testCol", result.getName());
    Assert.assertEquals(4, result.asPrimitiveType().getTypeLength());
    Assert.assertEquals(0, result.asPrimitiveType().getId().intValue());

    Assert.assertEquals(
        PrimitiveType.PrimitiveTypeName.INT32, result.asPrimitiveType().getPrimitiveTypeName());
    Assert.assertEquals(
        LogicalTypeAnnotation.decimalType(testCol.getScale(), testCol.getPrecision()),
        result.getLogicalTypeAnnotation());
    Assert.assertEquals(Type.Repetition.OPTIONAL, result.getRepetition());
  }

  @Test
  public void buildFieldFixedSB2() {
    ColumnMetadata testCol = RowBufferTest.generateCol("SB2", "FIXED", false);
    Type result = this.rowBufferOnErrorContinue.getColumnParquetType(testCol, 0);

    Assert.assertEquals("testCol", result.getName());
    Assert.assertEquals(4, result.asPrimitiveType().getTypeLength());
    Assert.assertEquals(0, result.asPrimitiveType().getId().intValue());

    Assert.assertEquals(
        PrimitiveType.PrimitiveTypeName.INT32, result.asPrimitiveType().getPrimitiveTypeName());
    Assert.assertEquals(
        LogicalTypeAnnotation.decimalType(testCol.getScale(), testCol.getPrecision()),
        result.getLogicalTypeAnnotation());
    Assert.assertEquals(Type.Repetition.REQUIRED, result.getRepetition());
  }

  @Test
  public void buildFieldFixedSB4() {
    ColumnMetadata testCol = RowBufferTest.generateCol("SB4", "FIXED", true);
    Type result = this.rowBufferOnErrorContinue.getColumnParquetType(testCol, 0);

    Assert.assertEquals("testCol", result.getName());
    Assert.assertEquals(4, result.asPrimitiveType().getTypeLength());
    Assert.assertEquals(0, result.asPrimitiveType().getId().intValue());

    Assert.assertEquals(
        PrimitiveType.PrimitiveTypeName.INT32, result.asPrimitiveType().getPrimitiveTypeName());
    Assert.assertEquals(
        LogicalTypeAnnotation.decimalType(testCol.getScale(), testCol.getPrecision()),
        result.getLogicalTypeAnnotation());
    Assert.assertEquals(Type.Repetition.OPTIONAL, result.getRepetition());
  }

  @Test
  public void buildFieldFixedSB8() {
    ColumnMetadata testCol = RowBufferTest.generateCol("SB8", "FIXED", true);
    Type result = this.rowBufferOnErrorContinue.getColumnParquetType(testCol, 0);

    Assert.assertEquals("testCol", result.getName());
    Assert.assertEquals(8, result.asPrimitiveType().getTypeLength());
    Assert.assertEquals(0, result.asPrimitiveType().getId().intValue());

    Assert.assertEquals(
        PrimitiveType.PrimitiveTypeName.INT64, result.asPrimitiveType().getPrimitiveTypeName());
    Assert.assertEquals(
        LogicalTypeAnnotation.decimalType(testCol.getScale(), testCol.getPrecision()),
        result.getLogicalTypeAnnotation());
    Assert.assertEquals(Type.Repetition.OPTIONAL, result.getRepetition());
  }

  @Test
  public void buildFieldFixedSB16() {
    ColumnMetadata testCol = RowBufferTest.generateCol("SB16", "FIXED", true);
    Type result = this.rowBufferOnErrorContinue.getColumnParquetType(testCol, 0);

    Assert.assertEquals("testCol", result.getName());
    Assert.assertEquals(16, result.asPrimitiveType().getTypeLength());
    Assert.assertEquals(0, result.asPrimitiveType().getId().intValue());

    Assert.assertEquals(
        PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY,
        result.asPrimitiveType().getPrimitiveTypeName());
    Assert.assertEquals(
        LogicalTypeAnnotation.decimalType(testCol.getScale(), testCol.getPrecision()),
        result.getLogicalTypeAnnotation());
    Assert.assertEquals(Type.Repetition.OPTIONAL, result.getRepetition());
  }

  @Test
  public void buildFieldLobVariant() {
    ColumnMetadata testCol = RowBufferTest.generateCol("LOB", "VARIANT", true);
  }

  @Test
  public void buildFieldTimestampNtzSB8() {
    ColumnMetadata testCol = RowBufferTest.generateCol("SB8", "TIMESTAMP_NTZ", true);
    Type result = this.rowBufferOnErrorContinue.getColumnParquetType(testCol, 0);

    Assert.assertEquals("testCol", result.getName());
    // Assert.assertEquals(8, result.asPrimitiveType().getTypeLength());
    Assert.assertEquals(0, result.asPrimitiveType().getId().intValue());

    Assert.assertEquals(
        PrimitiveType.PrimitiveTypeName.INT64, result.asPrimitiveType().getPrimitiveTypeName());
    // precision in RowBufferTest.generateCol is set to 4, hence MILLIS
    Assert.assertEquals(
        LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MILLIS),
        result.getLogicalTypeAnnotation());
    Assert.assertEquals(Type.Repetition.OPTIONAL, result.getRepetition());
  }

  @Test
  public void buildFieldTimestampNtzSB16() {
    ColumnMetadata testCol = RowBufferTest.generateCol("SB16", "TIMESTAMP_NTZ", true);
    Type result = this.rowBufferOnErrorContinue.getColumnParquetType(testCol, 0);

    Assert.assertEquals("testCol", result.getName());
    Assert.assertEquals(16, result.asPrimitiveType().getTypeLength());
    Assert.assertEquals(0, result.asPrimitiveType().getId().intValue());

    Assert.assertEquals(
        PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY,
        result.asPrimitiveType().getPrimitiveTypeName());
    Assert.assertEquals(
        LogicalTypeAnnotation.decimalType(testCol.getScale(), 38),
        result.getLogicalTypeAnnotation());
    Assert.assertEquals(Type.Repetition.OPTIONAL, result.getRepetition());
  }

  @Test
  public void buildFieldTimestampTzSB8() {
    ColumnMetadata testCol = RowBufferTest.generateCol("SB8", "TIMESTAMP_TZ", true);
    Type result = this.rowBufferOnErrorContinue.getColumnParquetType(testCol, 0);

    Assert.assertEquals("testCol", result.getName());
    // Assert.assertEquals(8, result.asPrimitiveType().getTypeLength());
    Assert.assertEquals(0, result.asPrimitiveType().getId().intValue());

    Assert.assertEquals(
        PrimitiveType.PrimitiveTypeName.INT64, result.asPrimitiveType().getPrimitiveTypeName());
    // precision in RowBufferTest.generateCol is set to 4, hence MILLIS
    Assert.assertEquals(
        LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MILLIS),
        result.getLogicalTypeAnnotation());
    Assert.assertEquals(Type.Repetition.OPTIONAL, result.getRepetition());
  }

  @Test
  public void buildFieldTimestampTzSB16() {
    ColumnMetadata testCol = RowBufferTest.generateCol("SB16", "TIMESTAMP_TZ", true);
    Type result = this.rowBufferOnErrorContinue.getColumnParquetType(testCol, 0);

    Assert.assertEquals("testCol", result.getName());
    Assert.assertEquals(16, result.asPrimitiveType().getTypeLength());
    Assert.assertEquals(0, result.asPrimitiveType().getId().intValue());

    Assert.assertEquals(
        PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY,
        result.asPrimitiveType().getPrimitiveTypeName());
    Assert.assertEquals(
        LogicalTypeAnnotation.decimalType(testCol.getScale(), 38),
        result.getLogicalTypeAnnotation());
    Assert.assertEquals(Type.Repetition.OPTIONAL, result.getRepetition());
  }

  @Test
  public void buildFieldDate() {
    ColumnMetadata testCol = RowBufferTest.generateCol("SB8", "DATE", true);
    Type result = this.rowBufferOnErrorContinue.getColumnParquetType(testCol, 0);

    Assert.assertEquals("testCol", result.getName());
    // Assert.assertEquals(4, result.asPrimitiveType().getTypeLength());
    Assert.assertEquals(0, result.asPrimitiveType().getId().intValue());

    Assert.assertEquals(
        PrimitiveType.PrimitiveTypeName.INT32, result.asPrimitiveType().getPrimitiveTypeName());
    Assert.assertEquals(LogicalTypeAnnotation.dateType(), result.getLogicalTypeAnnotation());
    Assert.assertEquals(Type.Repetition.OPTIONAL, result.getRepetition());
  }

  @Test
  public void buildFieldTimeSB4() {
    ColumnMetadata testCol = RowBufferTest.generateCol("SB4", "TIME", true);
    Type result = this.rowBufferOnErrorContinue.getColumnParquetType(testCol, 0);

    Assert.assertEquals("testCol", result.getName());
    // Assert.assertEquals(4, result.asPrimitiveType().getTypeLength());
    Assert.assertEquals(0, result.asPrimitiveType().getId().intValue());

    Assert.assertEquals(
        PrimitiveType.PrimitiveTypeName.INT32, result.asPrimitiveType().getPrimitiveTypeName());
    // precision in RowBufferTest.generateCol is set to 4, hence MILLIS
    Assert.assertEquals(
        LogicalTypeAnnotation.timeType(false, LogicalTypeAnnotation.TimeUnit.MILLIS),
        result.getLogicalTypeAnnotation());
    Assert.assertEquals(Type.Repetition.OPTIONAL, result.getRepetition());
  }

  @Test
  public void buildFieldTimeSB8() {
    /**
     * ColumnMetadata testCol = RowBufferTest.generateCol("SB8", "TIME", true); Type result =
     * this.rowBufferOnErrorContinue.getColumnParquetType(testCol, 0); // -> exception
     * java.lang.IllegalStateException: TIME(MILLIS,false) can only annotate INT32
     *
     * <p>Assert.assertEquals("testCol", result.getName()); Assert.assertEquals(8,
     * result.asPrimitiveType().getTypeLength()); Assert.assertEquals(0,
     * result.asPrimitiveType().getId().intValue());
     *
     * <p>Assert.assertEquals(PrimitiveType.PrimitiveTypeName.INT64,
     * result.asPrimitiveType().getPrimitiveTypeName()); // precision in RowBufferTest.generateCol
     * is set to 4, hence MILLIS Assert.assertEquals(LogicalTypeAnnotation.timeType(false,
     * LogicalTypeAnnotation.TimeUnit.MILLIS), result.getLogicalTypeAnnotation());
     * Assert.assertEquals(Type.Repetition.OPTIONAL, result.getRepetition());
     */
  }

  @Test
  public void buildFieldBoolean() {
    ColumnMetadata testCol = RowBufferTest.generateCol("BINARY", "BOOLEAN", true);
    Type result = this.rowBufferOnErrorContinue.getColumnParquetType(testCol, 0);

    Assert.assertEquals("testCol", result.getName());
    // Assert.assertEquals(1, result.asPrimitiveType().getTypeLength());
    Assert.assertEquals(0, result.asPrimitiveType().getId().intValue());

    Assert.assertEquals(
        PrimitiveType.PrimitiveTypeName.BOOLEAN, result.asPrimitiveType().getPrimitiveTypeName());
    Assert.assertNull(result.getLogicalTypeAnnotation());
    Assert.assertEquals(Type.Repetition.OPTIONAL, result.getRepetition());
  }

  @Test
  public void buildFieldRealSB16() {
    ColumnMetadata testCol = RowBufferTest.generateCol("SB16", "REAL", true);
    Type result = this.rowBufferOnErrorContinue.getColumnParquetType(testCol, 0);

    Assert.assertEquals("testCol", result.getName());
    // Assert.assertEquals(8, result.asPrimitiveType().getTypeLength());
    Assert.assertEquals(0, result.asPrimitiveType().getId().intValue());

    Assert.assertEquals(
        PrimitiveType.PrimitiveTypeName.DOUBLE, result.asPrimitiveType().getPrimitiveTypeName());
    Assert.assertNull(result.getLogicalTypeAnnotation());
    Assert.assertEquals(Type.Repetition.OPTIONAL, result.getRepetition());
  }
}
