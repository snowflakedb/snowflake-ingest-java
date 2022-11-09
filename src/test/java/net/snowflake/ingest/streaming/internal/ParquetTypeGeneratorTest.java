package net.snowflake.ingest.streaming.internal;

import java.util.Map;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.Assert;
import org.junit.Test;

public class ParquetTypeGeneratorTest {

  @Test
  public void buildFieldFixedSB1() {
    ColumnMetadata testCol =
        ColumnMetadataBuilder.newBuilder()
            .logicalType("FIXED")
            .physicalType("SB1")
            .nullable(true)
            .build();

    ParquetTypeGenerator.ParquetTypeInfo typeInfo =
        ParquetTypeGenerator.generateColumnParquetTypeInfo(testCol, 0);
    ParquetTypeInfoAssertionBuilder.newBuilder()
        .typeInfo(typeInfo)
        .expectedFieldName("testCol")
        .expectedFieldId(0)
        .expectedTypeLength(4)
        .expectedPrimitiveTypeName(PrimitiveType.PrimitiveTypeName.INT32)
        .expectedLogicalTypeAnnotation(
            LogicalTypeAnnotation.decimalType(testCol.getScale(), testCol.getPrecision()))
        .expectedRepetition(Type.Repetition.OPTIONAL)
        .expectedColMetaData(
            AbstractRowBuffer.ColumnLogicalType.FIXED.getOrdinal()
                + ","
                + AbstractRowBuffer.ColumnPhysicalType.SB1.getOrdinal())
        .assertMatches();
  }

  @Test
  public void buildFieldFixedSB2() {
    ColumnMetadata testCol =
        ColumnMetadataBuilder.newBuilder()
            .logicalType("FIXED")
            .physicalType("SB2")
            .nullable(false)
            .build();

    ParquetTypeGenerator.ParquetTypeInfo typeInfo =
        ParquetTypeGenerator.generateColumnParquetTypeInfo(testCol, 0);
    ParquetTypeInfoAssertionBuilder.newBuilder()
        .typeInfo(typeInfo)
        .expectedFieldName("testCol")
        .expectedFieldId(0)
        .expectedTypeLength(4)
        .expectedPrimitiveTypeName(PrimitiveType.PrimitiveTypeName.INT32)
        .expectedLogicalTypeAnnotation(
            LogicalTypeAnnotation.decimalType(testCol.getScale(), testCol.getPrecision()))
        .expectedRepetition(Type.Repetition.REQUIRED)
        .expectedColMetaData(
            AbstractRowBuffer.ColumnLogicalType.FIXED.getOrdinal()
                + ","
                + AbstractRowBuffer.ColumnPhysicalType.SB2.getOrdinal())
        .assertMatches();
  }

  @Test
  public void buildFieldFixedSB4() {
    ColumnMetadata testCol =
        ColumnMetadataBuilder.newBuilder()
            .logicalType("FIXED")
            .physicalType("SB4")
            .nullable(true)
            .build();

    ParquetTypeGenerator.ParquetTypeInfo typeInfo =
        ParquetTypeGenerator.generateColumnParquetTypeInfo(testCol, 0);
    ParquetTypeInfoAssertionBuilder.newBuilder()
        .typeInfo(typeInfo)
        .expectedFieldName("testCol")
        .expectedFieldId(0)
        .expectedTypeLength(4)
        .expectedPrimitiveTypeName(PrimitiveType.PrimitiveTypeName.INT32)
        .expectedLogicalTypeAnnotation(
            LogicalTypeAnnotation.decimalType(testCol.getScale(), testCol.getPrecision()))
        .expectedRepetition(Type.Repetition.OPTIONAL)
        .expectedColMetaData(
            AbstractRowBuffer.ColumnLogicalType.FIXED.getOrdinal()
                + ","
                + AbstractRowBuffer.ColumnPhysicalType.SB4.getOrdinal())
        .assertMatches();
  }

  @Test
  public void buildFieldFixedSB8() {
    ColumnMetadata testCol =
        ColumnMetadataBuilder.newBuilder()
            .logicalType("FIXED")
            .physicalType("SB8")
            .nullable(true)
            .build();

    ParquetTypeGenerator.ParquetTypeInfo typeInfo =
        ParquetTypeGenerator.generateColumnParquetTypeInfo(testCol, 0);
    ParquetTypeInfoAssertionBuilder.newBuilder()
        .typeInfo(typeInfo)
        .expectedFieldName("testCol")
        .expectedFieldId(0)
        .expectedTypeLength(8)
        .expectedPrimitiveTypeName(PrimitiveType.PrimitiveTypeName.INT64)
        .expectedLogicalTypeAnnotation(
            LogicalTypeAnnotation.decimalType(testCol.getScale(), testCol.getPrecision()))
        .expectedRepetition(Type.Repetition.OPTIONAL)
        .expectedColMetaData(
            AbstractRowBuffer.ColumnLogicalType.FIXED.getOrdinal()
                + ","
                + AbstractRowBuffer.ColumnPhysicalType.SB8.getOrdinal())
        .assertMatches();
  }

  @Test
  public void buildFieldFixedSB16() {
    ColumnMetadata testCol =
        ColumnMetadataBuilder.newBuilder()
            .logicalType("FIXED")
            .physicalType("SB16")
            .nullable(true)
            .build();

    ParquetTypeGenerator.ParquetTypeInfo typeInfo =
        ParquetTypeGenerator.generateColumnParquetTypeInfo(testCol, 0);
    ParquetTypeInfoAssertionBuilder.newBuilder()
        .typeInfo(typeInfo)
        .expectedFieldName("testCol")
        .expectedFieldId(0)
        .expectedTypeLength(16)
        .expectedPrimitiveTypeName(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
        .expectedLogicalTypeAnnotation(
            LogicalTypeAnnotation.decimalType(testCol.getScale(), testCol.getPrecision()))
        .expectedRepetition(Type.Repetition.OPTIONAL)
        .expectedColMetaData(
            AbstractRowBuffer.ColumnLogicalType.FIXED.getOrdinal()
                + ","
                + AbstractRowBuffer.ColumnPhysicalType.SB16.getOrdinal())
        .assertMatches();
  }

  @Test
  public void buildFieldLobVariant() {
    ColumnMetadata testCol =
        ColumnMetadataBuilder.newBuilder()
            .logicalType("VARIANT")
            .physicalType("LOB")
            .nullable(true)
            .build();

    ParquetTypeGenerator.ParquetTypeInfo typeInfo =
        ParquetTypeGenerator.generateColumnParquetTypeInfo(testCol, 0);
    ParquetTypeInfoAssertionBuilder.newBuilder()
        .typeInfo(typeInfo)
        .expectedFieldName("testCol")
        .expectedFieldId(0)
        .expectedTypeLength(0)
        .expectedPrimitiveTypeName(PrimitiveType.PrimitiveTypeName.BINARY)
        .expectedLogicalTypeAnnotation(LogicalTypeAnnotation.stringType())
        .expectedRepetition(Type.Repetition.OPTIONAL)
        .expectedColMetaData(
            AbstractRowBuffer.ColumnLogicalType.VARIANT.getOrdinal()
                + ","
                + AbstractRowBuffer.ColumnPhysicalType.LOB.getOrdinal())
        .assertMatches();

    Assert.assertEquals("1", typeInfo.getMetadata().get(0 + ":obj_enc"));
  }

  @Test
  public void buildFieldTimestampNtzSB8() {
    ColumnMetadata testCol =
        ColumnMetadataBuilder.newBuilder()
            .logicalType("TIMESTAMP_NTZ")
            .physicalType("SB8")
            .nullable(true)
            .build();

    ParquetTypeGenerator.ParquetTypeInfo typeInfo =
        ParquetTypeGenerator.generateColumnParquetTypeInfo(testCol, 0);
    ParquetTypeInfoAssertionBuilder.newBuilder()
        .typeInfo(typeInfo)
        .expectedFieldName("testCol")
        .expectedFieldId(0)
        .expectedTypeLength(8)
        .expectedPrimitiveTypeName(PrimitiveType.PrimitiveTypeName.INT64)
        .expectedLogicalTypeAnnotation(LogicalTypeAnnotation.decimalType(testCol.getScale(), 18))
        .expectedRepetition(Type.Repetition.OPTIONAL)
        .expectedColMetaData(
            AbstractRowBuffer.ColumnLogicalType.TIMESTAMP_NTZ.getOrdinal()
                + ","
                + AbstractRowBuffer.ColumnPhysicalType.SB8.getOrdinal())
        .assertMatches();
  }

  @Test
  public void buildFieldTimestampNtzSB16() {
    ColumnMetadata testCol =
        ColumnMetadataBuilder.newBuilder()
            .logicalType("TIMESTAMP_NTZ")
            .physicalType("SB16")
            .nullable(true)
            .build();

    ParquetTypeGenerator.ParquetTypeInfo typeInfo =
        ParquetTypeGenerator.generateColumnParquetTypeInfo(testCol, 0);
    ParquetTypeInfoAssertionBuilder.newBuilder()
        .typeInfo(typeInfo)
        .expectedFieldName("testCol")
        .expectedFieldId(0)
        .expectedTypeLength(16)
        .expectedPrimitiveTypeName(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
        .expectedLogicalTypeAnnotation(LogicalTypeAnnotation.decimalType(testCol.getScale(), 38))
        .expectedRepetition(Type.Repetition.OPTIONAL)
        .expectedColMetaData(
            AbstractRowBuffer.ColumnLogicalType.TIMESTAMP_NTZ.getOrdinal()
                + ","
                + AbstractRowBuffer.ColumnPhysicalType.SB16.getOrdinal())
        .assertMatches();
  }

  @Test
  public void buildFieldTimestampTzSB8() {
    ColumnMetadata testCol =
        ColumnMetadataBuilder.newBuilder()
            .logicalType("TIMESTAMP_TZ")
            .physicalType("SB8")
            .nullable(true)
            .build();

    ParquetTypeGenerator.ParquetTypeInfo typeInfo =
        ParquetTypeGenerator.generateColumnParquetTypeInfo(testCol, 0);
    ParquetTypeInfoAssertionBuilder.newBuilder()
        .typeInfo(typeInfo)
        .expectedFieldName("testCol")
        .expectedFieldId(0)
        .expectedTypeLength(8)
        .expectedPrimitiveTypeName(PrimitiveType.PrimitiveTypeName.INT64)
        .expectedLogicalTypeAnnotation(LogicalTypeAnnotation.decimalType(testCol.getScale(), 18))
        .expectedRepetition(Type.Repetition.OPTIONAL)
        .expectedColMetaData(
            AbstractRowBuffer.ColumnLogicalType.TIMESTAMP_TZ.getOrdinal()
                + ","
                + AbstractRowBuffer.ColumnPhysicalType.SB8.getOrdinal())
        .assertMatches();
  }

  @Test
  public void buildFieldTimestampTzSB16() {
    ColumnMetadata testCol =
        ColumnMetadataBuilder.newBuilder()
            .logicalType("TIMESTAMP_TZ")
            .physicalType("SB16")
            .nullable(true)
            .build();

    ParquetTypeGenerator.ParquetTypeInfo typeInfo =
        ParquetTypeGenerator.generateColumnParquetTypeInfo(testCol, 0);
    ParquetTypeInfoAssertionBuilder.newBuilder()
        .typeInfo(typeInfo)
        .expectedFieldName("testCol")
        .expectedFieldId(0)
        .expectedTypeLength(16)
        .expectedPrimitiveTypeName(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
        .expectedLogicalTypeAnnotation(LogicalTypeAnnotation.decimalType(testCol.getScale(), 38))
        .expectedRepetition(Type.Repetition.OPTIONAL)
        .expectedColMetaData(
            AbstractRowBuffer.ColumnLogicalType.TIMESTAMP_TZ.getOrdinal()
                + ","
                + AbstractRowBuffer.ColumnPhysicalType.SB16.getOrdinal())
        .assertMatches();
  }

  @Test
  public void buildFieldDate() {
    ColumnMetadata testCol =
        ColumnMetadataBuilder.newBuilder()
            .logicalType("DATE")
            .physicalType("SB8")
            .nullable(true)
            .build();

    ParquetTypeGenerator.ParquetTypeInfo typeInfo =
        ParquetTypeGenerator.generateColumnParquetTypeInfo(testCol, 0);
    ParquetTypeInfoAssertionBuilder.newBuilder()
        .typeInfo(typeInfo)
        .expectedFieldName("testCol")
        .expectedFieldId(0)
        .expectedTypeLength(0)
        .expectedPrimitiveTypeName(PrimitiveType.PrimitiveTypeName.INT32)
        .expectedLogicalTypeAnnotation(LogicalTypeAnnotation.dateType())
        .expectedRepetition(Type.Repetition.OPTIONAL)
        .expectedColMetaData(
            AbstractRowBuffer.ColumnLogicalType.DATE.getOrdinal()
                + ","
                + AbstractRowBuffer.ColumnPhysicalType.SB8.getOrdinal())
        .assertMatches();
  }

  @Test
  public void buildFieldTimeSB4() {
    ColumnMetadata testCol =
        ColumnMetadataBuilder.newBuilder()
            .logicalType("TIME")
            .physicalType("SB4")
            .nullable(true)
            .build();

    ParquetTypeGenerator.ParquetTypeInfo typeInfo =
        ParquetTypeGenerator.generateColumnParquetTypeInfo(testCol, 0);
    ParquetTypeInfoAssertionBuilder.newBuilder()
        .typeInfo(typeInfo)
        .expectedFieldName("testCol")
        .expectedFieldId(0)
        .expectedTypeLength(4)
        .expectedPrimitiveTypeName(PrimitiveType.PrimitiveTypeName.INT32)
        .expectedLogicalTypeAnnotation(LogicalTypeAnnotation.decimalType(testCol.getScale(), 9))
        .expectedRepetition(Type.Repetition.OPTIONAL)
        .expectedColMetaData(
            AbstractRowBuffer.ColumnLogicalType.TIME.getOrdinal()
                + ","
                + AbstractRowBuffer.ColumnPhysicalType.SB4.getOrdinal())
        .assertMatches();
  }

  @Test
  public void buildFieldTimeSB8() {
    ColumnMetadata testCol =
        ColumnMetadataBuilder.newBuilder()
            .logicalType("TIME")
            .physicalType("SB8")
            .nullable(true)
            .build();

    ParquetTypeGenerator.ParquetTypeInfo typeInfo =
        ParquetTypeGenerator.generateColumnParquetTypeInfo(testCol, 0);
    ParquetTypeInfoAssertionBuilder.newBuilder()
        .typeInfo(typeInfo)
        .expectedFieldName("testCol")
        .expectedFieldId(0)
        .expectedTypeLength(8)
        .expectedPrimitiveTypeName(PrimitiveType.PrimitiveTypeName.INT64)
        .expectedLogicalTypeAnnotation(LogicalTypeAnnotation.decimalType(testCol.getScale(), 18))
        .expectedRepetition(Type.Repetition.OPTIONAL)
        .expectedColMetaData(
            AbstractRowBuffer.ColumnLogicalType.TIME.getOrdinal()
                + ","
                + AbstractRowBuffer.ColumnPhysicalType.SB8.getOrdinal())
        .assertMatches();
  }

  @Test
  public void buildFieldBoolean() {
    ColumnMetadata testCol =
        ColumnMetadataBuilder.newBuilder()
            .logicalType("BOOLEAN")
            .physicalType("BINARY")
            .nullable(true)
            .build();

    ParquetTypeGenerator.ParquetTypeInfo typeInfo =
        ParquetTypeGenerator.generateColumnParquetTypeInfo(testCol, 0);
    ParquetTypeInfoAssertionBuilder.newBuilder()
        .typeInfo(typeInfo)
        .expectedFieldName("testCol")
        .expectedFieldId(0)
        .expectedTypeLength(0)
        .expectedPrimitiveTypeName(PrimitiveType.PrimitiveTypeName.BOOLEAN)
        .expectedLogicalTypeAnnotation(null)
        .expectedRepetition(Type.Repetition.OPTIONAL)
        .expectedColMetaData(
            AbstractRowBuffer.ColumnLogicalType.BOOLEAN.getOrdinal()
                + ","
                + AbstractRowBuffer.ColumnPhysicalType.BINARY.getOrdinal())
        .assertMatches();
  }

  @Test
  public void buildFieldRealSB16() {
    ColumnMetadata testCol =
        ColumnMetadataBuilder.newBuilder()
            .logicalType("REAL")
            .physicalType("SB16")
            .nullable(true)
            .build();

    ParquetTypeGenerator.ParquetTypeInfo typeInfo =
        ParquetTypeGenerator.generateColumnParquetTypeInfo(testCol, 0);
    ParquetTypeInfoAssertionBuilder.newBuilder()
        .typeInfo(typeInfo)
        .expectedFieldName("testCol")
        .expectedFieldId(0)
        .expectedTypeLength(0)
        .expectedPrimitiveTypeName(PrimitiveType.PrimitiveTypeName.DOUBLE)
        .expectedLogicalTypeAnnotation(null)
        .expectedRepetition(Type.Repetition.OPTIONAL)
        .expectedColMetaData(
            AbstractRowBuffer.ColumnLogicalType.REAL.getOrdinal()
                + ","
                + AbstractRowBuffer.ColumnPhysicalType.SB16.getOrdinal())
        .assertMatches();
  }

  /** Builder that helps to assert parquet type info */
  private static class ParquetTypeInfoAssertionBuilder {
    private String fieldName;
    private int fieldId;
    private PrimitiveType.PrimitiveTypeName primitiveTypeName;
    private LogicalTypeAnnotation logicalTypeAnnotation;
    private Type.Repetition repetition;
    private int typeLength;
    private String colMetadata;
    private ParquetTypeGenerator.ParquetTypeInfo typeInfo;

    static ParquetTypeInfoAssertionBuilder newBuilder() {
      ParquetTypeInfoAssertionBuilder builder = new ParquetTypeInfoAssertionBuilder();
      return builder;
    }

    ParquetTypeInfoAssertionBuilder typeInfo(ParquetTypeGenerator.ParquetTypeInfo typeInfo) {
      this.typeInfo = typeInfo;
      return this;
    }

    ParquetTypeInfoAssertionBuilder expectedFieldName(String fieldName) {
      this.fieldName = fieldName;
      return this;
    }

    ParquetTypeInfoAssertionBuilder expectedFieldId(int fieldId) {
      this.fieldId = fieldId;
      return this;
    }

    ParquetTypeInfoAssertionBuilder expectedPrimitiveTypeName(
        PrimitiveType.PrimitiveTypeName primitiveTypeName) {
      this.primitiveTypeName = primitiveTypeName;
      return this;
    }

    ParquetTypeInfoAssertionBuilder expectedLogicalTypeAnnotation(
        LogicalTypeAnnotation logicalTypeAnnotation) {
      this.logicalTypeAnnotation = logicalTypeAnnotation;
      return this;
    }

    ParquetTypeInfoAssertionBuilder expectedRepetition(Type.Repetition repetition) {
      this.repetition = repetition;
      return this;
    }

    ParquetTypeInfoAssertionBuilder expectedTypeLength(int typeLength) {
      this.typeLength = typeLength;
      return this;
    }

    ParquetTypeInfoAssertionBuilder expectedColMetaData(String colMetaData) {
      this.colMetadata = colMetaData;
      return this;
    }

    void assertMatches() {
      Type type = typeInfo.getParquetType();
      Map<String, String> metadata = typeInfo.getMetadata();
      Assert.assertEquals(fieldName, type.getName());
      Assert.assertEquals(typeLength, type.asPrimitiveType().getTypeLength());
      Assert.assertEquals(fieldId, type.asPrimitiveType().getId().intValue());

      Assert.assertEquals(primitiveTypeName, type.asPrimitiveType().getPrimitiveTypeName());
      Assert.assertEquals(logicalTypeAnnotation, type.getLogicalTypeAnnotation());
      Assert.assertEquals(repetition, type.getRepetition());
      Assert.assertEquals(colMetadata, metadata.get(type.getId().toString()));
    }
  }
}
