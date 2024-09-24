/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import java.util.Map;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.Assert;
import org.junit.Test;

public class ParquetTypeGeneratorTest {
  private static final int COL_ORDINAL = 11;

  @Test
  public void buildFieldFixedSB1() {
    ColumnMetadata testCol =
        createColumnMetadataBuilder()
            .logicalType("FIXED")
            .physicalType("SB1")
            .nullable(true)
            .build();

    ParquetTypeInfo typeInfo = ParquetTypeGenerator.generateColumnParquetTypeInfo(testCol, 0);
    createParquetTypeInfoAssertionBuilder()
        .typeInfo(typeInfo)
        .expectedFieldName("TESTCOL")
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
        createColumnMetadataBuilder()
            .logicalType("FIXED")
            .physicalType("SB2")
            .nullable(false)
            .build();

    ParquetTypeInfo typeInfo = ParquetTypeGenerator.generateColumnParquetTypeInfo(testCol, 0);
    createParquetTypeInfoAssertionBuilder()
        .typeInfo(typeInfo)
        .expectedFieldName("TESTCOL")
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
        createColumnMetadataBuilder()
            .logicalType("FIXED")
            .physicalType("SB4")
            .nullable(true)
            .build();

    ParquetTypeInfo typeInfo = ParquetTypeGenerator.generateColumnParquetTypeInfo(testCol, 0);
    createParquetTypeInfoAssertionBuilder()
        .typeInfo(typeInfo)
        .expectedFieldName("TESTCOL")
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
        createColumnMetadataBuilder()
            .logicalType("FIXED")
            .physicalType("SB8")
            .nullable(true)
            .build();

    ParquetTypeInfo typeInfo = ParquetTypeGenerator.generateColumnParquetTypeInfo(testCol, 0);
    createParquetTypeInfoAssertionBuilder()
        .typeInfo(typeInfo)
        .expectedFieldName("TESTCOL")
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
        createColumnMetadataBuilder()
            .logicalType("FIXED")
            .physicalType("SB16")
            .nullable(true)
            .build();

    ParquetTypeInfo typeInfo = ParquetTypeGenerator.generateColumnParquetTypeInfo(testCol, 0);
    createParquetTypeInfoAssertionBuilder()
        .typeInfo(typeInfo)
        .expectedFieldName("TESTCOL")
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
        createColumnMetadataBuilder()
            .logicalType("VARIANT")
            .physicalType("LOB")
            .nullable(true)
            .build();

    ParquetTypeInfo typeInfo = ParquetTypeGenerator.generateColumnParquetTypeInfo(testCol, 0);
    createParquetTypeInfoAssertionBuilder()
        .typeInfo(typeInfo)
        .expectedFieldName("TESTCOL")
        .expectedTypeLength(0)
        .expectedPrimitiveTypeName(PrimitiveType.PrimitiveTypeName.BINARY)
        .expectedLogicalTypeAnnotation(LogicalTypeAnnotation.stringType())
        .expectedRepetition(Type.Repetition.OPTIONAL)
        .expectedColMetaData(
            AbstractRowBuffer.ColumnLogicalType.VARIANT.getOrdinal()
                + ","
                + AbstractRowBuffer.ColumnPhysicalType.LOB.getOrdinal())
        .assertMatches();

    Assert.assertEquals("1", typeInfo.getMetadata().get(COL_ORDINAL + ":obj_enc"));
  }

  @Test
  public void buildFieldTimestampNtzSB8() {
    ColumnMetadata testCol =
        createColumnMetadataBuilder()
            .logicalType("TIMESTAMP_NTZ")
            .physicalType("SB8")
            .nullable(true)
            .build();

    ParquetTypeInfo typeInfo = ParquetTypeGenerator.generateColumnParquetTypeInfo(testCol, 0);
    createParquetTypeInfoAssertionBuilder()
        .typeInfo(typeInfo)
        .expectedFieldName("TESTCOL")
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
        createColumnMetadataBuilder()
            .logicalType("TIMESTAMP_NTZ")
            .physicalType("SB16")
            .nullable(true)
            .build();

    ParquetTypeInfo typeInfo = ParquetTypeGenerator.generateColumnParquetTypeInfo(testCol, 0);
    createParquetTypeInfoAssertionBuilder()
        .typeInfo(typeInfo)
        .expectedFieldName("TESTCOL")
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
        createColumnMetadataBuilder()
            .logicalType("TIMESTAMP_TZ")
            .physicalType("SB8")
            .nullable(true)
            .build();

    ParquetTypeInfo typeInfo = ParquetTypeGenerator.generateColumnParquetTypeInfo(testCol, 0);
    createParquetTypeInfoAssertionBuilder()
        .typeInfo(typeInfo)
        .expectedFieldName("TESTCOL")
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
        createColumnMetadataBuilder()
            .logicalType("TIMESTAMP_TZ")
            .physicalType("SB16")
            .nullable(true)
            .build();

    ParquetTypeInfo typeInfo = ParquetTypeGenerator.generateColumnParquetTypeInfo(testCol, 0);
    createParquetTypeInfoAssertionBuilder()
        .typeInfo(typeInfo)
        .expectedFieldName("TESTCOL")
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
        createColumnMetadataBuilder()
            .logicalType("DATE")
            .physicalType("SB8")
            .nullable(true)
            .build();

    ParquetTypeInfo typeInfo = ParquetTypeGenerator.generateColumnParquetTypeInfo(testCol, 0);
    createParquetTypeInfoAssertionBuilder()
        .typeInfo(typeInfo)
        .expectedFieldName("TESTCOL")
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
        createColumnMetadataBuilder()
            .logicalType("TIME")
            .physicalType("SB4")
            .nullable(true)
            .build();

    ParquetTypeInfo typeInfo = ParquetTypeGenerator.generateColumnParquetTypeInfo(testCol, 0);
    createParquetTypeInfoAssertionBuilder()
        .typeInfo(typeInfo)
        .expectedFieldName("TESTCOL")
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
        createColumnMetadataBuilder()
            .logicalType("TIME")
            .physicalType("SB8")
            .nullable(true)
            .build();

    ParquetTypeInfo typeInfo = ParquetTypeGenerator.generateColumnParquetTypeInfo(testCol, 0);
    createParquetTypeInfoAssertionBuilder()
        .typeInfo(typeInfo)
        .expectedFieldName("TESTCOL")
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
        createColumnMetadataBuilder()
            .logicalType("BOOLEAN")
            .physicalType("BINARY")
            .nullable(true)
            .build();

    ParquetTypeInfo typeInfo = ParquetTypeGenerator.generateColumnParquetTypeInfo(testCol, 0);
    createParquetTypeInfoAssertionBuilder()
        .typeInfo(typeInfo)
        .expectedFieldName("TESTCOL")
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
        createColumnMetadataBuilder()
            .logicalType("REAL")
            .physicalType("SB16")
            .nullable(true)
            .build();

    ParquetTypeInfo typeInfo = ParquetTypeGenerator.generateColumnParquetTypeInfo(testCol, 0);
    createParquetTypeInfoAssertionBuilder()
        .typeInfo(typeInfo)
        .expectedFieldName("TESTCOL")
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

  @Test
  public void buildFieldIcebergBoolean() {
    ColumnMetadata testCol =
        createColumnMetadataBuilder()
            .logicalType("")
            .sourceIcebergDataType("\"boolean\"")
            .nullable(true)
            .build();

    ParquetTypeInfo typeInfo = ParquetTypeGenerator.generateColumnParquetTypeInfo(testCol, 0);
    createParquetTypeInfoAssertionBuilder()
        .typeInfo(typeInfo)
        .expectedFieldName("TESTCOL")
        .expectedTypeLength(0)
        .expectedPrimitiveTypeName(PrimitiveType.PrimitiveTypeName.BOOLEAN)
        .expectedLogicalTypeAnnotation(null)
        .expectedRepetition(Type.Repetition.OPTIONAL)
        .expectedColMetaData(null)
        .assertMatches();
  }

  @Test
  public void buildFieldIcebergInt() {
    ColumnMetadata testCol =
        createColumnMetadataBuilder()
            .logicalType("")
            .sourceIcebergDataType("\"int\"")
            .nullable(true)
            .build();

    ParquetTypeInfo typeInfo = ParquetTypeGenerator.generateColumnParquetTypeInfo(testCol, 0);
    createParquetTypeInfoAssertionBuilder()
        .typeInfo(typeInfo)
        .expectedFieldName("TESTCOL")
        .expectedTypeLength(0)
        .expectedPrimitiveTypeName(PrimitiveType.PrimitiveTypeName.INT32)
        .expectedLogicalTypeAnnotation(null)
        .expectedRepetition(Type.Repetition.OPTIONAL)
        .expectedColMetaData(null)
        .assertMatches();
  }

  @Test
  public void buildFieldIcebergLong() {
    ColumnMetadata testCol =
        createColumnMetadataBuilder()
            .logicalType("")
            .sourceIcebergDataType("\"long\"")
            .nullable(true)
            .build();

    ParquetTypeInfo typeInfo = ParquetTypeGenerator.generateColumnParquetTypeInfo(testCol, 0);
    createParquetTypeInfoAssertionBuilder()
        .typeInfo(typeInfo)
        .expectedFieldName("TESTCOL")
        .expectedTypeLength(0)
        .expectedPrimitiveTypeName(PrimitiveType.PrimitiveTypeName.INT64)
        .expectedLogicalTypeAnnotation(null)
        .expectedRepetition(Type.Repetition.OPTIONAL)
        .expectedColMetaData(null)
        .assertMatches();
  }

  @Test
  public void buildFieldIcebergFloat() {
    ColumnMetadata testCol =
        createColumnMetadataBuilder()
            .logicalType("")
            .sourceIcebergDataType("\"float\"")
            .nullable(true)
            .build();

    ParquetTypeInfo typeInfo = ParquetTypeGenerator.generateColumnParquetTypeInfo(testCol, 0);
    createParquetTypeInfoAssertionBuilder()
        .typeInfo(typeInfo)
        .expectedFieldName("TESTCOL")
        .expectedTypeLength(0)
        .expectedPrimitiveTypeName(PrimitiveType.PrimitiveTypeName.FLOAT)
        .expectedLogicalTypeAnnotation(null)
        .expectedRepetition(Type.Repetition.OPTIONAL)
        .expectedColMetaData(null)
        .assertMatches();
  }

  @Test
  public void buildFieldIcebergDouble() {
    ColumnMetadata testCol =
        createColumnMetadataBuilder()
            .logicalType("")
            .sourceIcebergDataType("\"double\"")
            .nullable(true)
            .build();

    ParquetTypeInfo typeInfo = ParquetTypeGenerator.generateColumnParquetTypeInfo(testCol, 0);
    createParquetTypeInfoAssertionBuilder()
        .typeInfo(typeInfo)
        .expectedFieldName("TESTCOL")
        .expectedTypeLength(0)
        .expectedPrimitiveTypeName(PrimitiveType.PrimitiveTypeName.DOUBLE)
        .expectedLogicalTypeAnnotation(null)
        .expectedRepetition(Type.Repetition.OPTIONAL)
        .expectedColMetaData(null)
        .assertMatches();
  }

  @Test
  public void buildFieldIcebergDecimal() {
    ColumnMetadata testCol =
        createColumnMetadataBuilder()
            .logicalType("")
            .sourceIcebergDataType("\"decimal(9, 2)\"")
            .nullable(true)
            .build();

    ParquetTypeInfo typeInfo = ParquetTypeGenerator.generateColumnParquetTypeInfo(testCol, 0);
    createParquetTypeInfoAssertionBuilder()
        .typeInfo(typeInfo)
        .expectedFieldName("TESTCOL")
        .expectedTypeLength(0)
        .expectedPrimitiveTypeName(PrimitiveType.PrimitiveTypeName.INT32)
        .expectedLogicalTypeAnnotation(LogicalTypeAnnotation.decimalType(2, 9))
        .expectedRepetition(Type.Repetition.OPTIONAL)
        .expectedColMetaData(null)
        .assertMatches();

    testCol =
        createColumnMetadataBuilder()
            .logicalType("")
            .sourceIcebergDataType("\"decimal(10, 4)\"")
            .nullable(true)
            .build();

    typeInfo = ParquetTypeGenerator.generateColumnParquetTypeInfo(testCol, 0);
    createParquetTypeInfoAssertionBuilder()
        .typeInfo(typeInfo)
        .expectedFieldName("TESTCOL")
        .expectedTypeLength(0)
        .expectedPrimitiveTypeName(PrimitiveType.PrimitiveTypeName.INT64)
        .expectedLogicalTypeAnnotation(LogicalTypeAnnotation.decimalType(4, 10))
        .expectedRepetition(Type.Repetition.OPTIONAL)
        .expectedColMetaData(null)
        .assertMatches();

    testCol =
        createColumnMetadataBuilder()
            .logicalType("")
            .sourceIcebergDataType("\"decimal(19, 1)\"")
            .nullable(true)
            .build();

    typeInfo = ParquetTypeGenerator.generateColumnParquetTypeInfo(testCol, 0);
    createParquetTypeInfoAssertionBuilder()
        .typeInfo(typeInfo)
        .expectedFieldName("TESTCOL")
        .expectedTypeLength(9)
        .expectedPrimitiveTypeName(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
        .expectedLogicalTypeAnnotation(LogicalTypeAnnotation.decimalType(1, 19))
        .expectedRepetition(Type.Repetition.OPTIONAL)
        .expectedColMetaData(null)
        .assertMatches();
  }

  @Test
  public void buildFieldIcebergDate() {
    ColumnMetadata testCol =
        createColumnMetadataBuilder()
            .logicalType("")
            .sourceIcebergDataType("\"date\"")
            .nullable(true)
            .build();

    ParquetTypeInfo typeInfo = ParquetTypeGenerator.generateColumnParquetTypeInfo(testCol, 0);
    createParquetTypeInfoAssertionBuilder()
        .typeInfo(typeInfo)
        .expectedFieldName("TESTCOL")
        .expectedTypeLength(0)
        .expectedPrimitiveTypeName(PrimitiveType.PrimitiveTypeName.INT32)
        .expectedLogicalTypeAnnotation(LogicalTypeAnnotation.dateType())
        .expectedRepetition(Type.Repetition.OPTIONAL)
        .expectedColMetaData(null)
        .assertMatches();
  }

  @Test
  public void buildFieldIcebergTime() {
    ColumnMetadata testCol =
        createColumnMetadataBuilder()
            .logicalType("")
            .sourceIcebergDataType("\"time\"")
            .nullable(true)
            .build();

    ParquetTypeInfo typeInfo = ParquetTypeGenerator.generateColumnParquetTypeInfo(testCol, 0);
    createParquetTypeInfoAssertionBuilder()
        .typeInfo(typeInfo)
        .expectedFieldName("TESTCOL")
        .expectedTypeLength(0)
        .expectedPrimitiveTypeName(PrimitiveType.PrimitiveTypeName.INT64)
        .expectedLogicalTypeAnnotation(
            LogicalTypeAnnotation.timeType(false, LogicalTypeAnnotation.TimeUnit.MICROS))
        .expectedRepetition(Type.Repetition.OPTIONAL)
        .expectedColMetaData(null)
        .assertMatches();
  }

  @Test
  public void buildFieldIcebergTimeStamp() {
    ColumnMetadata testCol =
        createColumnMetadataBuilder()
            .logicalType("")
            .sourceIcebergDataType("\"timestamp\"")
            .nullable(true)
            .build();

    ParquetTypeInfo typeInfo = ParquetTypeGenerator.generateColumnParquetTypeInfo(testCol, 0);
    createParquetTypeInfoAssertionBuilder()
        .typeInfo(typeInfo)
        .expectedFieldName("TESTCOL")
        .expectedTypeLength(0)
        .expectedPrimitiveTypeName(PrimitiveType.PrimitiveTypeName.INT64)
        .expectedLogicalTypeAnnotation(
            LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MICROS))
        .expectedRepetition(Type.Repetition.OPTIONAL)
        .expectedColMetaData(null)
        .assertMatches();
  }

  @Test
  public void buildFieldIcebergTimeStampTZ() {
    ColumnMetadata testCol =
        createColumnMetadataBuilder()
            .logicalType("")
            .sourceIcebergDataType("\"timestamptz\"")
            .nullable(true)
            .build();

    ParquetTypeInfo typeInfo = ParquetTypeGenerator.generateColumnParquetTypeInfo(testCol, 0);
    createParquetTypeInfoAssertionBuilder()
        .typeInfo(typeInfo)
        .expectedFieldName("TESTCOL")
        .expectedTypeLength(0)
        .expectedPrimitiveTypeName(PrimitiveType.PrimitiveTypeName.INT64)
        .expectedLogicalTypeAnnotation(
            LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS))
        .expectedRepetition(Type.Repetition.OPTIONAL)
        .expectedColMetaData(null)
        .assertMatches();
  }

  @Test
  public void buildFieldIcebergString() {
    ColumnMetadata testCol =
        createColumnMetadataBuilder()
            .logicalType("")
            .sourceIcebergDataType("\"string\"")
            .nullable(true)
            .build();

    ParquetTypeInfo typeInfo = ParquetTypeGenerator.generateColumnParquetTypeInfo(testCol, 0);
    createParquetTypeInfoAssertionBuilder()
        .typeInfo(typeInfo)
        .expectedFieldName("TESTCOL")
        .expectedTypeLength(0)
        .expectedPrimitiveTypeName(PrimitiveType.PrimitiveTypeName.BINARY)
        .expectedLogicalTypeAnnotation(LogicalTypeAnnotation.stringType())
        .expectedRepetition(Type.Repetition.OPTIONAL)
        .expectedColMetaData(null)
        .assertMatches();
  }

  @Test
  public void buildFieldIcebergFixed() {
    ColumnMetadata testCol =
        createColumnMetadataBuilder()
            .logicalType("")
            .sourceIcebergDataType("\"fixed[16]\"")
            .nullable(true)
            .build();

    ParquetTypeInfo typeInfo = ParquetTypeGenerator.generateColumnParquetTypeInfo(testCol, 0);
    createParquetTypeInfoAssertionBuilder()
        .typeInfo(typeInfo)
        .expectedFieldName("TESTCOL")
        .expectedTypeLength(16)
        .expectedPrimitiveTypeName(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
        .expectedLogicalTypeAnnotation(null)
        .expectedRepetition(Type.Repetition.OPTIONAL)
        .expectedColMetaData(null)
        .assertMatches();
  }

  @Test
  public void buildFieldIcebergBinary() {
    ColumnMetadata testCol =
        createColumnMetadataBuilder()
            .logicalType("")
            .sourceIcebergDataType("\"binary\"")
            .nullable(true)
            .build();

    ParquetTypeInfo typeInfo = ParquetTypeGenerator.generateColumnParquetTypeInfo(testCol, 0);
    createParquetTypeInfoAssertionBuilder()
        .typeInfo(typeInfo)
        .expectedFieldName("TESTCOL")
        .expectedTypeLength(0)
        .expectedPrimitiveTypeName(PrimitiveType.PrimitiveTypeName.BINARY)
        .expectedLogicalTypeAnnotation(null)
        .expectedRepetition(Type.Repetition.OPTIONAL)
        .expectedColMetaData(null)
        .assertMatches();
  }

  @Test
  public void buildFieldIcebergStruct() {
    ColumnMetadata testCol =
        createColumnMetadataBuilder()
            .logicalType("")
            .sourceIcebergDataType(
                "{"
                    + "  \"type\": \"struct\","
                    + "  \"fields\":"
                    + "  ["
                    + "    {"
                    + "      \"id\": 1,"
                    + "      \"name\": \"id\","
                    + "      \"required\": true,"
                    + "      \"type\": \"string\""
                    + "    },"
                    + "    {"
                    + "      \"id\": 2,"
                    + "      \"name\": \"age\","
                    + "      \"required\": false,"
                    + "      \"type\": \"int\""
                    + "    }"
                    + "  ]"
                    + "}")
            .nullable(true)
            .build();

    ParquetTypeInfo typeInfo = ParquetTypeGenerator.generateColumnParquetTypeInfo(testCol, 0);
    createParquetTypeInfoAssertionBuilder(false)
        .typeInfo(typeInfo)
        .expectedFieldName("TESTCOL")
        .expectedLogicalTypeAnnotation(null)
        .expectedRepetition(Type.Repetition.OPTIONAL)
        .expectedColMetaData(null)
        .expectedFieldCount(2)
        .assertMatches();

    ParquetTypeInfo firstFieldTypeInfo =
        new ParquetTypeInfo(typeInfo.getParquetType().asGroupType().getType(0), null);
    createParquetTypeInfoAssertionBuilder(false)
        .typeInfo(firstFieldTypeInfo)
        .expectedFieldName("id")
        .expectedTypeLength(0)
        .expectedPrimitiveTypeName(PrimitiveType.PrimitiveTypeName.BINARY)
        .expectedLogicalTypeAnnotation(LogicalTypeAnnotation.stringType())
        .expectedRepetition(Type.Repetition.REQUIRED)
        .expectedColMetaData(null)
        .assertMatches();

    ParquetTypeInfo secondFieldTypeInfo =
        new ParquetTypeInfo(typeInfo.getParquetType().asGroupType().getType(1), null);
    createParquetTypeInfoAssertionBuilder(false)
        .typeInfo(secondFieldTypeInfo)
        .expectedFieldName("age")
        .expectedTypeLength(0)
        .expectedPrimitiveTypeName(PrimitiveType.PrimitiveTypeName.INT32)
        .expectedLogicalTypeAnnotation(null)
        .expectedRepetition(Type.Repetition.OPTIONAL)
        .expectedColMetaData(null)
        .assertMatches();
  }

  @Test
  public void buildFieldIcebergList() {
    ColumnMetadata testCol =
        createColumnMetadataBuilder()
            .logicalType("")
            .sourceIcebergDataType(
                "{"
                    + "  \"type\": \"list\","
                    + "  \"element\": \"int\","
                    + "  \"element-required\": true,"
                    + "  \"element-id\": 1"
                    + "}")
            .nullable(true)
            .build();

    ParquetTypeInfo typeInfo = ParquetTypeGenerator.generateColumnParquetTypeInfo(testCol, 0);
    createParquetTypeInfoAssertionBuilder(false)
        .typeInfo(typeInfo)
        .expectedFieldName("TESTCOL")
        .expectedLogicalTypeAnnotation(LogicalTypeAnnotation.listType())
        .expectedRepetition(Type.Repetition.OPTIONAL)
        .expectedColMetaData(null)
        .expectedFieldCount(1)
        .assertMatches();

    ParquetTypeInfo elementTypeInfo =
        new ParquetTypeInfo(typeInfo.getParquetType().asGroupType().getType(0), null);
    createParquetTypeInfoAssertionBuilder(false)
        .typeInfo(elementTypeInfo)
        .expectedFieldName("list")
        .expectedLogicalTypeAnnotation(null)
        .expectedRepetition(Type.Repetition.REPEATED)
        .expectedColMetaData(null)
        .expectedFieldCount(1)
        .assertMatches();

    ParquetTypeInfo elementFieldTypeInfo =
        new ParquetTypeInfo(elementTypeInfo.getParquetType().asGroupType().getType(0), null);
    createParquetTypeInfoAssertionBuilder(false)
        .typeInfo(elementFieldTypeInfo)
        .expectedFieldName("element")
        .expectedTypeLength(0)
        .expectedPrimitiveTypeName(PrimitiveType.PrimitiveTypeName.INT32)
        .expectedLogicalTypeAnnotation(null)
        .expectedRepetition(Type.Repetition.REQUIRED)
        .expectedColMetaData(null)
        .assertMatches();
  }

  @Test
  public void buildFieldIcebergMap() {
    ColumnMetadata testCol =
        createColumnMetadataBuilder()
            .logicalType("")
            .sourceIcebergDataType(
                "{"
                    + "  \"type\": \"map\","
                    + "  \"key\": \"string\","
                    + "  \"value\": \"int\","
                    + "  \"key-required\": true,"
                    + "  \"value-required\": false,"
                    + "  \"key-id\": 1,"
                    + "  \"value-id\": 2"
                    + "}")
            .nullable(true)
            .build();

    ParquetTypeInfo typeInfo = ParquetTypeGenerator.generateColumnParquetTypeInfo(testCol, 0);
    createParquetTypeInfoAssertionBuilder(false)
        .typeInfo(typeInfo)
        .expectedFieldName("TESTCOL")
        .expectedLogicalTypeAnnotation(LogicalTypeAnnotation.mapType())
        .expectedRepetition(Type.Repetition.OPTIONAL)
        .expectedColMetaData(null)
        .expectedFieldCount(1)
        .assertMatches();

    ParquetTypeInfo mapTypeInfo =
        new ParquetTypeInfo(typeInfo.getParquetType().asGroupType().getType(0), null);
    createParquetTypeInfoAssertionBuilder(false)
        .typeInfo(mapTypeInfo)
        .expectedFieldName("key_value")
        .expectedLogicalTypeAnnotation(null)
        .expectedRepetition(Type.Repetition.REPEATED)
        .expectedColMetaData(null)
        .expectedFieldCount(2)
        .assertMatches();

    ParquetTypeInfo keyTypeInfo =
        new ParquetTypeInfo(mapTypeInfo.getParquetType().asGroupType().getType(0), null);
    createParquetTypeInfoAssertionBuilder(false)
        .typeInfo(keyTypeInfo)
        .expectedFieldName("key")
        .expectedTypeLength(0)
        .expectedPrimitiveTypeName(PrimitiveType.PrimitiveTypeName.BINARY)
        .expectedLogicalTypeAnnotation(LogicalTypeAnnotation.stringType())
        .expectedRepetition(Type.Repetition.REQUIRED)
        .expectedColMetaData(null)
        .assertMatches();

    ParquetTypeInfo valueTypeInfo =
        new ParquetTypeInfo(mapTypeInfo.getParquetType().asGroupType().getType(1), null);
    createParquetTypeInfoAssertionBuilder(false)
        .typeInfo(valueTypeInfo)
        .expectedFieldName("value")
        .expectedTypeLength(0)
        .expectedPrimitiveTypeName(PrimitiveType.PrimitiveTypeName.INT32)
        .expectedLogicalTypeAnnotation(null)
        .expectedRepetition(Type.Repetition.OPTIONAL)
        .expectedColMetaData(null)
        .assertMatches();
  }

  @Test
  public void buildFieldIcebergNestedStructuredDataType() {
    ColumnMetadata testCol =
        createColumnMetadataBuilder()
            .logicalType("")
            .sourceIcebergDataType(
                "{"
                    + "  \"type\": \"map\","
                    + "  \"key\": \"string\","
                    + "  \"value\": {"
                    + "    \"type\": \"list\","
                    + "    \"element\": {"
                    + "      \"type\": \"struct\","
                    + "      \"fields\":"
                    + "      ["
                    + "        {"
                    + "          \"id\": 1,"
                    + "          \"name\": \"id\","
                    + "          \"required\": true,"
                    + "          \"type\": \"string\""
                    + "        },"
                    + "        {"
                    + "          \"id\": 2,"
                    + "          \"name\": \"age\","
                    + "          \"required\": false,"
                    + "          \"type\": \"int\""
                    + "        }"
                    + "      ]"
                    + "    },"
                    + "    \"element-required\": true,"
                    + "    \"element-id\": 1"
                    + "  },"
                    + "  \"key-required\": true,"
                    + "  \"value-required\": false,"
                    + "  \"key-id\": 1,"
                    + "  \"value-id\": 2"
                    + "}")
            .nullable(true)
            .build();

    ParquetTypeInfo typeInfo = ParquetTypeGenerator.generateColumnParquetTypeInfo(testCol, 0);

    createParquetTypeInfoAssertionBuilder(false)
        .typeInfo(typeInfo)
        .expectedFieldName("TESTCOL")
        .expectedLogicalTypeAnnotation(LogicalTypeAnnotation.mapType())
        .expectedRepetition(Type.Repetition.OPTIONAL)
        .expectedColMetaData(null)
        .expectedFieldCount(1)
        .assertMatches();

    ParquetTypeInfo mapTypeInfo =
        new ParquetTypeInfo(typeInfo.getParquetType().asGroupType().getType(0), null);
    createParquetTypeInfoAssertionBuilder(false)
        .typeInfo(mapTypeInfo)
        .expectedFieldName("key_value")
        .expectedLogicalTypeAnnotation(null)
        .expectedRepetition(Type.Repetition.REPEATED)
        .expectedColMetaData(null)
        .expectedFieldCount(2)
        .assertMatches();

    ParquetTypeInfo keyTypeInfo =
        new ParquetTypeInfo(mapTypeInfo.getParquetType().asGroupType().getType(0), null);
    createParquetTypeInfoAssertionBuilder(false)
        .typeInfo(keyTypeInfo)
        .expectedFieldName("key")
        .expectedTypeLength(0)
        .expectedPrimitiveTypeName(PrimitiveType.PrimitiveTypeName.BINARY)
        .expectedLogicalTypeAnnotation(LogicalTypeAnnotation.stringType())
        .expectedRepetition(Type.Repetition.REQUIRED)
        .expectedColMetaData(null)
        .assertMatches();

    ParquetTypeInfo valueTypeInfo =
        new ParquetTypeInfo(mapTypeInfo.getParquetType().asGroupType().getType(1), null);
    createParquetTypeInfoAssertionBuilder(false)
        .typeInfo(valueTypeInfo)
        .expectedFieldName("value")
        .expectedLogicalTypeAnnotation(LogicalTypeAnnotation.listType())
        .expectedRepetition(Type.Repetition.OPTIONAL)
        .expectedColMetaData(null)
        .expectedFieldCount(1)
        .assertMatches();

    ParquetTypeInfo elementTypeInfo =
        new ParquetTypeInfo(valueTypeInfo.getParquetType().asGroupType().getType(0), null);
    createParquetTypeInfoAssertionBuilder(false)
        .typeInfo(elementTypeInfo)
        .expectedFieldName("list")
        .expectedLogicalTypeAnnotation(null)
        .expectedRepetition(Type.Repetition.REPEATED)
        .expectedColMetaData(null)
        .expectedFieldCount(1)
        .assertMatches();

    ParquetTypeInfo elementFieldTypeInfo =
        new ParquetTypeInfo(elementTypeInfo.getParquetType().asGroupType().getType(0), null);
    createParquetTypeInfoAssertionBuilder(false)
        .typeInfo(elementFieldTypeInfo)
        .expectedFieldName("element")
        .expectedLogicalTypeAnnotation(null)
        .expectedRepetition(Type.Repetition.REQUIRED)
        .expectedColMetaData(null)
        .expectedFieldCount(2)
        .assertMatches();

    ParquetTypeInfo firstFieldTypeInfo =
        new ParquetTypeInfo(elementFieldTypeInfo.getParquetType().asGroupType().getType(0), null);
    createParquetTypeInfoAssertionBuilder(false)
        .typeInfo(firstFieldTypeInfo)
        .expectedFieldName("id")
        .expectedTypeLength(0)
        .expectedPrimitiveTypeName(PrimitiveType.PrimitiveTypeName.BINARY)
        .expectedLogicalTypeAnnotation(LogicalTypeAnnotation.stringType())
        .expectedRepetition(Type.Repetition.REQUIRED)
        .expectedColMetaData(null)
        .assertMatches();

    ParquetTypeInfo secondFieldTypeInfo =
        new ParquetTypeInfo(elementFieldTypeInfo.getParquetType().asGroupType().getType(1), null);
    createParquetTypeInfoAssertionBuilder(false)
        .typeInfo(secondFieldTypeInfo)
        .expectedFieldName("age")
        .expectedTypeLength(0)
        .expectedPrimitiveTypeName(PrimitiveType.PrimitiveTypeName.INT32)
        .expectedLogicalTypeAnnotation(null)
        .expectedRepetition(Type.Repetition.OPTIONAL)
        .expectedColMetaData(null)
        .assertMatches();
  }

  /** Builder that helps to assert parquet type info */
  private static class ParquetTypeInfoAssertionBuilder {
    private String fieldName;
    private Integer fieldId;
    private PrimitiveType.PrimitiveTypeName primitiveTypeName;
    private LogicalTypeAnnotation logicalTypeAnnotation;
    private Type.Repetition repetition;
    private Integer typeLength;
    private String colMetadata;
    private ParquetTypeInfo typeInfo;
    private Integer fieldCount;

    static ParquetTypeInfoAssertionBuilder newBuilder() {
      ParquetTypeInfoAssertionBuilder builder = new ParquetTypeInfoAssertionBuilder();
      return builder;
    }

    ParquetTypeInfoAssertionBuilder typeInfo(ParquetTypeInfo typeInfo) {
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

    ParquetTypeInfoAssertionBuilder expectedFieldCount(int fieldCount) {
      this.fieldCount = fieldCount;
      return this;
    }

    void assertMatches() {
      Type type = typeInfo.getParquetType();
      Map<String, String> metadata = typeInfo.getMetadata();
      Assert.assertEquals(fieldName, type.getName());
      if (typeLength != null) {
        Assert.assertEquals(typeLength.intValue(), type.asPrimitiveType().getTypeLength());
      }
      if (fieldId != null) {
        Assert.assertEquals(fieldId.intValue(), type.asPrimitiveType().getId().intValue());
      }
      if (primitiveTypeName != null) {
        Assert.assertEquals(primitiveTypeName, type.asPrimitiveType().getPrimitiveTypeName());
      }
      Assert.assertEquals(logicalTypeAnnotation, type.getLogicalTypeAnnotation());
      Assert.assertEquals(repetition, type.getRepetition());
      if (metadata != null) {
        Assert.assertEquals(colMetadata, metadata.get(type.getId().toString()));
      }
      if (fieldCount != null) {
        Assert.assertEquals(fieldCount.intValue(), type.asGroupType().getFieldCount());
      }
    }
  }

  private static ColumnMetadataBuilder createColumnMetadataBuilder() {
    return ColumnMetadataBuilder.newBuilder().ordinal(COL_ORDINAL);
  }

  private static ParquetTypeInfoAssertionBuilder createParquetTypeInfoAssertionBuilder() {
    return createParquetTypeInfoAssertionBuilder(true);
  }

  private static ParquetTypeInfoAssertionBuilder createParquetTypeInfoAssertionBuilder(
      boolean expectedFieldId) {
    return expectedFieldId
        ? ParquetTypeInfoAssertionBuilder.newBuilder().expectedFieldId(COL_ORDINAL)
        : ParquetTypeInfoAssertionBuilder.newBuilder();
  }
}
