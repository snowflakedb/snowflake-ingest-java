/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import static java.time.ZoneOffset.UTC;
import static net.snowflake.ingest.streaming.internal.ParquetBufferValue.BYTE_ARRAY_LENGTH_ENCODING_BYTE_LEN;
import static net.snowflake.ingest.streaming.internal.ParquetBufferValue.DEFINITION_LEVEL_ENCODING_BYTE_LEN;

import java.math.BigDecimal;
import java.math.BigInteger;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;
import org.apache.parquet.schema.Types;
import org.junit.Test;

public class IcebergParquetValueParserTest {

  @Test
  public void parseValueBoolean() {
    Type type =
        Types.primitive(PrimitiveTypeName.BOOLEAN, Repetition.OPTIONAL).named("BOOLEAN_COL");

    RowBufferStats rowBufferStats = new RowBufferStats("BOOLEAN_COL");
    ParquetBufferValue pv =
        IcebergParquetValueParser.parseColumnValueToParquet(
            true, type, rowBufferStats, UTC, 0, false);
    ParquetValueParserAssertionBuilder.newBuilder()
        .parquetBufferValue(pv)
        .rowBufferStats(rowBufferStats)
        .expectedValueClass(Boolean.class)
        .expectedParsedValue(true)
        .expectedSize(ParquetBufferValue.BIT_ENCODING_BYTE_LEN + DEFINITION_LEVEL_ENCODING_BYTE_LEN)
        .expectedMinMax(BigInteger.valueOf(1))
        .assertMatches();
  }

  @Test
  public void parseValueInt() {
    Type type = Types.primitive(PrimitiveTypeName.INT32, Repetition.OPTIONAL).named("INT_COL");

    RowBufferStats rowBufferStats = new RowBufferStats("INT_COL");
    ParquetBufferValue pv =
        IcebergParquetValueParser.parseColumnValueToParquet(
            Integer.MAX_VALUE, type, rowBufferStats, UTC, 0, false);
    ParquetValueParserAssertionBuilder.newBuilder()
        .parquetBufferValue(pv)
        .rowBufferStats(rowBufferStats)
        .expectedValueClass(Integer.class)
        .expectedParsedValue(Integer.MAX_VALUE)
        .expectedSize(4.0f + DEFINITION_LEVEL_ENCODING_BYTE_LEN)
        .expectedMinMax(BigInteger.valueOf(Integer.MAX_VALUE))
        .assertMatches();
  }

  @Test
  public void parseValueDecimalToInt() {
    Type type =
        Types.primitive(PrimitiveTypeName.INT32, Repetition.OPTIONAL)
            .as(LogicalTypeAnnotation.decimalType(4, 9))
            .named("DECIMAL_COL");

    RowBufferStats rowBufferStats = new RowBufferStats("DECIMAL_COL");
    ParquetBufferValue pv =
        IcebergParquetValueParser.parseColumnValueToParquet(
            new BigDecimal("12345.6789"), type, rowBufferStats, UTC, 0, false);
    ParquetValueParserAssertionBuilder.newBuilder()
        .parquetBufferValue(pv)
        .rowBufferStats(rowBufferStats)
        .expectedValueClass(Integer.class)
        .expectedParsedValue(123456789)
        .expectedSize(4.0f + DEFINITION_LEVEL_ENCODING_BYTE_LEN)
        .expectedMinMax(BigInteger.valueOf(123456789))
        .assertMatches();
  }

  @Test
  public void parseValueDateToInt() {
    Type type =
        Types.primitive(PrimitiveTypeName.INT32, Repetition.OPTIONAL)
            .as(LogicalTypeAnnotation.dateType())
            .named("DATE_COL");

    RowBufferStats rowBufferStats = new RowBufferStats("DATE_COL");
    ParquetBufferValue pv =
        IcebergParquetValueParser.parseColumnValueToParquet(
            "2024-01-01", type, rowBufferStats, UTC, 0, false);
    ParquetValueParserAssertionBuilder.newBuilder()
        .parquetBufferValue(pv)
        .rowBufferStats(rowBufferStats)
        .expectedValueClass(Integer.class)
        .expectedParsedValue(19723)
        .expectedSize(4.0f + DEFINITION_LEVEL_ENCODING_BYTE_LEN)
        .expectedMinMax(BigInteger.valueOf(19723))
        .assertMatches();
  }

  @Test
  public void parseValueLong() {
    Type type = Types.primitive(PrimitiveTypeName.INT64, Repetition.OPTIONAL).named("LONG_COL");

    RowBufferStats rowBufferStats = new RowBufferStats("LONG_COL");
    ParquetBufferValue pv =
        IcebergParquetValueParser.parseColumnValueToParquet(
            Long.MAX_VALUE, type, rowBufferStats, UTC, 0, false);
    ParquetValueParserAssertionBuilder.newBuilder()
        .parquetBufferValue(pv)
        .rowBufferStats(rowBufferStats)
        .expectedValueClass(Long.class)
        .expectedParsedValue(Long.MAX_VALUE)
        .expectedSize(8.0f + DEFINITION_LEVEL_ENCODING_BYTE_LEN)
        .expectedMinMax(BigInteger.valueOf(Long.MAX_VALUE))
        .assertMatches();
  }

  @Test
  public void parseValueDecimalToLong() {
    Type type =
        Types.primitive(PrimitiveTypeName.INT64, Repetition.OPTIONAL)
            .as(LogicalTypeAnnotation.decimalType(9, 18))
            .named("DECIMAL_COL");

    RowBufferStats rowBufferStats = new RowBufferStats("DECIMAL_COL");
    ParquetBufferValue pv =
        IcebergParquetValueParser.parseColumnValueToParquet(
            new BigDecimal("123456789.123456789"), type, rowBufferStats, UTC, 0, false);
    ParquetValueParserAssertionBuilder.newBuilder()
        .parquetBufferValue(pv)
        .rowBufferStats(rowBufferStats)
        .expectedValueClass(Long.class)
        .expectedParsedValue(123456789123456789L)
        .expectedSize(8.0f + DEFINITION_LEVEL_ENCODING_BYTE_LEN)
        .expectedMinMax(BigInteger.valueOf(123456789123456789L))
        .assertMatches();
  }

  @Test
  public void parseValueTimeToLong() {
    Type type =
        Types.primitive(PrimitiveTypeName.INT64, Repetition.OPTIONAL)
            .as(LogicalTypeAnnotation.timeType(false, LogicalTypeAnnotation.TimeUnit.MICROS))
            .named("TIME_COL");

    RowBufferStats rowBufferStats = new RowBufferStats("TIME_COL");
    ParquetBufferValue pv =
        IcebergParquetValueParser.parseColumnValueToParquet(
            "12:34:56.789", type, rowBufferStats, UTC, 0, false);
    ParquetValueParserAssertionBuilder.newBuilder()
        .parquetBufferValue(pv)
        .rowBufferStats(rowBufferStats)
        .expectedValueClass(Long.class)
        .expectedParsedValue(45296789000L)
        .expectedSize(8.0f + DEFINITION_LEVEL_ENCODING_BYTE_LEN)
        .expectedMinMax(BigInteger.valueOf(45296789000L))
        .assertMatches();
  }

  @Test
  public void parseValueTimestampToLong() {
    Type type =
        Types.primitive(PrimitiveTypeName.INT64, Repetition.OPTIONAL)
            .as(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MICROS))
            .named("TIMESTAMP_COL");

    RowBufferStats rowBufferStats = new RowBufferStats("TIMESTAMP_COL");
    ParquetBufferValue pv =
        IcebergParquetValueParser.parseColumnValueToParquet(
            "2024-01-01T12:34:56.789+08:00", type, rowBufferStats, UTC, 0, false);
    ParquetValueParserAssertionBuilder.newBuilder()
        .parquetBufferValue(pv)
        .rowBufferStats(rowBufferStats)
        .expectedValueClass(Long.class)
        .expectedParsedValue(1704112496789000L)
        .expectedSize(8.0f + DEFINITION_LEVEL_ENCODING_BYTE_LEN)
        .expectedMinMax(BigInteger.valueOf(1704112496789000L))
        .assertMatches();
  }

  @Test
  public void parseValueTimestampTZToLong() {
    Type type =
        Types.primitive(PrimitiveTypeName.INT64, Repetition.OPTIONAL)
            .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS))
            .named("TIMESTAMP_TZ_COL");

    RowBufferStats rowBufferStats = new RowBufferStats("TIMESTAMP_TZ_COL");
    ParquetBufferValue pv =
        IcebergParquetValueParser.parseColumnValueToParquet(
            "2024-01-01T12:34:56.789+08:00", type, rowBufferStats, UTC, 0, false);
    ParquetValueParserAssertionBuilder.newBuilder()
        .parquetBufferValue(pv)
        .rowBufferStats(rowBufferStats)
        .expectedValueClass(Long.class)
        .expectedParsedValue(1704083696789000L)
        .expectedSize(8.0f + DEFINITION_LEVEL_ENCODING_BYTE_LEN)
        .expectedMinMax(BigInteger.valueOf(1704083696789000L))
        .assertMatches();
  }

  @Test
  public void parseValueFloat() {
    Type type = Types.primitive(PrimitiveTypeName.FLOAT, Repetition.OPTIONAL).named("FLOAT_COL");

    RowBufferStats rowBufferStats = new RowBufferStats("FLOAT_COL");
    ParquetBufferValue pv =
        IcebergParquetValueParser.parseColumnValueToParquet(
            Float.MAX_VALUE, type, rowBufferStats, UTC, 0, false);
    ParquetValueParserAssertionBuilder.newBuilder()
        .parquetBufferValue(pv)
        .rowBufferStats(rowBufferStats)
        .expectedValueClass(Float.class)
        .expectedParsedValue(Float.MAX_VALUE)
        .expectedSize(4.0f + DEFINITION_LEVEL_ENCODING_BYTE_LEN)
        .expectedMinMax((double) Float.MAX_VALUE)
        .assertMatches();
  }

  @Test
  public void parseValueDouble() {
    Type type = Types.primitive(PrimitiveTypeName.DOUBLE, Repetition.OPTIONAL).named("DOUBLE_COL");

    RowBufferStats rowBufferStats = new RowBufferStats("DOUBLE_COL");
    ParquetBufferValue pv =
        IcebergParquetValueParser.parseColumnValueToParquet(
            Double.MAX_VALUE, type, rowBufferStats, UTC, 0, false);
    ParquetValueParserAssertionBuilder.newBuilder()
        .parquetBufferValue(pv)
        .rowBufferStats(rowBufferStats)
        .expectedValueClass(Double.class)
        .expectedParsedValue(Double.MAX_VALUE)
        .expectedSize(8.0f + DEFINITION_LEVEL_ENCODING_BYTE_LEN)
        .expectedMinMax(Double.MAX_VALUE)
        .assertMatches();
  }

  @Test
  public void parseValueBinary() {
    Type type = Types.primitive(PrimitiveTypeName.BINARY, Repetition.OPTIONAL).named("BINARY_COL");

    RowBufferStats rowBufferStats = new RowBufferStats("BINARY_COL");
    byte[] value = "snowflake_to_the_moon".getBytes();
    ParquetBufferValue pv =
        IcebergParquetValueParser.parseColumnValueToParquet(
            value, type, rowBufferStats, UTC, 0, false);
    ParquetValueParserAssertionBuilder.newBuilder()
        .parquetBufferValue(pv)
        .rowBufferStats(rowBufferStats)
        .expectedValueClass(byte[].class)
        .expectedParsedValue(value)
        .expectedSize(
            value.length + DEFINITION_LEVEL_ENCODING_BYTE_LEN + BYTE_ARRAY_LENGTH_ENCODING_BYTE_LEN)
        .expectedMinMax(value)
        .assertMatches();
  }

  @Test
  public void parseValueStringToBinary() {
    Type type =
        Types.primitive(PrimitiveTypeName.BINARY, Repetition.OPTIONAL)
            .as(LogicalTypeAnnotation.stringType())
            .named("BINARY_COL");

    RowBufferStats rowBufferStats = new RowBufferStats("BINARY_COL");
    String value = "snowflake_to_the_moon";
    ParquetBufferValue pv =
        IcebergParquetValueParser.parseColumnValueToParquet(
            value, type, rowBufferStats, UTC, 0, false);
    ParquetValueParserAssertionBuilder.newBuilder()
        .parquetBufferValue(pv)
        .rowBufferStats(rowBufferStats)
        .expectedValueClass(byte[].class)
        .expectedParsedValue(value.getBytes())
        .expectedSize(
            value.getBytes().length
                + DEFINITION_LEVEL_ENCODING_BYTE_LEN
                + BYTE_ARRAY_LENGTH_ENCODING_BYTE_LEN)
        .expectedMinMax(value.getBytes())
        .assertMatches();
  }

  @Test
  public void parseValueFixed() {
    Type type =
        Types.primitive(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, Repetition.OPTIONAL)
            .length(4)
            .named("FIXED_COL");

    RowBufferStats rowBufferStats = new RowBufferStats("FIXED_COL");
    byte[] value = "snow".getBytes();
    ParquetBufferValue pv =
        IcebergParquetValueParser.parseColumnValueToParquet(
            value, type, rowBufferStats, UTC, 0, false);
    ParquetValueParserAssertionBuilder.newBuilder()
        .parquetBufferValue(pv)
        .rowBufferStats(rowBufferStats)
        .expectedValueClass(byte[].class)
        .expectedParsedValue(value)
        .expectedSize(
            4.0f + DEFINITION_LEVEL_ENCODING_BYTE_LEN + BYTE_ARRAY_LENGTH_ENCODING_BYTE_LEN)
        .expectedMinMax(value)
        .assertMatches();
  }

  @Test
  public void parseValueDecimalToFixed() {
    Type type =
        Types.primitive(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, Repetition.OPTIONAL)
            .length(9)
            .as(LogicalTypeAnnotation.decimalType(10, 20))
            .named("FIXED_COL");

    RowBufferStats rowBufferStats = new RowBufferStats("FIXED_COL");
    BigDecimal value = new BigDecimal("1234567890.0123456789");
    ParquetBufferValue pv =
        IcebergParquetValueParser.parseColumnValueToParquet(
            value, type, rowBufferStats, UTC, 0, false);
    ParquetValueParserAssertionBuilder.newBuilder()
        .parquetBufferValue(pv)
        .rowBufferStats(rowBufferStats)
        .expectedValueClass(byte[].class)
        .expectedParsedValue(value.unscaledValue().toByteArray())
        .expectedSize(
            9.0f + DEFINITION_LEVEL_ENCODING_BYTE_LEN + BYTE_ARRAY_LENGTH_ENCODING_BYTE_LEN)
        .expectedMinMax(value.unscaledValue())
        .assertMatches();
  }
}
