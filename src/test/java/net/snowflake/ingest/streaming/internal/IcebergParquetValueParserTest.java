/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import static java.time.ZoneOffset.UTC;
import static net.snowflake.ingest.streaming.internal.ParquetBufferValue.BYTE_ARRAY_LENGTH_ENCODING_BYTE_LEN;
import static net.snowflake.ingest.streaming.internal.ParquetBufferValue.DEFINITION_LEVEL_ENCODING_BYTE_LEN;
import static net.snowflake.ingest.streaming.internal.ParquetBufferValue.REPETITION_LEVEL_ENCODING_BYTE_LEN;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import net.snowflake.ingest.utils.Pair;
import net.snowflake.ingest.utils.SFException;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;
import org.apache.parquet.schema.Types;
import org.junit.Assert;
import org.junit.Test;

public class IcebergParquetValueParserTest {

  static ObjectMapper objectMapper = new ObjectMapper();

  @Test
  public void parseValueBoolean() {
    Type type =
        Types.primitive(PrimitiveTypeName.BOOLEAN, Repetition.OPTIONAL).named("BOOLEAN_COL");

    ParquetBufferValue pv =
        IcebergParquetValueParser.parseColumnValueToParquet(true, type, UTC, 0, false);
    ParquetValueParserAssertionBuilder.newBuilder()
        .parquetBufferValue(pv)
        .expectedValueClass(Boolean.class)
        .expectedParsedValue(true)
        .expectedSize(ParquetBufferValue.BIT_ENCODING_BYTE_LEN + DEFINITION_LEVEL_ENCODING_BYTE_LEN)
        .expectedMinMax(BigInteger.valueOf(1))
        .assertMatches();
  }

  @Test
  public void parseValueInt() {
    Type type = Types.primitive(PrimitiveTypeName.INT32, Repetition.OPTIONAL).named("INT_COL");

    ParquetBufferValue pv =
        IcebergParquetValueParser.parseColumnValueToParquet(Integer.MAX_VALUE, type, UTC, 0, false);
    ParquetValueParserAssertionBuilder.newBuilder()
        .parquetBufferValue(pv)
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

    ParquetBufferValue pv =
        IcebergParquetValueParser.parseColumnValueToParquet(
            new BigDecimal("12345.6789"), type, UTC, 0, false);
    ParquetValueParserAssertionBuilder.newBuilder()
        .parquetBufferValue(pv)
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

    ParquetBufferValue pv =
        IcebergParquetValueParser.parseColumnValueToParquet("2024-01-01", type, UTC, 0, false);
    ParquetValueParserAssertionBuilder.newBuilder()
        .parquetBufferValue(pv)
        .expectedValueClass(Integer.class)
        .expectedParsedValue(19723)
        .expectedSize(4.0f + DEFINITION_LEVEL_ENCODING_BYTE_LEN)
        .expectedMinMax(BigInteger.valueOf(19723))
        .assertMatches();
  }

  @Test
  public void parseValueLong() {
    Type type = Types.primitive(PrimitiveTypeName.INT64, Repetition.OPTIONAL).named("LONG_COL");

    ParquetBufferValue pv =
        IcebergParquetValueParser.parseColumnValueToParquet(Long.MAX_VALUE, type, UTC, 0, false);
    ParquetValueParserAssertionBuilder.newBuilder()
        .parquetBufferValue(pv)
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

    ParquetBufferValue pv =
        IcebergParquetValueParser.parseColumnValueToParquet(
            new BigDecimal("123456789.123456789"), type, UTC, 0, false);
    ParquetValueParserAssertionBuilder.newBuilder()
        .parquetBufferValue(pv)
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

    ParquetBufferValue pv =
        IcebergParquetValueParser.parseColumnValueToParquet("12:34:56.789", type, UTC, 0, false);
    ParquetValueParserAssertionBuilder.newBuilder()
        .parquetBufferValue(pv)
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

    ParquetBufferValue pv =
        IcebergParquetValueParser.parseColumnValueToParquet(
            "2024-01-01T12:34:56.789+08:00", type, UTC, 0, false);
    ParquetValueParserAssertionBuilder.newBuilder()
        .parquetBufferValue(pv)
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

    ParquetBufferValue pv =
        IcebergParquetValueParser.parseColumnValueToParquet(
            "2024-01-01T12:34:56.789+08:00", type, UTC, 0, false);
    ParquetValueParserAssertionBuilder.newBuilder()
        .parquetBufferValue(pv)
        .expectedValueClass(Long.class)
        .expectedParsedValue(1704083696789000L)
        .expectedSize(8.0f + DEFINITION_LEVEL_ENCODING_BYTE_LEN)
        .expectedMinMax(BigInteger.valueOf(1704083696789000L))
        .assertMatches();
  }

  @Test
  public void parseValueFloat() {
    Type type = Types.primitive(PrimitiveTypeName.FLOAT, Repetition.OPTIONAL).named("FLOAT_COL");

    ParquetBufferValue pv =
        IcebergParquetValueParser.parseColumnValueToParquet(Float.MAX_VALUE, type, UTC, 0, false);
    ParquetValueParserAssertionBuilder.newBuilder()
        .parquetBufferValue(pv)
        .expectedValueClass(Float.class)
        .expectedParsedValue(Float.MAX_VALUE)
        .expectedSize(4.0f + DEFINITION_LEVEL_ENCODING_BYTE_LEN)
        .expectedMinMax((double) Float.MAX_VALUE)
        .assertMatches();
  }

  @Test
  public void parseValueDouble() {
    Type type = Types.primitive(PrimitiveTypeName.DOUBLE, Repetition.OPTIONAL).named("DOUBLE_COL");

    ParquetBufferValue pv =
        IcebergParquetValueParser.parseColumnValueToParquet(Double.MAX_VALUE, type, UTC, 0, false);
    ParquetValueParserAssertionBuilder.newBuilder()
        .parquetBufferValue(pv)
        .expectedValueClass(Double.class)
        .expectedParsedValue(Double.MAX_VALUE)
        .expectedSize(8.0f + DEFINITION_LEVEL_ENCODING_BYTE_LEN)
        .expectedMinMax(Double.MAX_VALUE)
        .assertMatches();
  }

  @Test
  public void parseValueBinary() {
    Type type = Types.primitive(PrimitiveTypeName.BINARY, Repetition.OPTIONAL).named("BINARY_COL");

    byte[] value = "snowflake_to_the_moon".getBytes();
    ParquetBufferValue pv =
        IcebergParquetValueParser.parseColumnValueToParquet(value, type, UTC, 0, false);
    ParquetValueParserAssertionBuilder.newBuilder()
        .parquetBufferValue(pv)
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

    String value = "snowflake_to_the_moon";
    ParquetBufferValue pv =
        IcebergParquetValueParser.parseColumnValueToParquet(value, type, UTC, 0, false);
    ParquetValueParserAssertionBuilder.newBuilder()
        .parquetBufferValue(pv)
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

    byte[] value = "snow".getBytes();
    ParquetBufferValue pv =
        IcebergParquetValueParser.parseColumnValueToParquet(value, type, UTC, 0, false);
    ParquetValueParserAssertionBuilder.newBuilder()
        .parquetBufferValue(pv)
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

    BigDecimal value = new BigDecimal("1234567890.0123456789");
    ParquetBufferValue pv =
        IcebergParquetValueParser.parseColumnValueToParquet(value, type, UTC, 0, false);
    ParquetValueParserAssertionBuilder.newBuilder()
        .parquetBufferValue(pv)
        .expectedValueClass(byte[].class)
        .expectedParsedValue(value.unscaledValue().toByteArray())
        .expectedSize(
            9.0f + DEFINITION_LEVEL_ENCODING_BYTE_LEN + BYTE_ARRAY_LENGTH_ENCODING_BYTE_LEN)
        .expectedMinMax(value.unscaledValue())
        .assertMatches();
  }

  @Test
  public void parseList() throws JsonProcessingException {
    Type list =
        Types.optionalList()
            .element(Types.optional(PrimitiveTypeName.INT32).named("element"))
            .named("LIST_COL");
    IcebergParquetValueParser.parseColumnValueToParquet(null, list, UTC, 0, false);
    ParquetBufferValue pv =
        IcebergParquetValueParser.parseColumnValueToParquet(
            Arrays.asList(1, 2, 3, 4, 5), list, UTC, 0, false);
    ParquetValueParserAssertionBuilder.newBuilder()
        .parquetBufferValue(pv)
        .expectedValueClass(ArrayList.class)
        .expectedParsedValue(
            convertToArrayList(
                objectMapper.readValue("[[1], [2], [3], [4], [5]]", ArrayList.class)))
        .expectedSize(
            (4.0f + REPETITION_LEVEL_ENCODING_BYTE_LEN + DEFINITION_LEVEL_ENCODING_BYTE_LEN) * 5)
        .expectedMinMax(BigInteger.valueOf(1))
        .assertMatches();

    /* Test required list */
    Type requiredList =
        Types.requiredList()
            .element(Types.optional(PrimitiveTypeName.INT32).named("element"))
            .named("LIST_COL");
    Assert.assertThrows(
        SFException.class,
        () ->
            IcebergParquetValueParser.parseColumnValueToParquet(null, requiredList, UTC, 0, false));
    pv =
        IcebergParquetValueParser.parseColumnValueToParquet(
            new ArrayList<>(), requiredList, UTC, 0, false);
    ParquetValueParserAssertionBuilder.newBuilder()
        .parquetBufferValue(pv)
        .expectedValueClass(ArrayList.class)
        .expectedParsedValue(convertToArrayList(objectMapper.readValue("[]", ArrayList.class)))
        .expectedSize(0)
        .expectedMinMax(BigInteger.valueOf(1))
        .assertMatches();

    /* Test required list with required elements */
    Type requiredElements =
        Types.requiredList()
            .element(Types.required(PrimitiveTypeName.INT32).named("element"))
            .named("LIST_COL");
    Assert.assertThrows(
        SFException.class,
        () ->
            IcebergParquetValueParser.parseColumnValueToParquet(
                Collections.singletonList(null), requiredElements, UTC, 0, false));
  }

  @Test
  public void parseMap() throws JsonProcessingException {
    Type map =
        Types.optionalMap()
            .key(Types.required(PrimitiveTypeName.INT32).named("key"))
            .value(Types.optional(PrimitiveTypeName.INT32).named("value"))
            .named("MAP_COL");
    IcebergParquetValueParser.parseColumnValueToParquet(null, map, UTC, 0, false);
    ParquetBufferValue pv =
        IcebergParquetValueParser.parseColumnValueToParquet(
            new java.util.HashMap<Integer, Integer>() {
              {
                put(1, 1);
                put(2, 2);
              }
            },
            map,
            UTC,
            0,
            false);
    ParquetValueParserAssertionBuilder.newBuilder()
        .parquetBufferValue(pv)
        .expectedValueClass(ArrayList.class)
        .expectedParsedValue(
            convertToArrayList(objectMapper.readValue("[[1, 1], [2, 2]]", ArrayList.class)))
        .expectedSize(
            (4.0f + REPETITION_LEVEL_ENCODING_BYTE_LEN + DEFINITION_LEVEL_ENCODING_BYTE_LEN) * 2
                + (4.0f + DEFINITION_LEVEL_ENCODING_BYTE_LEN) * 2)
        .expectedMinMax(BigInteger.valueOf(1))
        .assertMatches();

    /* Test required map */
    Type requiredMap =
        Types.requiredMap()
            .key(Types.required(PrimitiveTypeName.INT32).named("key"))
            .value(Types.optional(PrimitiveTypeName.INT32).named("value"))
            .named("MAP_COL");
    Assert.assertThrows(
        SFException.class,
        () ->
            IcebergParquetValueParser.parseColumnValueToParquet(null, requiredMap, UTC, 0, false));
    pv =
        IcebergParquetValueParser.parseColumnValueToParquet(
            new java.util.HashMap<Integer, Integer>(), requiredMap, UTC, 0, false);
    ParquetValueParserAssertionBuilder.newBuilder()
        .parquetBufferValue(pv)
        .expectedValueClass(ArrayList.class)
        .expectedParsedValue(convertToArrayList(objectMapper.readValue("[]", ArrayList.class)))
        .expectedSize(0)
        .expectedMinMax(BigInteger.valueOf(1))
        .assertMatches();

    /* Test required map with required values */
    Type requiredValues =
        Types.requiredMap()
            .key(Types.required(PrimitiveTypeName.INT32).named("key"))
            .value(Types.required(PrimitiveTypeName.INT32).named("value"))
            .named("MAP_COL");
    Assert.assertThrows(
        SFException.class,
        () ->
            IcebergParquetValueParser.parseColumnValueToParquet(
                new java.util.HashMap<Integer, Integer>() {
                  {
                    put(1, null);
                  }
                },
                requiredValues,
                UTC,
                0,
                false));
  }

  @Test
  public void parseStruct() throws JsonProcessingException {
    Type struct =
        Types.optionalGroup()
            .addField(Types.optional(PrimitiveTypeName.INT32).named("a"))
            .addField(
                Types.required(PrimitiveTypeName.BINARY)
                    .as(LogicalTypeAnnotation.stringType())
                    .named("b"))
            .named("STRUCT_COL");

    IcebergParquetValueParser.parseColumnValueToParquet(null, struct, UTC, 0, false);
    Assert.assertThrows(
        SFException.class,
        () ->
            IcebergParquetValueParser.parseColumnValueToParquet(
                new java.util.HashMap<String, Object>() {
                  {
                    put("a", 1);
                  }
                },
                struct,
                UTC,
                0,
                false));
    ParquetBufferValue pv =
        IcebergParquetValueParser.parseColumnValueToParquet(
            new java.util.HashMap<String, Object>() {
              {
                // a is null
                put("b", "2");
                put("c", 1); // Ignored
              }
            },
            struct,
            UTC,
            0,
            false);
    ParquetValueParserAssertionBuilder.newBuilder()
        .parquetBufferValue(pv)
        .expectedValueClass(ArrayList.class)
        .expectedParsedValue(
            convertToArrayList(objectMapper.readValue("[null, \"2\"]", ArrayList.class)))
        .expectedSize(1 + BYTE_ARRAY_LENGTH_ENCODING_BYTE_LEN + DEFINITION_LEVEL_ENCODING_BYTE_LEN)
        .expectedMinMax(BigInteger.valueOf(1))
        .assertMatches();

    /* Test required struct */
    Type requiredStruct =
        Types.requiredGroup()
            .addField(Types.optional(PrimitiveTypeName.INT32).named("a"))
            .addField(
                Types.optional(PrimitiveTypeName.BINARY)
                    .as(LogicalTypeAnnotation.stringType())
                    .named("b"))
            .named("STRUCT_COL");
    Assert.assertThrows(
        SFException.class,
        () ->
            IcebergParquetValueParser.parseColumnValueToParquet(
                null, requiredStruct, UTC, 0, false));
    pv =
        IcebergParquetValueParser.parseColumnValueToParquet(
            new java.util.HashMap<String, Object>(), requiredStruct, UTC, 0, false);
    ParquetValueParserAssertionBuilder.newBuilder()
        .parquetBufferValue(pv)
        .expectedValueClass(ArrayList.class)
        .expectedParsedValue(
            convertToArrayList(objectMapper.readValue("[null, null]", ArrayList.class)))
        .expectedSize(0)
        .expectedMinMax(BigInteger.valueOf(1))
        .assertMatches();
  }

  @Test
  public void parseNestedTypes() {
    for (int depth = 1; depth <= 100; depth *= 10) {
      Type type = generateNestedType(depth);
      Pair<Object, Object> res = generateNestedValueAndReference(depth);
      Object value = res.getFirst();
      List<?> reference = (List<?>) res.getSecond();
      ParquetBufferValue pv =
          IcebergParquetValueParser.parseColumnValueToParquet(value, type, UTC, 0, false);
      ParquetValueParserAssertionBuilder.newBuilder()
          .parquetBufferValue(pv)
          .expectedValueClass(ArrayList.class)
          .expectedParsedValue(convertToArrayList(reference))
          .expectedSize(
              (4.0f + REPETITION_LEVEL_ENCODING_BYTE_LEN + DEFINITION_LEVEL_ENCODING_BYTE_LEN)
                  * (depth / 3 + 1))
          .assertMatches();
    }
  }

  private static Type generateNestedType(int depth) {
    if (depth == 0) {
      return Types.optional(PrimitiveTypeName.INT32).named("a");
    }
    switch (depth % 3) {
      case 1:
        return Types.optionalList().element(generateNestedType(depth - 1)).named("a");
      case 2:
        return Types.optionalGroup().addField(generateNestedType(depth - 1)).named("a");
      case 0:
        return Types.optionalMap()
            .key(Types.required(PrimitiveTypeName.INT32).named("key"))
            .value(generateNestedType(depth - 1))
            .named("a");
    }
    return null;
  }

  private static Pair<Object, Object> generateNestedValueAndReference(int depth) {
    if (depth == 0) {
      return new Pair<>(1, 1);
    }
    Pair<Object, Object> res = generateNestedValueAndReference(depth - 1);
    Assert.assertNotNull(res);
    switch (depth % 3) {
      case 1:
        return new Pair<>(
            Collections.singletonList(res.getFirst()),
            Collections.singletonList(Collections.singletonList(res.getSecond())));
      case 2:
        return new Pair<>(
            new java.util.HashMap<String, Object>() {
              {
                put("a", res.getFirst());
              }
            },
            Collections.singletonList(res.getSecond()));
      case 0:
        return new Pair<>(
            new java.util.HashMap<Integer, Object>() {
              {
                put(1, res.getFirst());
              }
            },
            Collections.singletonList(Arrays.asList(1, res.getSecond())));
    }
    return null;
  }

  private static ArrayList<Object> convertToArrayList(List<?> list) {
    ArrayList<Object> arrayList = new ArrayList<>();
    for (Object element : list) {
      if (element instanceof List) {
        // Recursively convert nested lists
        arrayList.add(convertToArrayList((List<?>) element));
      } else if (element instanceof String) {
        // Convert string to byte array
        arrayList.add(((String) element).getBytes());
      } else {
        arrayList.add(element);
      }
    }
    return arrayList;
  }
}
