/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.utils;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nonnull;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.JsonUtil;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;

/**
 * This class is used to Iceberg data type (include primitive types and nested types) serialization
 * and deserialization.
 *
 * <p>This code is modified from
 * GlobalServices/modules/data-lake/datalake-api/src/main/java/com/snowflake/metadata/iceberg
 * /IcebergDataTypeParser.java and <a
 * href="https://github.com/apache/iceberg/blob/main/parquet/src/main/java/org/apache/iceberg/parquet/TypeToMessageType.java">
 * TypeToMessageType.java</a>
 */
public class IcebergDataTypeParser {
  private static final String TYPE = "type";
  private static final String STRUCT = "struct";
  private static final String LIST = "list";
  private static final String MAP = "map";
  private static final String FIELDS = "fields";
  private static final String ELEMENT = "element";
  private static final String KEY = "key";
  private static final String VALUE = "value";
  private static final String DOC = "doc";
  private static final String NAME = "name";
  private static final String ID = "id";
  private static final String ELEMENT_ID = "element-id";
  private static final String KEY_ID = "key-id";
  private static final String VALUE_ID = "value-id";
  private static final String REQUIRED = "required";
  private static final String ELEMENT_REQUIRED = "element-required";
  private static final String VALUE_REQUIRED = "value-required";

  private static final LogicalTypeAnnotation STRING = LogicalTypeAnnotation.stringType();
  private static final LogicalTypeAnnotation DATE = LogicalTypeAnnotation.dateType();
  private static final LogicalTypeAnnotation TIME_MICROS =
      LogicalTypeAnnotation.timeType(
          false /* not adjusted to UTC */, LogicalTypeAnnotation.TimeUnit.MICROS);
  private static final LogicalTypeAnnotation TIMESTAMP_MICROS =
      LogicalTypeAnnotation.timestampType(
          false /* not adjusted to UTC */, LogicalTypeAnnotation.TimeUnit.MICROS);
  private static final LogicalTypeAnnotation TIMESTAMPTZ_MICROS =
      LogicalTypeAnnotation.timestampType(
          true /* adjusted to UTC */, LogicalTypeAnnotation.TimeUnit.MICROS);

  private static final int DECIMAL_INT32_MAX_DIGITS = 9;
  private static final int DECIMAL_INT64_MAX_DIGITS = 18;
  private static final int DECIMAL_MAX_DIGITS = 38;
  private static final int DECIMAL_MAX_BYTES = 16;

  /** Object mapper for this class */
  private static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * Get Iceberg data type information by deserialization.
   *
   * @param icebergDataType string representation of Iceberg data type
   * @param repetition repetition of the Parquet data type
   * @param id column id
   * @param name column name
   * @return Iceberg data type
   */
  public static org.apache.parquet.schema.Type parseIcebergDataTypeStringToParquetType(
      String icebergDataType,
      org.apache.parquet.schema.Type.Repetition repetition,
      int id,
      String name) {
    Type icebergType = deserializeIcebergType(icebergDataType);
    if (icebergType.isPrimitiveType()) {
      return primitive(icebergType.asPrimitiveType(), repetition, id, name);
    } else {
      switch (icebergType.typeId()) {
        case LIST:
          return list(icebergType.asListType(), repetition, id, name);
        case MAP:
          return map(icebergType.asMapType(), repetition, id, name);
        case STRUCT:
          return struct(icebergType.asStructType(), repetition, id, name);
        default:
          throw new SFException(
              ErrorCode.INTERNAL_ERROR,
              String.format(
                  "Cannot convert Iceberg column to parquet type, name=%s, dataType=%s",
                  name, icebergDataType));
      }
    }
  }

  /**
   * Get Iceberg data type information by deserialization.
   *
   * @param icebergDataType string representation of Iceberg data type
   * @return Iceberg data type
   */
  public static Type deserializeIcebergType(String icebergDataType) {
    try {
      JsonNode json = MAPPER.readTree(icebergDataType);
      return getTypeFromJson(json);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException(
          String.format("Failed to deserialize Iceberg data type: %s", icebergDataType));
    }
  }

  /**
   * Get corresponding Iceberg data type from JsonNode.
   *
   * @param jsonNode JsonNode parsed from Iceberg type string.
   * @return Iceberg data type
   */
  public static Type getTypeFromJson(@Nonnull JsonNode jsonNode) {
    if (jsonNode.isTextual()) {
      return Types.fromPrimitiveString(jsonNode.asText());
    } else if (jsonNode.isObject()) {
      if (!jsonNode.has(TYPE)) {
        throw new IllegalArgumentException(
            String.format("Missing key '%s' in schema: %s", TYPE, jsonNode));
      }
      String type = jsonNode.get(TYPE).asText();
      if (STRUCT.equals(type)) {
        return structFromJson(jsonNode);
      } else if (LIST.equals(type)) {
        return listFromJson(jsonNode);
      } else if (MAP.equals(type)) {
        return mapFromJson(jsonNode);
      }
      throw new IllegalArgumentException(
          String.format("Cannot parse Iceberg type: %s, schema: %s", type, jsonNode));
    }

    throw new IllegalArgumentException("Cannot parse Iceberg type from schema: " + jsonNode);
  }

  /**
   * Get Iceberg struct type information from JsonNode.
   *
   * @param json JsonNode parsed from Iceberg type string.
   * @return struct type
   */
  public static @Nonnull Types.StructType structFromJson(@Nonnull JsonNode json) {
    if (!json.has(FIELDS)) {
      throw new IllegalArgumentException(
          String.format("Missing key '%s' in schema: %s", FIELDS, json));
    }
    JsonNode fieldArray = json.get(FIELDS);
    Preconditions.checkArgument(fieldArray != null, "Field array cannot be null");
    Preconditions.checkArgument(
        fieldArray.isArray(), "Cannot parse struct fields from non-array: %s", fieldArray);

    List<Types.NestedField> fields = Lists.newArrayListWithExpectedSize(fieldArray.size());
    Iterator<JsonNode> iterator = fieldArray.elements();
    while (iterator.hasNext()) {
      JsonNode field = iterator.next();
      Preconditions.checkArgument(
          field.isObject(), "Cannot parse struct field from non-object: %s", field);

      int id = JsonUtil.getInt(ID, field);
      String name = JsonUtil.getString(NAME, field);
      Type type = getTypeFromJson(field.get(TYPE));

      String doc = JsonUtil.getStringOrNull(DOC, field);
      boolean isRequired = JsonUtil.getBool(REQUIRED, field);
      if (isRequired) {
        fields.add(Types.NestedField.required(id, name, type, doc));
      } else {
        fields.add(Types.NestedField.optional(id, name, type, doc));
      }
    }

    return Types.StructType.of(fields);
  }

  /**
   * Get Iceberg list type information from JsonNode.
   *
   * @param json JsonNode parsed from Iceberg type string.
   * @return list type
   */
  public static Types.ListType listFromJson(JsonNode json) {
    int elementId = JsonUtil.getInt(ELEMENT_ID, json);
    Type elementType = getTypeFromJson(json.get(ELEMENT));
    boolean isRequired = JsonUtil.getBool(ELEMENT_REQUIRED, json);

    if (isRequired) {
      return Types.ListType.ofRequired(elementId, elementType);
    } else {
      return Types.ListType.ofOptional(elementId, elementType);
    }
  }

  /**
   * Get Iceberg map type from JsonNode.
   *
   * @param json JsonNode parsed from Iceberg type string.
   * @return map type
   */
  public static Types.MapType mapFromJson(JsonNode json) {
    int keyId = JsonUtil.getInt(KEY_ID, json);
    Type keyType = getTypeFromJson(json.get(KEY));

    int valueId = JsonUtil.getInt(VALUE_ID, json);
    Type valueType = getTypeFromJson(json.get(VALUE));

    boolean isRequired = JsonUtil.getBool(VALUE_REQUIRED, json);

    if (isRequired) {
      return Types.MapType.ofRequired(keyId, valueId, keyType, valueType);
    } else {
      return Types.MapType.ofOptional(keyId, valueId, keyType, valueType);
    }
  }

  private static GroupType struct(
      Types.StructType struct,
      org.apache.parquet.schema.Type.Repetition repetition,
      int id,
      String name) {
    org.apache.parquet.schema.Types.GroupBuilder<GroupType> builder =
        org.apache.parquet.schema.Types.buildGroup(repetition);

    for (Types.NestedField field : struct.fields()) {
      builder.addField(field(field));
    }

    return builder.id(id).named(name);
  }

  private static org.apache.parquet.schema.Type field(Types.NestedField field) {
    org.apache.parquet.schema.Type.Repetition repetition =
        field.isOptional()
            ? org.apache.parquet.schema.Type.Repetition.OPTIONAL
            : org.apache.parquet.schema.Type.Repetition.REQUIRED;
    int id = field.fieldId();
    String name = field.name();

    if (field.type().isPrimitiveType()) {
      return primitive(field.type().asPrimitiveType(), repetition, id, name);

    } else {
      Type.NestedType nested = field.type().asNestedType();
      if (nested.isStructType()) {
        return struct(nested.asStructType(), repetition, id, name);
      } else if (nested.isMapType()) {
        return map(nested.asMapType(), repetition, id, name);
      } else if (nested.isListType()) {
        return list(nested.asListType(), repetition, id, name);
      }
      throw new UnsupportedOperationException("Can't convert unknown type: " + nested);
    }
  }

  private static GroupType list(
      Types.ListType list,
      org.apache.parquet.schema.Type.Repetition repetition,
      int id,
      String name) {
    Types.NestedField elementField = list.fields().get(0);
    return org.apache.parquet.schema.Types.list(repetition)
        .element(field(elementField))
        .id(id)
        .named(name);
  }

  private static GroupType map(
      Types.MapType map,
      org.apache.parquet.schema.Type.Repetition repetition,
      int id,
      String name) {
    Types.NestedField keyField = map.fields().get(0);
    Types.NestedField valueField = map.fields().get(1);
    return org.apache.parquet.schema.Types.map(repetition)
        .key(field(keyField))
        .value(field(valueField))
        .id(id)
        .named(name);
  }

  public static org.apache.parquet.schema.Type primitive(
      Type.PrimitiveType primitive,
      org.apache.parquet.schema.Type.Repetition repetition,
      int id,
      String name) {
    switch (primitive.typeId()) {
      case BOOLEAN:
        return org.apache.parquet.schema.Types.primitive(BOOLEAN, repetition).id(id).named(name);
      case INTEGER:
        return org.apache.parquet.schema.Types.primitive(INT32, repetition).id(id).named(name);
      case LONG:
        return org.apache.parquet.schema.Types.primitive(INT64, repetition).id(id).named(name);
      case FLOAT:
        return org.apache.parquet.schema.Types.primitive(FLOAT, repetition).id(id).named(name);
      case DOUBLE:
        return org.apache.parquet.schema.Types.primitive(DOUBLE, repetition).id(id).named(name);
      case DATE:
        return org.apache.parquet.schema.Types.primitive(INT32, repetition)
            .as(DATE)
            .id(id)
            .named(name);
      case TIME:
        return org.apache.parquet.schema.Types.primitive(INT64, repetition)
            .as(TIME_MICROS)
            .id(id)
            .named(name);
      case TIMESTAMP:
        if (((Types.TimestampType) primitive).shouldAdjustToUTC()) {
          return org.apache.parquet.schema.Types.primitive(INT64, repetition)
              .as(TIMESTAMPTZ_MICROS)
              .id(id)
              .named(name);
        } else {
          return org.apache.parquet.schema.Types.primitive(INT64, repetition)
              .as(TIMESTAMP_MICROS)
              .id(id)
              .named(name);
        }
      case STRING:
        return org.apache.parquet.schema.Types.primitive(BINARY, repetition)
            .as(STRING)
            .id(id)
            .named(name);
      case BINARY:
        return org.apache.parquet.schema.Types.primitive(BINARY, repetition).id(id).named(name);
      case FIXED:
        Types.FixedType fixed = (Types.FixedType) primitive;

        return org.apache.parquet.schema.Types.primitive(FIXED_LEN_BYTE_ARRAY, repetition)
            .length(fixed.length())
            .id(id)
            .named(name);

      case DECIMAL:
        Types.DecimalType decimal = (Types.DecimalType) primitive;

        if (decimal.precision() <= DECIMAL_INT32_MAX_DIGITS) {
          /* Store as an int. */
          return org.apache.parquet.schema.Types.primitive(INT32, repetition)
              .as(decimalAnnotation(decimal.precision(), decimal.scale()))
              .id(id)
              .named(name);

        } else if (decimal.precision() <= DECIMAL_INT64_MAX_DIGITS) {
          /* Store as a long. */
          return org.apache.parquet.schema.Types.primitive(INT64, repetition)
              .as(decimalAnnotation(decimal.precision(), decimal.scale()))
              .id(id)
              .named(name);

        } else {
          /* Does not follow Iceberg spec which should be minimum number of bytes. Use fix(16) (SB16) instead. */
          if (decimal.precision() > DECIMAL_MAX_DIGITS) {
            throw new IllegalArgumentException(
                "Unsupported decimal precision: " + decimal.precision());
          }
          return org.apache.parquet.schema.Types.primitive(FIXED_LEN_BYTE_ARRAY, repetition)
              .length(DECIMAL_MAX_BYTES)
              .as(decimalAnnotation(decimal.precision(), decimal.scale()))
              .id(id)
              .named(name);
        }

      case UUID:
        return org.apache.parquet.schema.Types.primitive(FIXED_LEN_BYTE_ARRAY, repetition)
            .length(16)
            .as(LogicalTypeAnnotation.uuidType())
            .id(id)
            .named(name);

      default:
        throw new UnsupportedOperationException("Unsupported type for Parquet: " + primitive);
    }
  }

  private static LogicalTypeAnnotation decimalAnnotation(int precision, int scale) {
    return LogicalTypeAnnotation.decimalType(scale, precision);
  }
}
