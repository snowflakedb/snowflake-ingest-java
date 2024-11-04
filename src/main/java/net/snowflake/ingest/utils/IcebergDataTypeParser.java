/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nonnull;
import org.apache.iceberg.parquet.TypeToMessageType;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.JsonUtil;

/**
 * This class is used to Iceberg data type (include primitive types and nested types) serialization
 * and deserialization.
 *
 * <p>This code is modified from
 * GlobalServices/modules/data-lake/datalake-api/src/main/java/com/snowflake/metadata/iceberg
 * /IcebergDataTypeParser.java
 */
public class IcebergDataTypeParser {
  public static final String ELEMENT = "element";
  public static final String KEY = "key";
  public static final String VALUE = "value";
  private static final String TYPE = "type";
  private static final String STRUCT = "struct";
  private static final String LIST = "list";
  private static final String MAP = "map";
  private static final String FIELDS = "fields";
  private static final String DOC = "doc";
  private static final String NAME = "name";
  private static final String ID = "id";
  private static final String ELEMENT_ID = "element-id";
  private static final String KEY_ID = "key-id";
  private static final String VALUE_ID = "value-id";
  private static final String REQUIRED = "required";
  private static final String ELEMENT_REQUIRED = "element-required";
  private static final String VALUE_REQUIRED = "value-required";

  private static final String EMPTY_FIELD_CHAR = "\\";

  /** Object mapper for this class */
  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** Util class that contains the mapping between Iceberg data type and Parquet data type */
  private static final TypeToMessageType typeToMessageType = new TypeToMessageType();

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
    org.apache.parquet.schema.Type parquetType;
    if (icebergType.isPrimitiveType()) {
      parquetType =
          typeToMessageType.primitive(icebergType.asPrimitiveType(), repetition, id, name);
    } else {
      switch (icebergType.typeId()) {
        case LIST:
          parquetType = typeToMessageType.list(icebergType.asListType(), repetition, id, name);
          break;
        case MAP:
          parquetType = typeToMessageType.map(icebergType.asMapType(), repetition, id, name);
          break;
        case STRUCT:
          parquetType = typeToMessageType.struct(icebergType.asStructType(), repetition, id, name);
          break;
        default:
          throw new SFException(
              ErrorCode.INTERNAL_ERROR,
              String.format(
                  "Cannot convert Iceberg column to parquet type, name=%s, dataType=%s",
                  name, icebergDataType));
      }
    }
    return replaceWithOriginalFieldName(parquetType, icebergType, name);
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

      /* TypeToMessageType throws on empty field name, use a backslash to represent it and escape remaining backslash. */
      String name =
          JsonUtil.getString(NAME, field)
              .replace(EMPTY_FIELD_CHAR, EMPTY_FIELD_CHAR + EMPTY_FIELD_CHAR);
      if (name.isEmpty()) {
        name = EMPTY_FIELD_CHAR;
      }
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

  private static org.apache.parquet.schema.Type replaceWithOriginalFieldName(
      org.apache.parquet.schema.Type parquetType, Type icebergType, String fieldName) {
    if (parquetType.isPrimitive() != icebergType.isPrimitiveType()
        || (!parquetType.isPrimitive()
            && parquetType.getLogicalTypeAnnotation()
                == null /* ignore outer layer of map or list */
            && parquetType.asGroupType().getFieldCount()
                != icebergType.asNestedType().fields().size())) {
      throw new IllegalArgumentException(
          String.format(
              "Parquet type and Iceberg type mismatch: %s, %s", parquetType, icebergType));
    }
    if (parquetType.isPrimitive()) {
      /* rename field name */
      return org.apache.parquet.schema.Types.primitive(
              parquetType.asPrimitiveType().getPrimitiveTypeName(), parquetType.getRepetition())
          .as(parquetType.asPrimitiveType().getLogicalTypeAnnotation())
          .id(parquetType.getId().intValue())
          .length(parquetType.asPrimitiveType().getTypeLength())
          .named(fieldName);
    } else {
      org.apache.parquet.schema.Types.GroupBuilder<org.apache.parquet.schema.GroupType> builder =
          org.apache.parquet.schema.Types.buildGroup(parquetType.getRepetition());
      for (org.apache.parquet.schema.Type parquetFieldType :
          parquetType.asGroupType().getFields()) {
        if (parquetFieldType.getId() == null) {
          /* middle layer of map or list. Skip this level as parquet's using 3-level list/map while iceberg's using 2-level list/map */
          builder.addField(
              replaceWithOriginalFieldName(
                  parquetFieldType, icebergType, parquetFieldType.getName()));
        } else {
          Types.NestedField icebergField =
              icebergType.asNestedType().field(parquetFieldType.getId().intValue());
          if (icebergField == null) {
            throw new IllegalArgumentException(
                String.format(
                    "Cannot find Iceberg field with id %d in Iceberg type: %s",
                    parquetFieldType.getId().intValue(), icebergType));
          }
          builder.addField(
              replaceWithOriginalFieldName(
                  parquetFieldType,
                  icebergField.type(),
                  icebergField.name().equals(EMPTY_FIELD_CHAR)
                      ? ""
                      : icebergField
                          .name()
                          .replace(EMPTY_FIELD_CHAR + EMPTY_FIELD_CHAR, EMPTY_FIELD_CHAR)));
        }
      }
      if (parquetType.getId() != null) {
        builder.id(parquetType.getId().intValue());
      }
      return builder.as(parquetType.getLogicalTypeAnnotation()).named(fieldName);
    }
  }
}
