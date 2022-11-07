/*
 * Copyright (c) 2022 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

/** Generates the Parquet types for the Snowflake's column types */
public class ParquetTypeGenerator {

  /**
   * Util class that contains Parquet type and other metadata for that type needed by the Snowflake
   * server side scanner
   */
  static class ParquetTypeInfo {
    private Type parquetType;
    private Map<String, String> metadata;

    public Type getParquetType() {
      return this.parquetType;
    }

    public Map<String, String> getMetadata() {
      return this.metadata;
    }

    public void setParquetType(Type parquetType) {
      this.parquetType = parquetType;
    }

    public void setMetadata(Map<String, String> metadata) {
      this.metadata = metadata;
    }
  }

  private static final Set<AbstractRowBuffer.ColumnPhysicalType> TIME_SUPPORTED_PHYSICAL_TYPES =
      new HashSet<>(
          Arrays.asList(
              AbstractRowBuffer.ColumnPhysicalType.SB4, AbstractRowBuffer.ColumnPhysicalType.SB8));
  private static final Set<AbstractRowBuffer.ColumnPhysicalType>
      TIMESTAMP_SUPPORTED_PHYSICAL_TYPES =
          new HashSet<>(
              Arrays.asList(
                  AbstractRowBuffer.ColumnPhysicalType.SB8,
                  AbstractRowBuffer.ColumnPhysicalType.SB16));

  /**
   * Generate the column parquet type and metadata from the column metadata received from server
   * side.
   *
   * @param column column metadata as received from server side
   * @param id column id
   * @return column parquet type
   */
  static ParquetTypeInfo generateColumnParquetTypeInfo(ColumnMetadata column, int id) {
    ParquetTypeInfo res = new ParquetTypeInfo();
    Type parquetType;
    Map<String, String> metadata = new HashMap<>();
    String name = column.getName();

    AbstractRowBuffer.ColumnPhysicalType physicalType;
    AbstractRowBuffer.ColumnLogicalType logicalType;
    try {
      physicalType = AbstractRowBuffer.ColumnPhysicalType.valueOf(column.getPhysicalType());
      logicalType = AbstractRowBuffer.ColumnLogicalType.valueOf(column.getLogicalType());
    } catch (IllegalArgumentException e) {
      throw new SFException(
          ErrorCode.UNKNOWN_DATA_TYPE, column.getLogicalType(), column.getPhysicalType());
    }

    metadata.put(Integer.toString(id), logicalType.getOrdinal() + "," + physicalType.getOrdinal());

    // Parquet Type.Repetition in general supports repeated values for the same row column, like a
    // list of values.
    // This generator uses only either 0 or 1 value for nullable data type (OPTIONAL: 0 or none
    // value if it is null)
    // or exactly 1 value for non-nullable data type (REQUIRED)
    Type.Repetition repetition =
        column.getNullable() ? Type.Repetition.OPTIONAL : Type.Repetition.REQUIRED;

    // Handle differently depends on the column logical and physical types
    switch (logicalType) {
      case FIXED:
        parquetType = getFixedColumnParquetType(column, id, physicalType, repetition);
        break;
      case ARRAY:
      case OBJECT:
      case VARIANT:
        // mark the column metadata as being an object json for the server side scanner
        metadata.put(id + ":obj_enc", "1");
        // parquetType is same as the next one
      case ANY:
      case CHAR:
      case TEXT:
      case BINARY:
        parquetType =
            Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, repetition)
                .as(LogicalTypeAnnotation.stringType())
                .id(id)
                .named(name);
        break;
      case TIMESTAMP_LTZ:
      case TIMESTAMP_NTZ:
      case TIMESTAMP_TZ:
        parquetType =
            getTimeColumnParquetType(
                column.getScale(),
                physicalType,
                logicalType,
                TIMESTAMP_SUPPORTED_PHYSICAL_TYPES,
                repetition,
                id,
                name);
        break;
      case DATE:
        parquetType =
            Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition)
                .as(LogicalTypeAnnotation.dateType())
                .id(id)
                .named(name);
        break;
      case TIME:
        parquetType =
            getTimeColumnParquetType(
                column.getScale(),
                physicalType,
                logicalType,
                TIME_SUPPORTED_PHYSICAL_TYPES,
                repetition,
                id,
                name);
        break;
      case BOOLEAN:
        parquetType =
            Types.primitive(PrimitiveType.PrimitiveTypeName.BOOLEAN, repetition).id(id).named(name);
        break;
      case REAL:
        parquetType =
            Types.primitive(PrimitiveType.PrimitiveTypeName.DOUBLE, repetition).id(id).named(name);
        break;
      default:
        throw new SFException(
            ErrorCode.UNKNOWN_DATA_TYPE, column.getLogicalType(), column.getPhysicalType());
    }
    res.setParquetType(parquetType);
    res.setMetadata(metadata);
    return res;
  }

  /**
   * Get the parquet type for column of Snowflake FIXED logical type.
   *
   * @param column column metadata
   * @param id column id in Snowflake table schema
   * @param physicalType Snowflake physical type of column
   * @param repetition parquet repetition type of column
   * @return column parquet type
   */
  private static Type getFixedColumnParquetType(
      ColumnMetadata column,
      int id,
      AbstractRowBuffer.ColumnPhysicalType physicalType,
      Type.Repetition repetition) {
    String name = column.getName();
    // the LogicalTypeAnnotation.DecimalLogicalTypeAnnotation is used by server side scanner
    // to discover data type scale and precision
    LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalLogicalTypeAnnotation =
        column.getScale() != null && column.getPrecision() != null
            ? LogicalTypeAnnotation.DecimalLogicalTypeAnnotation.decimalType(
                column.getScale(), column.getPrecision())
            : null;
    Type parquetType;
    if ((column.getScale() != null && column.getScale() != 0)
        || physicalType == AbstractRowBuffer.ColumnPhysicalType.SB16) {
      parquetType =
          Types.primitive(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, repetition)
              .length(16)
              .as(decimalLogicalTypeAnnotation)
              .id(id)
              .named(name);
    } else {
      switch (physicalType) {
        case SB1:
        case SB2:
        case SB4:
          parquetType =
              Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition)
                  .as(decimalLogicalTypeAnnotation)
                  .id(id)
                  .length(4)
                  .named(name);
          break;
        case SB8:
          parquetType =
              Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, repetition)
                  .as(decimalLogicalTypeAnnotation)
                  .id(id)
                  .length(8)
                  .named(name);
          break;
        default:
          throw new SFException(
              ErrorCode.UNKNOWN_DATA_TYPE, column.getLogicalType(), column.getPhysicalType());
      }
    }
    return parquetType;
  }

  /**
   * Get the parquet type for column of a Snowflake time logical type.
   *
   * @param scale column scale
   * @param physicalType Snowflake physical type of column
   * @param logicalType Snowflake logical type of column
   * @param supportedPhysicalTypes supported Snowflake physical types for the given column
   * @param repetition parquet repetition type of column
   * @param id column id in Snowflake table schema
   * @param name column name
   * @return column parquet type
   */
  private static Type getTimeColumnParquetType(
      Integer scale,
      AbstractRowBuffer.ColumnPhysicalType physicalType,
      AbstractRowBuffer.ColumnLogicalType logicalType,
      Set<AbstractRowBuffer.ColumnPhysicalType> supportedPhysicalTypes,
      Type.Repetition repetition,
      int id,
      String name) {
    if (scale == null || scale > 9 || scale < 0 || !supportedPhysicalTypes.contains(physicalType)) {
      throw new SFException(
          ErrorCode.UNKNOWN_DATA_TYPE,
          "Data type: " + logicalType + ", " + physicalType + ", scale: " + scale);
    }

    PrimitiveType.PrimitiveTypeName type = getTimePrimitiveType(physicalType);
    LogicalTypeAnnotation typeAnnotation;
    int length;
    switch (physicalType) {
      case SB4:
        typeAnnotation = LogicalTypeAnnotation.decimalType(scale, 9);
        length = 4;
        break;
      case SB8:
        typeAnnotation = LogicalTypeAnnotation.decimalType(scale, 18);
        length = 8;
        break;
      case SB16:
        typeAnnotation = LogicalTypeAnnotation.decimalType(scale, 38);
        length = 16;
        break;
      default:
        throw new SFException(ErrorCode.UNKNOWN_DATA_TYPE, logicalType, physicalType);
    }
    return Types.primitive(type, repetition).as(typeAnnotation).length(length).id(id).named(name);
  }

  /**
   * Get the parquet primitive type name for column of a Snowflake time logical type.
   *
   * @param physicalType Snowflake physical type of column
   * @return column parquet primitive type name
   */
  private static PrimitiveType.PrimitiveTypeName getTimePrimitiveType(
      AbstractRowBuffer.ColumnPhysicalType physicalType) {
    PrimitiveType.PrimitiveTypeName type;
    switch (physicalType) {
      case SB4:
        type = PrimitiveType.PrimitiveTypeName.INT32;
        break;
      case SB8:
        type = PrimitiveType.PrimitiveTypeName.INT64;
        break;
      case SB16:
        type = PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
        break;
      default:
        throw new UnsupportedOperationException("Time physical type: " + physicalType);
    }
    return type;
  }
}
