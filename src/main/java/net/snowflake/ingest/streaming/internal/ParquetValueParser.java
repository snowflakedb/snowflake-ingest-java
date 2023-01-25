/*
 * Copyright (c) 2022 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import javax.annotation.Nullable;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;
import net.snowflake.ingest.utils.Utils;
import org.apache.parquet.schema.PrimitiveType;

/** Parses a user column value into Parquet internal representation for buffering. */
class ParquetValueParser {
  /** Parquet internal value representation for buffering. */
  static class ParquetBufferValue {
    private final Object value;
    private final float size;

    ParquetBufferValue(Object value, float size) {
      this.value = value;
      this.size = size;
    }

    Object getValue() {
      return value;
    }

    float getSize() {
      return size;
    }
  }

  /**
   * Parses a user column value into Parquet internal representation for buffering.
   *
   * @param value column value provided by user in a row
   * @param columnMetadata column metadata
   * @param typeName Parquet primitive type name
   * @param stats column stats to update
   * @return parsed value and byte size of Parquet internal representation
   */
  static ParquetBufferValue parseColumnValueToParquet(
      Object value,
      ColumnMetadata columnMetadata,
      PrimitiveType.PrimitiveTypeName typeName,
      RowBufferStats stats) {
    Utils.assertNotNull("Parquet column stats", stats);
    float size = 0F;
    if (value != null) {
      AbstractRowBuffer.ColumnLogicalType logicalType =
          AbstractRowBuffer.ColumnLogicalType.valueOf(columnMetadata.getLogicalType());
      AbstractRowBuffer.ColumnPhysicalType physicalType =
          AbstractRowBuffer.ColumnPhysicalType.valueOf(columnMetadata.getPhysicalType());
      switch (typeName) {
        case BOOLEAN:
          int intValue =
              DataValidationUtil.validateAndParseBoolean(columnMetadata.getName(), value);
          value = intValue > 0;
          stats.addIntValue(BigInteger.valueOf(intValue));
          size = 1;
          break;
        case INT32:
          int intVal =
              getInt32Value(
                  columnMetadata.getName(),
                  value,
                  columnMetadata.getScale(),
                  Optional.ofNullable(columnMetadata.getPrecision()).orElse(0),
                  logicalType,
                  physicalType);
          value = intVal;
          stats.addIntValue(BigInteger.valueOf(intVal));
          size = 4;
          break;
        case INT64:
          long longValue =
              getInt64Value(
                  columnMetadata.getName(),
                  value,
                  columnMetadata.getScale(),
                  Optional.ofNullable(columnMetadata.getPrecision()).orElse(0),
                  logicalType,
                  physicalType);
          value = longValue;
          stats.addIntValue(BigInteger.valueOf(longValue));
          size = 8;
          break;
        case DOUBLE:
          double doubleValue =
              DataValidationUtil.validateAndParseReal(columnMetadata.getName(), value);
          value = doubleValue;
          stats.addRealValue(doubleValue);
          size = 8;
          break;
        case BINARY:
          if (logicalType == AbstractRowBuffer.ColumnLogicalType.BINARY) {
            value = getBinaryValueForLogicalBinary(value, stats, columnMetadata);
            size = ((byte[]) value).length;
          } else {
            String str = getBinaryValue(value, stats, columnMetadata);
            value = str;
            if (str != null) {
              size = str.getBytes().length;
            }
          }
          break;
        case FIXED_LEN_BYTE_ARRAY:
          BigInteger intRep =
              getSb16Value(
                  columnMetadata.getName(),
                  value,
                  columnMetadata.getScale(),
                  Optional.ofNullable(columnMetadata.getPrecision()).orElse(0),
                  logicalType,
                  physicalType);
          stats.addIntValue(intRep);
          value = getSb16Bytes(intRep);
          size += 16;
          break;
        default:
          throw new SFException(ErrorCode.UNKNOWN_DATA_TYPE, logicalType, physicalType);
      }
    }

    if (value == null) {
      if (!columnMetadata.getNullable()) {
        throw new SFException(
            ErrorCode.INVALID_ROW, columnMetadata.getName(), "Passed null to non nullable field");
      }
      stats.incCurrentNullCount();
    }

    return new ParquetBufferValue(value, size);
  }

  /**
   * Parses an int32 value based on Snowflake logical type.
   *
   * @param value column value provided by user in a row
   * @param scale data type scale
   * @param precision data type precision
   * @param logicalType Snowflake logical type
   * @param physicalType Snowflake physical type
   * @return parsed int32 value
   */
  private static int getInt32Value(
      String columnName,
      Object value,
      @Nullable Integer scale,
      Integer precision,
      AbstractRowBuffer.ColumnLogicalType logicalType,
      AbstractRowBuffer.ColumnPhysicalType physicalType) {
    int intVal;
    switch (logicalType) {
      case DATE:
        intVal = DataValidationUtil.validateAndParseDate(columnName, value);
        break;
      case TIME:
        Utils.assertNotNull("Unexpected null scale for TIME data type", scale);
        intVal = DataValidationUtil.validateAndParseTime(columnName, value, scale).intValue();
        break;
      case FIXED:
        BigDecimal bigDecimalValue =
            DataValidationUtil.validateAndParseBigDecimal(columnName, value);
        bigDecimalValue = bigDecimalValue.setScale(scale, RoundingMode.HALF_UP);
        DataValidationUtil.checkValueInRange(bigDecimalValue, scale, precision);
        intVal = bigDecimalValue.intValue();
        break;
      default:
        throw new SFException(ErrorCode.UNKNOWN_DATA_TYPE, logicalType, physicalType);
    }
    return intVal;
  }

  /**
   * Parses an int64 value based on Snowflake logical type.
   *
   * @param value column value provided by user in a row
   * @param scale data type scale
   * @param precision data type precision
   * @param logicalType Snowflake logical type
   * @param physicalType Snowflake physical type
   * @return parsed int64 value
   */
  private static long getInt64Value(
      String columnName,
      Object value,
      int scale,
      int precision,
      AbstractRowBuffer.ColumnLogicalType logicalType,
      AbstractRowBuffer.ColumnPhysicalType physicalType) {
    long longValue;
    switch (logicalType) {
      case TIME:
        Utils.assertNotNull("Unexpected null scale for TIME data type", scale);
        longValue = DataValidationUtil.validateAndParseTime(columnName, value, scale).longValue();
        break;
      case TIMESTAMP_LTZ:
      case TIMESTAMP_NTZ:
        boolean ignoreTimezone = logicalType == AbstractRowBuffer.ColumnLogicalType.TIMESTAMP_NTZ;
        longValue =
            DataValidationUtil.validateAndParseTimestamp(columnName, value, scale, ignoreTimezone)
                .getTimeInScale()
                .longValue();
        break;
      case TIMESTAMP_TZ:
        longValue =
            DataValidationUtil.validateAndParseTimestamp(columnName, value, scale, false)
                .getSfTimestamp()
                .orElseThrow(
                    () ->
                        new SFException(
                            ErrorCode.INVALID_ROW,
                            value,
                            "Unable to parse timestamp for TIMESTAMP_TZ column"))
                .toBinary(scale, true)
                .longValue();
        break;
      case FIXED:
        BigDecimal bigDecimalValue =
            DataValidationUtil.validateAndParseBigDecimal(columnName, value);
        bigDecimalValue = bigDecimalValue.setScale(scale, RoundingMode.HALF_UP);
        DataValidationUtil.checkValueInRange(bigDecimalValue, scale, precision);
        longValue = bigDecimalValue.longValue();
        break;
      default:
        throw new SFException(ErrorCode.UNKNOWN_DATA_TYPE, logicalType, physicalType);
    }
    return longValue;
  }

  /**
   * Parses an int128 value based on Snowflake logical type.
   *
   * @param value column value provided by user in a row
   * @param scale data type scale
   * @param precision data type precision
   * @param logicalType Snowflake logical type
   * @param physicalType Snowflake physical type
   * @return parsed int64 value
   */
  private static BigInteger getSb16Value(
      String columnName,
      Object value,
      int scale,
      int precision,
      AbstractRowBuffer.ColumnLogicalType logicalType,
      AbstractRowBuffer.ColumnPhysicalType physicalType) {
    switch (logicalType) {
      case TIMESTAMP_TZ:
        return DataValidationUtil.validateAndParseTimestamp(columnName, value, scale, false)
            .getSfTimestamp()
            .orElseThrow(
                () ->
                    new SFException(
                        ErrorCode.INVALID_ROW,
                        value,
                        "Unable to parse timestamp for TIMESTAMP_TZ column"))
            .toBinary(scale, true);
      case TIMESTAMP_LTZ:
      case TIMESTAMP_NTZ:
        boolean ignoreTimezone = logicalType == AbstractRowBuffer.ColumnLogicalType.TIMESTAMP_NTZ;
        return DataValidationUtil.validateAndParseTimestamp(
                columnName, value, scale, ignoreTimezone)
            .getTimeInScale();
      case FIXED:
        BigDecimal bigDecimalValue =
            DataValidationUtil.validateAndParseBigDecimal(columnName, value);
        // explicitly match the BigDecimal input scale with the Snowflake data type scale
        bigDecimalValue = bigDecimalValue.setScale(scale, RoundingMode.HALF_UP);
        DataValidationUtil.checkValueInRange(bigDecimalValue, scale, precision);
        return bigDecimalValue.unscaledValue();
      default:
        throw new SFException(ErrorCode.UNKNOWN_DATA_TYPE, logicalType, physicalType);
    }
  }

  /**
   * Converts an int128 value to its byte array representation.
   *
   * @param intRep int128 value
   * @return byte array representation
   */
  static byte[] getSb16Bytes(BigInteger intRep) {
    byte[] bytes = intRep.toByteArray();
    byte padByte = (byte) (bytes[0] < 0 ? -1 : 0);
    byte[] bytesBE = new byte[16];
    for (int i = 0; i < 16 - bytes.length; i++) {
      bytesBE[i] = padByte;
    }
    System.arraycopy(bytes, 0, bytesBE, 16 - bytes.length, bytes.length);
    return bytesBE;
  }

  /**
   * Converts an object or string to its byte array representation.
   *
   * @param value value to parse
   * @param stats column stats to update
   * @param columnMetadata column metadata
   * @return string representation
   */
  private static String getBinaryValue(
      Object value, RowBufferStats stats, ColumnMetadata columnMetadata) {
    AbstractRowBuffer.ColumnLogicalType logicalType =
        AbstractRowBuffer.ColumnLogicalType.valueOf(columnMetadata.getLogicalType());
    String str;
    if (logicalType.isObject()) {
      switch (logicalType) {
        case OBJECT:
          str = DataValidationUtil.validateAndParseObject(columnMetadata.getName(), value);
          break;
        case VARIANT:
          str = DataValidationUtil.validateAndParseVariant(columnMetadata.getName(), value);
          break;
        case ARRAY:
          str = DataValidationUtil.validateAndParseArray(columnMetadata.getName(), value);
          break;
        default:
          throw new SFException(
              ErrorCode.UNKNOWN_DATA_TYPE, logicalType, columnMetadata.getPhysicalType());
      }
    } else {
      String maxLengthString = columnMetadata.getLength().toString();
      str =
          DataValidationUtil.validateAndParseString(
              columnMetadata.getName(), value, Optional.of(maxLengthString).map(Integer::parseInt));
      stats.addStrValue(str);
    }
    return str;
  }
  /**
   * Converts a binary value to its byte array representation.
   *
   * @param value value to parse
   * @param stats column stats to update
   * @param columnMetadata column metadata
   * @return byte array representation
   */
  private static byte[] getBinaryValueForLogicalBinary(
      Object value, RowBufferStats stats, ColumnMetadata columnMetadata) {
    String maxLengthString = columnMetadata.getByteLength().toString();
    byte[] bytes =
        DataValidationUtil.validateAndParseBinary(
            columnMetadata.getName(), value, Optional.of(maxLengthString).map(Integer::parseInt));

    String str = new String(bytes, StandardCharsets.UTF_8);
    stats.addStrValue(str);

    return bytes;
  }
}
