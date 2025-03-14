/*
 * Copyright (c) 2022-2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.time.ZoneId;
import java.util.Optional;
import javax.annotation.Nullable;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;
import net.snowflake.ingest.utils.Utils;
import org.apache.parquet.schema.PrimitiveType;

/** Parses a user Snowflake column value into Parquet internal representation for buffering. */
class SnowflakeParquetValueParser {

  /**
   * Parses a user column value into Parquet internal representation for buffering.
   *
   * @param value column value provided by user in a row
   * @param columnMetadata column metadata
   * @param typeName Parquet primitive type name
   * @param stats column stats to update
   * @param insertRowsCurrIndex Row index corresponding the row to parse (w.r.t input rows in
   *     insertRows API, and not buffered row)
   * @return parsed value and byte size of Parquet internal representation
   */
  static ParquetBufferValue parseColumnValueToParquet(
      Object value,
      ColumnMetadata columnMetadata,
      PrimitiveType.PrimitiveTypeName typeName,
      RowBufferStats stats,
      ZoneId defaultTimezone,
      long insertRowsCurrIndex,
      boolean enableNewJsonParsingLogic) {
    Utils.assertNotNull("Parquet column stats", stats);
    float estimatedParquetSize = 0F;
    estimatedParquetSize += ParquetBufferValue.DEFINITION_LEVEL_ENCODING_BYTE_LEN;
    if (value != null) {
      AbstractRowBuffer.ColumnLogicalType logicalType =
          AbstractRowBuffer.ColumnLogicalType.valueOf(columnMetadata.getLogicalType());
      AbstractRowBuffer.ColumnPhysicalType physicalType =
          AbstractRowBuffer.ColumnPhysicalType.valueOf(columnMetadata.getPhysicalType());
      switch (typeName) {
        case BOOLEAN:
          int intValue =
              DataValidationUtil.validateAndParseBoolean(
                  columnMetadata.getName(), value, insertRowsCurrIndex);
          value = intValue > 0;
          stats.addIntValue(BigInteger.valueOf(intValue));
          estimatedParquetSize += ParquetBufferValue.BIT_ENCODING_BYTE_LEN;
          break;
        case INT32:
          int intVal =
              getInt32Value(
                  columnMetadata.getName(),
                  value,
                  columnMetadata.getScale(),
                  Optional.ofNullable(columnMetadata.getPrecision()).orElse(0),
                  logicalType,
                  physicalType,
                  insertRowsCurrIndex);
          value = intVal;
          stats.addIntValue(BigInteger.valueOf(intVal));
          estimatedParquetSize += 4;
          break;
        case INT64:
          long longValue =
              getInt64Value(
                  columnMetadata.getName(),
                  value,
                  columnMetadata.getScale(),
                  Optional.ofNullable(columnMetadata.getPrecision()).orElse(0),
                  logicalType,
                  physicalType,
                  defaultTimezone,
                  insertRowsCurrIndex);
          value = longValue;
          stats.addIntValue(BigInteger.valueOf(longValue));
          estimatedParquetSize += 8;
          break;
        case DOUBLE:
          double doubleValue =
              DataValidationUtil.validateAndParseReal(
                  columnMetadata.getName(), value, insertRowsCurrIndex);
          value = doubleValue;
          stats.addRealValue(doubleValue);
          estimatedParquetSize += 8;
          break;
        case BINARY:
          int length = 0;
          if (logicalType == AbstractRowBuffer.ColumnLogicalType.BINARY) {
            value =
                getBinaryValueForLogicalBinary(value, stats, columnMetadata, insertRowsCurrIndex);
            length = ((byte[]) value).length;
          } else {
            String str =
                getBinaryValue(
                    value, stats, columnMetadata, insertRowsCurrIndex, enableNewJsonParsingLogic);
            value = str;
            if (str != null) {
              length = str.getBytes().length;
            }
          }
          if (value != null) {
            estimatedParquetSize +=
                (ParquetBufferValue.BYTE_ARRAY_LENGTH_ENCODING_BYTE_LEN + length);
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
                  physicalType,
                  defaultTimezone,
                  insertRowsCurrIndex);
          stats.addIntValue(intRep);
          value = getSb16Bytes(intRep);
          estimatedParquetSize += 16;
          break;
        default:
          throw new SFException(
              ErrorCode.UNKNOWN_DATA_TYPE, columnMetadata.getName(), logicalType, physicalType);
      }
    }

    if (value == null) {
      if (!columnMetadata.getNullable()) {
        throw new SFException(
            ErrorCode.INVALID_FORMAT_ROW,
            columnMetadata.getName(),
            String.format("Passed null to non nullable field, rowIndex:%d", insertRowsCurrIndex));
      }
      stats.incCurrentNullCount();
    }

    return new ParquetBufferValue(value, estimatedParquetSize);
  }

  /**
   * Parses an int32 value based on Snowflake logical type.
   *
   * @param value column value provided by user in a row
   * @param scale data type scale
   * @param precision data type precision
   * @param logicalType Snowflake logical type
   * @param physicalType Snowflake physical type
   * @param insertRowsCurrIndex Used for logging the row of index given in insertRows API
   * @return parsed int32 value
   */
  private static int getInt32Value(
      String columnName,
      Object value,
      @Nullable Integer scale,
      Integer precision,
      AbstractRowBuffer.ColumnLogicalType logicalType,
      AbstractRowBuffer.ColumnPhysicalType physicalType,
      final long insertRowsCurrIndex) {
    int intVal;
    switch (logicalType) {
      case DATE:
        intVal = DataValidationUtil.validateAndParseDate(columnName, value, insertRowsCurrIndex);
        break;
      case TIME:
        Utils.assertNotNull("Unexpected null scale for TIME data type", scale);
        intVal =
            DataValidationUtil.validateAndParseTime(columnName, value, scale, insertRowsCurrIndex)
                .intValue();
        break;
      case FIXED:
        BigDecimal bigDecimalValue =
            DataValidationUtil.validateAndParseBigDecimal(columnName, value, insertRowsCurrIndex);
        bigDecimalValue = bigDecimalValue.setScale(scale, RoundingMode.HALF_UP);
        DataValidationUtil.checkValueInRange(
            columnName, bigDecimalValue, scale, precision, insertRowsCurrIndex);
        intVal = bigDecimalValue.intValue();
        break;
      default:
        throw new SFException(ErrorCode.UNKNOWN_DATA_TYPE, columnName, logicalType, physicalType);
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
      AbstractRowBuffer.ColumnPhysicalType physicalType,
      ZoneId defaultTimezone,
      final long insertRowsCurrIndex) {
    long longValue;
    switch (logicalType) {
      case TIME:
        Utils.assertNotNull("Unexpected null scale for TIME data type", scale);
        longValue =
            DataValidationUtil.validateAndParseTime(columnName, value, scale, insertRowsCurrIndex)
                .longValue();
        break;
      case TIMESTAMP_LTZ:
      case TIMESTAMP_NTZ:
        boolean trimTimezone = logicalType == AbstractRowBuffer.ColumnLogicalType.TIMESTAMP_NTZ;
        longValue =
            DataValidationUtil.validateAndParseTimestamp(
                    columnName, value, scale, defaultTimezone, trimTimezone, insertRowsCurrIndex)
                .toBinary(false)
                .longValue();
        break;
      case TIMESTAMP_TZ:
        longValue =
            DataValidationUtil.validateAndParseTimestamp(
                    columnName, value, scale, defaultTimezone, false, insertRowsCurrIndex)
                .toBinary(true)
                .longValue();
        break;
      case FIXED:
        BigDecimal bigDecimalValue =
            DataValidationUtil.validateAndParseBigDecimal(columnName, value, insertRowsCurrIndex);
        bigDecimalValue = bigDecimalValue.setScale(scale, RoundingMode.HALF_UP);
        DataValidationUtil.checkValueInRange(
            columnName, bigDecimalValue, scale, precision, insertRowsCurrIndex);
        longValue = bigDecimalValue.longValue();
        break;
      default:
        throw new SFException(ErrorCode.UNKNOWN_DATA_TYPE, columnName, logicalType, physicalType);
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
      AbstractRowBuffer.ColumnPhysicalType physicalType,
      ZoneId defaultTimezone,
      final long insertRowsCurrIndex) {
    switch (logicalType) {
      case TIMESTAMP_TZ:
        return DataValidationUtil.validateAndParseTimestamp(
                columnName, value, scale, defaultTimezone, false, insertRowsCurrIndex)
            .toBinary(true);
      case TIMESTAMP_LTZ:
      case TIMESTAMP_NTZ:
        boolean trimTimezone = logicalType == AbstractRowBuffer.ColumnLogicalType.TIMESTAMP_NTZ;
        return DataValidationUtil.validateAndParseTimestamp(
                columnName, value, scale, defaultTimezone, trimTimezone, insertRowsCurrIndex)
            .toBinary(false);
      case FIXED:
        BigDecimal bigDecimalValue =
            DataValidationUtil.validateAndParseBigDecimal(columnName, value, insertRowsCurrIndex);
        // explicitly match the BigDecimal input scale with the Snowflake data type scale
        bigDecimalValue = bigDecimalValue.setScale(scale, RoundingMode.HALF_UP);
        DataValidationUtil.checkValueInRange(
            columnName, bigDecimalValue, scale, precision, insertRowsCurrIndex);
        return bigDecimalValue.unscaledValue();
      default:
        throw new SFException(ErrorCode.UNKNOWN_DATA_TYPE, columnName, logicalType, physicalType);
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
   * @param insertRowsCurrIndex Used for logging the row of index given in insertRows API
   * @return string representation
   */
  private static String getBinaryValue(
      Object value,
      RowBufferStats stats,
      ColumnMetadata columnMetadata,
      final long insertRowsCurrIndex,
      boolean enableNewJsonParsingLogic) {
    AbstractRowBuffer.ColumnLogicalType logicalType =
        AbstractRowBuffer.ColumnLogicalType.valueOf(columnMetadata.getLogicalType());
    String str;
    if (logicalType.isObject()) {
      switch (logicalType) {
        case OBJECT:
          str =
              enableNewJsonParsingLogic
                  ? DataValidationUtil.validateAndParseObjectNew(
                      columnMetadata.getName(), value, insertRowsCurrIndex)
                  : DataValidationUtil.validateAndParseObject(
                      columnMetadata.getName(), value, insertRowsCurrIndex);
          break;
        case VARIANT:
          str =
              enableNewJsonParsingLogic
                  ? DataValidationUtil.validateAndParseVariantNew(
                      columnMetadata.getName(), value, insertRowsCurrIndex)
                  : DataValidationUtil.validateAndParseVariant(
                      columnMetadata.getName(), value, insertRowsCurrIndex);
          break;
        case ARRAY:
          str =
              enableNewJsonParsingLogic
                  ? DataValidationUtil.validateAndParseArrayNew(
                      columnMetadata.getName(), value, insertRowsCurrIndex)
                  : DataValidationUtil.validateAndParseArray(
                      columnMetadata.getName(), value, insertRowsCurrIndex);
          break;
        default:
          throw new SFException(
              ErrorCode.UNKNOWN_DATA_TYPE,
              columnMetadata.getName(),
              logicalType,
              columnMetadata.getPhysicalType());
      }
    } else {
      String maxLengthString = columnMetadata.getLength().toString();
      str =
          DataValidationUtil.validateAndParseString(
              columnMetadata.getName(),
              value,
              Optional.of(maxLengthString).map(Integer::parseInt),
              insertRowsCurrIndex);
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
      Object value,
      RowBufferStats stats,
      ColumnMetadata columnMetadata,
      final long insertRowsCurrIndex) {
    String maxLengthString = columnMetadata.getByteLength().toString();
    byte[] bytes =
        DataValidationUtil.validateAndParseBinary(
            columnMetadata.getName(),
            value,
            Optional.of(maxLengthString).map(Integer::parseInt),
            insertRowsCurrIndex);
    stats.addBinaryValue(bytes);
    return bytes;
  }
}
