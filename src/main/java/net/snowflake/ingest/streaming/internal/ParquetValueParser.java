/*
 * Copyright (c) 2022 Snowflake Computing Inc. All rights reserved.
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

/** Parses a user column value into Parquet internal representation for buffering. */
class ParquetValueParser {

  // Parquet uses BitPacking to encode boolean, hence 1 bit per value
  public static final float BIT_ENCODING_BYTE_LEN = 1.0f / 8;

  /**
   * On average parquet needs 2 bytes / 8 values for the RLE+bitpack encoded definition level.
   *
   * <ul>
   *   There are two cases how definition level (0 for null values, 1 for non-null values) is
   *   encoded:
   *   <li>If there are at least 8 repeated values in a row, they are run-length encoded (length +
   *       value itself). E.g. 11111111 -> 8 1
   *   <li>If there are less than 8 repeated values, they are written in group as part of a
   *       bit-length encoded run, e.g. 1111 -> 15 A bit-length encoded run ends when either 64
   *       groups of 8 values have been written or if a new RLE run starts.
   *       <p>To distinguish between RLE and bitpack run, there is 1 extra bytes written as header
   *       when a bitpack run starts.
   * </ul>
   *
   * <ul>
   *   For more details see ColumnWriterV1#createDLWriter and {@link
   *   org.apache.parquet.column.values.rle.RunLengthBitPackingHybridEncoder#writeInt(int)}
   * </ul>
   *
   * <p>Since we don't have nested types, repetition level is always 0 and is not stored at all by
   * Parquet.
   */
  public static final float DEFINITION_LEVEL_ENCODING_BYTE_LEN = 2.0f / 8;

  // Parquet stores length in 4 bytes before the actual data bytes
  public static final int BYTE_ARRAY_LENGTH_ENCODING_BYTE_LEN = 4;

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
   * @param curRowIndex Row index corresponding the row to parse
   * @return parsed value and byte size of Parquet internal representation
   */
  static ParquetBufferValue parseColumnValueToParquet(
      Object value,
      ColumnMetadata columnMetadata,
      PrimitiveType.PrimitiveTypeName typeName,
      RowBufferStats stats,
      ZoneId defaultTimezone,
      int curRowIndex) {
    Utils.assertNotNull("Parquet column stats", stats);
    float estimatedParquetSize = 0F;
    estimatedParquetSize += DEFINITION_LEVEL_ENCODING_BYTE_LEN;
    if (value != null) {
      AbstractRowBuffer.ColumnLogicalType logicalType =
          AbstractRowBuffer.ColumnLogicalType.valueOf(columnMetadata.getLogicalType());
      AbstractRowBuffer.ColumnPhysicalType physicalType =
          AbstractRowBuffer.ColumnPhysicalType.valueOf(columnMetadata.getPhysicalType());
      switch (typeName) {
        case BOOLEAN:
          int intValue =
              DataValidationUtil.validateAndParseBoolean(
                  columnMetadata.getName(), value, curRowIndex);
          value = intValue > 0;
          stats.addIntValue(BigInteger.valueOf(intValue));
          estimatedParquetSize += BIT_ENCODING_BYTE_LEN;
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
                  curRowIndex);
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
                  curRowIndex);
          value = longValue;
          stats.addIntValue(BigInteger.valueOf(longValue));
          estimatedParquetSize += 8;
          break;
        case DOUBLE:
          double doubleValue =
              DataValidationUtil.validateAndParseReal(columnMetadata.getName(), value, curRowIndex);
          value = doubleValue;
          stats.addRealValue(doubleValue);
          estimatedParquetSize += 8;
          break;
        case BINARY:
          int length = 0;
          if (logicalType == AbstractRowBuffer.ColumnLogicalType.BINARY) {
            value = getBinaryValueForLogicalBinary(value, stats, columnMetadata, curRowIndex);
            length = ((byte[]) value).length;
          } else {
            String str = getBinaryValue(value, stats, columnMetadata, curRowIndex);
            value = str;
            if (str != null) {
              length = str.getBytes().length;
            }
          }
          if (value != null) {
            estimatedParquetSize += (BYTE_ARRAY_LENGTH_ENCODING_BYTE_LEN + length);
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
                  curRowIndex);
          stats.addIntValue(intRep);
          value = getSb16Bytes(intRep);
          estimatedParquetSize += 16;
          break;
        default:
          throw new SFException(ErrorCode.UNKNOWN_DATA_TYPE, logicalType, physicalType);
      }
    }

    if (value == null) {
      if (!columnMetadata.getNullable()) {
        throw new SFException(
            ErrorCode.INVALID_TYPE_ROW,
            columnMetadata.getName(),
            "Passed null to non nullable field");
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
   * @return parsed int32 value
   */
  private static int getInt32Value(
      String columnName,
      Object value,
      @Nullable Integer scale,
      Integer precision,
      AbstractRowBuffer.ColumnLogicalType logicalType,
      AbstractRowBuffer.ColumnPhysicalType physicalType,
      final int curRowIndex) {
    int intVal;
    switch (logicalType) {
      case DATE:
        intVal = DataValidationUtil.validateAndParseDate(columnName, value, curRowIndex);
        break;
      case TIME:
        Utils.assertNotNull("Unexpected null scale for TIME data type", scale);
        intVal =
            DataValidationUtil.validateAndParseTime(columnName, value, scale, curRowIndex)
                .intValue();
        break;
      case FIXED:
        BigDecimal bigDecimalValue =
            DataValidationUtil.validateAndParseBigDecimal(columnName, value, curRowIndex);
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
      AbstractRowBuffer.ColumnPhysicalType physicalType,
      ZoneId defaultTimezone,
      final int curRowIndex) {
    long longValue;
    switch (logicalType) {
      case TIME:
        Utils.assertNotNull("Unexpected null scale for TIME data type", scale);
        longValue =
            DataValidationUtil.validateAndParseTime(columnName, value, scale, curRowIndex)
                .longValue();
        break;
      case TIMESTAMP_LTZ:
      case TIMESTAMP_NTZ:
        boolean trimTimezone = logicalType == AbstractRowBuffer.ColumnLogicalType.TIMESTAMP_NTZ;
        longValue =
            DataValidationUtil.validateAndParseTimestamp(
                    columnName, value, scale, defaultTimezone, trimTimezone, curRowIndex)
                .toBinary(false)
                .longValue();
        break;
      case TIMESTAMP_TZ:
        longValue =
            DataValidationUtil.validateAndParseTimestamp(
                    columnName, value, scale, defaultTimezone, false, curRowIndex)
                .toBinary(true)
                .longValue();
        break;
      case FIXED:
        BigDecimal bigDecimalValue =
            DataValidationUtil.validateAndParseBigDecimal(columnName, value, curRowIndex);
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
      AbstractRowBuffer.ColumnPhysicalType physicalType,
      ZoneId defaultTimezone,
      final int curRowIndex) {
    switch (logicalType) {
      case TIMESTAMP_TZ:
        return DataValidationUtil.validateAndParseTimestamp(
                columnName, value, scale, defaultTimezone, false, curRowIndex)
            .toBinary(true);
      case TIMESTAMP_LTZ:
      case TIMESTAMP_NTZ:
        boolean trimTimezone = logicalType == AbstractRowBuffer.ColumnLogicalType.TIMESTAMP_NTZ;
        return DataValidationUtil.validateAndParseTimestamp(
                columnName, value, scale, defaultTimezone, trimTimezone, curRowIndex)
            .toBinary(false);
      case FIXED:
        BigDecimal bigDecimalValue =
            DataValidationUtil.validateAndParseBigDecimal(columnName, value, curRowIndex);
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
      Object value, RowBufferStats stats, ColumnMetadata columnMetadata, final int curRowIndex) {
    AbstractRowBuffer.ColumnLogicalType logicalType =
        AbstractRowBuffer.ColumnLogicalType.valueOf(columnMetadata.getLogicalType());
    String str;
    if (logicalType.isObject()) {
      switch (logicalType) {
        case OBJECT:
          str =
              DataValidationUtil.validateAndParseObject(
                  columnMetadata.getName(), value, curRowIndex);
          break;
        case VARIANT:
          str =
              DataValidationUtil.validateAndParseVariant(
                  columnMetadata.getName(), value, curRowIndex);
          break;
        case ARRAY:
          str =
              DataValidationUtil.validateAndParseArray(
                  columnMetadata.getName(), value, curRowIndex);
          break;
        default:
          throw new SFException(
              ErrorCode.UNKNOWN_DATA_TYPE, logicalType, columnMetadata.getPhysicalType());
      }
    } else {
      String maxLengthString = columnMetadata.getLength().toString();
      str =
          DataValidationUtil.validateAndParseString(
              columnMetadata.getName(),
              value,
              Optional.of(maxLengthString).map(Integer::parseInt),
              curRowIndex);
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
      Object value, RowBufferStats stats, ColumnMetadata columnMetadata, final int curRowIndex) {
    String maxLengthString = columnMetadata.getByteLength().toString();
    byte[] bytes =
        DataValidationUtil.validateAndParseBinary(
            columnMetadata.getName(),
            value,
            Optional.of(maxLengthString).map(Integer::parseInt),
            curRowIndex);
    stats.addBinaryValue(bytes);
    return bytes;
  }
}
