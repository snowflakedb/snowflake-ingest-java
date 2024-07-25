/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.streaming.internal.DataValidationUtil.checkFixedLengthByteArray;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;
import net.snowflake.ingest.utils.Utils;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.ListLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.MapLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.StringLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;

/** Parses a user Iceberg column value into Parquet internal representation for buffering. */
class IcebergParquetValueParser {

  /**
   * Parses a user column value into Parquet internal representation for buffering.
   *
   * @param value column value provided by user in a row
   * @param type Parquet column type
   * @param stats column stats to update
   * @param insertRowsCurrIndex Row index corresponding the row to parse (w.r.t input rows in
   *     insertRows API, and not buffered row)
   * @return parsed value and byte size of Parquet internal representation
   */
  static ParquetBufferValue parseColumnValueToParquet(
      Object value,
      Type type,
      RowBufferStats stats,
      ZoneId defaultTimezone,
      long insertRowsCurrIndex) {
    Utils.assertNotNull("Parquet column stats", stats);
    float estimatedParquetSize = 0F;
    estimatedParquetSize += ParquetBufferValue.DEFINITION_LEVEL_ENCODING_BYTE_LEN;
    if (value != null) {
      if (type.isPrimitive()) {
        PrimitiveType primitiveType = type.asPrimitiveType();
        switch (type.asPrimitiveType().getPrimitiveTypeName()) {
          case BOOLEAN:
            int intValue =
                DataValidationUtil.validateAndParseBoolean(
                    type.getName(), value, insertRowsCurrIndex);
            value = intValue > 0;
            stats.addIntValue(BigInteger.valueOf(intValue));
            estimatedParquetSize += ParquetBufferValue.BIT_ENCODING_BYTE_LEN;
            break;
          case INT32:
            int intVal = getInt32Value(value, primitiveType, insertRowsCurrIndex);
            value = intVal;
            stats.addIntValue(BigInteger.valueOf(intVal));
            estimatedParquetSize += 4;
            break;
          case INT64:
            long longVal =
                getInt64Value(value, primitiveType, defaultTimezone, insertRowsCurrIndex);
            value = longVal;
            stats.addIntValue(BigInteger.valueOf(longVal));
            estimatedParquetSize += 8;
            break;
          case FLOAT:
            float floatVal =
                DataValidationUtil.validateAndParseIcebergNumber(
                    type.getName(), value, insertRowsCurrIndex, Float.class);
            value = floatVal;
            stats.addRealValue((double) floatVal);
            estimatedParquetSize += 4;
            break;
          case DOUBLE:
            double doubleVal =
                DataValidationUtil.validateAndParseIcebergNumber(
                    type.getName(), value, insertRowsCurrIndex, Double.class);
            value = doubleVal;
            stats.addRealValue(doubleVal);
            estimatedParquetSize += 8;
            break;
          case BINARY:
            byte[] byteVal = getBinaryValue(value, primitiveType, stats, insertRowsCurrIndex);
            value = byteVal;
            estimatedParquetSize +=
                ParquetBufferValue.BYTE_ARRAY_LENGTH_ENCODING_BYTE_LEN + byteVal.length;
            break;
          case FIXED_LEN_BYTE_ARRAY:
            byte[] fixedLenByteArrayVal =
                getFixedLenByteArrayValue(value, primitiveType, stats, insertRowsCurrIndex);
            value = fixedLenByteArrayVal;
            estimatedParquetSize +=
                ParquetBufferValue.BYTE_ARRAY_LENGTH_ENCODING_BYTE_LEN
                    + fixedLenByteArrayVal.length;
            break;
          default:
            throw new SFException(
                ErrorCode.UNKNOWN_DATA_TYPE,
                type.getLogicalTypeAnnotation(),
                primitiveType.getPrimitiveTypeName());
        }
      } else {
        return getGroupValue(
            value, type.asGroupType(), stats, defaultTimezone, insertRowsCurrIndex);
      }
    }

    if (value == null) {
      if (!type.isRepetition(Repetition.REQUIRED)) {
        throw new SFException(
            ErrorCode.INVALID_FORMAT_ROW, type.getName(), "Passed null to non nullable field");
      }
      stats.incCurrentNullCount();
    }

    return new ParquetBufferValue(value, estimatedParquetSize);
  }

  /**
   * Parses an int32 value based on Parquet logical type.
   *
   * @param value column value provided by user in a row
   * @param type Parquet column type
   * @param insertRowsCurrIndex Used for logging the row of index given in insertRows API
   * @return parsed int32 value
   */
  private static int getInt32Value(
      Object value, PrimitiveType type, final long insertRowsCurrIndex) {
    LogicalTypeAnnotation logicalTypeAnnotation = type.getLogicalTypeAnnotation();
    if (logicalTypeAnnotation == null) {
      return DataValidationUtil.validateAndParseIcebergNumber(
          type.getName(), value, insertRowsCurrIndex, Integer.class);
    }
    if (logicalTypeAnnotation instanceof DecimalLogicalTypeAnnotation) {
      return getDecimalValue(value, type, insertRowsCurrIndex).unscaledValue().intValue();
    }
    if (logicalTypeAnnotation == LogicalTypeAnnotation.dateType()) {
      return DataValidationUtil.validateAndParseDate(type.getName(), value, insertRowsCurrIndex);
    }
    throw new SFException(
        ErrorCode.UNKNOWN_DATA_TYPE, logicalTypeAnnotation, type.getPrimitiveTypeName());
  }

  /**
   * Parses an int64 value based on Parquet logical type.
   *
   * @param value column value provided by user in a row
   * @param type Parquet column type
   * @param insertRowsCurrIndex Used for logging the row of index given in insertRows API
   * @return parsed int64 value
   */
  private static long getInt64Value(
      Object value, PrimitiveType type, ZoneId defaultTimezone, final long insertRowsCurrIndex) {
    LogicalTypeAnnotation logicalTypeAnnotation = type.getLogicalTypeAnnotation();
    if (logicalTypeAnnotation == null) {
      return DataValidationUtil.validateAndParseIcebergNumber(
          type.getName(), value, insertRowsCurrIndex, Long.class);
    }
    if (logicalTypeAnnotation instanceof DecimalLogicalTypeAnnotation) {
      return getDecimalValue(value, type, insertRowsCurrIndex).unscaledValue().longValue();
    }
    if (logicalTypeAnnotation instanceof TimeLogicalTypeAnnotation) {
      return DataValidationUtil.validateAndParseTime(
              type.getName(),
              value,
              timeUnitToScale(((TimeLogicalTypeAnnotation) logicalTypeAnnotation).getUnit()),
              insertRowsCurrIndex)
          .longValue();
    }
    if (logicalTypeAnnotation instanceof TimestampLogicalTypeAnnotation) {
      boolean includeTimeZone =
          ((TimestampLogicalTypeAnnotation) logicalTypeAnnotation).isAdjustedToUTC();
      return DataValidationUtil.validateAndParseTimestamp(
              type.getName(),
              value,
              timeUnitToScale(((TimestampLogicalTypeAnnotation) logicalTypeAnnotation).getUnit()),
              defaultTimezone,
              !includeTimeZone,
              insertRowsCurrIndex)
          .toBinary(false)
          .longValue();
    }
    throw new SFException(
        ErrorCode.UNKNOWN_DATA_TYPE,
        logicalTypeAnnotation,
        type.asPrimitiveType().getPrimitiveTypeName());
  }

  /**
   * Converts an Iceberg binary or string column to its byte array representation.
   *
   * @param value value to parse
   * @param type Parquet column type
   * @param stats column stats to update
   * @param insertRowsCurrIndex Used for logging the row of index given in insertRows API
   * @return string representation
   */
  private static byte[] getBinaryValue(
      Object value, PrimitiveType type, RowBufferStats stats, final long insertRowsCurrIndex) {
    LogicalTypeAnnotation logicalTypeAnnotation = type.getLogicalTypeAnnotation();
    if (logicalTypeAnnotation == null) {
      byte[] bytes =
          DataValidationUtil.validateAndParseBinary(
              type.getName(), value, Optional.empty(), insertRowsCurrIndex);
      stats.addBinaryValue(bytes);
      return bytes;
    }
    if (logicalTypeAnnotation instanceof StringLogicalTypeAnnotation) {
      String string =
          DataValidationUtil.validateAndParseString(
              type.getName(), value, Optional.empty(), insertRowsCurrIndex);
      stats.addStrValue(string);
      return string.getBytes(StandardCharsets.UTF_8);
    }
    throw new SFException(
        ErrorCode.UNKNOWN_DATA_TYPE, logicalTypeAnnotation, type.getPrimitiveTypeName());
  }

  /**
   * Converts an Iceberg fixed length byte array column to its byte array representation.
   *
   * @param value value to parse
   * @param type Parquet column type
   * @param stats column stats to update
   * @param insertRowsCurrIndex Used for logging the row of index given in insertRows API
   * @return string representation
   */
  private static byte[] getFixedLenByteArrayValue(
      Object value, PrimitiveType type, RowBufferStats stats, final long insertRowsCurrIndex) {
    LogicalTypeAnnotation logicalTypeAnnotation = type.getLogicalTypeAnnotation();
    int length = type.getTypeLength();
    byte[] bytes = null;
    if (logicalTypeAnnotation == null) {
      bytes =
          DataValidationUtil.validateAndParseBinary(
              type.getName(), value, Optional.of(length), insertRowsCurrIndex);
      stats.addBinaryValue(bytes);
    }
    if (logicalTypeAnnotation instanceof DecimalLogicalTypeAnnotation) {
      BigInteger bigIntegerVal = getDecimalValue(value, type, insertRowsCurrIndex).unscaledValue();
      stats.addIntValue(bigIntegerVal);
      bytes = bigIntegerVal.toByteArray();
      if (bytes.length < length) {
        byte[] newBytes = new byte[length];
        Arrays.fill(newBytes, (byte) (bytes[0] < 0 ? -1 : 0));
        System.arraycopy(bytes, 0, newBytes, length - bytes.length, bytes.length);
        bytes = newBytes;
      }
    }
    if (bytes != null) {
      checkFixedLengthByteArray(bytes, length, insertRowsCurrIndex);
      return bytes;
    }
    throw new SFException(
        ErrorCode.UNKNOWN_DATA_TYPE, logicalTypeAnnotation, type.getPrimitiveTypeName());
  }

  /**
   * Parses a group value based on Parquet group logical type.
   *
   * @param value value to parse
   * @param type Parquet column type
   * @param stats column stats to update
   * @param defaultTimezone default timezone to use for timestamp parsing
   * @param insertRowsCurrIndex Used for logging the row of index given in insertRows API
   * @return list of parsed values
   */
  private static ParquetBufferValue getGroupValue(
      Object value,
      GroupType type,
      RowBufferStats stats,
      ZoneId defaultTimezone,
      final long insertRowsCurrIndex) {
    LogicalTypeAnnotation logicalTypeAnnotation = type.getLogicalTypeAnnotation();
    if (logicalTypeAnnotation == null) {
      return getStructValue(value, type, stats, defaultTimezone, insertRowsCurrIndex);
    }
    if (logicalTypeAnnotation instanceof ListLogicalTypeAnnotation) {
      return get3LevelListValue(value, type, stats, defaultTimezone, insertRowsCurrIndex);
    }
    if (logicalTypeAnnotation instanceof MapLogicalTypeAnnotation) {
      return get3LevelMapValue(value, type, stats, defaultTimezone, insertRowsCurrIndex);
    }
    throw new SFException(
        ErrorCode.UNKNOWN_DATA_TYPE, logicalTypeAnnotation, type.getClass().getSimpleName());
  }

  /**
   * Parses a struct value based on Parquet group logical type. The parsed value is a list of
   * values, where each element represents a field in the group. For example, an input {@code
   * {"field1": 1, "field2": 2}} will be parsed as {@code [1, 2]}.
   */
  private static ParquetBufferValue getStructValue(
      Object value,
      GroupType type,
      RowBufferStats stats,
      ZoneId defaultTimezone,
      final long insertRowsCurrIndex) {
    Map<String, Object> structVal =
        DataValidationUtil.validateAndParseIcebergStruct(
            type.getName(), value, insertRowsCurrIndex);
    List<Object> listVal = new ArrayList<>(type.getFieldCount());
    float estimatedParquetSize = ParquetBufferValue.DEFINITION_LEVEL_ENCODING_BYTE_LEN;
    for (int i = 0; i < type.getFieldCount(); i++) {
      ParquetBufferValue parsedValue =
          parseColumnValueToParquet(
              structVal.getOrDefault(type.getFieldName(i), null),
              type.getType(i),
              stats,
              defaultTimezone,
              insertRowsCurrIndex);
      listVal.add(parsedValue.getValue());
      estimatedParquetSize += parsedValue.getSize();
    }
    return new ParquetBufferValue(listVal, estimatedParquetSize);
  }

  /**
   * Parses an iterable value based on Parquet 3-level list logical type. Please check <a
   * href="https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#lists">Parquet
   * Logical Types#Lists</a> for more details. The parsed value is a list of lists, where each inner
   * list represents a list of elements in the group. For example, an input {@code [1, 2, 3, 4]}
   * will be parsed as {@code [[1], [2], [3], [4]]}.
   */
  private static ParquetBufferValue get3LevelListValue(
      Object value,
      GroupType type,
      RowBufferStats stats,
      ZoneId defaultTimezone,
      final long insertRowsCurrIndex) {
    Iterable<Object> iterableVal =
        DataValidationUtil.validateAndParseIcebergList(type.getName(), value, insertRowsCurrIndex);
    List<Object> listVal = new ArrayList<>();
    final AtomicReference<Float> estimatedParquetSize =
        new AtomicReference<>(ParquetBufferValue.DEFINITION_LEVEL_ENCODING_BYTE_LEN);
    iterableVal.forEach(
        element -> {
          ParquetBufferValue parsedValue =
              parseColumnValueToParquet(
                  element,
                  type.getType(0).asGroupType().getType(0),
                  stats,
                  defaultTimezone,
                  insertRowsCurrIndex);
          listVal.add(Collections.singletonList(parsedValue.getValue()));
          estimatedParquetSize.updateAndGet(sz -> sz + parsedValue.getSize());
        });
    return new ParquetBufferValue(listVal, estimatedParquetSize.get());
  }

  /**
   * Parses a map value based on Parquet 3-level map logical type. Please check <a
   * href="https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#maps">Parquet
   * Logical Types#Maps</a> for more details. The parsed value is a list of lists, where each inner
   * list represents a key-value pair in the group. For example, an input {@code {"a": 1, "b": 2}}
   * will be parsed as {@code [["a", 1], ["b", 2]]}.
   */
  private static ParquetBufferValue get3LevelMapValue(
      Object value,
      GroupType type,
      RowBufferStats stats,
      ZoneId defaultTimezone,
      final long insertRowsCurrIndex) {
    Map<Object, Object> mapVal =
        DataValidationUtil.validateAndParseIcebergMap(type.getName(), value, insertRowsCurrIndex);
    List<Object> listVal = new ArrayList<>();
    final AtomicReference<Float> estimatedParquetSize =
        new AtomicReference<>(ParquetBufferValue.DEFINITION_LEVEL_ENCODING_BYTE_LEN);
    mapVal.forEach(
        (k, v) -> {
          ParquetBufferValue parsedKey =
              parseColumnValueToParquet(
                  k,
                  type.getType(0).asGroupType().getType(0),
                  stats,
                  defaultTimezone,
                  insertRowsCurrIndex);
          ParquetBufferValue parsedValue =
              parseColumnValueToParquet(
                  v,
                  type.getType(0).asGroupType().getType(1),
                  stats,
                  defaultTimezone,
                  insertRowsCurrIndex);
          listVal.add(Arrays.asList(parsedKey.getValue(), parsedValue.getValue()));
          estimatedParquetSize.updateAndGet(sz -> sz + parsedKey.getSize() + parsedValue.getSize());
        });
    return new ParquetBufferValue(listVal, estimatedParquetSize.get());
  }

  /**
   * Converts a decimal value to its BigDecimal representation.
   *
   * @param value value to parse
   * @param type Parquet column type
   * @param insertRowsCurrIndex Used for logging the row of index given in insertRows API
   * @return BigDecimal representation
   */
  private static BigDecimal getDecimalValue(
      Object value, PrimitiveType type, final long insertRowsCurrIndex) {
    int scale = ((DecimalLogicalTypeAnnotation) type.getLogicalTypeAnnotation()).getScale();
    int precision = ((DecimalLogicalTypeAnnotation) type.getLogicalTypeAnnotation()).getPrecision();
    BigDecimal bigDecimalValue =
        DataValidationUtil.validateAndParseBigDecimal(type.getName(), value, insertRowsCurrIndex);
    bigDecimalValue = bigDecimalValue.setScale(scale, RoundingMode.HALF_UP);
    DataValidationUtil.checkValueInRange(bigDecimalValue, scale, precision, insertRowsCurrIndex);
    return bigDecimalValue;
  }

  private static int timeUnitToScale(LogicalTypeAnnotation.TimeUnit timeUnit) {
    switch (timeUnit) {
      case MILLIS:
        return 3;
      case MICROS:
        return 6;
      case NANOS:
        return 9;
      default:
        throw new SFException(
            ErrorCode.INTERNAL_ERROR, String.format("Unknown time unit: %s", timeUnit));
    }
  }
}
