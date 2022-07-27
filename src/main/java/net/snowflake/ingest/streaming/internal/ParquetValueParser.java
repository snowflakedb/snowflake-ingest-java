package net.snowflake.ingest.streaming.internal;

import static java.math.RoundingMode.UNNECESSARY;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import javax.annotation.Nullable;
import net.snowflake.client.jdbc.internal.snowflake.common.util.Power10;
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
    Utils.assertNotNull("Arrow column stats", stats);
    float size = 0F;
    if (value != null) {
      AbstractRowBuffer.ColumnLogicalType logicalType =
          AbstractRowBuffer.ColumnLogicalType.valueOf(columnMetadata.getLogicalType());
      AbstractRowBuffer.ColumnPhysicalType physicalType =
          AbstractRowBuffer.ColumnPhysicalType.valueOf(columnMetadata.getPhysicalType());
      switch (typeName) {
        case BOOLEAN:
          int intValue = DataValidationUtil.validateAndParseBoolean(value);
          value = intValue > 0;
          stats.addIntValue(BigInteger.valueOf(intValue));
          size = 1;
          break;
        case INT32:
          int intVal = getInt32Value(value, columnMetadata.getScale(), logicalType);
          value = intVal;
          stats.addIntValue(BigInteger.valueOf(intVal));
          size = 4;
          break;
        case INT64:
          long longValue = getInt64Value(value, columnMetadata.getScale(), logicalType);
          value = longValue;
          stats.addIntValue(BigInteger.valueOf(longValue));
          size = 8;
          break;
        case DOUBLE:
          double doubleValue = DataValidationUtil.validateAndParseReal(value);
          value = doubleValue;
          stats.addRealValue(doubleValue);
          size = 8;
          break;
        case BINARY:
          String str = getBinaryValue(value, stats, columnMetadata);
          value = str;
          size = str.getBytes().length;
          break;
        case FIXED_LEN_BYTE_ARRAY:
          BigInteger intRep = getSb16Value(value, columnMetadata.getScale(), logicalType);
          stats.addIntValue(intRep);
          value = getSb16Bytes(intRep);
          size += 16;
          break;
        default:
          throw new SFException(ErrorCode.UNKNOWN_DATA_TYPE, logicalType, physicalType);
      }
    } else {
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
   * @param logicalType Snowflake logical type
   * @return parsed int32 value
   */
  private static int getInt32Value(
      Object value, @Nullable Integer scale, AbstractRowBuffer.ColumnLogicalType logicalType) {
    int intVal;
    switch (logicalType) {
      case DATE:
        intVal = DataValidationUtil.validateAndParseDate(value);
        break;
      case TIME:
        Utils.assertNotNull("Unexpected null scale for TIME data type", scale);
        intVal =
            DataValidationUtil.getTimeInScale(DataValidationUtil.getStringValue(value), scale)
                .intValue();
        break;
      default:
        intVal = DataValidationUtil.validateAndParseInteger(value);
        break;
    }
    return intVal;
  }

  /**
   * Parses an int64 value based on Snowflake logical type.
   *
   * @param value column value provided by user in a row
   * @param scale data type scale
   * @param logicalType Snowflake logical type
   * @return parsed int64 value
   */
  private static long getInt64Value(
      Object value, int scale, AbstractRowBuffer.ColumnLogicalType logicalType) {
    long longValue;
    switch (logicalType) {
      case TIME:
      case TIMESTAMP_LTZ:
      case TIMESTAMP_NTZ:
        longValue =
            DataValidationUtil.getTimeInScale(DataValidationUtil.getStringValue(value), scale)
                .longValue();
        break;
      case TIMESTAMP_TZ:
        longValue =
            DataValidationUtil.validateAndParseTimestampTz(value, scale)
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
      default:
        longValue = DataValidationUtil.validateAndParseLong(value);
        break;
    }
    return longValue;
  }

  /**
   * Parses an int128 value based on Snowflake logical type.
   *
   * @param value column value provided by user in a row
   * @param scale data type scale
   * @param logicalType Snowflake logical type
   * @return parsed int64 value
   */
  private static BigInteger getSb16Value(
      Object value, int scale, AbstractRowBuffer.ColumnLogicalType logicalType) {
    switch (logicalType) {
      case TIMESTAMP_TZ:
        return DataValidationUtil.validateAndParseTimestampTz(value, scale)
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
        return DataValidationUtil.validateAndParseTimestampNtzSb16(value, scale).getTimeInScale();
      default:
        BigDecimal bigDecimalValue = DataValidationUtil.validateAndParseBigDecimal(value);
        // explicitly match the BigDecimal input scale with the Snowflake data type scale
        bigDecimalValue = bigDecimalValue.setScale(scale, UNNECESSARY);
        return bigDecimalValue.multiply(BigDecimal.valueOf(Power10.intTable[scale])).toBigInteger();
    }
  }

  /**
   * Converts an int128 value to its byte array representation.
   *
   * @param intRep int128 value
   * @return byte array representation
   */
  private static byte[] getSb16Bytes(BigInteger intRep) {
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
   * Converts an object, string or binary value to its byte array representation.
   *
   * @param value value to parse
   * @param stats column stats to update
   * @param columnMetadata column metadata
   * @return string (byte array) representation
   */
  private static String getBinaryValue(
      Object value, RowBufferStats stats, ColumnMetadata columnMetadata) {
    AbstractRowBuffer.ColumnLogicalType logicalType =
        AbstractRowBuffer.ColumnLogicalType.valueOf(columnMetadata.getLogicalType());
    String str;
    if (logicalType.isObject()) {
      str =
          logicalType == AbstractRowBuffer.ColumnLogicalType.OBJECT
              ? DataValidationUtil.validateAndParseObject(value)
              : DataValidationUtil.validateAndParseVariant(value);
    } else if (logicalType == AbstractRowBuffer.ColumnLogicalType.BINARY) {
      String maxLengthString = columnMetadata.getLength().toString();
      byte[] bytes =
          DataValidationUtil.validateAndParseBinary(
              value,
              Optional.of(maxLengthString)
                  .map(s -> DataValidationUtil.validateAndParseInteger(maxLengthString)));
      str = new String(bytes, StandardCharsets.UTF_8);
      stats.addStrValue(str);
    } else {
      String maxLengthString = columnMetadata.getLength().toString();
      str =
          DataValidationUtil.validateAndParseString(
              value,
              Optional.of(maxLengthString)
                  .map(s -> DataValidationUtil.validateAndParseInteger(maxLengthString)));
      stats.addStrValue(str);
    }
    return str;
  }
}
