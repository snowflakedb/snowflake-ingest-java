/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.xml.bind.DatatypeConverter;
import net.snowflake.client.jdbc.internal.google.common.collect.Sets;
import net.snowflake.client.jdbc.internal.snowflake.common.core.SFDate;
import net.snowflake.client.jdbc.internal.snowflake.common.core.SFTimestamp;
import net.snowflake.client.jdbc.internal.snowflake.common.core.SnowflakeDateTimeFormat;
import net.snowflake.client.jdbc.internal.snowflake.common.util.Power10;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;

/** Utility class for parsing and validating inputs based on Snowflake types */
class DataValidationUtil {
  static final int MAX_STRING_LENGTH = 16 * 1024 * 1024;
  static final int MAX_BINARY_LENGTH = 8 * 1024 * 1024;
  static final BigInteger MAX_BIGINTEGER = BigInteger.valueOf(10).pow(38);
  static final BigInteger MIN_BIGINTEGER =
      BigInteger.valueOf(-1).multiply(BigInteger.valueOf(10).pow(38));
  private static final ObjectMapper objectMapper = new ObjectMapper();
  private static final SnowflakeDateTimeFormat snowflakeDateTimeFormatter =
      SnowflakeDateTimeFormat.fromSqlFormat("auto");

  /**
   * Expects string JSON
   *
   * @param input the input data, must be able to convert to String
   */
  static String validateAndParseVariant(Object input) {
    String output;
    try {
      output = input.toString();
    } catch (Exception e) {
      throw new SFException(
          e, ErrorCode.INVALID_ROW, input, "Input column can't be convert to String.");
    }

    if (output.length() > MAX_STRING_LENGTH) {
      throw new SFException(
          ErrorCode.INVALID_ROW,
          input.toString(),
          String.format(
              "Variant too long: length=%d maxLength=%d", output.length(), MAX_STRING_LENGTH));
    }
    return output;
  }

  /**
   * Expects an Array or List object
   *
   * @param input the input data, must be able to convert to String
   */
  static String validateAndParseArray(Object input) {
    if (!input.getClass().isArray() && !(input instanceof List)) {
      throw new SFException(
          ErrorCode.INVALID_ROW, input, "Input column must be an Array or List object.");
    }

    String output;
    try {
      output = objectMapper.writeValueAsString(input);
    } catch (Exception e) {
      throw new SFException(
          e,
          ErrorCode.INVALID_ROW,
          input.toString(),
          "Input column can't be convert to a valid string");
    }

    // Throw an exception if the size is too large
    if (output.length() > MAX_STRING_LENGTH) {
      throw new SFException(
          ErrorCode.INVALID_ROW,
          input.toString(),
          String.format(
              "Array too large. length=%d maxLength=%d", output.length(), MAX_STRING_LENGTH));
    }
    return output;
  }

  /**
   * Expects string JSON or JsonNode
   *
   * @param input Must be valid string JSON or JsonNode
   */
  static String validateAndParseObject(Object input) {
    String output;
    try {
      JsonNode node = objectMapper.readTree(input.toString());
      output = node.toString();
    } catch (Exception e) {
      throw new SFException(
          e, ErrorCode.INVALID_ROW, input.toString(), "Input column can't be convert to Json");
    }

    if (output.length() > MAX_STRING_LENGTH) {
      throw new SFException(
          ErrorCode.INVALID_ROW,
          input.toString(),
          String.format(
              "Object too large. length=%d maxLength=%d", output.length(), MAX_STRING_LENGTH));
    }
    return output;
  }

  /**
   * Validates and parses input for TIMESTAMP_NTZ Snowflake type
   *
   * @param input String date in valid format or seconds past the epoch. Accepts fractional seconds
   *     with precision up to the column's scale
   * @param metadata
   * @return TimestampWrapper with epoch seconds, fractional seconds, and epoch time in the column
   *     scale
   */
  static TimestampWrapper validateAndParseTimestampNtzSb16(
      Object input, Map<String, String> metadata) {
    int scale = Integer.parseInt(metadata.get(ArrowRowBuffer.COLUMN_SCALE));
    return validateAndParseTimestampNtzSb16(input, scale);
  }

  /**
   * Validates and parses input for TIMESTAMP_NTZ Snowflake type
   *
   * @param input String date in valid format or seconds past the epoch. Accepts fractional seconds
   *     with precision up to the column's scale
   * @param scale decimal scale of timestamp 16 byte integer
   * @return TimestampWrapper with epoch seconds, fractional seconds, and epoch time in the column
   *     scale
   */
  static TimestampWrapper validateAndParseTimestampNtzSb16(Object input, int scale) {
    try {
      String valueString = getStringValue(input);

      long epoch;
      int fraction;

      SFTimestamp timestamp = snowflakeDateTimeFormatter.parse(valueString);
      // The formatter will not correctly parse numbers with decimal values
      if (timestamp != null) {
        epoch = timestamp.getSeconds().longValue();
        fraction = getFractionFromTimestamp(timestamp);
      } else {
        /*
         * Assume the data is of the form "<epoch_seconds>.<fraction of second>". For example "10.5"
         * is interpreted as meaning 10.5 seconds past the epoch. The following logic breaks out and
         * stores the epoch seconds and fractional seconds values separately
         */
        String[] items = valueString.split("\\.");
        if (items.length != 2) {
          throw new SFException(
              ErrorCode.INVALID_ROW,
              input.toString(),
              String.format("Unable to parse timestamp from value.  value=%s", valueString));
        }
        epoch = Long.parseLong(items[0]);
        int l = items.length > 1 ? items[1].length() : 0;
        /* Fraction is in nanoseconds, but Snowflake will error if the fraction gives
         * accuracy greater than the scale
         * */
        fraction = l == 0 ? 0 : Integer.parseInt(items[1]) * (l < 9 ? Power10.intTable[9 - l] : 1);
      }
      if (fraction % Power10.intTable[9 - scale] != 0) {
        throw new SFException(
            ErrorCode.INVALID_ROW,
            input.toString(),
            String.format(
                "Row specifies accuracy greater than column scale.  fraction=%d, scale=%d",
                fraction, scale));
      }
      return new TimestampWrapper(epoch, fraction, getTimeInScale(valueString, scale));
    } catch (NumberFormatException e) {
      throw new SFException(ErrorCode.INVALID_ROW, input.toString(), e.getMessage());
    }
  }

  /**
   * Given a SFTimestamp, get the number of nanoseconds since the last whole second. This
   * corresponds to the fraction component for Timestamp types
   *
   * @param input SFTimestamp
   * @return Timestamp fraction value, the number of nanoseconds since the last whole second
   */
  private static int getFractionFromTimestamp(SFTimestamp input) {
    BigDecimal epochSecondsInNanoSeconds =
        new BigDecimal(input.getSeconds().multiply(BigInteger.valueOf(Power10.intTable[9])));
    return input.getNanosSinceEpoch().subtract(epochSecondsInNanoSeconds).intValue();
  }

  /**
   * Validates and parses input for TIMESTAMP_TZ Snowflake type
   *
   * @param input TIMESTAMP_TZ in "2021-01-01 01:00:00 +0100" format
   * @param metadata Column metadata containing scale
   * @return TimestampWrapper with epoch seconds, fractional seconds, and epoch time in the column
   *     scale
   */
  static TimestampWrapper validateAndParseTimestampTz(Object input, Map<String, String> metadata) {
    int scale = Integer.parseInt(metadata.get(ArrowRowBuffer.COLUMN_SCALE));
    return validateAndParseTimestampTz(input, scale);
  }

  /**
   * Validates and parses input for TIMESTAMP_TZ Snowflake type
   *
   * @param input TIMESTAMP_TZ in "2021-01-01 01:00:00 +0100" format
   * @param scale decimal scale of timestamp 16 byte integer
   * @return TimestampWrapper with epoch seconds, fractional seconds, and epoch time in the column
   *     scale
   */
  static TimestampWrapper validateAndParseTimestampTz(Object input, int scale) {
    try {
      if (input instanceof String) {
        String stringInput = (String) input;
        SFTimestamp timestamp = snowflakeDateTimeFormatter.parse(stringInput);
        if (timestamp == null) {
          throw new SFException(
              ErrorCode.INVALID_ROW, input, "Cannot be converted to a timestamp_tz value");
        }

        int fraction = getFractionFromTimestamp(timestamp);

        BigDecimal epochTimeAtSecondsScale =
            timestamp.getNanosSinceEpoch().divide(BigDecimal.valueOf(Power10.intTable[9]));
        if (fraction % Power10.intTable[9 - scale] != 0) {
          throw new SFException(
              ErrorCode.INVALID_ROW,
              input.toString(),
              String.format(
                  "Row specifies accuracy greater than column scale.  fraction=%d, scale=%d",
                  fraction, scale));
        }
        TimestampWrapper wrapper =
            new TimestampWrapper(
                timestamp.getSeconds().longValue(),
                fraction,
                getTimeInScale(getStringValue(epochTimeAtSecondsScale), scale),
                timestamp);
        return wrapper;
      } else {
        throw new SFException(
            ErrorCode.INVALID_ROW, input, "Cannot be converted to a timestamp_tz value");
      }
    } catch (NumberFormatException e) {
      throw new SFException(ErrorCode.INVALID_ROW, input.toString(), e.getMessage());
    }
  }

  /**
   * Validates the input is less than the supposed maximum allowed string
   *
   * @param input Object to validate and parse to String
   * @param maxLengthOptional Maximum allowed length of the output String, if empty then uses
   *     maximum allowed by Snowflake
   *     (https://docs.snowflake.com/en/sql-reference/data-types-text.html#varchar)
   */
  static String validateAndParseString(Object input, Optional<Integer> maxLengthOptional) {
    int maxLength = maxLengthOptional.orElse(MAX_STRING_LENGTH);
    String output = getStringValue(input);
    if (output.length() > maxLength) {
      throw new SFException(
          ErrorCode.INVALID_ROW,
          input.toString(),
          String.format("String too long: length=%d maxLength=%d", output.length(), maxLength));
    }
    return output;
  }

  /**
   * Returns a BigDecimal representation of the input. Strings of the form "1.23E4" will be treated
   * as being written in * scientific notation (e.g. 1.23 * 10^4)
   *
   * @param input
   */
  static BigDecimal validateAndParseBigDecimal(Object input) {
    BigDecimal output;
    try {
      if (input instanceof BigDecimal) {
        output = (BigDecimal) input;
      } else if (input instanceof BigInteger) {
        output = new BigDecimal((BigInteger) input);
      } else if (input instanceof String) {
        output = stringToBigDecimal((String) input);
      } else {
        output = stringToBigDecimal((input.toString()));
      }
      if (output.toBigInteger().compareTo(MAX_BIGINTEGER) >= 0
          || output.toBigInteger().compareTo(MIN_BIGINTEGER) <= 0) {
        throw new SFException(
            ErrorCode.INVALID_ROW,
            input.toString(),
            String.format(
                "Number out of representable range, Max=%s, Min=%s",
                MIN_BIGINTEGER, MAX_BIGINTEGER));
      }
      return output;
    } catch (NumberFormatException e) {
      throw new SFException(ErrorCode.INVALID_ROW, input.toString(), e.getMessage());
    }
  }

  static BigDecimal handleScientificNotationError(String input, NumberFormatException e) {
    // Support scientific notation on string input
    String[] splitInput = input.toLowerCase().split("e");
    if (splitInput.length != 2) {
      throw e;
    }
    return (new BigDecimal(splitInput[0]))
        .multiply(BigDecimal.valueOf(10).pow(Integer.parseInt(splitInput[1])));
  }

  static BigDecimal stringToBigDecimal(String input) {
    try {
      return new BigDecimal(input);
    } catch (NumberFormatException e) {
      return handleScientificNotationError(input, e);
    }
  }

  /**
   * Validates the input represents a valid NUMBER(38,0) and returns a BigInteger. Accepts Java
   * number types and strings. Strings of the form "1.23E4" will be treated as being written in
   * scientific notation (e.g. 1.23 * 10^4)
   *
   * @param input
   */
  static BigInteger validateAndParseBigInteger(Object input) {
    try {
      if (input instanceof Double) {
        checkInteger((double) input);
        return new BigDecimal(input.toString()).toBigInteger();
      } else if (input instanceof Float) {
        checkInteger((float) input);
        return new BigDecimal(input.toString()).toBigInteger();
      }
      BigInteger output;
      if (input instanceof BigInteger) {
        output = (BigInteger) input;
      } else if (input instanceof String) {
        BigDecimal bigDecimal = stringToBigDecimal((String) input);
        if (bigDecimal.stripTrailingZeros().scale() <= 0) {
          return bigDecimal.toBigInteger();
        } else {
          throw new SFException(ErrorCode.INVALID_ROW, input.toString(), "Invalid integer");
        }
      } else {
        output = new BigInteger(input.toString());
      }
      if (output.compareTo(MAX_BIGINTEGER) >= 0 || output.compareTo(MIN_BIGINTEGER) <= 0) {
        throw new SFException(
            ErrorCode.INVALID_ROW,
            input.toString(),
            String.format(
                "Number out of representable range, Max=%s, Min=%s",
                MIN_BIGINTEGER, MAX_BIGINTEGER));
      }
      return output;
    } catch (NumberFormatException e) {
      throw new SFException(ErrorCode.INVALID_ROW, input.toString(), e.getMessage());
    }
  }

  static void checkInteger(float input) {
    if (Math.floor(input) != input) {
      throw new SFException(ErrorCode.INVALID_ROW, input, "Value must be integer");
    }
  }

  static void checkInteger(double input) {
    if (Math.floor(input) != input) {
      throw new SFException(ErrorCode.INVALID_ROW, input, "Value must be integer");
    }
  }

  /**
   * Validates input is an integer @see {@link #validateAndParseInteger(Object)} and that it does
   * not exceed the MIN/MAX Short value
   *
   * @param input
   */
  static short validateAndParseShort(Object input) {
    Integer intValue = validateAndParseInteger(input);
    if (intValue > Short.MAX_VALUE || intValue < Short.MIN_VALUE) {
      throw new SFException(
          ErrorCode.INVALID_ROW,
          input.toString(),
          String.format(
              "Value exceeds min/max short.  min=%s, max%s", Short.MIN_VALUE, Short.MAX_VALUE));
    }
    return intValue.shortValue();
  }

  /**
   * Validates input is an integer @see {@link #validateAndParseInteger(Object)} and that it does
   * not exceed the MIN/MAX Byte value
   *
   * @param input
   */
  static byte validateAndParseByte(Object input) {
    Integer intValue = validateAndParseInteger(input);
    if (intValue > Byte.MAX_VALUE || intValue < Byte.MIN_VALUE) {
      throw new SFException(
          ErrorCode.INVALID_ROW,
          input.toString(),
          String.format(
              "Value exceeds min/max byte.  min=%s, max%s", Byte.MIN_VALUE, Byte.MAX_VALUE));
    }
    return intValue.byteValue();
  }

  /**
   * Validates the input can be represented as an integer with value between Integer.MIN_VALUE and
   * Integer.MAX_VALUE
   *
   * @param input
   */
  static int validateAndParseInteger(Object input) {
    try {
      if (input instanceof Integer) {
        return (int) input;
      } else if (input instanceof Long) {
        if ((long) input > Integer.MAX_VALUE || (long) input < Integer.MIN_VALUE) {
          throw new SFException(
              ErrorCode.INVALID_ROW, input.toString(), "Value greater than max integer");
        }
        return ((Long) input).intValue();
      } else if (input instanceof Double) {
        // Number must be integer
        checkInteger((double) input);
        if (((Number) input).longValue() > Integer.MAX_VALUE
            || ((Number) input).longValue() < Integer.MIN_VALUE) {
          throw new SFException(
              ErrorCode.INVALID_ROW, input.toString(), "Value greater than max integer");
        }
        return ((Number) input).intValue();
      } else if (input instanceof Float) {
        // Number must be integer
        checkInteger((float) input);

        if (((Number) input).longValue() > Integer.MAX_VALUE
            || ((Number) input).longValue() < Integer.MIN_VALUE) {
          throw new SFException(
              ErrorCode.INVALID_ROW, input.toString(), "Value greater than max integer");
        }
        return ((Number) input).intValue();
      } else if (input instanceof BigInteger) {
        if (((BigInteger) input).compareTo(BigInteger.valueOf(Integer.MAX_VALUE)) > 0
            || ((BigInteger) input).compareTo(BigInteger.valueOf(Integer.MIN_VALUE)) < 0) {
          throw new SFException(
              ErrorCode.INVALID_ROW, input.toString(), "Value greater than max integer");
        }
        return ((BigInteger) input).intValue();
      } else {
        try {
          return Integer.parseInt(input.toString());
        } catch (NumberFormatException e) {
          Double doubleValue = handleScientificNotationError(input.toString(), e).doubleValue();
          if (Math.floor(doubleValue) == doubleValue) {
            return doubleValue.intValue();
          } else {
            throw new SFException(ErrorCode.INVALID_ROW, input.toString(), "Value must be integer");
          }
        }
      }
    } catch (NumberFormatException err) {
      throw new SFException(ErrorCode.INVALID_ROW, input.toString(), err.getMessage());
    }
  }

  /**
   * Validates the input can be represented as an integer with value between Long.MIN_VALUE and
   * Long.MAX_VALUE
   *
   * @param input
   */
  static long validateAndParseLong(Object input) {
    try {
      if (input instanceof Integer) {
        return Long.valueOf((int) input);
      } else if (input instanceof Long) {
        return (long) input;
      } else if (input instanceof Double) {
        // Number must be integer
        checkInteger((double) input);
        BigInteger bigInt = validateAndParseBigInteger(input);
        if (bigInt.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) > 0
            || bigInt.compareTo(BigInteger.valueOf(Long.MIN_VALUE)) < 0) {
          throw new SFException(
              ErrorCode.INVALID_ROW, input.toString(), "Value greater than max long");
        }
        return ((Number) input).longValue();
      } else if (input instanceof Float) {
        // Number must be integer
        checkInteger((float) input);
        BigInteger bigInt = validateAndParseBigInteger(input);
        if (bigInt.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) > 0
            || bigInt.compareTo(BigInteger.valueOf(Long.MIN_VALUE)) < 0) {
          throw new SFException(
              ErrorCode.INVALID_ROW, input.toString(), "Value greater than max long");
        }
        return ((Number) input).longValue();
      } else if (input instanceof BigInteger) {
        if (((BigInteger) input).compareTo(BigInteger.valueOf(Long.MAX_VALUE)) > 0
            || ((BigInteger) input).compareTo(BigInteger.valueOf(Long.MIN_VALUE)) < 0) {
          throw new SFException(
              ErrorCode.INVALID_ROW, input.toString(), "Value greater than max long");
        }
        return ((BigInteger) input).longValue();
      } else {
        try {
          return Long.parseLong(input.toString());
        } catch (NumberFormatException e) {
          Double doubleValue = handleScientificNotationError(input.toString(), e).doubleValue();
          if (Math.floor(doubleValue) == doubleValue) {
            return doubleValue.longValue();
          } else {
            throw new SFException(ErrorCode.INVALID_ROW, input.toString(), "Value must be integer");
          }
        }
      }
    } catch (NumberFormatException err) {
      throw new SFException(ErrorCode.INVALID_ROW, input.toString(), err.getMessage());
    }
  }

  /**
   * Expects input in epoch days or "YYYY-MM-DD" format. Uses @see {@link
   * #validateAndParseInteger(Object)} to validate and parse input as integer
   *
   * @param input epoch days or "YYYY-MM-DD" date format
   */
  static int validateAndParseDate(Object input) {
    try {
      return validateAndParseInteger(input);
    } catch (SFException e) {
      // Try parsing the time from a String
      Optional<SFTimestamp> timestamp =
          Optional.ofNullable(snowflakeDateTimeFormatter.parse(input.toString()));
      return timestamp
          .map(
              t ->
                  // Adjust nanoseconds past the epoch to match the column scale value
                  (int) TimeUnit.MILLISECONDS.toDays(SFDate.fromTimestamp(t).getTime()))
          .orElseThrow(
              () ->
                  new SFException(
                      ErrorCode.INVALID_ROW, input, "Cannot be converted to a Date value"));
    }
  }

  static byte[] validateAndParseBinary(Object input, Optional<Integer> maxLengthOptional) {
    byte[] output;
    if (input instanceof byte[]) {
      output = (byte[]) input;
    } else {
      try {
        output = DatatypeConverter.parseHexBinary(input.toString());
      } catch (IllegalArgumentException e) {
        throw new SFException(ErrorCode.INVALID_ROW, input, e.getMessage());
      }
    }

    int maxLength = maxLengthOptional.orElse(MAX_BINARY_LENGTH);
    if (output.length > maxLength) {
      throw new SFException(
          ErrorCode.INVALID_ROW,
          input.toString(),
          String.format("Binary too long: length=%d maxLength=%d", output.length, maxLength));
    }
    return output;
  }

  /**
   * @param input Seconds past the epoch. or String Time representation
   * @param metadata
   */
  static BigInteger validateAndParseTime(Object input, Map<String, String> metadata) {
    int scale = Integer.parseInt(metadata.get(ArrowRowBuffer.COLUMN_SCALE));
    return getTimeInScale(getStringValue(input), scale);
  }

  /**
   * Returns the time past the epoch in the given scale.
   *
   * @param value
   * @param scale Value between 0 and 9. For seconds scale = 0, for milliseconds scale = 3, for
   *     nanoseconds scale = 9
   */
  static BigInteger getTimeInScale(String value, int scale) {
    try {
      BigDecimal decVal = new BigDecimal(value);
      BigDecimal epochScale = decVal.multiply(BigDecimal.valueOf(Power10.intTable[scale]));
      return epochScale.toBigInteger();
    } catch (NumberFormatException e) {
      // Try parsing the time from a String
      Optional<SFTimestamp> timestamp =
          Optional.ofNullable(snowflakeDateTimeFormatter.parse(value));
      return timestamp
          .map(
              t ->
                  // Adjust nanoseconds past the epoch to match the column scale value
                  t.getNanosSinceEpoch()
                      .divide(BigDecimal.valueOf(Power10.intTable[9 - scale]))
                      .toBigInteger())
          .orElseThrow(
              () ->
                  new SFException(
                      ErrorCode.INVALID_ROW, value, "Cannot be converted to a time value"));
    }
  }

  static String getStringValue(Object value) {
    String stringValue;

    if (value instanceof String) {
      stringValue = (String) value;
    } else if (value instanceof BigDecimal) {
      stringValue = ((BigDecimal) value).toPlainString();
    } else {
      stringValue = value.toString();
    }
    return stringValue;
  }

  /**
   * Converts input to double value.
   *
   * @param input
   */
  static double validateAndParseReal(Object input) {
    double doubleValue;
    try {
      if (input instanceof Double) {
        doubleValue = (double) input;
      } else if (input instanceof Float) {
        doubleValue = Double.parseDouble(input.toString());
      } else if (input instanceof BigDecimal) {
        doubleValue = ((BigDecimal) input).doubleValue();
      } else {
        doubleValue = Double.parseDouble((String) input);
      }
    } catch (NumberFormatException err) {
      throw new SFException(ErrorCode.INVALID_ROW, input.toString());
    }

    return doubleValue;
  }

  /**
   * Validate and parse input to integer output, 1=true, 0=false. String values converted to boolean
   * according to https://docs.snowflake.com/en/sql-reference/functions/to_boolean.html#usage-notes
   *
   * @param input
   * @return 1 or 0 where 1=true, 0=false
   */
  static int validateAndParseBoolean(Object input) {
    int output = 0;
    if (input instanceof Boolean) {
      output = (boolean) input ? 1 : 0;
    } else if (input instanceof Number) {
      output = ((Number) input).doubleValue() > 0 ? 1 : 0;
    } else {
      output = convertStringToBoolean((String) input) ? 1 : 0;
    }
    return output;
  }

  static Set<String> allowedBooleanStringsLowerCased =
      Sets.newHashSet("1", "0", "yes", "no", "y", "n", "t", "f", "true", "false", "on", "off");

  static boolean convertStringToBoolean(String value) {
    String lowerCasedValue = value.toLowerCase();
    if (!allowedBooleanStringsLowerCased.contains(lowerCasedValue)) {
      throw new SFException(ErrorCode.INVALID_ROW, value);
    }
    return "1".equals(lowerCasedValue)
        || "yes".equals(lowerCasedValue)
        || "y".equals(lowerCasedValue)
        || "t".equals(lowerCasedValue)
        || "true".equals(lowerCasedValue)
        || "on".equals(lowerCasedValue);
  }
}
