/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import static net.snowflake.client.jdbc.internal.snowflake.common.core.SnowflakeDateTimeFormat.DATE;
import static net.snowflake.client.jdbc.internal.snowflake.common.core.SnowflakeDateTimeFormat.TIMESTAMP;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
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

  private static final TimeZone DEFAULT_TIMEZONE =
      TimeZone.getTimeZone("America/Los_Angeles"); // default value of TIMEZONE system parameter
  private static final TimeZone GMT = TimeZone.getTimeZone("GMT");

  private static final ObjectMapper objectMapper = new ObjectMapper();

  /**
   * Creates a new SnowflakeDateTimeFormat. In order to avoid SnowflakeDateTimeFormat's
   * synchronization blocks, we create a new instance when needed instead of sharing one instance.
   */
  private static SnowflakeDateTimeFormat createDateTimeFormatter() {
    return SnowflakeDateTimeFormat.fromSqlFormat("auto");
  }

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
   * Validates and parses input for TIMESTAMP_NTZ or TIMESTAMP_LTZ Snowflake type
   *
   * @param input String date in valid format or seconds past the epoch. Accepts fractional seconds
   *     with precision up to the column's scale
   * @param scale decimal scale of timestamp 16 byte integer
   * @param ignoreTimezone Must be true for TIMESTAMP_NTZ
   * @return TimestampWrapper with epoch seconds, fractional seconds, and epoch time in the column
   *     scale
   */
  static TimestampWrapper validateAndParseTimestampNtzSb16(
      Object input, int scale, boolean ignoreTimezone) {
    String valueString;
    if (input instanceof String) valueString = (String) input;
    else if (input instanceof LocalDate) valueString = input.toString();
    else if (input instanceof LocalDateTime) valueString = input.toString();
    else if (input instanceof ZonedDateTime)
      valueString =
          ignoreTimezone
              ? ((ZonedDateTime) input).toLocalDateTime().toString()
              : DateTimeFormatter.ISO_DATE_TIME.format(((ZonedDateTime) input).toOffsetDateTime());
    else if (input instanceof OffsetDateTime)
      valueString =
          ignoreTimezone
              ? ((OffsetDateTime) input).toLocalDateTime().toString()
              : DateTimeFormatter.ISO_DATE_TIME.format((OffsetDateTime) input);
    else
      throw new SFException(
          ErrorCode.INVALID_ROW, input.toString(), "Data type not supported for timestamp");

    SnowflakeDateTimeFormat snowflakeDateTimeFormatter = createDateTimeFormatter();
    TimeZone effectiveTimeZone = ignoreTimezone ? GMT : DEFAULT_TIMEZONE;
    SFTimestamp timestamp =
        snowflakeDateTimeFormatter.parse(
            valueString, effectiveTimeZone, 0, DATE | TIMESTAMP, ignoreTimezone, null);
    if (timestamp != null) {
      long epoch = timestamp.getSeconds().longValue();
      int fraction = getFractionFromTimestamp(timestamp) / Power10.intTable[9 - scale];
      BigInteger timeInScale =
          BigInteger.valueOf(epoch)
              .multiply(Power10.sb16Table[scale])
              .add(BigInteger.valueOf(fraction));
      return new TimestampWrapper(epoch, fraction, timeInScale);
    } else {
      throw new SFException(
          ErrorCode.INVALID_ROW, input.toString(), "Value cannot be converted to timestamp");
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
   * @param scale decimal scale of timestamp 16 byte integer
   * @return TimestampWrapper with epoch seconds, fractional seconds, and epoch time in the column
   *     scale
   */
  static TimestampWrapper validateAndParseTimestampTz(Object input, int scale) {

    String stringInput;
    if (input instanceof String) stringInput = (String) input;
    else if (input instanceof LocalDate) stringInput = input.toString();
    else if (input instanceof LocalDateTime) stringInput = input.toString();
    else if (input instanceof ZonedDateTime)
      stringInput =
          DateTimeFormatter.ISO_DATE_TIME.format(((ZonedDateTime) input).toOffsetDateTime());
    else if (input instanceof OffsetDateTime)
      stringInput = DateTimeFormatter.ISO_DATE_TIME.format((OffsetDateTime) input);
    else
      throw new SFException(
          ErrorCode.INVALID_ROW, input.toString(), "Data type not supported for timestamp");

    SnowflakeDateTimeFormat snowflakeDateTimeFormatter = createDateTimeFormatter();

    SFTimestamp timestamp =
        snowflakeDateTimeFormatter.parse(
            stringInput, DEFAULT_TIMEZONE, 0, DATE | TIMESTAMP, false, null);
    if (timestamp != null) {
      long epoch = timestamp.getSeconds().longValue();
      int fraction = getFractionFromTimestamp(timestamp) / Power10.intTable[9 - scale];
      return new TimestampWrapper(
          epoch,
          fraction,
          BigInteger.valueOf(epoch * Power10.intTable[scale] + fraction),
          timestamp);
    } else {
      throw new SFException(
          ErrorCode.INVALID_ROW, input, "Cannot be converted to a timestamp_tz value");
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

  /** Returns the number of days between the epoch and the passed date. */
  static int validateAndParseDate(Object input) {
    String inputString;
    if (input instanceof String) {
      inputString = (String) input;
    } else if (input instanceof LocalDate) {
      inputString = input.toString();
    } else if (input instanceof LocalDateTime) {
      inputString = ((LocalDateTime) input).toLocalDate().toString();
    } else if (input instanceof ZonedDateTime) {
      inputString = ((ZonedDateTime) input).toLocalDate().toString();
    } else if (input instanceof OffsetDateTime) {
      inputString = ((OffsetDateTime) input).toLocalDate().toString();
    } else {
      throw cannotConvertException(input, "date");
    }

    SFTimestamp timestamp =
        createDateTimeFormatter().parse(inputString, GMT, 0, DATE | TIMESTAMP, true, null);
    if (timestamp == null)
      throw new SFException(ErrorCode.INVALID_ROW, input, "Cannot be converted to a Date value");

    return (int) TimeUnit.MILLISECONDS.toDays(SFDate.fromTimestamp(timestamp).getTime());
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
   * Returns the number of units since 00:00, depending on the scale (scale=0: seconds, scale=3:
   * milliseconds, scale=9: nanoseconds
   */
  static BigInteger validateAndParseTime(Object input, int scale) {
    String stringInput;
    if (input instanceof String) {
      stringInput = (String) input;
    } else if (input instanceof LocalTime) {
      stringInput = input.toString();
    } else if (input instanceof OffsetTime) {
      stringInput = ((OffsetTime) input).toLocalTime().toString();
    } else {
      throw cannotConvertException(input, "time");
    }

    SFTimestamp timestamp =
        createDateTimeFormatter()
            .parse(stringInput, GMT, 0, SnowflakeDateTimeFormat.TIME, true, null);
    if (timestamp == null) {
      throw new SFException(ErrorCode.INVALID_ROW, input, "Cannot be converted to a time value");
    } else {
      return timestamp
          .getNanosSinceEpoch()
          .divide(BigDecimal.valueOf(Power10.intTable[9 - scale]))
          .toBigInteger()
          .mod(BigInteger.valueOf(24L * 60 * 60 * Power10.intTable[scale]));
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
    if (input instanceof Boolean) {
      return (boolean) input ? 1 : 0;
    } else if (input instanceof Number) {
      return new BigDecimal(input.toString()).compareTo(BigDecimal.ZERO) == 0 ? 0 : 1;
    } else if (input instanceof String) {
      return convertStringToBoolean((String) input) ? 1 : 0;
    }

    throw cannotConvertException(input, "boolean");
  }

  static Set<String> allowedBooleanStringsLowerCased =
      Sets.newHashSet("1", "0", "yes", "no", "y", "n", "t", "f", "true", "false", "on", "off");

  private static boolean convertStringToBoolean(String value) {
    String lowerCasedValue = value.toLowerCase();
    if (!allowedBooleanStringsLowerCased.contains(lowerCasedValue)) {
      throw cannotConvertException(value, "boolean");
    }
    return "1".equals(lowerCasedValue)
        || "yes".equals(lowerCasedValue)
        || "y".equals(lowerCasedValue)
        || "t".equals(lowerCasedValue)
        || "true".equals(lowerCasedValue)
        || "on".equals(lowerCasedValue);
  }

  /**
   * Create exception when value cannot be ingested into column of a specific type.
   *
   * @param value Invalid value causing the exception
   * @param type Snowflake column type
   */
  private static SFException cannotConvertException(Object value, String type) {
    return new SFException(
        ErrorCode.INVALID_ROW,
        sanitizeValueForExceptionMessage(value),
        String.format("Value cannot be converted to %s", type));
  }

  /**
   * Before passing a value to an exception string, we should limit how many characters are
   * displayed. Important especially for "value exceeds max column size" exceptions.
   *
   * @param value Value to adapt for exception message
   */
  private static String sanitizeValueForExceptionMessage(Object value) {
    int maxSize = 20;
    String valueString = value.toString();
    return valueString.length() <= maxSize ? valueString : valueString.substring(0, 20) + "...";
  }
}
