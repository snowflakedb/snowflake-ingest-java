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
  public static final int BYTES_8_MB = 8 * 1024 * 1024;
  public static final int BYTES_16_MB = 2 * BYTES_8_MB;

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

    if (output.length() > BYTES_16_MB) {
      throw new SFException(
          ErrorCode.INVALID_ROW,
          input.toString(),
          String.format("Variant too long: length=%d maxLength=%d", output.length(), BYTES_16_MB));
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
    if (output.length() > BYTES_16_MB) {
      throw new SFException(
          ErrorCode.INVALID_ROW,
          input.toString(),
          String.format("Array too large. length=%d maxLength=%d", output.length(), BYTES_16_MB));
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

    if (output.length() > BYTES_16_MB) {
      throw new SFException(
          ErrorCode.INVALID_ROW,
          input.toString(),
          String.format("Object too large. length=%d maxLength=%d", output.length(), BYTES_16_MB));
    }
    return output;
  }

  /**
   * Validates and parses input for TIMESTAMP_NTZ or TIMESTAMP_LTZ Snowflake type. Allowed Java
   * types:
   *
   * <ul>
   *   <li>String
   *   <li>LocalDate
   *   <li>LocalDateTime
   *   <li>OffsetDateTime
   *   <li>ZonedDateTime
   * </ul>
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
      throw typeNotAllowedException(
          input.getClass(),
          "TIMESTAMP",
          new String[] {"String", "LocalDate", "LocalDateTime", "ZonedDateTime", "OffsetDateTime"});

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
      throw valueFormatNotAllowedException(
          input.toString(),
          "TIMESTAMP",
          "Not a valid timestamp, see"
              + " https://docs.snowflake.com/en/user-guide/date-time-input-output.html#timestamp-formats"
              + " for the list of supported formats");
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
   * Validates and parses input for TIMESTAMP_TZ Snowflake type. Allowed Java types:
   *
   * <ul>
   *   <li>String
   *   <li>LocalDate
   *   <li>LocalDateTime
   *   <li>OffsetDateTime
   *   <li>ZonedDateTime
   * </ul>
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
      throw typeNotAllowedException(
          input.getClass(),
          "TIMESTAMP",
          new String[] {"String", "LocalDate", "LocalDateTime", "ZonedDateTime", "OffsetDateTime"});

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
      throw valueFormatNotAllowedException(
          input.toString(),
          "TIMESTAMP",
          "Not a valid timestamp, see"
              + " https://docs.snowflake.com/en/user-guide/date-time-input-output.html#timestamp-formats"
              + " for the list of supported formats");
    }
  }

  /**
   * Converts input to string, validates that length is less than max allowed string size
   * https://docs.snowflake.com/en/sql-reference/data-types-text.html#varchar. Allowed data types:
   *
   * <ul>
   *   <li>String
   *   <li>Number
   *   <li>boolean
   *   <li>char
   * </ul>
   *
   * @param input Object to validate and parse to String
   * @param maxLengthOptional Maximum allowed length of the output String, if empty then uses
   *     maximum allowed by Snowflake
   *     (https://docs.snowflake.com/en/sql-reference/data-types-text.html#varchar)
   */
  static String validateAndParseString(Object input, Optional<Integer> maxLengthOptional) {
    String output;
    if (input instanceof String) {
      output = (String) input;
    } else if (input instanceof Number) {
      output = new BigDecimal(input.toString()).stripTrailingZeros().toPlainString();
    } else if (input instanceof Boolean || input instanceof Character) {
      output = input.toString();
    } else {
      throw typeNotAllowedException(
          input.getClass(), "STRING", new String[] {"String", "Number", "boolean", "char"});
    }
    int maxLength = maxLengthOptional.orElse(BYTES_16_MB);

    if (output.length() > maxLength) {
      throw valueFormatNotAllowedException(
          input,
          "STRING",
          String.format("String too long: length=%d maxLength=%d", output.length(), maxLength));
    }
    return output;
  }
  /**
   * Returns a BigDecimal representation of the input. Strings of the form "1.23E4" will be treated
   * as being written in * scientific notation (e.g. 1.23 * 10^4). Does not perform any size
   * validation. Allowed Java types:
   * <li>byte, short, int, long
   * <li>float, double
   * <li>BigInteger, BigDecimal
   * <li>String
   */
  static BigDecimal validateAndParseBigDecimal(Object input) {
    if (input instanceof BigDecimal) {
      return (BigDecimal) input;
    } else if (input instanceof BigInteger) {
      return new BigDecimal((BigInteger) input);
    } else if (input instanceof Byte
        || input instanceof Short
        || input instanceof Integer
        || input instanceof Long) {
      return BigDecimal.valueOf(((Number) input).longValue());
    } else if (input instanceof Float || input instanceof Double) {
      return BigDecimal.valueOf(((Number) input).doubleValue());
    } else if (input instanceof String) {
      try {
        return new BigDecimal((String) input);
      } catch (NumberFormatException e) {
        throw valueFormatNotAllowedException(input, "NUMBER", "Not a valid number");
      }
    } else {
      throw typeNotAllowedException(
          input.getClass(),
          "NUMBER",
          new String[] {
            "int", "long", "byte", "short", "float", "double", "BigDecimal", "BigInteger", "String"
          });
    }
  }

  /**
   * Returns the number of days between the epoch and the passed date. Allowed Java types:
   *
   * <ul>
   *   <li>String
   *   <li>LocalDate
   *   <li>LocalDateTime
   *   <li>OffsetDateTime
   *   <li>ZonedDateTime
   * </ul>
   */
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
      throw typeNotAllowedException(
          input.getClass(),
          "DATE",
          new String[] {"String", "LocalDate", "LocalDateTime", "ZonedDateTime", "OffsetDateTime"});
    }

    SFTimestamp timestamp =
        createDateTimeFormatter().parse(inputString, GMT, 0, DATE | TIMESTAMP, true, null);
    if (timestamp == null)
      throw valueFormatNotAllowedException(
          input,
          "DATE",
          "Not a valid date, see"
              + " https://docs.snowflake.com/en/user-guide/date-time-input-output.html#date-formats"
              + " for the list of supported formats");

    return (int) TimeUnit.MILLISECONDS.toDays(SFDate.fromTimestamp(timestamp).getTime());
  }

  /**
   * Validates input for data type BINARY. Allowed Java types:
   *
   * <ul>
   *   <li>byte[]
   *   <li>String (hex-encoded)
   * </ul>
   *
   * @param input Array to validate
   * @param maxLengthOptional Max array length, defaults to 8MB, which is the max allowed length for
   *     BINARY column
   * @return Validated array
   */
  static byte[] validateAndParseBinary(Object input, Optional<Integer> maxLengthOptional) {
    byte[] output;
    if (input instanceof byte[]) {
      output = (byte[]) input;
    } else if (input instanceof String) {
      try {
        output = DatatypeConverter.parseHexBinary((String) input);
      } catch (IllegalArgumentException e) {
        throw valueFormatNotAllowedException(input, "BINARY", "Not a valid hex string");
      }
    } else {
      throw typeNotAllowedException(input.getClass(), "BINARY", new String[] {"byte[]", "String"});
    }

    int maxLength = maxLengthOptional.orElse(BYTES_8_MB);
    if (output.length > maxLength) {
      throw valueFormatNotAllowedException(
          String.format("byte[%d]", output.length),
          "BINARY",
          String.format("Binary too long: length=%d maxLength=%d", output.length, maxLength));
    }
    return output;
  }

  /**
   * Returns the number of units since 00:00, depending on the scale (scale=0: seconds, scale=3:
   * milliseconds, scale=9: nanoseconds. Allowed Java types:
   *
   * <ul>
   *   <li>String
   *   <li>LocalTime
   *   <li>OffsetTime
   * </ul>
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
      throw typeNotAllowedException(
          input.getClass(), "TIME", new String[] {"String", "LocalTime", "OffsetTime"});
    }

    SFTimestamp timestamp =
        createDateTimeFormatter()
            .parse(stringInput, GMT, 0, SnowflakeDateTimeFormat.TIME, true, null);
    if (timestamp == null) {
      throw valueFormatNotAllowedException(
          input,
          "TIME",
          "Not a valid time, see"
              + " https://docs.snowflake.com/en/user-guide/date-time-input-output.html#time-formats"
              + " for the list of supported formats");
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
   * Converts input to double value. Allowed Java types:
   *
   * <ul>
   *   <li>Number
   *   <li>String
   * </ul>
   *
   * @param input
   */
  static double validateAndParseReal(Object input) {
    if (input instanceof Float) {
      return Double.parseDouble(input.toString());
    } else if (input instanceof Number) {
      return ((Number) input).doubleValue();
    } else if (input instanceof String) {
      try {
        return Double.parseDouble((String) input);
      } catch (NumberFormatException err) {
        throw valueFormatNotAllowedException(input, "REAL", "Not a valid decimal number");
      }
    }

    throw typeNotAllowedException(input.getClass(), "REAL", new String[] {"Number", "String"});
  }

  /**
   * Validate and parse input to integer output, 1=true, 0=false. String values converted to boolean
   * according to https://docs.snowflake.com/en/sql-reference/functions/to_boolean.html#usage-notes
   * Allowed Java types:
   *
   * <ul>
   *   <li>boolean
   *   <li>String
   *   <li>java.lang.Number
   * </ul>
   *
   * @param input Input to be converted
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

    throw typeNotAllowedException(
        input.getClass(), "BOOLEAN", new String[] {"boolean", "Number", "String"});
  }

  static Set<String> allowedBooleanStringsLowerCased =
      Sets.newHashSet("1", "0", "yes", "no", "y", "n", "t", "f", "true", "false", "on", "off");

  private static boolean convertStringToBoolean(String value) {
    String lowerCasedValue = value.toLowerCase();
    if (!allowedBooleanStringsLowerCased.contains(lowerCasedValue)) {
      throw valueFormatNotAllowedException(
          value,
          "BOOLEAN",
          "Not a valid boolean, see"
              + " https://docs.snowflake.com/en/sql-reference/data-types-logical.html#conversion-to-boolean"
              + " for the list of supported formats");
    }
    return "1".equals(lowerCasedValue)
        || "yes".equals(lowerCasedValue)
        || "y".equals(lowerCasedValue)
        || "t".equals(lowerCasedValue)
        || "true".equals(lowerCasedValue)
        || "on".equals(lowerCasedValue);
  }

  /**
   * Create exception that a Java type cannot be ingested into a specific Snowflake column type
   *
   * @param javaType Java type failing the validation
   * @param snowflakeType Target Snowflake column type
   * @param allowedJavaTypes Java types supported for the Java type
   */
  private static SFException typeNotAllowedException(
      Class<?> javaType, String snowflakeType, String[] allowedJavaTypes) {
    return new SFException(
        ErrorCode.INVALID_ROW,
        String.format(
            "Object of type %s cannot be ingested into Snowflake column of type %s",
            javaType.getName(), snowflakeType),
        String.format(
            String.format("Allowed Java types: %s", String.join(", ", allowedJavaTypes))));
  }

  /**
   * Create exception when the Java type is correct, but the value is invalid (e.g. boolean cannot
   * be parsed from a string)
   *
   * @param value Invalid value causing the exception
   * @param snowflakeType Snowflake column type
   */
  private static SFException valueFormatNotAllowedException(
      Object value, String snowflakeType, String reason) {
    return new SFException(
        ErrorCode.INVALID_ROW,
        sanitizeValueForExceptionMessage(value),
        String.format(
            "Value cannot be ingested into Snowflake column %s: %s", snowflakeType, reason));
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
