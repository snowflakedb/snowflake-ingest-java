/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import static net.snowflake.client.jdbc.internal.snowflake.common.core.SnowflakeDateTimeFormat.DATE;
import static net.snowflake.client.jdbc.internal.snowflake.common.core.SnowflakeDateTimeFormat.TIMESTAMP;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.TimeUnit;
import javax.xml.bind.DatatypeConverter;
import net.snowflake.client.jdbc.internal.google.common.collect.Sets;
import net.snowflake.client.jdbc.internal.snowflake.common.core.SFDate;
import net.snowflake.client.jdbc.internal.snowflake.common.core.SFTimestamp;
import net.snowflake.client.jdbc.internal.snowflake.common.core.SnowflakeDateTimeFormat;
import net.snowflake.client.jdbc.internal.snowflake.common.util.Power10;
import net.snowflake.ingest.streaming.internal.serialization.ByteArraySerializer;
import net.snowflake.ingest.streaming.internal.serialization.ZonedDateTimeSerializer;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;

/** Utility class for parsing and validating inputs based on Snowflake types */
class DataValidationUtil {
  private static final int BYTES_16_MB = 16 * 1024 * 1024;
  private static final int BYTES_8_MB = 8 * 1024 * 1024;

  /** For "value too large" errors, how many characters from the large string should be displayed */
  private static final int MAX_CHARS_IN_EXCEPTION = 20;

  private static final TimeZone DEFAULT_TIMEZONE =
      TimeZone.getTimeZone("America/Los_Angeles"); // default value of TIMEZONE system parameter
  private static final TimeZone GMT = TimeZone.getTimeZone("GMT");

  private static final ObjectMapper objectMapper = new ObjectMapper();

  static {
    SimpleModule module = new SimpleModule();
    module.addSerializer(byte[].class, new ByteArraySerializer());
    module.addSerializer(ZonedDateTime.class, new ZonedDateTimeSerializer());
    module.addSerializer(LocalTime.class, new ToStringSerializer());
    module.addSerializer(OffsetTime.class, new ToStringSerializer());
    module.addSerializer(LocalDate.class, new ToStringSerializer());
    module.addSerializer(LocalDateTime.class, new ToStringSerializer());
    module.addSerializer(OffsetDateTime.class, new ToStringSerializer());
    objectMapper.registerModule(module);
  }

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
  private static JsonNode validateAndParseVariantAsJsonTree(Object input) {
    if (input instanceof String) {
      try {
        return objectMapper.readTree((String) input);
      } catch (JsonProcessingException e) {
        throw new SFException(ErrorCode.INVALID_ROW, input.toString(), "Input is not a valid JSON");
      }
    } else if (isAllowedVariantType(input)) {
      return objectMapper.valueToTree(input);
    }

    throw cannotConvertException(input, "variant");
  }

  static String validateAndParseVariantAsString(Object input) {
    String output = validateAndParseVariantAsJsonTree(input).toString();
    int stringLength = output.getBytes(StandardCharsets.UTF_8).length;
    if (stringLength > BYTES_16_MB) {
      throw new SFException(
          ErrorCode.INVALID_ROW,
          output.length() <= MAX_CHARS_IN_EXCEPTION
              ? output
              : output.substring(0, MAX_CHARS_IN_EXCEPTION) + "...",
          String.format("Variant too long: length=%d maxLength=%d", output.length(), BYTES_16_MB));
    }

    return output;
  }

  static boolean isAllowedVariantType(Object o) {
    if (o == null) return true;
    if (o instanceof String || o instanceof Boolean || o instanceof Character) return true;
    if (o instanceof Long
        || o instanceof Integer
        || o instanceof Short
        || o instanceof Byte
        || o instanceof BigInteger) return true;
    if (o instanceof Float || o instanceof Double || o instanceof BigDecimal) return true;
    if (o instanceof JsonNode) return true;
    if (o instanceof java.time.LocalTime || o instanceof OffsetTime) return true;
    if (o instanceof LocalDate
        || o instanceof LocalDateTime
        || o instanceof ZonedDateTime
        || o instanceof OffsetDateTime) return true;
    if (o instanceof Map) {
      boolean allKeysAreStrings =
          ((Map<?, ?>) o).keySet().stream().allMatch(x -> x instanceof String);
      boolean allValuesAreAllowed =
          ((Map<?, ?>) o).values().stream().allMatch(DataValidationUtil::isAllowedVariantType);
      return allKeysAreStrings && allValuesAreAllowed;
    }
    if (o instanceof byte[]
        || o instanceof short[]
        || o instanceof int[]
        || o instanceof long[]
        || o instanceof float[]
        || o instanceof double[]
        || o instanceof boolean[]
        || o instanceof char[]) return true;
    if (o.getClass().isArray())
      return Arrays.stream((Object[]) o).allMatch(DataValidationUtil::isAllowedVariantType);
    if (o instanceof List)
      return ((List<?>) o).stream().allMatch(DataValidationUtil::isAllowedVariantType);
    return false;
  }

  /**
   * Expects an Array or List object
   *
   * @param input the input data, must be able to convert to String
   */
  static String validateAndParseArray(Object input) {
    JsonNode jsonNode = validateAndParseVariantAsJsonTree(input);

    // Non-array values are ingested as single-element arrays
    if (!jsonNode.isArray()) {
      jsonNode = objectMapper.createArrayNode().add(jsonNode);
    }

    String output = jsonNode.toString();
    // Throw an exception if the size is too large
    if (output.getBytes(StandardCharsets.UTF_8).length > BYTES_16_MB) {
      throw new SFException(
          ErrorCode.INVALID_ROW,
          output.length() <= MAX_CHARS_IN_EXCEPTION
              ? output
              : output.substring(0, MAX_CHARS_IN_EXCEPTION) + "...",
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
    JsonNode jsonNode = validateAndParseVariantAsJsonTree(input);
    if (!jsonNode.isObject()) {
      throw cannotConvertException(input, "object");
    }

    String output = jsonNode.toString();
    // Throw an exception if the size is too large
    if (output.getBytes(StandardCharsets.UTF_8).length > BYTES_16_MB) {
      throw new SFException(
          ErrorCode.INVALID_ROW,
          output.length() <= MAX_CHARS_IN_EXCEPTION
              ? output
              : output.substring(0, MAX_CHARS_IN_EXCEPTION) + "...",
          String.format("Object too large. length=%d maxLength=%d", output.length(), BYTES_16_MB));
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
   * Converts input to string, validates that length is less than max allowed string size
   * https://docs.snowflake.com/en/sql-reference/data-types-text.html#varchar
   *
   * @param input Input to convert
   * @param maxLengthOptional Max size to validate, is empty() fallbacks to max allowed Snowflake
   *     string size
   * @return Validated string
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
      throw cannotConvertException(input, "string");
    }
    int maxLength = maxLengthOptional.orElse(BYTES_16_MB);

    if (output.length() > maxLength) {
      throw new SFException(
          ErrorCode.INVALID_ROW,
          output.length() <= MAX_CHARS_IN_EXCEPTION
              ? output
              : output.substring(0, MAX_CHARS_IN_EXCEPTION) + "...",
          String.format("String too long: length=%d maxLength=%d", output.length(), maxLength));
    }
    return output;
  }

  /**
   * Returns a BigDecimal representation of the input. Strings of the form "1.23E4" will be treated
   * as being written in * scientific notation (e.g. 1.23 * 10^4). Does not perform any size
   * validation.
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
        throw new SFException(ErrorCode.INVALID_ROW, input.toString(), e.getMessage());
      }
    } else {
      throw new SFException(
          ErrorCode.INVALID_ROW,
          input,
          String.format("Type %s is not convertible to a number", input.getClass().getName()));
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
    } else if (input instanceof String) {
      try {
        output = DatatypeConverter.parseHexBinary((String) input);
      } catch (IllegalArgumentException e) {
        throw new SFException(ErrorCode.INVALID_ROW, input, e.getMessage());
      }
    } else {
      throw cannotConvertException(input, "binary");
    }

    int maxLength = maxLengthOptional.orElse(BYTES_8_MB);
    if (output.length > maxLength) {
      throw new SFException(
          ErrorCode.INVALID_ROW,
          String.format("byte[%d]", output.length),
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

  /** Converts input to double value. */
  static double validateAndParseReal(Object input) {
    if (input instanceof Float) {
      return Double.parseDouble(input.toString());
    } else if (input instanceof Number) {
      return ((Number) input).doubleValue();
    } else if (input instanceof String) {
      try {
        return Double.parseDouble((String) input);
      } catch (NumberFormatException err) {
        throw new SFException(ErrorCode.INVALID_ROW, input.toString());
      }
    }

    throw cannotConvertException(input, "double");
  }

  /**
   * Validate and parse input to integer output, 1=true, 0=false. String values converted to boolean
   * according to https://docs.snowflake.com/en/sql-reference/functions/to_boolean.html#usage-notes
   *
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

  private static SFException cannotConvertException(Object value, String type) {
    return new SFException(
        ErrorCode.INVALID_ROW,
        value.toString(),
        String.format("Value cannot be converted to %s", type));
  }
}
