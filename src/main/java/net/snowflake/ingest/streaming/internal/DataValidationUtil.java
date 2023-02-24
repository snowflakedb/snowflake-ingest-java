/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.streaming.internal.BinaryStringUtils.unicodeCharactersCount;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import net.snowflake.client.jdbc.internal.google.common.collect.Sets;
import net.snowflake.client.jdbc.internal.snowflake.common.core.SnowflakeDateTimeFormat;
import net.snowflake.client.jdbc.internal.snowflake.common.util.Power10;
import net.snowflake.ingest.streaming.internal.serialization.ByteArraySerializer;
import net.snowflake.ingest.streaming.internal.serialization.ZonedDateTimeSerializer;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

/** Utility class for parsing and validating inputs based on Snowflake types */
class DataValidationUtil {

  /**
   * Seconds limit used for integer-stored timestamp scale guessing. Value needs to be aligned with
   * the value from {@link SnowflakeDateTimeFormat#parse}
   */
  private static final long SECONDS_LIMIT_FOR_EPOCH = 31536000000L;

  /**
   * Milliseconds limit used for integer-stored timestamp scale guessing. Value needs to be aligned
   * with the value from {@link SnowflakeDateTimeFormat#parse}
   */
  private static final long MILLISECONDS_LIMIT_FOR_EPOCH = SECONDS_LIMIT_FOR_EPOCH * 1000L;

  /**
   * Microseconds limit used for integer-stored timestamp scale guessing. Value needs to be aligned
   * with the value from {@link SnowflakeDateTimeFormat#parse}
   */
  private static final long MICROSECONDS_LIMIT_FOR_EPOCH = SECONDS_LIMIT_FOR_EPOCH * 1000000L;

  public static final int BYTES_8_MB = 8 * 1024 * 1024;
  public static final int BYTES_16_MB = 2 * BYTES_8_MB;

  // TODO SNOW-664249: There is a few-byte mismatch between the value sent by the user and its
  // server-side representation. Validation leaves a small buffer for this difference.
  static final int MAX_SEMI_STRUCTURED_LENGTH = BYTES_16_MB - 64;

  private static final ObjectMapper objectMapper = new ObjectMapper();

  // The version of Jackson we are using does not support serialization of date objects from the
  // java.time package. Here we define a module with custom java.time serializers. Additionally, we
  // define custom serializer for byte[] because the Jackson default is to serialize it as
  // base64-encoded string, and we would like to serialize it as JSON array of numbers.
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
   * Validates and parses input as JSON. All types in the object tree must be valid variant types,
   * see {@link DataValidationUtil#isAllowedSemiStructuredType}.
   *
   * @param input Object to validate
   * @return JSON tree representing the input
   */
  private static JsonNode validateAndParseSemiStructuredAsJsonTree(
      String columnName, Object input, String snowflakeType) {
    if (input instanceof String) {
      String stringInput = (String) input;
      verifyValidUtf8(stringInput, columnName, snowflakeType);
      try {
        return objectMapper.readTree(stringInput);
      } catch (JsonProcessingException e) {
        throw valueFormatNotAllowedException(columnName, input, snowflakeType, "Not a valid JSON");
      }
    } else if (isAllowedSemiStructuredType(input)) {
      return objectMapper.valueToTree(input);
    }

    throw typeNotAllowedException(
        columnName,
        input.getClass(),
        snowflakeType,
        new String[] {
          "String",
          "Primitive data types and their arrays",
          "java.time.*",
          "List<T>",
          "Map<String, T>",
          "T[]"
        });
  }

  /**
   * Validates and parses input as JSON. All types in the object tree must be valid variant types,
   * see {@link DataValidationUtil#isAllowedSemiStructuredType}.
   *
   * @param input Object to validate
   * @return JSON string representing the input
   */
  static String validateAndParseVariant(String columnName, Object input) {
    JsonNode node = validateAndParseSemiStructuredAsJsonTree(columnName, input, "VARIANT");

    // Missing nodes are not valid json, ingest them as NULL instead
    if (node.isMissingNode()) {
      return null;
    }

    String output = node.toString();
    int stringLength = output.getBytes(StandardCharsets.UTF_8).length;
    if (stringLength > MAX_SEMI_STRUCTURED_LENGTH) {
      throw valueFormatNotAllowedException(
          columnName,
          output,
          "VARIANT",
          String.format(
              "Variant too long: length=%d maxLength=%d",
              stringLength, MAX_SEMI_STRUCTURED_LENGTH));
    }
    return output;
  }

  /**
   * Validates that passed object is allowed data type for semi-structured columns (i.e. VARIANT,
   * ARRAY, OBJECT). For non-trivial types like maps, arrays or lists, it recursively traverses the
   * object tree and validates that all types in the tree are also allowed. Allowed Java types:
   *
   * <ul>
   *   <li>primitive types (int, long, boolean, ...)
   *   <li>String
   *   <li>BigInteger
   *   <li>BigDecimal
   *   <li>LocalTime
   *   <li>OffsetTime
   *   <li>LocalDate
   *   <li>LocalDateTime
   *   <li>OffsetDateTime
   *   <li>ZonedDateTime
   *   <li>Map<String, T> where T is an allowed semi-structured type
   *   <li>List<T> where T is an allowed semi-structured type
   *   <li>primitive arrays (char[], int[], ...)
   *   <li>T[] where T is an allowed semi-structured type
   * </ul>
   *
   * @param o Object to validate
   * @return If the passed object is allowed for ingestion into semi-structured column
   */
  static boolean isAllowedSemiStructuredType(Object o) {
    // Allow null
    if (o == null) {
      return true;
    }

    // Allow string
    if (o instanceof String) {
      return true;
    }

    // Allow all primitive Java data types
    if (o instanceof Long
        || o instanceof Integer
        || o instanceof Short
        || o instanceof Byte
        || o instanceof Float
        || o instanceof Double
        || o instanceof Boolean
        || o instanceof Character) {
      return true;
    }

    // Allow BigInteger and BigDecimal
    if (o instanceof BigInteger || o instanceof BigDecimal) {
      return true;
    }

    // Allow supported types from java.time package
    if (o instanceof java.time.LocalTime
        || o instanceof OffsetTime
        || o instanceof LocalDate
        || o instanceof LocalDateTime
        || o instanceof ZonedDateTime
        || o instanceof OffsetDateTime) {
      return true;
    }

    // Map<String, T> is allowed, as long as T is also a supported semi-structured type
    if (o instanceof Map) {
      boolean allKeysAreStrings =
          ((Map<?, ?>) o).keySet().stream().allMatch(x -> x instanceof String);
      if (!allKeysAreStrings) {
        return false;
      }
      boolean allValuesAreAllowed =
          ((Map<?, ?>) o)
              .values().stream().allMatch(DataValidationUtil::isAllowedSemiStructuredType);
      return allValuesAreAllowed;
    }

    // Allow arrays of primitive data types
    if (o instanceof byte[]
        || o instanceof short[]
        || o instanceof int[]
        || o instanceof long[]
        || o instanceof float[]
        || o instanceof double[]
        || o instanceof boolean[]
        || o instanceof char[]) {
      return true;
    }

    // Allow arrays of allowed semi-structured objects
    if (o.getClass().isArray()) {
      return Arrays.stream((Object[]) o).allMatch(DataValidationUtil::isAllowedSemiStructuredType);
    }

    // Allow lists consisting of allowed semi-structured objects
    if (o instanceof List) {
      return ((List<?>) o).stream().allMatch(DataValidationUtil::isAllowedSemiStructuredType);
    }

    // If nothing matches, reject the input
    return false;
  }

  /**
   * Validates and parses JSON array. Non-array types are converted into single-element arrays. All
   * types in the array tree must be valid variant types, see {@link
   * DataValidationUtil#isAllowedSemiStructuredType}.
   *
   * @param input Object to validate
   * @return JSON array representing the input
   */
  static String validateAndParseArray(String columnName, Object input) {
    JsonNode jsonNode = validateAndParseSemiStructuredAsJsonTree(columnName, input, "ARRAY");

    // Non-array values are ingested as single-element arrays, mimicking the Worksheets behavior
    if (!jsonNode.isArray()) {
      jsonNode = objectMapper.createArrayNode().add(jsonNode);
    }

    String output = jsonNode.toString();
    // Throw an exception if the size is too large
    int stringLength = output.getBytes(StandardCharsets.UTF_8).length;
    if (stringLength > MAX_SEMI_STRUCTURED_LENGTH) {
      throw valueFormatNotAllowedException(
          columnName,
          output,
          "ARRAY",
          String.format(
              "Array too large. length=%d maxLength=%d", stringLength, MAX_SEMI_STRUCTURED_LENGTH));
    }
    return output;
  }

  /**
   * Validates and parses JSON object. Input is rejected if the value does not represent JSON object
   * (e.g. String '{}' or Map<String, T>). All types in the object tree must be valid variant types,
   * see {@link DataValidationUtil#isAllowedSemiStructuredType}.
   *
   * @param input Object to validate
   * @return JSON object representing the input
   */
  static String validateAndParseObject(String columnName, Object input) {
    JsonNode jsonNode = validateAndParseSemiStructuredAsJsonTree(columnName, input, "OBJECT");
    if (!jsonNode.isObject()) {
      throw valueFormatNotAllowedException(columnName, jsonNode, "OBJECT", "Not an object");
    }

    String output = jsonNode.toString();
    // Throw an exception if the size is too large
    int stringLength = output.getBytes(StandardCharsets.UTF_8).length;
    if (stringLength > MAX_SEMI_STRUCTURED_LENGTH) {
      throw valueFormatNotAllowedException(
          columnName,
          output,
          "OBJECT",
          String.format(
              "Object too large. length=%d maxLength=%d",
              stringLength, MAX_SEMI_STRUCTURED_LENGTH));
    }
    return output;
  }

  /**
   * Converts user input to offset date time, which is the canonical representation of dates and
   * timestamps.
   */
  private static OffsetDateTime inputToOffsetDateTime(
      String columnName, String typeName, Object input, ZoneId defaultTimezone) {
    if (input instanceof OffsetDateTime) {
      return (OffsetDateTime) input;
    }

    if (input instanceof ZonedDateTime) {
      return ((ZonedDateTime) input).toOffsetDateTime();
    }

    if (input instanceof LocalDateTime) {
      return ((LocalDateTime) input).atZone(defaultTimezone).toOffsetDateTime();
    }

    if (input instanceof LocalDate) {
      return ((LocalDate) input).atStartOfDay().atZone(defaultTimezone).toOffsetDateTime();
    }

    if (input instanceof Instant) {
      // Just like integer-stored timestamps, instants are always interpreted in UTC
      return ((Instant) input).atZone(ZoneOffset.UTC).toOffsetDateTime();
    }

    if (input instanceof String) {
      String stringInput = (String) input;
      {
        // First, try to parse ZonedDateTime
        ZonedDateTime zoned = catchParsingError(() -> ZonedDateTime.parse(stringInput));
        if (zoned != null) {
          return zoned.toOffsetDateTime();
        }
      }

      {
        // Next, try to parse OffsetDateTime
        OffsetDateTime offset = catchParsingError(() -> OffsetDateTime.parse(stringInput));
        if (offset != null) {
          return offset;
        }
      }

      {
        // Alternatively, try to parse LocalDateTime
        LocalDateTime localDateTime = catchParsingError(() -> LocalDateTime.parse(stringInput));
        if (localDateTime != null) {
          return localDateTime.atZone(defaultTimezone).toOffsetDateTime();
        }
      }

      {
        // Alternatively, try to parse LocalDate
        LocalDate localDate = catchParsingError(() -> LocalDate.parse(stringInput));
        if (localDate != null) {
          return localDate.atStartOfDay().atZone(defaultTimezone).toOffsetDateTime();
        }
      }

      {
        // Alternatively, try to parse integer-stored timestamp
        // Just like in Snowflake, integer-stored timestamps are always in UTC
        Instant instant = catchParsingError(() -> parseInstantGuessScale(stringInput));
        if (instant != null) {
          return instant.atOffset(ZoneOffset.UTC);
        }
      }

      // Couldn't parse anything, throw an exception
      // TODO Change URL when out of private preview
      throw valueFormatNotAllowedException(
          columnName,
          input.toString(),
          typeName,
          "Not a valid value, see"
              + " https://docs.snowflake.com/en/LIMITEDACCESS/snowpipe-streaming.html"
              + " for the list of supported formats");
    }

    // Type is not supported, throw an exception
    throw typeNotAllowedException(
        columnName,
        input.getClass(),
        typeName,
        new String[] {"String", "LocalDate", "LocalDateTime", "ZonedDateTime", "OffsetDateTime"});
  }

  private static <T> T catchParsingError(Supplier<T> op) {
    try {
      return op.get();
    } catch (DateTimeParseException | NumberFormatException e) {
      return null;
    }
  }

  /**
   * Validates and parses input for TIMESTAMP_NTZ, TIMESTAMP_LTZ and TIMEATAMP_TZ Snowflake types.
   * Allowed Java types:
   *
   * <ul>
   *   <li>String
   *   <li>LocalDate
   *   <li>LocalDateTime
   *   <li>OffsetDateTime
   *   <li>ZonedDateTime
   * </ul>
   *
   * @param columnName Column name, used in validation error messages
   * @param input String date in valid format, seconds past the epoch or java.time.* object. Accepts
   *     fractional seconds with precision up to the column's scale
   * @param scale decimal scale of timestamp 16 byte integer
   * @param defaultTimezone Input, which does not carry timezone information is going to be
   *     interpreted in the default timezone.
   * @param trimTimezone Whether timezone information should be removed from the resulting date,
   *     should be true for TIMESTAMP_NTZ columns.
   * @return TimestampWrapper
   */
  static TimestampWrapper validateAndParseTimestamp(
      String columnName, Object input, int scale, ZoneId defaultTimezone, boolean trimTimezone) {
    OffsetDateTime offsetDateTime =
        inputToOffsetDateTime(columnName, "TIMESTAMP", input, defaultTimezone);

    if (trimTimezone) {
      offsetDateTime = offsetDateTime.withOffsetSameLocal(ZoneOffset.UTC);
    }
    return new TimestampWrapper(offsetDateTime, scale);
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
  static String validateAndParseString(
      String columnName, Object input, Optional<Integer> maxLengthOptional) {
    String output;
    if (input instanceof String) {
      output = (String) input;
      verifyValidUtf8(output, columnName, "STRING");
    } else if (input instanceof Number) {
      output = new BigDecimal(input.toString()).stripTrailingZeros().toPlainString();
    } else if (input instanceof Boolean || input instanceof Character) {
      output = input.toString();
    } else {
      throw typeNotAllowedException(
          columnName,
          input.getClass(),
          "STRING",
          new String[] {"String", "Number", "boolean", "char"});
    }
    byte[] utf8Bytes = output.getBytes(StandardCharsets.UTF_8);

    // Strings can never be larger than 16MB
    if (utf8Bytes.length > BYTES_16_MB) {
      throw valueFormatNotAllowedException(
          columnName,
          input,
          "STRING",
          String.format(
              "String too long: length=%d bytes maxLength=%d bytes",
              utf8Bytes.length, BYTES_16_MB));
    }

    // If max allowed length is specified (e.g. VARCHAR(10)), the number of unicode characters must
    // not exceed this value
    maxLengthOptional.ifPresent(
        maxAllowedCharacters -> {
          int actualCharacters = unicodeCharactersCount(output);
          if (actualCharacters > maxAllowedCharacters) {
            throw valueFormatNotAllowedException(
                columnName,
                input,
                "STRING",
                String.format(
                    "String too long: length=%d characters maxLength=%d characters",
                    actualCharacters, maxAllowedCharacters));
          }
        });
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
  static BigDecimal validateAndParseBigDecimal(String columnName, Object input) {
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
        throw valueFormatNotAllowedException(columnName, input, "NUMBER", "Not a valid number");
      }
    } else {
      throw typeNotAllowedException(
          columnName,
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
   *   <li>{@link LocalDate}
   *   <li>{@link LocalDateTime}
   *   <li>{@link OffsetDateTime}
   *   <li>{@link ZonedDateTime}
   *   <li>{@link Instant}
   * </ul>
   */
  static int validateAndParseDate(String columnName, Object input) {
    OffsetDateTime offsetDateTime =
        inputToOffsetDateTime(columnName, "DATE", input, ZoneOffset.UTC);
    return Math.toIntExact(offsetDateTime.toLocalDate().toEpochDay());
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
  static byte[] validateAndParseBinary(
      String columnName, Object input, Optional<Integer> maxLengthOptional) {
    byte[] output;
    if (input instanceof byte[]) {
      // byte[] is a mutable object, we need to create a defensive copy to protect against
      // concurrent modifications of the array, which could lead to mismatch between data
      // and metadata
      byte[] originalInputArray = (byte[]) input;
      output = new byte[originalInputArray.length];
      System.arraycopy(originalInputArray, 0, output, 0, originalInputArray.length);
    } else if (input instanceof String) {
      try {
        output = Hex.decodeHex((String) input);
      } catch (DecoderException e) {
        throw valueFormatNotAllowedException(columnName, input, "BINARY", "Not a valid hex string");
      }
    } else {
      throw typeNotAllowedException(
          columnName, input.getClass(), "BINARY", new String[] {"byte[]", "String"});
    }

    int maxLength = maxLengthOptional.orElse(BYTES_8_MB);
    if (output.length > maxLength) {
      throw valueFormatNotAllowedException(
          columnName,
          String.format("byte[%d]", output.length),
          "BINARY",
          String.format("Binary too long: length=%d maxLength=%d", output.length, maxLength));
    }
    return output;
  }

  /**
   * Returns the number of units since 00:00, depending on the scale (scale=0: seconds, scale=3:
   * milliseconds, scale=9: nanoseconds). Allowed Java types:
   *
   * <ul>
   *   <li>String
   *   <li>{@link LocalTime}
   *   <li>{@link OffsetTime}
   * </ul>
   */
  static BigInteger validateAndParseTime(String columnName, Object input, int scale) {
    if (input instanceof LocalTime) {
      LocalTime localTime = (LocalTime) input;
      return BigInteger.valueOf(localTime.toNanoOfDay()).divide(Power10.sb16Table[9 - scale]);
    } else if (input instanceof OffsetTime) {
      return validateAndParseTime(columnName, ((OffsetTime) input).toLocalTime(), scale);
    } else if (input instanceof String) {
      String stringInput = (String) input;
      {
        // First, try to parse LocalTime
        LocalTime localTime = catchParsingError(() -> LocalTime.parse(stringInput));
        if (localTime != null) {
          return validateAndParseTime(columnName, localTime, scale);
        }
      }

      {
        // Alternatively, try to parse OffsetTime
        OffsetTime offsetTime = catchParsingError((() -> OffsetTime.parse(stringInput)));
        if (offsetTime != null) {
          return validateAndParseTime(columnName, offsetTime.toLocalTime(), scale);
        }
      }

      {
        // Alternatively, try to parse integer-stored time
        Instant parsedInstant = catchParsingError(() -> parseInstantGuessScale(stringInput));
        if (parsedInstant != null) {
          return validateAndParseTime(
              columnName,
              LocalDateTime.ofInstant(parsedInstant, ZoneOffset.UTC).toLocalTime(),
              scale);
        }
      }

      // TODO Change URL when out of private preview
      throw valueFormatNotAllowedException(
          columnName,
          input,
          "TIME",
          "Not a valid time, see"
              + " https://docs.snowflake.com/en/LIMITEDACCESS/snowpipe-streaming.html"
              + " for the list of supported formats");

    } else {
      throw typeNotAllowedException(
          columnName, input.getClass(), "TIME", new String[] {"String", "LocalTime", "OffsetTime"});
    }
  }

  /**
   * Attempts to parse integer-stored date from string input. Tries to guess the scale according to
   * the rules documented at
   * https://docs.snowflake.com/en/user-guide/date-time-input-output.html#auto-detection-of-integer-stored-date-time-and-timestamp-values.
   *
   * @param input String to parse, must represent a valid long
   * @return Instant representing the input
   * @throws NumberFormatException If the input in not a valid long
   */
  private static Instant parseInstantGuessScale(String input) {
    long epochNanos;
    long val = Long.parseLong(input);

    if (val > -SECONDS_LIMIT_FOR_EPOCH && val < SECONDS_LIMIT_FOR_EPOCH) {
      epochNanos = val * Power10.intTable[9];
    } else if (val > -MILLISECONDS_LIMIT_FOR_EPOCH && val < MILLISECONDS_LIMIT_FOR_EPOCH) {
      epochNanos = val * Power10.intTable[6];
    } else if (val > -MICROSECONDS_LIMIT_FOR_EPOCH && val < MICROSECONDS_LIMIT_FOR_EPOCH) {
      epochNanos = val * Power10.intTable[3];
    } else {
      epochNanos = val;
    }
    return Instant.ofEpochSecond(
        epochNanos / Power10.intTable[9], epochNanos % Power10.intTable[9]);
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
  static double validateAndParseReal(String columnName, Object input) {
    if (input instanceof Float) {
      return Double.parseDouble(input.toString());
    } else if (input instanceof Number) {
      return ((Number) input).doubleValue();
    } else if (input instanceof String) {
      String stringInput = (String) input;
      try {
        return Double.parseDouble(stringInput);
      } catch (NumberFormatException err) {
        stringInput = stringInput.toLowerCase();
        switch (stringInput) {
          case "nan":
            return Double.NaN;
          case "inf":
            return Double.POSITIVE_INFINITY;
          case "-inf":
            return Double.NEGATIVE_INFINITY;
          default:
            throw valueFormatNotAllowedException(
                columnName, input, "REAL", "Not a valid decimal number");
        }
      }
    }
    throw typeNotAllowedException(
        columnName, input.getClass(), "REAL", new String[] {"Number", "String"});
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
  static int validateAndParseBoolean(String columnName, Object input) {
    if (input instanceof Boolean) {
      return (boolean) input ? 1 : 0;
    } else if (input instanceof Number) {
      return new BigDecimal(input.toString()).compareTo(BigDecimal.ZERO) == 0 ? 0 : 1;
    } else if (input instanceof String) {
      return convertStringToBoolean(columnName, (String) input) ? 1 : 0;
    }

    throw typeNotAllowedException(
        columnName, input.getClass(), "BOOLEAN", new String[] {"boolean", "Number", "String"});
  }

  static void checkValueInRange(BigDecimal bigDecimalValue, int scale, int precision) {
    if (bigDecimalValue.abs().compareTo(BigDecimal.TEN.pow(precision - scale)) >= 0) {
      throw new SFException(
          ErrorCode.INVALID_ROW,
          bigDecimalValue,
          String.format(
              "Number out of representable exclusive range of (-1e%s..1e%s)",
              precision - scale, precision - scale));
    }
  }

  static Set<String> allowedBooleanStringsLowerCased =
      Sets.newHashSet("1", "0", "yes", "no", "y", "n", "t", "f", "true", "false", "on", "off");

  private static boolean convertStringToBoolean(String columnName, String value) {
    String lowerCasedValue = value.toLowerCase();
    if (!allowedBooleanStringsLowerCased.contains(lowerCasedValue)) {
      throw valueFormatNotAllowedException(
          columnName,
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
      String columnName, Class<?> javaType, String snowflakeType, String[] allowedJavaTypes) {
    return new SFException(
        ErrorCode.INVALID_ROW,
        String.format(
            "Object of type %s cannot be ingested into Snowflake column %s of type %s",
            javaType.getName(), columnName, snowflakeType),
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
      String columnName, Object value, String snowflakeType, String reason) {
    return new SFException(
        ErrorCode.INVALID_ROW,
        sanitizeValueForExceptionMessage(value),
        String.format(
            "Value cannot be ingested into Snowflake column %s of type %s: %s",
            columnName, snowflakeType, reason));
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

  /**
   * Validates that a string is valid UTF-8 string. It catches situations like unmatched high/low
   * UTF-16 surrogate, for example.
   */
  private static void verifyValidUtf8(String input, String columnName, String dataType) {
    CharsetEncoder charsetEncoder =
        StandardCharsets.UTF_8
            .newEncoder()
            .onMalformedInput(CodingErrorAction.REPORT)
            .onUnmappableCharacter(CodingErrorAction.REPORT);
    try {
      charsetEncoder.encode(CharBuffer.wrap(input));
    } catch (CharacterCodingException e) {
      throw valueFormatNotAllowedException(columnName, input, dataType, "Invalid Unicode string");
    }
  }
}
