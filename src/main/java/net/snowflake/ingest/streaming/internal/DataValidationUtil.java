/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Sets;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.xml.bind.DatatypeConverter;
import net.snowflake.client.jdbc.internal.snowflake.common.util.Power10;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;

/** Utility class for parsing and validating inputs based on Snowflake types */
class DataValidationUtil {
  static final int MAX_STRING_LENGTH = 16777216;
  static final BigInteger MAX_BIGINTEGER = BigInteger.valueOf(10).pow(38);
  static final BigInteger MIN_BIGINTEGER =
      BigInteger.valueOf(-1).multiply(BigInteger.valueOf(10).pow(38));

  /**
   * Expects string JSON
   *
   * @param input
   */
  // TODO enforce max size? TODO enforce valid JSON?
  static String validateAndParseVariant(Object input) {
    if (input instanceof String) {
      return (String) input;
    } else if (input instanceof JsonNode) {
      return input.toString();
    } else {
      throw new SFException(
          ErrorCode.INVALID_ROW,
          input.toString(),
          String.format("OBJECT, ARRAY, and VARIANT columns only accept String or JsonNode"));
    }
  }

  /**
   * Validates and parses input for TIMESTAMP_NTZ Snowflake type
   *
   * @param input Seconds past the epoch. Accepts fractional seconds with precision up to the
   *     column's scale
   * @param metadata
   * @return TimestampWrapper with epoch seconds, fractional seconds, and epoch time in the column
   *     scale
   */
  static TimestampWrapper validateAndParseTimestampNtzSb16(
      Object input, Map<String, String> metadata) {
    try {
      int scale = Integer.parseInt(metadata.get(ArrowRowBuffer.COLUMN_SCALE));
      String valueString = getStringValue(input);

      String[] items = valueString.split("\\.");
      long epoch = Long.parseLong(items[0]);
      int l = items.length > 1 ? items[1].length() : 0;
      // Fraction is in nanoseconds, but Snowflake will error if the fraction gives
      // accuracy greater than the scale
      int fraction =
          l == 0 ? 0 : Integer.parseInt(items[1]) * (l < 9 ? Power10.intTable[9 - l] : 1);
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
   * Validates the input is less than the suppoed maximum allowed string
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
          String.format("String too long.  length=%d maxLength=%d", output.length(), maxLength));
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
   * Expects input in epoch days. Uses @see {@link #validateAndParseInteger(Object)} to validate and
   * parse input as integer
   *
   * @param input epoch days
   */
  static int validateAndParseDate(Object input) {
    return validateAndParseInteger(input);
  }

  static byte[] validateAndParseBinary(Object input) {
    if (input instanceof byte[]) {
      return (byte[]) input;
    }
    try {
      return DatatypeConverter.parseHexBinary(input.toString());
    } catch (IllegalArgumentException e) {
      throw new SFException(ErrorCode.INVALID_ROW, input, e.getMessage());
    }
  }

  static BigInteger validateAndParseTime(Object input, Map<String, String> metadata) {
    try {
      int scale = Integer.parseInt(metadata.get(ArrowRowBuffer.COLUMN_SCALE));
      return getTimeInScale(getStringValue(input), scale);
    } catch (NumberFormatException e) {
      throw new SFException(ErrorCode.INVALID_ROW, input.toString(), e.getMessage());
    }
  }

  /**
   * Returns the time past the epoch in the given scale.
   *
   * @param value
   * @param scale Value between 0 and 9. For seconds scale = 0, for milliseconds scale = 3, for
   *     nanoseconds scale = 9
   */
  static BigInteger getTimeInScale(String value, int scale) {
    BigDecimal decVal = new BigDecimal(value);
    BigDecimal epochScale = decVal.multiply(BigDecimal.valueOf(Power10.intTable[scale]));
    return epochScale.toBigInteger();
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
