/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import com.ibm.icu.lang.UCharacter;
import com.ibm.icu.text.CollationKey;
import com.ibm.icu.text.Collator;
import com.ibm.icu.util.ULocale;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;

/** Keeps track of the active EP stats, used to generate a file EP info */
class RowBufferStats {
  private static class CollationDefinition {
    /** The name of this collation */
    private String name;

    /** Derived ICU collation name */
    private String icuCollationString = null;

    /** Defines if a given collation trims strings on the left side */
    private boolean ltrim = false;

    /** Defines if a given collation trims strings on the right side */
    private boolean rtrim = false;

    /** Defines if a given collation applies "lower" before comparison */
    private boolean lower = false;

    /** Defines if a given collation applies "upper" before comparison */
    private boolean upper = false;

    /** If true or null, the collation is case sensitive */
    private Boolean caseSensitive = null;

    /** If true or null, the collation is accent sensitive */
    private Boolean accentSensitive = null;

    /** If true or null, the collation is punctuation sensitive */
    private Boolean punctuationSensitive = null;

    /**
     * Defines if a given collation is forced, i.e. goes through the full collation pipeline even if
     * not strictly needed (i.e. UTF8)
     */
    private boolean forced = false;

    private static String LANG_UTF8 = "utf8";
    private static final String LANG_BIN = "bin";
    private static final String LANG_EMPTY = "";

    /** Language, e.g. "en_EN". LANG_UTF8 is the default */
    private String language = LANG_UTF8;

    private Collator collator;

    /** Constructor - only called internally */
    private CollationDefinition(String name) {
      name = name.toLowerCase();
      this.name = name;

      // Empty collation is UTF8, treat it as such
      if (name.isEmpty()) {
        name = LANG_UTF8;
      }

      /** Language, e.g. "en_EN". LANG_UTF8 is the default */
      String lang = LANG_UTF8;

      // Variables used to construct the ICU string
      boolean firstLower = false;
      boolean firstLowerIsSet = false;

      // Parse the collation as a sequence of "-" divided options
      String[] elements = name.split("-");
      assert (elements.length > 0); // empty string detected earlier

      // Then other options
      for (int i = 0; i < elements.length; i++) {
        switch (elements[i]) {
            // ICU options
          case "ci":
            caseSensitive = false;
            break;
          case "cs":
            caseSensitive = true;
            break;
          case "ai":
            accentSensitive = false;
            break;
          case "as":
            accentSensitive = true;
            break;
          case "pi":
            punctuationSensitive = false;
            break;
          case "ps":
            punctuationSensitive = true;
            break;
          case "fu":
            firstLower = false;
            firstLowerIsSet = true;
            break;
          case "fl":
            firstLower = true;
            firstLowerIsSet = true;
            break;
            // Snowflake-specific options
          case "lower":
            lower = true;
            break;
          case "upper":
            upper = true;
            break;
          case "rtrim":
            rtrim = true;
            break;
          case "ltrim":
            ltrim = true;
            break;
          case "trim":
            ltrim = rtrim = true;
            break;
          case "forced":
            forced = true;
            break;
          default:
            // Unknown element can be the language identificator, if it's the first one
            if (i == 0) {
              language = elements[0];
            } else {
              throw new SFException(
                  ErrorCode.INVALID_COLLATION_STRING, name, "Unknown option: " + elements[i]);
            }
        }
      }

      /* Sanity checks */
      if (upper && lower)
        throw new SFException(
            ErrorCode.INVALID_COLLATION_STRING,
            name,
            "Options 'upper' and 'lower' are mutually exclusive");

      if ((language == null
          || language.equalsIgnoreCase(LANG_EMPTY)
          || language.equalsIgnoreCase(LANG_UTF8)
          || language.equalsIgnoreCase(LANG_BIN))) {
        // Check we didn't provide any of the ICU-specific options
        if (caseSensitive != null)
          throw new SFException(
              ErrorCode.INVALID_COLLATION_STRING,
              name,
              "Case sensitivity option not allowed for the UTF8 collation");
        if (accentSensitive != null)
          throw new SFException(
              ErrorCode.INVALID_COLLATION_STRING,
              name,
              "Accent sensitivity option not allowed for the UTF8 collation");
        if (firstLowerIsSet)
          throw new SFException(
              ErrorCode.INVALID_COLLATION_STRING,
              name,
              "Upper/lower preference option not allowed for the UTF8 collation");
        if (punctuationSensitive != null)
          throw new SFException(
              ErrorCode.INVALID_COLLATION_STRING,
              name,
              "Punctuation sensitivity option not allowed for the UTF8 collation");
      } else {
        // Set the defaults
        // NOTE: firstLower does not have a default value
        if (caseSensitive == null) caseSensitive = true;
        if (accentSensitive == null) accentSensitive = true;
        if (punctuationSensitive == null) punctuationSensitive = true;

        // Construct an ICU string, e.g. "de@colStrength=primary;colCaseFirst=lower"
        // This string will be used by COLLATE_TO_BINARY and others.
        // See http://userguide.icu-project.org/collation/concepts#TOC-Collator-naming-scheme

        // We start with the language, and then construct a series of ";option=something".
        // At the end, we'll switch the first ';' to '@'
        String icu = language;

        /*
         * From https://www.unicode.org/reports/tr35/tr35-collation.html#Collation_Element :
         * 3.4.1 Common settings combinations
         * Some commonly used parametric collation settings are available via combinations of LDML settings attributes:
         * - “Ignore accents”: strength=primary
         * - “Ignore accents” but take case into account: strength=primary caseLevel=on
         * - “Ignore case”: strength=secondary
         */
        if (!accentSensitive) {
          icu += ";colStrength=primary";
          // In ICU, "primary" implies case-insensitive.
          // If we're case sensitive, force it with "colCaseLevel"
          icu += ";colCaseLevel=" + (caseSensitive ? "yes" : "no");
        } else if (!caseSensitive) {
          icu += ";colStrength=secondary";
        }
        if (firstLowerIsSet) {
          icu += ";colCaseFirst=" + (firstLower ? "lower" : "upper");
        }
        if (punctuationSensitive != null) {
          icu += ";colAlternate=" + (punctuationSensitive ? "non-ignorable" : "shifted");
        }

        // Fix the first ";" to be "@"
        this.icuCollationString = icu.replaceFirst(";", "@");
      }
      if (this.icuCollationString != null) {
        ULocale locale = new ULocale(icuCollationString);
        collator = Collator.getInstance(locale);
      }
    }

    byte[] performConversion(String input) {
      /*
       * WARNING: the logic should be in sync with CollationEvaluator.cpp !!!
       */
      if (ltrim || rtrim) {
        // Apply trim
        int inputIdx = 0;
        int inputLen = input.length();

        if (ltrim) {
          while (inputLen > 0 && input.charAt(inputIdx) == ' ') {
            inputLen--;
            inputIdx++;
          }
        }
        if (rtrim) {
          while (inputLen > 0 && input.charAt(inputIdx + inputLen - 1) == ' ') {
            inputLen--;
          }
        }
        input = input.substring(inputIdx, inputIdx + inputLen);
      }

      // Handle upper/lower
      if (upper) {
        input = UCharacter.toUpperCase(input);
      }
      if (lower) {
        input = UCharacter.toLowerCase(input);
      }

      if (icuCollationString == null) {
        // No need for ICU key generation, convert (modified) input to UTF-8 bytes
        try {
          return input.getBytes("UTF-8");
        } catch (java.io.UnsupportedEncodingException e) {
          throw new SFException(ErrorCode.INTERNAL_ERROR, "Failed encoding string to UTF-8");
        }
      }

      // ICU collation is needed
      // Perform the conversion
      CollationKey cltKey = collator.getCollationKey(input);
      byte[] cltKeyBytes = cltKey.toByteArray();
      int cltKeySize = cltKeyBytes.length;

      if (cltKeySize > 0) {
        // Get rid of the trailing 0
        if (cltKeyBytes[cltKeySize - 1] != 0) {
          throw new SFException(
              ErrorCode.INTERNAL_ERROR, "unexpected_non_zero_end_of_collated_key");
        }
        cltKeySize -= 1;

        // Check if the produced key is all 0x01 - this can happen when we collate to an empty
        // string,
        // but the ICU still produces 0x01 dividers between various parts of the collation key.
        // It's better to just treat these as an empty string.
        int idx;
        for (idx = 0; idx < cltKeySize; idx++) {
          if (cltKeyBytes[idx] != 0x01) {
            break;
          }
        }

        if (idx == cltKeySize) {
          // Mark as an empty string
          cltKeySize = 0;
        }

        cltKeyBytes = Arrays.copyOf(cltKeyBytes, cltKeySize);
      }
      // Create an SFBinary
      return cltKeyBytes;
    }
  }

  private String currentMinStrValue;
  private String currentMaxStrValue;
  private String currentMinColStrValue;
  private String currentMaxColStrValue;
  private byte[] currentMinColStrValueInBytes;
  private byte[] currentMaxColStrValueInBytes;
  private BigInteger currentMinIntValue;
  private BigInteger currentMaxIntValue;
  private Double currentMinRealValue;
  private Double currentMaxRealValue;
  private long currentNullCount;
  // for binary or string columns
  private long currentMaxLength;
  private CollationDefinition collationDefinition;
  private final String collationDefinitionString;

  private static final int MAX_LOB_LEN = 32;

  /** Creates empty stats */
  RowBufferStats(String collationDefinitionString) {
    this.collationDefinitionString = collationDefinitionString;
    if (collationDefinitionString != null) {
      this.collationDefinition = new CollationDefinition(collationDefinitionString);
    }
    reset();
  }

  RowBufferStats() {
    this(null);
  }

  void reset() {
    this.currentMaxStrValue = null;
    this.currentMinStrValue = null;
    this.currentMaxColStrValue = null;
    this.currentMinColStrValue = null;
    this.currentMaxColStrValueInBytes = null;
    this.currentMinColStrValueInBytes = null;
    this.currentMaxIntValue = null;
    this.currentMinIntValue = null;
    this.currentMaxRealValue = null;
    this.currentMinRealValue = null;
    this.currentNullCount = 0;
    this.currentMaxLength = 0;
  }

  byte[] getCollatedBytes(String value) {
    if (collationDefinition != null) {
      return collationDefinition.performConversion(value);
    }
    return value.getBytes(StandardCharsets.UTF_8);
  }

  // TODO performance test this vs in place update
  static RowBufferStats getCombinedStats(RowBufferStats left, RowBufferStats right) {
    if (left.getCollationDefinitionString() != right.collationDefinitionString) {
      throw new SFException(
          ErrorCode.INVALID_COLLATION_STRING,
          "Tried to combine stats for different collations",
          String.format(
              "left=%s, right=%s",
              left.getCollationDefinitionString(), right.getCollationDefinitionString()));
    }
    RowBufferStats combined = new RowBufferStats(left.getCollationDefinitionString());

    if (left.currentMinIntValue != null) {
      combined.addIntValue(left.currentMinIntValue);
      combined.addIntValue(left.currentMaxIntValue);
    }

    if (right.currentMinIntValue != null) {
      combined.addIntValue(right.currentMinIntValue);
      combined.addIntValue(right.currentMaxIntValue);
    }

    if (left.currentMinStrValue != null) {
      combined.addStrValue(left.currentMinStrValue);
      combined.addStrValue(left.currentMaxStrValue);
      combined.addStrValue(left.currentMinColStrValue);
      combined.addStrValue(left.currentMaxColStrValue);
    }

    if (right.currentMinStrValue != null) {
      combined.addStrValue(right.currentMinStrValue);
      combined.addStrValue(right.currentMaxStrValue);
      combined.addStrValue(right.currentMinColStrValue);
      combined.addStrValue(right.currentMaxColStrValue);
    }

    if (left.currentMinRealValue != null) {
      combined.addRealValue(left.currentMinRealValue);
      combined.addRealValue(left.currentMaxRealValue);
    }

    if (right.currentMinRealValue != null) {
      combined.addRealValue(right.currentMinRealValue);
      combined.addRealValue(right.currentMaxRealValue);
    }

    combined.currentNullCount = left.currentNullCount + right.currentNullCount;
    combined.currentMaxLength = Math.max(left.currentMaxLength, right.currentMaxLength);

    return combined;
  }

  void addStrValue(String inputValue) {
    this.setCurrentMaxLength(inputValue.length());
    String value =
        inputValue.length() > MAX_LOB_LEN ? inputValue.substring(0, MAX_LOB_LEN) : inputValue;

    byte[] valueBytes = value != null ? value.getBytes(StandardCharsets.UTF_8) : null;
    byte[] collatedValueBytes = value != null ? getCollatedBytes(value) : null;

    // Check if new min/max string
    if (this.currentMinStrValue == null) {
      this.currentMinStrValue = value;
      this.currentMinColStrValue = value;
      this.currentMinColStrValueInBytes = collatedValueBytes;

      /*
      Snowflake stores the first MAX_LOB_LEN characters of a string.
      When truncating the max value, we increment the last max value
      byte by one to ensure the max value stat is greater than the actual max value.
       */
      if (inputValue.length() > MAX_LOB_LEN) {
        byte[] incrementedValueBytes = valueBytes.clone();
        byte[] incrementedCollatedValueBytes = collatedValueBytes.clone();
        incrementedValueBytes[MAX_LOB_LEN - 1]++;
        incrementedCollatedValueBytes[MAX_LOB_LEN - 1]++;
        String incrementedValue = new String(incrementedValueBytes);
        this.currentMaxStrValue = incrementedValue;
        this.currentMaxColStrValue = incrementedValue;
        this.currentMaxColStrValueInBytes = incrementedCollatedValueBytes;
      } else {
        this.currentMaxStrValue = value;
        this.currentMaxColStrValue = value;
        this.currentMaxColStrValueInBytes = collatedValueBytes;
      }
    } else {
      // Collated comparison
      if (compare(currentMinColStrValueInBytes, collatedValueBytes) > 0) {
        this.currentMinColStrValue = value;
        this.currentMinStrValue = value;
        this.currentMinColStrValueInBytes = collatedValueBytes;
      } else if (compare(currentMaxColStrValueInBytes, collatedValueBytes) < 0) {
        /*
        Snowflake stores the first MAX_LOB_LEN characters of a string.
        When truncating the max value, we increment the last max value
        byte by one to ensure the max value stat is greater than the actual max value.
         */
        if (inputValue.length() > MAX_LOB_LEN) {
          byte[] incrementedValueBytes = valueBytes.clone();
          byte[] incrementedCollatedValueBytes = collatedValueBytes.clone();
          incrementedValueBytes[MAX_LOB_LEN - 1]++;
          incrementedCollatedValueBytes[MAX_LOB_LEN - 1]++;
          String incrementedValue = new String(incrementedValueBytes);
          this.currentMaxColStrValue = incrementedValue;
          this.currentMaxStrValue = incrementedValue;
          this.currentMaxColStrValueInBytes = incrementedCollatedValueBytes;
        } else {
          this.currentMaxColStrValue = value;
          this.currentMaxStrValue = value;
          this.currentMaxColStrValueInBytes = collatedValueBytes;
        }
      }
    }
  }

  String getCurrentMinStrValue() {
    return currentMinStrValue;
  }

  String getCurrentMaxStrValue() {
    return currentMaxStrValue;
  }

  String getCurrentMinColStrValue() {
    return currentMinColStrValue;
  }

  String getCurrentMaxColStrValue() {
    return currentMaxColStrValue;
  }

  void addIntValue(BigInteger value) {
    // Set new min/max value
    if (this.currentMinIntValue == null) {
      this.currentMinIntValue = value;
      this.currentMaxIntValue = value;
    } else if (this.currentMinIntValue.compareTo(value) > 0) {
      this.currentMinIntValue = value;
    } else if (this.currentMaxIntValue.compareTo(value) < 0) {
      this.currentMaxIntValue = value;
    }
  }

  BigInteger getCurrentMinIntValue() {
    return currentMinIntValue;
  }

  BigInteger getCurrentMaxIntValue() {
    return currentMaxIntValue;
  }

  void addRealValue(Double value) {
    // Set new min/max value
    if (this.currentMinRealValue == null) {
      this.currentMinRealValue = value;
      this.currentMaxRealValue = value;
    } else if (this.currentMinRealValue.compareTo(value) > 0) {
      this.currentMinRealValue = value;
    } else if (this.currentMaxRealValue.compareTo(value) < 0) {
      this.currentMaxRealValue = value;
    }
  }

  Double getCurrentMinRealValue() {
    return currentMinRealValue;
  }

  Double getCurrentMaxRealValue() {
    return currentMaxRealValue;
  }

  void incCurrentNullCount() {
    this.currentNullCount += 1;
  }

  long getCurrentNullCount() {
    return currentNullCount;
  }

  void setCurrentMaxLength(long currentMaxLength) {
    if (currentMaxLength > this.currentMaxLength) {
      this.currentMaxLength = currentMaxLength;
    }
  }

  long getCurrentMaxLength() {
    return currentMaxLength;
  }

  /**
   * Returns the number of distinct values (NDV). A value of -1 means the number is unknown
   *
   * @return -1 indicating the NDV is unknown
   */
  long getDistinctValues() {
    return -1;
  }

  String getCollationDefinitionString() {
    return collationDefinitionString;
  }

  /**
   * Compares two byte arrays lexicographically. If the two arrays share a common prefix then the
   * lexicographic comparison is the result of comparing two elements, as if by Byte.compare(byte,
   * byte), at an index within the respective arrays that is the prefix length. Otherwise, one array
   * is a proper prefix of the other and, lexicographic comparison is the result of comparing the
   * two array lengths.
   *
   * @param a the first array to compare
   * @param b the second array to compare
   * @return the value 0 if the first and second array are equal and contain the same elements in
   *     the same order; a value less than 0 if the first array is lexicographically less than the
   *     second array; and a value greater than 0 if the first array is lexicographically greater
   *     than the second array
   */
  static int compare(byte[] a, byte[] b) {
    if (a == b) return 0;

    for (int mismatchIdx = 0; mismatchIdx < Math.min(a.length, b.length); mismatchIdx++) {
      if (a[mismatchIdx] != b[mismatchIdx]) {
        return Byte.compare(a[mismatchIdx], b[mismatchIdx]);
      }
    }

    return a.length - b.length;
  }
}
