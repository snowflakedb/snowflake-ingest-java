/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import net.snowflake.common.core.CollationDefinitionBase;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;

/** Keeps track of the active EP stats, used to generate a file EP info */
class RowBufferStats {

  private String currentMinNonColStrValue;
  private String currentMaxNonColStrValue;
  private byte[] currentMinColStrValueInBytes;
  private byte[] currentMaxColStrValueInBytes;
  private BigInteger currentMinIntValue;
  private BigInteger currentMaxIntValue;
  private Double currentMinRealValue;
  private Double currentMaxRealValue;
  private long currentNullCount;
  // for binary or string columns
  private long currentMaxLength;
  private CollationDefinitionBase collationDefinition;
  private final String collationDefinitionString;

  /** Creates empty stats */
  RowBufferStats(String collationDefinitionString) {
    this.collationDefinitionString = collationDefinitionString;
    if (collationDefinitionString != null) {
      this.collationDefinition = new CollationDefinitionBase(collationDefinitionString, true);
    }
    reset();
  }

  RowBufferStats() {
    this(null);
  }

  void reset() {
    this.currentMaxNonColStrValue = null;
    this.currentMinNonColStrValue = null;
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
      return collationDefinition.performConversionToBinary(value).getBytes();
    }
    return value.getBytes(StandardCharsets.UTF_8);
  }

  // TODO performance test this vs in place update
  static RowBufferStats getCombinedStats(RowBufferStats left, RowBufferStats right) {
    if (!Objects.equals(left.getCollationDefinitionString(), right.collationDefinitionString)) {
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

    if (left.currentMinColStrValueInBytes != null) {
      combined.addStrValue(left.currentMinNonColStrValue);
      combined.addStrValue(left.currentMaxNonColStrValue);
    }

    if (right.currentMinColStrValueInBytes != null) {
      combined.addStrValue(right.currentMinNonColStrValue);
      combined.addStrValue(right.currentMaxNonColStrValue);
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

  void addStrValue(String value) {
    this.setCurrentMaxLength(value.getBytes(StandardCharsets.UTF_8).length);

    byte[] valueCollated = getCollatedBytes(value);

    // Check if new min/max string
    if (this.currentMinColStrValueInBytes == null) {
      this.currentMinColStrValueInBytes = valueCollated;
      this.currentMinNonColStrValue = value;
      this.currentMaxColStrValueInBytes = valueCollated;
      this.currentMaxNonColStrValue = value;
    } else {
      // Collated comparison
      if (compareUnsigned(currentMinColStrValueInBytes, valueCollated) > 0) {
        this.currentMinColStrValueInBytes = valueCollated;
        this.currentMinNonColStrValue = value;
      } else if (compareUnsigned(currentMaxColStrValueInBytes, valueCollated) < 0) {
        this.currentMaxColStrValueInBytes = valueCollated;
        this.currentMaxNonColStrValue = value;
      }
    }
  }

  byte[] getCurrentMinColStrValueInBytes() {
    return currentMinColStrValueInBytes;
  }

  byte[] getCurrentMaxColStrValueInBytes() {
    return currentMaxColStrValueInBytes;
  }

  public String getCurrentMinNonColStrValue() {
    return currentMinNonColStrValue;
  }

  public String getCurrentMaxNonColStrValue() {
    return currentMaxNonColStrValue;
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
  static int compareUnsigned(byte[] a, byte[] b) {
    if (a == b) return 0;

    for (int mismatchIdx = 0; mismatchIdx < Math.min(a.length, b.length); mismatchIdx++) {
      if (a[mismatchIdx] != b[mismatchIdx]) {
        return Byte.toUnsignedInt(a[mismatchIdx]) - Byte.toUnsignedInt(b[mismatchIdx]);
      }
    }

    return a.length - b.length;
  }
}
