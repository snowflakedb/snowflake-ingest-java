package net.snowflake.ingest.streaming.internal;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import org.apache.commons.codec.binary.Hex;

/** Audit register endpoint/FileColumnPropertyDTO property list. */
class FileColumnProperties {
  private String minStrValue;

  private String maxStrValue;

  private String collation;

  private String minStrNonCollated;

  private String maxStrNonCollated;

  // 128-bit precision needed
  private BigInteger minIntValue;

  // 128-bit precision needed
  private BigInteger maxIntValue;

  // 64-bit precision
  private Double minRealValue;

  // 64-bit precision
  private Double maxRealValue;

  private long distinctValues;

  private long nullCount;

  // for binary or string columns
  private long maxLength;

  // Default value to use for min/max int when all data in the given column is NULL
  public static final BigInteger DEFAULT_MIN_MAX_INT_VAL_FOR_EP = BigInteger.valueOf(0);

  // Default value to use for min/max real when all data in the given column is NULL
  public static final Double DEFAULT_MIN_MAX_REAL_VAL_FOR_EP = 0d;

  private static final int MAX_LOB_LEN = 32;

  FileColumnProperties(RowBufferStats stats) {
    this.setCollation(stats.getCollationDefinitionString());
    this.setMaxIntValue(
        stats.getCurrentMaxIntValue() == null
            ? DEFAULT_MIN_MAX_INT_VAL_FOR_EP
            : stats.getCurrentMaxIntValue());
    this.setMinIntValue(
        stats.getCurrentMinIntValue() == null
            ? DEFAULT_MIN_MAX_INT_VAL_FOR_EP
            : stats.getCurrentMinIntValue());
    this.setMinRealValue(
        stats.getCurrentMinRealValue() == null
            ? DEFAULT_MIN_MAX_REAL_VAL_FOR_EP
            : stats.getCurrentMinRealValue());
    this.setMaxRealValue(
        stats.getCurrentMaxRealValue() == null
            ? DEFAULT_MIN_MAX_REAL_VAL_FOR_EP
            : stats.getCurrentMaxRealValue());
    this.setMaxLength(stats.getCurrentMaxLength());

    // current hex-encoded non-collated min value. If it is longer than 32 bytes, it is null.
    // No truncation is happening here.
    if (stats.getCurrentMinNonColStrValue() != null) {
      byte[] minNonCollated = stats.getCurrentMinNonColStrValue().getBytes(StandardCharsets.UTF_8);
      this.setMinStrNonCollated(
          minNonCollated.length > 32 ? null : truncateBytesAsHex(minNonCollated, false));
    }

    // current hex-encoded non-collated max value. If it is longer than 32 bytes, it is null.
    // No truncation is happening here.
    if (stats.getCurrentMaxNonColStrValue() != null) {
      byte[] maxNonCollated = stats.getCurrentMaxNonColStrValue().getBytes(StandardCharsets.UTF_8);
      this.setMaxStrNonCollated(
          maxNonCollated.length > 32 ? null : truncateBytesAsHex(maxNonCollated, true));
    }

    // current hex-encoded collated min value, truncated down to 32 bytes
    if (stats.getCurrentMinColStrValueInBytes() != null) {
      String truncatedAsHex = truncateBytesAsHex(stats.getCurrentMinColStrValueInBytes(), false);
      this.setMinStrValue(truncatedAsHex);
    }

    // current hex-encoded collated max value, truncated up to 32 bytes
    if (stats.getCurrentMaxColStrValueInBytes() != null) {
      String truncatedAsHex = truncateBytesAsHex(stats.getCurrentMaxColStrValueInBytes(), true);
      this.setMaxStrValue(truncatedAsHex);
    }

    this.setNullCount(stats.getCurrentNullCount());
    this.setDistinctValues(stats.getDistinctValues());
  }

  // Annotation required in order to have package private fields serialized
  @JsonProperty("minStrValue")
  String getMinStrValue() {
    return minStrValue;
  }

  void setMinStrValue(String minStrValue) {
    this.minStrValue = minStrValue;
  }

  @JsonProperty("maxStrValue")
  String getMaxStrValue() {
    return maxStrValue;
  }

  void setMaxStrValue(String maxStrValue) {
    this.maxStrValue = maxStrValue;
  }

  @JsonProperty("minIntValue")
  BigInteger getMinIntValue() {
    return minIntValue;
  }

  void setMinIntValue(BigInteger minIntValue) {
    this.minIntValue = minIntValue;
  }

  @JsonProperty("maxIntValue")
  BigInteger getMaxIntValue() {
    return maxIntValue;
  }

  void setMaxIntValue(BigInteger maxIntValue) {
    this.maxIntValue = maxIntValue;
  }

  @JsonProperty("nullCount")
  long getNullCount() {
    return nullCount;
  }

  void setNullCount(long nullCount) {
    this.nullCount = nullCount;
  }

  @JsonProperty("minRealValue")
  Double getMinRealValue() {
    return minRealValue;
  }

  void setMinRealValue(Double minRealValue) {
    this.minRealValue = minRealValue;
  }

  @JsonProperty("maxRealValue")
  Double getMaxRealValue() {
    return maxRealValue;
  }

  void setMaxRealValue(Double maxRealValue) {
    this.maxRealValue = maxRealValue;
  }

  @JsonProperty("distinctValues")
  long getDistinctValues() {
    return distinctValues;
  }

  void setDistinctValues(long distinctValues) {
    this.distinctValues = distinctValues;
  }

  @JsonProperty("maxLength")
  long getMaxLength() {
    return maxLength;
  }

  void setMaxLength(long maxLength) {
    this.maxLength = maxLength;
  }

  @JsonProperty("collation")
  String getCollation() {
    return collation;
  }

  void setCollation(String collation) {
    this.collation = collation;
  }

  @JsonProperty("minStrNonCollated")
  String getMinStrNonCollated() {
    return minStrNonCollated;
  }

  void setMinStrNonCollated(String minStrNonCollated) {
    this.minStrNonCollated = minStrNonCollated;
  }

  @JsonProperty("maxStrNonCollated")
  String getMaxStrNonCollated() {
    return maxStrNonCollated;
  }

  void setMaxStrNonCollated(String maxStrNonCollated) {
    this.maxStrNonCollated = maxStrNonCollated;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("{");
    if (minIntValue != null) {
      sb.append(", \"minIntValue\": ").append(minIntValue);
      sb.append(", \"maxIntValue\": ").append(maxIntValue);
    } else if (minRealValue != null) {
      sb.append(", \"minRealValue\": ").append(minRealValue);
      sb.append(", \"maxRealValue\": ").append(maxRealValue);
    } else // string
    {
      sb.append(", \"minStrValue\": \"").append(minStrValue).append('"');
      sb.append(", \"maxStrValue\": \"").append(maxStrValue).append('"');
      sb.append(", \"minStrNonCollatedValue\": \"").append(minStrNonCollated).append('"');
      sb.append(", \"maxStrNonCollatedValue\": \"").append(maxStrNonCollated).append('"');
      sb.append(", \"collation\": \"").append(collation).append('"');
      sb.append(", \"maxLength\": ").append(maxLength);
    }
    sb.append(", \"distinctValues\": ").append(distinctValues);
    sb.append(", \"nullCount\": ").append(nullCount);
    return sb.append('}').toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    FileColumnProperties that = (FileColumnProperties) o;
    return distinctValues == that.distinctValues
        && nullCount == that.nullCount
        && maxLength == that.maxLength
        && Objects.equals(minStrValue, that.minStrValue)
        && Objects.equals(maxStrValue, that.maxStrValue)
        && Objects.equals(collation, that.collation)
        && Objects.equals(minStrNonCollated, that.minStrNonCollated)
        && Objects.equals(maxStrNonCollated, that.maxStrNonCollated)
        && Objects.equals(minIntValue, that.minIntValue)
        && Objects.equals(maxIntValue, that.maxIntValue)
        && Objects.equals(minRealValue, that.minRealValue)
        && Objects.equals(maxRealValue, that.maxRealValue);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        minStrValue,
        maxStrValue,
        collation,
        minStrNonCollated,
        maxStrNonCollated,
        minIntValue,
        maxIntValue,
        minRealValue,
        maxRealValue,
        distinctValues,
        nullCount,
        maxLength);
  }

  /** XP-compatible string truncation (FdnColDataContainer::Metrics::updateStrMinMax) */
  static String truncateBytesAsHex(byte[] bytes, boolean truncateUp) {
    if (bytes.length <= MAX_LOB_LEN) {
      return Hex.encodeHexString(bytes);
    }

    // Round the least significant byte(s) up
    if (truncateUp) {
      int idx;
      for (idx = MAX_LOB_LEN - 1; idx >= 0; idx--) {
        if (++bytes[idx] != 0) {
          break;
        }
      }
      // Whole prefix overflew, return infinity
      if (idx == -1) {
        // Switch full prefix back to 0xff
        for (int i = 0; i < MAX_LOB_LEN; i++) {
          bytes[i] = -1;
        }
        return Hex.encodeHexString(ByteBuffer.wrap(bytes, 0, MAX_LOB_LEN + 1));
      }
    }

    return Hex.encodeHexString(ByteBuffer.wrap(bytes, 0, MAX_LOB_LEN));
  }
}
