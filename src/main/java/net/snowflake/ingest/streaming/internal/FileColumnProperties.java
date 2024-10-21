/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.streaming.internal.BinaryStringUtils.truncateBytesAsHex;
import static net.snowflake.ingest.utils.Constants.EP_NV_UNKNOWN;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.math.BigInteger;
import java.util.Objects;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;
import org.apache.parquet.schema.LogicalTypeAnnotation;

/** Audit register endpoint/FileColumnPropertyDTO property list. */
class FileColumnProperties {
  private int columnOrdinal;
  private Integer fieldId;
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

  // for elements in repeated columns
  private Long numberOfValues;

  // for binary or string columns
  private long maxLength;

  // Default value to use for min/max int when all data in the given column is NULL
  public static final BigInteger DEFAULT_MIN_MAX_INT_VAL_FOR_EP = BigInteger.valueOf(0);

  // Default value to use for min/max real when all data in the given column is NULL
  public static final Double DEFAULT_MIN_MAX_REAL_VAL_FOR_EP = 0d;

  // Default value to use for min/max string when all data in the given Iceberg column is NULL
  public static final String DEFAULT_MIN_MAX_STR_VAL_FOR_EP = "";

  /**
   * @param setAllDefaultValues Whether to set defaults for ALL fields, or only some. BDEC sets it
   *     for all but iceberg does not.
   */
  FileColumnProperties(RowBufferStats stats, boolean setAllDefaultValues) {
    this.setColumnOrdinal(stats.getOrdinal());
    this.setFieldId(stats.getFieldId());
    this.setCollation(stats.getCollationDefinitionString());

    if (setAllDefaultValues) {
      /* Set every column to default value for FDN columns if the all row values are null */
      setIntValues(stats);
      setRealValues(stats);
      setStringValues(stats, false /* replaceNullWithEmptyString */);
    } else {
      /* Only set corresponding min/max stats to default value for Iceberg columns if the all row values are null */
      switch (stats.getPrimitiveType().getPrimitiveTypeName()) {
        case BOOLEAN:
        case INT32:
        case INT64:
          setIntValues(stats);
          break;

        case FLOAT:
        case DOUBLE:
          setRealValues(stats);
          break;

        case BINARY:
          setStringValues(stats, true /* replaceNullWithEmptyString */);
          break;

        case FIXED_LEN_BYTE_ARRAY:
          if (stats.getPrimitiveType().getLogicalTypeAnnotation()
              instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
            setIntValues(stats);
          } else {
            setStringValues(stats, true /* replaceNullWithEmptyString */);
          }
          break;

        default:
          throw new SFException(
              ErrorCode.INTERNAL_ERROR,
              "Unsupported Iceberg column type: "
                  + stats.getPrimitiveType().getPrimitiveTypeName());
      }
    }

    this.setMaxLength(stats.getCurrentMaxLength());
    this.setMaxStrNonCollated(null);
    this.setMinStrNonCollated(null);
    this.setNullCount(stats.getCurrentNullCount());
    this.setDistinctValues(stats.getDistinctValues());

    if (stats.getNumberOfValues() != EP_NV_UNKNOWN) {
      this.setNumberOfValues(stats.getNumberOfValues());
    }
  }

  private void setIntValues(RowBufferStats stats) {
    this.setMaxIntValue(
        stats.getCurrentMaxIntValue() == null
            ? DEFAULT_MIN_MAX_INT_VAL_FOR_EP
            : stats.getCurrentMaxIntValue());

    this.setMinIntValue(
        stats.getCurrentMinIntValue() == null
            ? DEFAULT_MIN_MAX_INT_VAL_FOR_EP
            : stats.getCurrentMinIntValue());
  }

  private void setRealValues(RowBufferStats stats) {
    this.setMaxRealValue(
        stats.getCurrentMaxRealValue() == null
            ? DEFAULT_MIN_MAX_REAL_VAL_FOR_EP
            : stats.getCurrentMaxRealValue());

    this.setMinRealValue(
        stats.getCurrentMinRealValue() == null
            ? DEFAULT_MIN_MAX_REAL_VAL_FOR_EP
            : stats.getCurrentMinRealValue());
  }

  private void setStringValues(RowBufferStats stats, boolean replaceNullWithEmptyString) {
    // current hex-encoded max value, truncated up to 32 bytes
    if (stats.getCurrentMaxStrValue() != null) {
      String truncatedAsHex =
          truncateBytesAsHex(stats.getCurrentMaxStrValue(), true /* truncateUp */);
      this.setMaxStrValue(truncatedAsHex);
    } else if (replaceNullWithEmptyString) {
      this.setMaxStrValue(DEFAULT_MIN_MAX_STR_VAL_FOR_EP);
    }

    // current hex-encoded min value, truncated down to 32 bytes
    if (stats.getCurrentMinStrValue() != null) {
      String truncatedAsHex =
          truncateBytesAsHex(stats.getCurrentMinStrValue(), false /* truncateUp */);
      this.setMinStrValue(truncatedAsHex);
    } else if (replaceNullWithEmptyString) {
      this.setMinStrValue(DEFAULT_MIN_MAX_STR_VAL_FOR_EP);
    }
  }

  @JsonProperty("columnId")
  public int getColumnOrdinal() {
    return columnOrdinal;
  }

  public void setColumnOrdinal(int columnOrdinal) {
    this.columnOrdinal = columnOrdinal;
  }

  @JsonProperty("fieldId")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Integer getFieldId() {
    return fieldId;
  }

  public void setFieldId(Integer fieldId) {
    this.fieldId = fieldId;
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

  @JsonProperty("numberOfValues")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  Long getNumberOfValues() {
    return numberOfValues;
  }

  void setNumberOfValues(Long numberOfValues) {
    this.numberOfValues = numberOfValues;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("{");
    sb.append("\"columnOrdinal\": ").append(columnOrdinal);
    sb.append(", \"fieldId\": ").append(fieldId);
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
    sb.append(", \"numberOfValues\": ").append(numberOfValues);
    return sb.append('}').toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    FileColumnProperties that = (FileColumnProperties) o;
    return Objects.equals(columnOrdinal, that.columnOrdinal)
        && distinctValues == that.distinctValues
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
        columnOrdinal,
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
}
