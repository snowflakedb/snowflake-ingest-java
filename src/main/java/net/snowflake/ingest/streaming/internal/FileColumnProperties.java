/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.streaming.internal.BinaryStringUtils.truncateBytesAsHex;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.math.BigInteger;
import java.util.Objects;
import org.apache.parquet.column.statistics.BinaryStatistics;
import org.apache.parquet.column.statistics.BooleanStatistics;
import org.apache.parquet.column.statistics.DoubleStatistics;
import org.apache.parquet.column.statistics.FloatStatistics;
import org.apache.parquet.column.statistics.IntStatistics;
import org.apache.parquet.column.statistics.LongStatistics;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.schema.LogicalTypeAnnotation;

/** Audit register endpoint/FileColumnPropertyDTO property list. */
class FileColumnProperties {
  private int columnOrdinal;
  private int fieldId;
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

  FileColumnProperties(RowBufferStats stats) {
    this.setColumnOrdinal(stats.getOrdinal());
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

    this.setMaxStrNonCollated(null);
    this.setMinStrNonCollated(null);

    // current hex-encoded min value, truncated down to 32 bytes
    if (stats.getCurrentMinStrValue() != null) {
      String truncatedAsHex = truncateBytesAsHex(stats.getCurrentMinStrValue(), false);
      this.setMinStrValue(truncatedAsHex);
    }

    // current hex-encoded max value, truncated up to 32 bytes
    if (stats.getCurrentMaxStrValue() != null) {
      String truncatedAsHex = truncateBytesAsHex(stats.getCurrentMaxStrValue(), true);
      this.setMaxStrValue(truncatedAsHex);
    }

    this.setNullCount(stats.getCurrentNullCount());
    this.setDistinctValues(stats.getDistinctValues());
  }

  FileColumnProperties(
      int columnOrdinal, int fieldId, Statistics<?> statistics, long ndv, long maxLength) {
    this.setColumnOrdinal(columnOrdinal);
    this.setFieldId(fieldId);
    this.setNullCount(statistics.getNumNulls());
    this.setDistinctValues(ndv);
    this.setCollation(null);
    this.setMaxStrNonCollated(null);
    this.setMinStrNonCollated(null);

    if (statistics instanceof BooleanStatistics) {
      this.setMinIntValue(
          ((BooleanStatistics) statistics).genericGetMin() ? BigInteger.ONE : BigInteger.ZERO);
      this.setMaxIntValue(
          ((BooleanStatistics) statistics).genericGetMax() ? BigInteger.ONE : BigInteger.ZERO);
    } else if (statistics instanceof IntStatistics || statistics instanceof LongStatistics) {
      this.setMinIntValue(BigInteger.valueOf(((Number) statistics.genericGetMin()).longValue()));
      this.setMaxIntValue(BigInteger.valueOf(((Number) statistics.genericGetMax()).longValue()));
    } else if (statistics instanceof FloatStatistics || statistics instanceof DoubleStatistics) {
      this.setMinRealValue((Double) statistics.genericGetMin());
      this.setMaxRealValue((Double) statistics.genericGetMax());
    } else if (statistics instanceof BinaryStatistics) {
      if (statistics.type().getLogicalTypeAnnotation()
          instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
        this.setMinIntValue(new BigInteger(statistics.getMinBytes()));
        this.setMaxIntValue(new BigInteger(statistics.getMaxBytes()));
      } else {
        this.setMinStrValue(truncateBytesAsHex(statistics.getMinBytes(), false));
        this.setMaxStrValue(truncateBytesAsHex(statistics.getMaxBytes(), true));
        this.setMaxLength(maxLength);
      }
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
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  int getFieldId() {
    return fieldId;
  }

  void setFieldId(int fieldId) {
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
