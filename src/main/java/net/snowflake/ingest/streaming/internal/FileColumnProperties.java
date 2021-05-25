package net.snowflake.ingest.streaming.internal;

import java.math.BigInteger;

// TODO https://snowflakecomputing.atlassian.net/browse/SNOW-354886.
//  Audit register endpoint/FileColumnPorpertyDTO property list.
class FileColumnProperties {
  private String minStrValue;

  private String maxStrValue;

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

  FileColumnProperties() {}

  FileColumnProperties(RowBufferStats stats) {
    this.setMaxIntValue(stats.getCurrentMaxIntValue());
    this.setMinIntValue(stats.getCurrentMinIntValue());
    this.setMinRealValue(stats.getCurrentMinRealValue());
    this.setMaxRealValue(stats.getCurrentMinRealValue());
    this.setMaxLength(stats.getCurrentMaxLength());
    this.setMaxStrValue(stats.getCurrentMaxStrValue());
    this.setMinStrValue(stats.getCurrentMinStrValue());
    this.setNullCount(stats.getCurrentNullCount());
    this.setDistinctValues(stats.getDistinctValues());
  }

  String getMinStrValue() {
    return minStrValue;
  }

  void setMinStrValue(String minStrValue) {
    this.minStrValue = minStrValue;
  }

  String getMaxStrValue() {
    return maxStrValue;
  }

  void setMaxStrValue(String maxStrValue) {
    this.maxStrValue = maxStrValue;
  }

  BigInteger getMinIntValue() {
    return minIntValue;
  }

  void setMinIntValue(BigInteger minIntValue) {
    this.minIntValue = minIntValue;
  }

  BigInteger getMaxIntValue() {
    return maxIntValue;
  }

  void setMaxIntValue(BigInteger maxIntValue) {
    this.maxIntValue = maxIntValue;
  }

  long getNullCount() {
    return nullCount;
  }

  void setNullCount(long nullCount) {
    this.nullCount = nullCount;
  }

  Double getMinRealValue() {
    return minRealValue;
  }

  void setMinRealValue(Double minRealValue) {
    this.minRealValue = minRealValue;
  }

  Double getMaxRealValue() {
    return maxRealValue;
  }

  void setMaxRealValue(Double maxRealValue) {
    this.maxRealValue = maxRealValue;
  }

  long getDistinctValues() {
    return distinctValues;
  }

  void setDistinctValues(long distinctValues) {
    this.distinctValues = distinctValues;
  }

  long getMaxLength() {
    return maxLength;
  }

  void setMaxLength(long maxLength) {
    this.maxLength = maxLength;
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
      sb.append(", \"maxLength\": ").append(maxLength);
    }
    sb.append(", \"distinctValues\": ").append(distinctValues);
    sb.append(", \"nullCount\": ").append(nullCount);
    return sb.append('}').toString();
  }
}
