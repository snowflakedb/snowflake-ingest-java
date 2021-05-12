package net.snowflake.ingest.streaming.internal;

import java.math.BigInteger;

// TODO https://snowflakecomputing.atlassian.net/browse/SNOW-354886.
//  Audit register endpoint/FileColumnPorpertyDTO property list.
public class FileColumnProperties {
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

  public FileColumnProperties() {}

  public FileColumnProperties(RowBufferStats stats) {
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

  public String getMinStrValue() {
    return minStrValue;
  }

  public void setMinStrValue(String minStrValue) {
    this.minStrValue = minStrValue;
  }

  public String getMaxStrValue() {
    return maxStrValue;
  }

  public void setMaxStrValue(String maxStrValue) {
    this.maxStrValue = maxStrValue;
  }

  public BigInteger getMinIntValue() {
    return minIntValue;
  }

  public void setMinIntValue(BigInteger minIntValue) {
    this.minIntValue = minIntValue;
  }

  public BigInteger getMaxIntValue() {
    return maxIntValue;
  }

  public void setMaxIntValue(BigInteger maxIntValue) {
    this.maxIntValue = maxIntValue;
  }

  public long getNullCount() {
    return nullCount;
  }

  public void setNullCount(long nullCount) {
    this.nullCount = nullCount;
  }

  public Double getMinRealValue() {
    return minRealValue;
  }

  public void setMinRealValue(Double minRealValue) {
    this.minRealValue = minRealValue;
  }

  public Double getMaxRealValue() {
    return maxRealValue;
  }

  public void setMaxRealValue(Double maxRealValue) {
    this.maxRealValue = maxRealValue;
  }

  public long getDistinctValues() {
    return distinctValues;
  }

  public void setDistinctValues(long distinctValues) {
    this.distinctValues = distinctValues;
  }

  public long getMaxLength() {
    return maxLength;
  }

  public void setMaxLength(long maxLength) {
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
