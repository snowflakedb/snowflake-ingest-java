package net.snowflake.ingest.streaming.internal;

/**
 * Class that encapsulates column properties. These are the same properties showed in the output of
 * <a href="https://docs.snowflake.com/en/sql-reference/sql/show-columns">SHOW COLUMNS</a>. Note
 * that this is slightly different than the internal column metadata used elsewhere in this SDK.
 */
public class ColumnProperties {
  private String type;

  private String logicalType;

  private Integer precision;

  private Integer scale;

  private Integer byteLength;

  private Integer length;

  private boolean nullable;

  ColumnProperties(ColumnMetadata columnMetadata) {
    this.type = columnMetadata.getType();
    this.logicalType = columnMetadata.getLogicalType();
    this.precision = columnMetadata.getPrecision();
    this.scale = columnMetadata.getScale();
    this.byteLength = columnMetadata.getByteLength();
    this.length = columnMetadata.getLength();
    this.nullable = columnMetadata.getNullable();
  }

  public String getType() {
    return type;
  }

  public String getLogicalType() {
    return logicalType;
  }

  public Integer getPrecision() {
    return precision;
  }

  public Integer getScale() {
    return scale;
  }

  public Integer getByteLength() {
    return byteLength;
  }

  public Integer getLength() {
    return length;
  }

  public boolean isNullable() {
    return nullable;
  }
}
