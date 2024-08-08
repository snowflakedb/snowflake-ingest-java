/*
 * Copyright (c) 2022-2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

/** Helper class to build ColumnMetadata */
public class ColumnMetadataBuilder {
  private String name;
  private String type;
  private String logicalType;
  private String physicalType;
  private Integer precision;
  private Integer scale;
  private Integer byteLength;
  private Integer length;
  private boolean nullable;
  private String collation;
  private String sourceIcebergDataType;

  private Integer ordinal;

  /**
   * Returns a new ColumnMetadata builder
   *
   * @return builder
   */
  public static ColumnMetadataBuilder newBuilder() {
    ColumnMetadataBuilder mb = new ColumnMetadataBuilder();
    mb.name = "testCol";
    mb.byteLength = 14;
    mb.length = 11;
    mb.scale = 0;
    mb.precision = 4;
    return mb;
  }

  /**
   * Set name
   *
   * @param name column name
   * @return columnMetadataBuilder object
   */
  public ColumnMetadataBuilder name(String name) {
    this.name = name;
    return this;
  }

  /**
   * Set type
   *
   * @param type type (as defined in ColumnMetadata)
   * @return columnMetadataBuilder object
   */
  public ColumnMetadataBuilder type(String type) {
    this.type = type;
    return this;
  }

  /**
   * Set column logical type
   *
   * @param logicalType logical type
   * @return columnMetadata object
   */
  public ColumnMetadataBuilder logicalType(String logicalType) {
    this.logicalType = logicalType;
    return this;
  }

  /**
   * Set column physical type
   *
   * @param physicalType physical type
   * @return columnMetadataBuilder object
   */
  public ColumnMetadataBuilder physicalType(String physicalType) {
    this.physicalType = physicalType;
    return this;
  }

  /**
   * Set column precision
   *
   * @param precision precision
   * @return columnMetadataBuilder object
   */
  public ColumnMetadataBuilder precision(int precision) {
    this.precision = precision;
    return this;
  }

  /**
   * Set column scale
   *
   * @param scale scale
   * @return columnMetadataBuilder object
   */
  public ColumnMetadataBuilder scale(int scale) {
    this.scale = scale;
    return this;
  }

  /**
   * Set column length in bytes
   *
   * @param byteLength length in bytes
   * @return columnMetadataBuilder object
   */
  public ColumnMetadataBuilder byteLength(int byteLength) {
    this.byteLength = byteLength;
    return this;
  }

  /**
   * Set column length (in chars)
   *
   * @param length length
   * @return columnMetadataBuilder object
   */
  public ColumnMetadataBuilder length(int length) {
    this.length = length;
    return this;
  }

  /**
   * Set column nullability
   *
   * @param nullable nullable
   * @return columnMetadataBuilder object
   */
  public ColumnMetadataBuilder nullable(boolean nullable) {
    this.nullable = nullable;
    return this;
  }

  /**
   * Set column collation
   *
   * @param collation collation
   * @return columnMetadataBuilder object
   */
  public ColumnMetadataBuilder collation(String collation) {
    this.collation = collation;
    return this;
  }

  /**
   * Set column source Iceberg data type
   *
   * @param sourceIcebergDataType source Iceberg data type string
   * @return columnMetadataBuilder object
   */
  public ColumnMetadataBuilder sourceIcebergDataType(String sourceIcebergDataType) {
    this.sourceIcebergDataType = sourceIcebergDataType;
    return this;
  }

  /**
   * Set column ordinal
   *
   * @param ordinal ordinal
   * @return columnMetadataBuilder object
   */
  public ColumnMetadataBuilder ordinal(int ordinal) {
    this.ordinal = ordinal;
    return this;
  }

  /**
   * Build a columnMetadata object from the set values
   *
   * @return columnMetadata object
   */
  public ColumnMetadata build() {
    ColumnMetadata colMetadata = new ColumnMetadata();
    colMetadata.setName(name);
    colMetadata.setType(type);
    colMetadata.setPhysicalType(physicalType);
    colMetadata.setNullable(nullable);
    colMetadata.setLogicalType(logicalType);
    colMetadata.setByteLength(byteLength);
    colMetadata.setLength(length);
    colMetadata.setScale(scale);
    colMetadata.setPrecision(precision);
    colMetadata.setCollation(collation);
    colMetadata.setSourceIcebergDataType(sourceIcebergDataType);
    colMetadata.setOrdinal(ordinal);
    return colMetadata;
  }
}
