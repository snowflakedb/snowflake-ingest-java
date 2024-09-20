/*
 * Copyright (c) 2021-2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.HashMap;
import java.util.Map;

/** Column metadata for each column in a Snowflake table */
class ColumnMetadata {
  private String name;
  private String internalName;
  private String type;
  private String logicalType;
  private String physicalType;
  private Integer precision;
  private Integer scale;
  private Integer byteLength;
  private Integer length;
  private boolean nullable;
  private String collation;

  /**
   * The Json serialization of Iceberg data type of the column, see <a
   * href="https://iceberg.apache.org/spec/#appendix-c-json-serialization">JSON serialization</a>
   * for more details.
   */
  private String sourceIcebergDataType;

  /**
   * The column ordinal is an internal id of the column used by server scanner for the column
   * identification.
   */
  private Integer ordinal;

  @JsonProperty("name")
  void setName(String name) {
    this.name = name;
    this.internalName = LiteralQuoteUtils.unquoteColumnName(name);
  }

  String getName() {
    return this.name;
  }

  @JsonProperty("collation")
  public String getCollation() {
    return collation;
  }

  public void setCollation(String collation) {
    this.collation = collation;
  }

  @JsonProperty("type")
  void setType(String type) {
    this.type = type;
  }

  String getType() {
    return this.type;
  }

  @JsonProperty("logical_type")
  void setLogicalType(String logicalType) {
    this.logicalType = logicalType.toUpperCase();
  }

  String getLogicalType() {
    return this.logicalType;
  }

  @JsonProperty("physical_type")
  void setPhysicalType(String physicalType) {
    this.physicalType = physicalType;
  }

  String getPhysicalType() {
    return this.physicalType;
  }

  @JsonProperty("precision")
  void setPrecision(Integer precision) {
    this.precision = precision;
  }

  Integer getPrecision() {
    return this.precision;
  }

  @JsonProperty("scale")
  void setScale(Integer scale) {
    this.scale = scale;
  }

  Integer getScale() {
    return this.scale;
  }

  @JsonProperty("byte_length")
  void setByteLength(Integer byteLength) {
    this.byteLength = byteLength;
  }

  Integer getByteLength() {
    return this.byteLength;
  }

  @JsonProperty("length")
  void setLength(Integer length) {
    this.length = length;
  }

  Integer getLength() {
    return this.length;
  }

  @JsonProperty("nullable")
  void setNullable(boolean nullable) {
    this.nullable = nullable;
  }

  boolean getNullable() {
    return this.nullable;
  }

  @JsonProperty("ordinal")
  void setOrdinal(Integer ordinal) {
    this.ordinal = ordinal;
  }

  public Integer getOrdinal() {
    return ordinal;
  }

  @JsonProperty("source_iceberg_data_type")
  void setSourceIcebergDataType(String sourceIcebergDataType) {
    this.sourceIcebergDataType = sourceIcebergDataType;
  }

  public String getSourceIcebergDataType() {
    return sourceIcebergDataType;
  }

  String getInternalName() {
    return internalName;
  }

  @Override
  public String toString() {
    Map<String, Object> map = new HashMap<>();
    map.put("name", this.name);
    map.put("type", this.type);
    map.put("logical_type", this.logicalType);
    map.put("physical_type", this.physicalType);
    map.put("precision", this.precision);
    map.put("scale", this.scale);
    map.put("byte_length", this.byteLength);
    map.put("length", this.length);
    map.put("nullable", this.nullable);
    map.put("source_iceberg_data_type", this.sourceIcebergDataType);
    return map.toString();
  }
}
