/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.HashMap;
import java.util.Map;

/** Column metadata for each column in a Snowflake table */
public class ColumnMetadata {
  private String name;
  private String type;
  private String logicalType;
  private String physicalType;
  private Integer precision;
  private Integer scale;
  private Integer byteLength;
  private Integer length;
  private boolean nullable;

  @JsonProperty("name")
  public void setName(String name) {
    this.name = name;
  }

  public String getName() {
    return this.name;
  }

  @JsonProperty("type")
  public void setType(String type) {
    this.type = type;
  }

  public String getType() {
    return this.type;
  }

  @JsonProperty("logical_type")
  public void setLogicalType(String logicalType) {
    this.logicalType = logicalType.toUpperCase();
  }

  public String getLogicalType() {
    return this.logicalType;
  }

  @JsonProperty("physical_type")
  public void setPhysicalType(String physicalType) {
    this.physicalType = physicalType;
  }

  public String getPhysicalType() {
    return this.physicalType;
  }

  @JsonProperty("precision")
  public void setPrecision(Integer precision) {
    this.precision = precision;
  }

  public Integer getPrecision() {
    return this.precision;
  }

  @JsonProperty("scale")
  public void setScale(Integer scale) {
    this.scale = scale;
  }

  public Integer getScale() {
    return this.scale;
  }

  @JsonProperty("byte_length")
  public void setByteLength(Integer byteLength) {
    this.byteLength = byteLength;
  }

  public Integer getByteLength() {
    return this.byteLength;
  }

  @JsonProperty("length")
  public void setLength(Integer length) {
    this.length = length;
  }

  public Integer getLength() {
    return this.length;
  }

  @JsonProperty("nullable")
  public void setNullable(boolean nullable) {
    this.nullable = nullable;
  }

  public boolean getNullable() {
    return this.nullable;
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
    return map.toString();
  }
}
