/*
 * Copyright (c) 2022 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Arrays;

/** Constraint metadata for each constraint on a Snowflake table */
class ConstraintMetadata {

  private String name;

  private String kind;

  private String[] columns;

  @JsonProperty("name")
  void setName(String name) {
    this.name = name;
  }

  String getName() {
    return this.name;
  }

  @JsonProperty("kind")
  public String getKind() {
    return kind;
  }

  public void setKind(String kind) {
    this.kind = kind;
  }

  @JsonProperty("columns")
  void setColumns(String[] columns) {
    this.columns = columns;
  }

  String[] getColumns() {
    return this.columns;
  }

  @Override
  public String toString() {
    return "ConstraintMetadata{"
        + "name='"
        + name
        + '\''
        + ", kind='"
        + kind
        + '\''
        + ", columns="
        + Arrays.toString(columns)
        + '}';
  }
}
