/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;

/** Represents an unique path to a column or sub-column. */
public class ColumnPath {
  private final List<String> path;

  public ColumnPath(@Nonnull List<String> path) {
    this.path = Collections.unmodifiableList(path);
  }

  public ColumnPath(@Nonnull String... path) {
    this.path = Collections.unmodifiableList(Arrays.asList(path));
  }

  public ColumnPath concat(@Nonnull String subColumn) {
    List<String> newPath = new ArrayList<>(this.path);
    newPath.add(subColumn);
    return new ColumnPath(newPath);
  }

  public boolean isDecedentOf(ColumnPath other) {
    if (other == null || other.path.size() >= this.path.size()) {
      return false;
    }
    for (int i = 0; i < other.path.size(); i++) {
      if (!other.path.get(i).equals(this.path.get(i))) {
        return false;
      }
    }
    return true;
  }

  @Override
  public String toString() {
    return String.join(".", path);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    ColumnPath columnPath = (ColumnPath) obj;
    return path.equals(columnPath.path);
  }

  @Override
  public int hashCode() {
    return path.hashCode();
  }
}
