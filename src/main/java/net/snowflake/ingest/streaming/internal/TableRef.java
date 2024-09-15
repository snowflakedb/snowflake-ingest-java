/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import javax.annotation.Nonnull;
import net.snowflake.ingest.utils.Utils;

/**
 * Class to carry around the table pointer across the SDK codebase. This is being retrofitted into
 * places that used to work with a fullyQualifiedTableName string. Can be used as a key in maps (has
 * equals/hashcode implemented)
 */
class TableRef {
  final String dbName;
  final String schemaName;
  final String tableName;
  final String fullyQualifiedName;

  TableRef(@Nonnull String dbName, @Nonnull String schemaName, @Nonnull String tableName) {
    this.dbName = dbName;
    this.schemaName = schemaName;
    this.tableName = tableName;
    this.fullyQualifiedName = Utils.getFullyQualifiedTableName(dbName, schemaName, tableName);
  }

  @Override
  public int hashCode() {
    return dbName.hashCode() ^ schemaName.hashCode() ^ tableName.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (this == obj) {
      return true;
    }

    if (TableRef.class != obj.getClass()) {
      return false;
    }

    final TableRef other = (TableRef) obj;
    return this.dbName.equals(other.dbName)
        && this.schemaName.equals(other.schemaName)
        && this.tableName.equals(other.tableName);
  }

  @Override
  public String toString() {
    return this.fullyQualifiedName;
  }
}
