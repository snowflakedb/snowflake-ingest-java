/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import org.apache.parquet.schema.Type;

/** Represents a column in a Parquet file. */
class ParquetColumn {
  final ColumnMetadata columnMetadata;
  final int index;
  final Type type;

  ParquetColumn(ColumnMetadata columnMetadata, int index, Type type) {
    this.columnMetadata = columnMetadata;
    this.index = index;
    this.type = type;
  }
}
