/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import java.util.Map;
import org.apache.parquet.schema.Type;

/**
 * Util class that contains Parquet type and other metadata for that type needed by the Snowflake
 * server side scanner
 */
class ParquetTypeInfo {
  private final Type parquetType;
  private final Map<String, String> metadata;

  ParquetTypeInfo(Type parquetType, Map<String, String> metadata) {
    this.parquetType = parquetType;
    this.metadata = metadata;
  }

  public Type getParquetType() {
    return this.parquetType;
  }

  public Map<String, String> getMetadata() {
    return this.metadata;
  }
}
