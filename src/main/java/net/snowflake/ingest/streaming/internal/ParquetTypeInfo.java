/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import java.util.Map;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

/**
 * Util class that contains Parquet type and other metadata for that type needed by the Snowflake
 * server side scanner
 */
class ParquetTypeInfo {
  private Type parquetType;
  private Map<String, String> metadata;

  public Type getParquetType() {
    return this.parquetType;
  }

  public Map<String, String> getMetadata() {
    return this.metadata;
  }

  public void setParquetType(Type parquetType) {
    this.parquetType = parquetType;
  }

  public void setMetadata(Map<String, String> metadata) {
    this.metadata = metadata;
  }

  public PrimitiveType.PrimitiveTypeName getPrimitiveTypeName() {
    return parquetType.asPrimitiveType().getPrimitiveTypeName();
  }
}
