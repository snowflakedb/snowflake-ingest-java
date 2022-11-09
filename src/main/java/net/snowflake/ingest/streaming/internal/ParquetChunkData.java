/*
 * Copyright (c) 2022 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import java.util.List;
import java.util.Map;

/** Parquet data holder to buffer rows. */
public class ParquetChunkData {
  final List<List<Object>> rows;
  final Map<String, String> metadata;

  /**
   * Construct parquet data chunk.
   *
   * @param rows chunk row set
   * @param metadata chunk metadata
   */
  public ParquetChunkData(List<List<Object>> rows, Map<String, String> metadata) {
    this.rows = rows;
    this.metadata = metadata;
  }
}
