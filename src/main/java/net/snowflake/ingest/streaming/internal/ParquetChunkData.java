/*
 * Copyright (c) 2022 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Parquet data holder to buffer rows. */
public class ParquetChunkData {
  // buffered rows serialized into Java objects. Needed for the Parquet w/o memory optimization.
  final List<List<Object>> rows;
  final Map<String, String> metadata;

  /**
   * Construct parquet data chunk.
   *
   * @param rows buffered row data as a list
   * @param metadata chunk metadata
   */
  public ParquetChunkData(List<List<Object>> rows, Map<String, String> metadata) {
    this.rows = rows;
    // create a defensive copy of the parameter map because the argument map passed here
    // may currently be shared across multiple threads.
    this.metadata = createDefensiveCopy(metadata);
  }

  private Map<String, String> createDefensiveCopy(final Map<String, String> metadata) {
    final Map<String, String> copy = new HashMap<>(metadata);
    for (String k : metadata.keySet()) {
      copy.put(k, metadata.get(k));
    }
    return copy;
  }
}
