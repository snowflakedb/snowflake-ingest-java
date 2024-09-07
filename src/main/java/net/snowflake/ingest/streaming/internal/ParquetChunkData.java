/*
 * Copyright (c) 2022 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Map;
import org.apache.parquet.hadoop.BdecParquetWriter;

/** Parquet data holder to buffer rows. */
public class ParquetChunkData {
  // buffered rows serialized into Java objects. Needed for the Parquet w/o memory optimization.
  final List<List<Object>> rows;

  final BdecParquetWriter parquetWriter;
  final ByteArrayOutputStream output;
  final Map<String, String> metadata;

  /**
   * Construct parquet data chunk.
   *
   * @param rows buffered row data as a list
   * @param parquetWriter buffered parquet row data
   * @param output byte array file output
   * @param metadata chunk metadata
   */
  public ParquetChunkData(
      List<List<Object>> rows,
      BdecParquetWriter parquetWriter,
      ByteArrayOutputStream output,
      Map<String, String> metadata) {
    this.rows = rows;
    this.parquetWriter = parquetWriter;
    this.output = output;
    this.metadata = metadata;
  }
}
