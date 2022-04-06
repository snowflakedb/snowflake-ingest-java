/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import java.util.HashMap;
import java.util.Map;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * Contains the data and metadata returned for each channel flush, which will be used to build the
 * blob and register blob request
 */
class ChannelData {
  private Long rowSequencer;
  private String offsetToken;
  private VectorSchemaRoot vectors;
  private float bufferSize;
  private SnowflakeStreamingIngestChannelInternal channel;
  private Map<String, RowBufferStats> columnEps;

  // TODO performance test this vs in place update
  /**
   * Combines two maps of column name to RowBufferStats. Matches left and right inputs on the column
   * name map key and then combines RowBufferStats using RowBufferStats.getCombinedStats. Left and
   * right are interchangeable.
   *
   * @param left Map of column name to RowBufferStats
   * @param right Map of column name to RowBufferStats
   * @return Map of column name to the combined RowBufferStats of left and right for the column
   */
  public static Map<String, RowBufferStats> getCombinedColumnStatsMap(
      Map<String, RowBufferStats> left, Map<String, RowBufferStats> right) {
    if (left == null || right == null) {
      throw new SFException(ErrorCode.INTERNAL_ERROR, "null column stats");
    }
    if (left.size() != right.size()) {
      throw new SFException(ErrorCode.INTERNAL_ERROR, "Column stats map key mismatch");
    }
    Map<String, RowBufferStats> result = new HashMap<>();

    try {
      for (String key : left.keySet()) {
        RowBufferStats leftStats = left.get(key);
        RowBufferStats rightStats = right.get(key);
        result.put(key, RowBufferStats.getCombinedStats(leftStats, rightStats));
      }
    } catch (NullPointerException npe) {
      throw new SFException(ErrorCode.INTERNAL_ERROR, "Column stats map key mismatch");
    }
    return result;
  }

  public Map<String, RowBufferStats> getColumnEps() {
    return columnEps;
  }

  public void setColumnEps(Map<String, RowBufferStats> columnEps) {
    this.columnEps = columnEps;
  }

  Long getRowSequencer() {
    return this.rowSequencer;
  }

  void setRowSequencer(Long rowSequencer) {
    this.rowSequencer = rowSequencer;
  }

  String getOffsetToken() {
    return this.offsetToken;
  }

  void setOffsetToken(String offsetToken) {
    this.offsetToken = offsetToken;
  }

  VectorSchemaRoot getVectors() {
    return this.vectors;
  }

  void setVectors(VectorSchemaRoot vectors) {
    this.vectors = vectors;
  }

  int getRowCount() {
    return this.vectors.getRowCount();
  }

  float getBufferSize() {
    return this.bufferSize;
  }

  void setBufferSize(float bufferSize) {
    this.bufferSize = bufferSize;
  }

  SnowflakeStreamingIngestChannelInternal getChannel() {
    return this.channel;
  }

  void setChannel(SnowflakeStreamingIngestChannelInternal channel) {
    this.channel = channel;
  }

  @Override
  public String toString() {
    return this.channel.toString();
  }
}
