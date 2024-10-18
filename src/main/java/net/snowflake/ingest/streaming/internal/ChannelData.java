/*
 * Copyright (c) 2021-2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.Pair;
import net.snowflake.ingest.utils.SFException;

/**
 * Contains the data and metadata returned for each channel flush, which will be used to build the
 * blob and register blob request
 *
 * @param <T> type of column data (Parquet {@link ParquetChunkData}
 */
class ChannelData<T> {
  private Long rowSequencer;
  private String endOffsetToken;
  private String startOffsetToken;
  private T vectors;
  private float bufferSize;
  private int rowCount;
  private Map<String, RowBufferStats> columnEps;
  private Pair<Long, Long> minMaxInsertTimeInMs;
  private ChannelFlushContext channelFlushContext;
  private Supplier<Flusher<T>> flusherFactory;

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
      throw new SFException(
          ErrorCode.INTERNAL_ERROR,
          String.format(
              "Column stats map key size mismatch, left=%d, right=%d, leftKeySet=%s,"
                  + " rightKeySet=%s",
              left.size(), right.size(), left.keySet(), right.keySet()));
    }

    Map<String, RowBufferStats> result = new HashMap<>();
    try {
      for (String key : left.keySet()) {
        RowBufferStats leftStats = left.get(key);
        RowBufferStats rightStats = right.get(key);
        result.put(key, RowBufferStats.getCombinedStats(leftStats, rightStats));
      }
    } catch (NullPointerException npe) {
      throw new SFException(npe, ErrorCode.INTERNAL_ERROR, "Column stats map key mismatch");
    }
    return result;
  }

  /**
   * Combines the two paris of min/max insert timestamp together
   *
   * @return A new pair which the first element is min(left min, right min) and the second element
   *     is max(left max, right max)
   */
  public static Pair<Long, Long> getCombinedMinMaxInsertTimeInMs(
      Pair<Long, Long> left, Pair<Long, Long> right) {
    return new Pair<>(
        Math.min(left.getFirst(), right.getFirst()), Math.max(left.getSecond(), right.getSecond()));
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

  String getEndOffsetToken() {
    return this.endOffsetToken;
  }

  String getStartOffsetToken() {
    return this.startOffsetToken;
  }

  void setEndOffsetToken(String endOffsetToken) {
    this.endOffsetToken = endOffsetToken;
  }

  void setStartOffsetToken(String startOffsetToken) {
    this.startOffsetToken = startOffsetToken;
  }

  T getVectors() {
    return this.vectors;
  }

  void setVectors(T vectors) {
    this.vectors = vectors;
  }

  int getRowCount() {
    return this.rowCount;
  }

  void setRowCount(int rowCount) {
    this.rowCount = rowCount;
  }

  float getBufferSize() {
    return this.bufferSize;
  }

  void setBufferSize(float bufferSize) {
    this.bufferSize = bufferSize;
  }

  public ChannelFlushContext getChannelContext() {
    return channelFlushContext;
  }

  public void setChannelContext(ChannelFlushContext channelFlushContext) {
    this.channelFlushContext = channelFlushContext;
  }

  public Flusher<T> createFlusher() {
    return flusherFactory.get();
  }

  public void setFlusherFactory(Supplier<Flusher<T>> flusherFactory) {
    this.flusherFactory = flusherFactory;
  }

  Pair<Long, Long> getMinMaxInsertTimeInMs() {
    return this.minMaxInsertTimeInMs;
  }

  void setMinMaxInsertTimeInMs(Pair<Long, Long> minMaxInsertTimeInMs) {
    this.minMaxInsertTimeInMs = minMaxInsertTimeInMs;
  }

  @Override
  public String toString() {
    return "ChannelData{"
        + "rowSequencer="
        + rowSequencer
        + ", endOffsetToken='"
        + endOffsetToken
        + ", startOffsetToken='"
        + startOffsetToken
        + '\''
        + ", bufferSize="
        + bufferSize
        + ", rowCount="
        + rowCount
        + ", channelContext="
        + channelFlushContext
        + '}';
  }
}
