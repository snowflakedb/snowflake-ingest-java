/*
 * Copyright (c) 2021-2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;

/**
 * In-memory cache that stores the active channels for a given Streaming Ingest client, and the
 * channels belong to the same table will be stored together in order to combine the channel data
 * during flush. The key is a fully qualified table name and the value is a set of channels that
 * belongs to this table
 *
 * @param <T> type of column data ({@link ParquetChunkData})
 */
class ChannelCache<T> {
  // Cache to hold all the valid channels, the key for the outer map is FullyQualifiedTableName and
  // the key for the inner map is ChannelName
  private final ConcurrentHashMap<
          String, ConcurrentHashMap<String, SnowflakeStreamingIngestChannelInternal<T>>>
      cache = new ConcurrentHashMap<>();

  /** Flush information for each table including last flush time and if flush is needed */
  static class FlushInfo {
    final long lastFlushTime;
    final boolean needFlush;

    FlushInfo(long lastFlushTime, boolean needFlush) {
      this.lastFlushTime = lastFlushTime;
      this.needFlush = needFlush;
    }
  }

  /** Flush information for each table, only used when max chunks in blob is 1 */
  private final ConcurrentHashMap<String, FlushInfo> tableFlushInfo = new ConcurrentHashMap<>();

  /**
   * Add a channel to the channel cache
   *
   * <p>Note: if there was a previous instance of the channel then the old one is considered
   * "invalid". Callers with a reference to the old channel object will have their writes rejected
   * as the channel reference will be marked "invalid". Similarly, calls to fetch the current status
   * of old versions of the channel will have an exception thrown as the channel is considered
   * invalid.
   *
   * @param channel
   */
  void addChannel(SnowflakeStreamingIngestChannelInternal<T> channel) {
    ConcurrentHashMap<String, SnowflakeStreamingIngestChannelInternal<T>> channels =
        this.cache.computeIfAbsent(
            channel.getFullyQualifiedTableName(), v -> new ConcurrentHashMap<>());

    // Update the last flush time for the table, add jitter to avoid all channels flush at the same
    // time when the blobs are not interleaved
    this.tableFlushInfo.putIfAbsent(
        channel.getFullyQualifiedTableName(), new FlushInfo(System.currentTimeMillis(), false));

    SnowflakeStreamingIngestChannelInternal<T> oldChannel =
        channels.put(channel.getName(), channel);
    // Invalidate old channel if it exits to block new inserts and return error to users earlier
    if (oldChannel != null) {
      String invalidationCause =
          String.format("Old channel removed from cache, channelName=%s", channel.getName());
      oldChannel.invalidate("removed from cache", invalidationCause);
    }
  }

  /**
   * Get the last flush time for a table
   *
   * @param fullyQualifiedTableName fully qualified table name
   * @return last flush time in milliseconds
   */
  Long getLastFlushTime(String fullyQualifiedTableName) {
    FlushInfo tableFlushInfo = this.tableFlushInfo.get(fullyQualifiedTableName);
    if (tableFlushInfo == null) {
      throw new SFException(
          ErrorCode.INTERNAL_ERROR,
          String.format("Last flush time for table %s not found", fullyQualifiedTableName));
    }
    return tableFlushInfo.lastFlushTime;
  }

  /**
   * Set the last flush time for a table as the current time
   *
   * @param fullyQualifiedTableName fully qualified table name
   * @param lastFlushTime last flush time in milliseconds
   */
  void setLastFlushTime(String fullyQualifiedTableName, Long lastFlushTime) {
    this.tableFlushInfo.compute(
        fullyQualifiedTableName,
        (k, v) -> {
          if (v == null) {
            throw new SFException(
                ErrorCode.INTERNAL_ERROR,
                String.format("Last flush time for table %s not found", fullyQualifiedTableName));
          }
          return new FlushInfo(lastFlushTime, v.needFlush);
        });
  }

  /**
   * Get need flush flag for a table
   *
   * @param fullyQualifiedTableName fully qualified table name
   * @return need flush flag
   */
  boolean getNeedFlush(String fullyQualifiedTableName) {
    FlushInfo tableFlushInfo = this.tableFlushInfo.get(fullyQualifiedTableName);
    if (tableFlushInfo == null) {
      throw new SFException(
          ErrorCode.INTERNAL_ERROR,
          String.format("Need flush flag for table %s not found", fullyQualifiedTableName));
    }
    return tableFlushInfo.needFlush;
  }

  /**
   * Set need flush flag for a table
   *
   * @param fullyQualifiedTableName fully qualified table name
   * @param needFlush need flush flag
   */
  void setNeedFlush(String fullyQualifiedTableName, boolean needFlush) {
    this.tableFlushInfo.compute(
        fullyQualifiedTableName,
        (k, v) -> {
          if (v == null) {
            throw new SFException(
                ErrorCode.INTERNAL_ERROR,
                String.format("Need flush flag for table %s not found", fullyQualifiedTableName));
          }
          return new FlushInfo(v.lastFlushTime, needFlush);
        });
  }

  /** Returns an immutable set view of the mappings contained in the channel cache. */
  Set<Map.Entry<String, ConcurrentHashMap<String, SnowflakeStreamingIngestChannelInternal<T>>>>
      entrySet() {
    return Collections.unmodifiableSet(cache.entrySet());
  }

  /** Returns an immutable set view of the keys contained in the channel cache. */
  Set<String> keySet() {
    return Collections.unmodifiableSet(cache.keySet());
  }

  /** Close all channels in the channel cache */
  void closeAllChannels() {
    this.cache
        .values()
        .forEach(channels -> channels.values().forEach(channel -> channel.markClosed()));
  }

  /** Remove a channel in the channel cache if the channel sequencer matches */
  // TODO: background cleaner to cleanup old stale channels that are not closed?
  void removeChannelIfSequencersMatch(SnowflakeStreamingIngestChannelInternal<T> channel) {
    cache.computeIfPresent(
        channel.getFullyQualifiedTableName(),
        (k, v) -> {
          SnowflakeStreamingIngestChannelInternal<T> channelInCache = v.get(channel.getName());
          // We need to compare the channel sequencer in case the old channel was already been
          // removed
          return channelInCache != null
                  && channelInCache.getChannelSequencer() == channel.getChannelSequencer()
                  && v.remove(channel.getName()) != null
                  && v.isEmpty()
              ? null
              : v;
        });
  }

  /** Invalidate a channel in the channel cache if the channel sequencer matches */
  void invalidateChannelIfSequencersMatch(
      String dbName,
      String schemaName,
      String tableName,
      String channelName,
      Long channelSequencer,
      String invalidationCause) {
    String fullyQualifiedTableName = String.format("%s.%s.%s", dbName, schemaName, tableName);
    ConcurrentHashMap<String, SnowflakeStreamingIngestChannelInternal<T>> channelsMapPerTable =
        cache.get(fullyQualifiedTableName);
    if (channelsMapPerTable != null) {
      SnowflakeStreamingIngestChannelInternal<T> channel = channelsMapPerTable.get(channelName);
      if (channel != null && channel.getChannelSequencer().equals(channelSequencer)) {
        channel.invalidate("invalidate with matched sequencer", invalidationCause);
      }
    }
  }

  /** Get the number of key-value pairs in the cache */
  int getSize() {
    return cache.size();
  }
}
