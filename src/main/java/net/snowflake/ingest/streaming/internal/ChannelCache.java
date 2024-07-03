/*
 * Copyright (c) 2021-2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

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

  // Last flush time for each table, the key is FullyQualifiedTableName.
  private final ConcurrentHashMap<String, Long> lastFlushTime = new ConcurrentHashMap<>();

  // Need flush flag for each table, the key is FullyQualifiedTableName.
  private final ConcurrentHashMap<String, Boolean> needFlush = new ConcurrentHashMap<>();

  /**
   * Add a channel to the channel cache
   *
   * @param channel
   */
  void addChannel(SnowflakeStreamingIngestChannelInternal<T> channel) {
    ConcurrentHashMap<String, SnowflakeStreamingIngestChannelInternal<T>> channels =
        this.cache.computeIfAbsent(
            channel.getFullyQualifiedTableName(), v -> new ConcurrentHashMap<>());

    // Update the last flush time for the table, add jitter to avoid all channels flush at the same
    // time when the blobs are not interleaved
    this.lastFlushTime.putIfAbsent(
        channel.getFullyQualifiedTableName(),
        System.currentTimeMillis() + (long) (Math.random() * 1000));

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
    return this.lastFlushTime.get(fullyQualifiedTableName);
  }

  /**
   * Set the last flush time for a table as the current time
   *
   * @param fullyQualifiedTableName fully qualified table name
   * @param lastFlushTime last flush time in milliseconds
   */
  void setLastFlushTime(String fullyQualifiedTableName, Long lastFlushTime) {
    this.lastFlushTime.put(fullyQualifiedTableName, lastFlushTime);
  }

  /**
   * Get need flush flag for a table
   *
   * @param fullyQualifiedTableName fully qualified table name
   * @return need flush flag
   */
  Boolean getNeedFlush(String fullyQualifiedTableName) {
    return this.needFlush.getOrDefault(fullyQualifiedTableName, false);
  }

  /**
   * Set need flush flag for a table
   *
   * @param fullyQualifiedTableName fully qualified table name
   * @param needFlush need flush flag
   */
  void setNeedFlush(String fullyQualifiedTableName, Boolean needFlush) {
    this.needFlush.put(fullyQualifiedTableName, needFlush);
  }

  /**
   * Returns an iterator over the (table, channels) in this map.
   *
   * @return
   */
  Iterator<Map.Entry<String, ConcurrentHashMap<String, SnowflakeStreamingIngestChannelInternal<T>>>>
      iterator() {
    return this.cache.entrySet().iterator();
  }

  /**
   * Returns an iterator over the (table, channels) in this map, filtered by the given table name
   * set
   *
   * @param tableNames the set of table names to filter
   * @return
   */
  Iterator<Map.Entry<String, ConcurrentHashMap<String, SnowflakeStreamingIngestChannelInternal<T>>>>
      iterator(Set<String> tableNames) {
    return this.cache.entrySet().stream()
        .filter(entry -> tableNames.contains(entry.getKey()))
        .iterator();
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

  public Set<
          Map.Entry<String, ConcurrentHashMap<String, SnowflakeStreamingIngestChannelInternal<T>>>>
      entrySet() {
    return cache.entrySet();
  }
}
