/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * In-memory cache that stores the active channels for a given Streaming Ingest client, and the
 * channels belong to the same table will be stored together in order to combine the channel data
 * during flush. The key is a fully qualified table name and the value is a set of channels that
 * belongs to this table
 */
class ChannelCache {
  // Cache to hold all the valid channels, the key for the outer map is FullyQualifiedTableName and
  // the key for the inner map is ChannelName
  private final ConcurrentHashMap<
          String, ConcurrentHashMap<String, SnowflakeStreamingIngestChannelInternal>>
      cache = new ConcurrentHashMap<>();

  /**
   * Add a channel to the channel cache
   *
   * @param channel
   */
  void addChannel(SnowflakeStreamingIngestChannelInternal channel) {
    ConcurrentHashMap<String, SnowflakeStreamingIngestChannelInternal> channels =
        this.cache.computeIfAbsent(
            channel.getFullyQualifiedTableName(), v -> new ConcurrentHashMap<>());

    SnowflakeStreamingIngestChannelInternal oldChannel = channels.put(channel.getName(), channel);
    // Invalidate old channel if it exits to block new inserts and return error to users earlier
    if (oldChannel != null) {
      oldChannel.invalidate();
    }
  }

  /**
   * Returns an iterator over the (table, channels) in this map.
   *
   * @return
   */
  Iterator<Map.Entry<String, ConcurrentHashMap<String, SnowflakeStreamingIngestChannelInternal>>>
      iterator() {
    return this.cache.entrySet().iterator();
  }

  /** Synchronously closes all channels in the channel cache */
  void closeAllChannels() throws Exception {
    final List<CompletableFuture<Void>> terminationFutures =
            cache.values().stream()
                    .flatMap(channels ->
                            channels.values().stream()
                                    .map(SnowflakeStreamingIngestChannelInternal::close))
                    .collect(Collectors.toList());

    // this will rethrow any exception found.
    CompletableFuture.allOf(terminationFutures.toArray(new CompletableFuture[0]))
            .thenApply(f ->
                    terminationFutures.stream()
                            .map(CompletableFuture::join)
                            .collect(Collectors.toList()))
            .get(5, TimeUnit.MINUTES);
  }

  /** Remove a channel in the channel cache if the channel sequencer matches */
  // TODO: background cleaner to cleanup old stale channels that are not closed?
  void removeChannelIfSequencersMatch(SnowflakeStreamingIngestChannelInternal channel) {
    cache.computeIfPresent(
        channel.getFullyQualifiedTableName(),
        (k, v) -> {
          SnowflakeStreamingIngestChannelInternal channelInCache = v.get(channel.getName());
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
      Long channelSequencer) {
    String fullyQualifiedTableName = String.format("%s.%s.%s", dbName, schemaName, tableName);
    ConcurrentHashMap<String, SnowflakeStreamingIngestChannelInternal> channelsMapPerTable =
        cache.get(fullyQualifiedTableName);
    if (channelsMapPerTable != null) {
      SnowflakeStreamingIngestChannelInternal channel = channelsMapPerTable.get(channelName);
      if (channel != null && channel.getChannelSequencer().equals(channelSequencer)) {
        channel.invalidate();
      }
    }
  }

  /** Get the number of key-value pairs in the cache */
  int getSize() {
    return cache.size();
  }
}
