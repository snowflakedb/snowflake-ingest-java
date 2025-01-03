package net.snowflake.ingest.streaming.internal;

import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;

import java.util.List;
import java.util.concurrent.CompletableFuture;

interface SnowflakeStreamingIngestChannelFlushable<TData> extends SnowflakeStreamingIngestChannel {
  Long getChannelSequencer();

  ChannelRuntimeState getChannelState();

  ChannelData<TData> getData();

  void invalidate(String message, String invalidationCause);

  void markClosed();

  CompletableFuture<Void> flush(boolean closing);

  // TODO: need to verify with the table schema when supporting sub-columns
  void setupSchema(List<ColumnMetadata> columns);
}
