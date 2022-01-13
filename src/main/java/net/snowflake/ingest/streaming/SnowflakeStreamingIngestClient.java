/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming;

import java.util.concurrent.CompletableFuture;

/**
 * A class that is the starting point for using the Streaming Ingest client APIs, a single client
 * maps to exactly one account in Snowflake; however, multiple clients can point to the same
 * account. Each client will contain information for Snowflake authentication and authorization, and
 * it will be used to create one or more {@link SnowflakeStreamingIngestChannel}
 */
public interface SnowflakeStreamingIngestClient {

  /**
   * Open a channel against a Snowflake table using a {@link OpenChannelRequest}
   *
   * @param request the open channel request
   * @return a {@link SnowflakeStreamingIngestChannel} object
   */
  SnowflakeStreamingIngestChannel openChannel(OpenChannelRequest request);

  /**
   * Close all the channels in the client, which will make sure all the data in all channels is
   * committed before closing
   *
   * @return a completable future which will be completed when all the channels are closed
   */
  CompletableFuture<Void> close();

  /**
   * Get the client name
   *
   * @return the client name
   */
  String getName();

  /** @return a boolean to indicate whether the client is closed */
  boolean isClosed();
}
