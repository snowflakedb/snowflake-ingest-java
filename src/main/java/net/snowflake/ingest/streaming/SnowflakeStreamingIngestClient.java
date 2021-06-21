/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming;

import java.util.concurrent.CompletableFuture;

/**
 * Starting point for using Streaming Ingest client APIs, need one client per account and contains
 * account level information for authentication and connecting to Snowflake.
 */
public interface SnowflakeStreamingIngestClient {

  /**
   * Open a channel against a Snowflake table
   *
   * @param request the open channel request
   * @return a SnowflakeStreamingIngestChannel object
   */
  SnowflakeStreamingIngestChannel openChannel(OpenChannelRequest request);

  /**
   * Close the client, which will flush first and then release all the resources
   *
   * @return future which will be complete when the channel is closed
   */
  CompletableFuture<Void> close();

  /**
   * Flush all data in memory to persistent storage and register with a Snowflake table
   *
   * @return future which will be complete when the flush the data is registered
   */
  CompletableFuture<Void> flush();

  /**
   * Get the client name
   *
   * @return the client name
   */
  String getName();

  /**
   * Get the role used by the client
   *
   * @return the client's role
   */
  String getRole();

  /** @return a boolean to indicate whether the client is closed or not */
  boolean isClosed();
}
