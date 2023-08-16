/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming;

import java.util.Map;

/**
 * A class that is the starting point for using the Streaming Ingest client APIs, a single client
 * maps to exactly one account in Snowflake; however, multiple clients can point to the same
 * account. Each client will contain information for Snowflake authentication and authorization, and
 * it will be used to create one or more {@link SnowflakeStreamingIngestChannel}
 *
 * <p>Thread safety note: Implementations of this interface are required to be thread safe.
 */
public interface SnowflakeStreamingIngestClient extends AutoCloseable {

  /**
   * Open a channel against a Snowflake table using a {@link OpenChannelRequest}
   *
   * @param request the open channel request
   * @return a {@link SnowflakeStreamingIngestChannel} object
   */
  SnowflakeStreamingIngestChannel openChannel(OpenChannelRequest request);

  /**
   * Get the client name
   *
   * @return the client name
   */
  String getName();

  /**
   * Return the latest committed offset token for all open channels.
   *
   * @return a map of channel to latest committed offset token
   */
  Map<SnowflakeStreamingIngestChannel, String> getALlLatestCommittedOffsetTokens();

  /**
   * Set refresh token, this method is for refresh token renewal without requiring to restart
   * client. This method only works when the authorization type is OAuth.
   *
   * @param refreshToken the new refresh token
   */
  void setRefreshToken(String refreshToken);

  /**
   * Check whether the client is closed or not, if you want to make sure all data are committed
   * before closing, please call {@link SnowflakeStreamingIngestClient#close()} before closing the
   * entire client
   *
   * @return a boolean to indicate whether the client is closed
   */
  boolean isClosed();
}
