/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming;

import java.util.List;
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
   * Drop the specified channel on the server using a {@link DropChannelRequest}
   *
   * <p>Note that this call will blindly drop the latest version of the channel and any pending data
   * will be lost. Also see {@link SnowflakeStreamingIngestChannel#close(boolean)} to drop channels
   * on close. That approach will drop the local version of the channel and if the channel has been
   * concurrently reopened by another client, that version of the channel won't be affected.
   *
   * @param request the drop channel request
   */
  void dropChannel(DropChannelRequest request);

  /**
   * Get the client name
   *
   * @return the client name
   */
  String getName();

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

  /**
   * Return the latest committed offset token for a list of channels
   *
   * @return a map of channel fully qualified name to latest committed offset token
   */
  Map<String, String> getLatestCommittedOffsetTokens(
      List<SnowflakeStreamingIngestChannel> channels);
}
