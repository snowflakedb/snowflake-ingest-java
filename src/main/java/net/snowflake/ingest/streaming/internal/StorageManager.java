/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import com.google.common.annotations.VisibleForTesting;
import java.util.Calendar;
import java.util.List;

/** Interface to manage {@link StreamingIngestStorage} for {@link FlushService} */
interface StorageManager<T> {
  // Default max upload retries for streaming ingest storage
  int DEFAULT_MAX_UPLOAD_RETRIES = 5;

  /**
   * Given a blob, return the target storage
   *
   * @param blobData the blob to upload
   * @return target stage
   */
  StreamingIngestStorage getStorage(List<List<ChannelData<T>>> blobData);

  /**
   * Add a storage to the manager
   *
   * @param openChannelResponse response from open channel
   */
  void addStorage(OpenChannelResponse openChannelResponse);

  /**
   * Generate a unique blob path for the blob
   *
   * @return the blob path
   */
  String generateBlobPath();

  /**
   * Get the blob path given time and client prefix, used for testing only
   *
   * @param calendar the time
   * @param clientPrefix the client prefix
   * @return the blob path
   */
  @VisibleForTesting
  String getBlobPath(Calendar calendar, String clientPrefix);

  /**
   * Get the unique client prefix generated by the Snowflake server
   *
   * @return the client prefix
   */
  String getClientPrefix();
}
