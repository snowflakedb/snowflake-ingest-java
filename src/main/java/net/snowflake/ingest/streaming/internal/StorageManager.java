/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

/** Interface to manage {@link StreamingIngestStorage} for {@link FlushService} */
interface StorageManager<T> {
  // Default max upload retries for streaming ingest storage
  int DEFAULT_MAX_UPLOAD_RETRIES = 5;

  /**
   * Given a blob, return the target storage
   *
   * @param channelFlushContext the blob to upload
   * @return target stage
   */
  StreamingIngestStorage<T> getStorage(ChannelFlushContext channelFlushContext);

  /**
   * Add a storage to the manager
   *
   * @param dbName the database name
   * @param schemaName the schema name
   * @param tableName the table name
   * @param fileLocationInfo file location info from configure response
   */
  void addStorage(
      String dbName, String schemaName, String tableName, FileLocationInfo fileLocationInfo);

  /**
   * Configure method for storage
   *
   * @param request the configure request
   * @return the configure response
   */
  ConfigureResponse configure(ConfigureRequest request);

  /**
   * Generate a unique blob path and increment the blob sequencer
   *
   * @return the blob path
   */
  String generateBlobPath();

  /**
   * Decrement the blob sequencer, this method is needed to prevent gap between file name sequencer.
   * See {@link StorageManager#generateBlobPath()} for more details.
   */
  void decrementBlobSequencer();

  /**
   * Get the unique client prefix generated by the Snowflake server
   *
   * @return the client prefix
   */
  String getClientPrefix();
}
