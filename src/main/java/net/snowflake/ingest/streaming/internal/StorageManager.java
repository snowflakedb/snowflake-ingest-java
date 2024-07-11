/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import java.util.Optional;

/**
 * Interface to manage {@link StreamingIngestStorage} for {@link FlushService}
 *
 * @param <T> The type of chunk data
 * @param <TLocation> the type of location that's being managed (internal stage / external volume)
 */
interface StorageManager<T, TLocation> {
  // Default max upload retries for streaming ingest storage
  int DEFAULT_MAX_UPLOAD_RETRIES = 5;

  /**
   * Given a fully qualified table name, return the target storage
   *
   * @param fullyQualifiedTableName the target fully qualified table name
   * @return target stage
   */
  StreamingIngestStorage<T, TLocation> getStorage(String fullyQualifiedTableName);

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
   * Gets the latest file location info (with a renewed short-lived access token) for the specified
   * location
   *
   * @param location A reference to the target location
   * @param fileName optional filename for single-file signed URL fetch from server
   * @return the new location information
   */
  FileLocationInfo getRefreshedLocation(TLocation location, Optional<String> fileName);

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
