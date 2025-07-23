/*
 * Copyright (c) 2021-2025 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import java.util.Optional;
import net.snowflake.client.jdbc.SnowflakeFileTransferMetadataV1;

/**
 * Wrapper class containing SnowflakeFileTransferMetadata, and the timestamp at which the metadata
 * was refreshed. File path is included to ensure atomic updates with metadata.
 */
class SnowflakeFileTransferMetadataWithAge {
  final SnowflakeFileTransferMetadataV1 fileTransferMetadata;
  final boolean isLocalFS;
  final String localLocation;

  /*
   * This is used to check if the blob path for Iceberg ingestion is up to date. For Iceberg, the value is
   * {nullableTableBasePath}/data/streaming_ingest/{figsId}/snow_{volumeHash}_{figsId}_{workerRank}_1_.
   */
  final String path;

  /* Do not always know the age of the metadata, so we use the empty
  state to record unknown age.
   */
  Optional<Long> timestamp;

  SnowflakeFileTransferMetadataWithAge(
      SnowflakeFileTransferMetadataV1 fileTransferMetadata, Optional<Long> timestamp, String path) {
    this.isLocalFS = false;
    this.fileTransferMetadata = fileTransferMetadata;
    this.timestamp = timestamp;
    this.localLocation = null;
    this.path = path;
  }

  SnowflakeFileTransferMetadataWithAge(
      String localLocation, Optional<Long> timestamp, String path) {
    this.isLocalFS = true;
    this.fileTransferMetadata = null;
    this.localLocation = localLocation;
    this.timestamp = timestamp;
    this.path = path;
  }
}
