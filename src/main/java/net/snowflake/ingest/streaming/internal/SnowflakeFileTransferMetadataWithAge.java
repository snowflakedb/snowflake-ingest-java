/*
 * Copyright (c) 2021-2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import java.util.Optional;
import net.snowflake.client.jdbc.SnowflakeFileTransferMetadataV1;

/**
 * Wrapper class containing SnowflakeFileTransferMetadata and the timestamp at which the metadata
 * was refreshed
 */
class SnowflakeFileTransferMetadataWithAge {
  final SnowflakeFileTransferMetadataV1 fileTransferMetadata;
  final boolean isLocalFS;
  final String localLocation;

  /* Do not always know the age of the metadata, so we use the empty
  state to record unknown age.
   */
  Optional<Long> timestamp;

  SnowflakeFileTransferMetadataWithAge(
      SnowflakeFileTransferMetadataV1 fileTransferMetadata, Optional<Long> timestamp) {
    this.isLocalFS = false;
    this.fileTransferMetadata = fileTransferMetadata;
    this.timestamp = timestamp;
    this.localLocation = null;
  }

  SnowflakeFileTransferMetadataWithAge(String localLocation, Optional<Long> timestamp) {
    this.isLocalFS = true;
    this.fileTransferMetadata = null;
    this.localLocation = localLocation;
    this.timestamp = timestamp;
  }
}
