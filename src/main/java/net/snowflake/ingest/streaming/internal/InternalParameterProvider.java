/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

/** A class to provide non-configurable constants depends on Iceberg or non-Iceberg mode */
class InternalParameterProvider {
  private final boolean isIcebergMode;

  InternalParameterProvider(boolean isIcebergMode) {
    this.isIcebergMode = isIcebergMode;
  }

  boolean getEnableChunkEncryption() {
    // When in Iceberg mode, chunk encryption is disabled. Otherwise, it is enabled. Since Iceberg
    // mode does not need client-side encryption.
    return !isIcebergMode;
  }
}
