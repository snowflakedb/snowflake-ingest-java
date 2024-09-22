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

  boolean setDefaultValuesInEp() {
    // When in Iceberg mode, we need to populate nulls (instead of zeroes) in the minIntValue /
    // maxIntValue / minRealValue / maxRealValue fields of the EP Metadata.
    return !isIcebergMode;
  }

  boolean setMajorMinorVersionInEp() {
    // When in Iceberg mode, we need to explicitly populate the major and minor version of parquet
    // in the EP metadata.
    return isIcebergMode;
  }
}
