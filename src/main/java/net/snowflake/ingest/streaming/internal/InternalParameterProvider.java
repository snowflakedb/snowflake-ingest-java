/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

/** A class to provide non-configurable constants depends on Iceberg or non-Iceberg mode */
class InternalParameterProvider {
  public static final Integer MAX_ROW_GROUP_COUNT_ICEBERG_MODE_DEFAULT = 1;
  public static final boolean ENABLE_DISTINCT_VALUES_COUNT_DEFAULT = false;
  public static final boolean ENABLE_VALUES_COUNT_DEFAULT = false;

  private final boolean isIcebergMode;

  InternalParameterProvider(boolean isIcebergMode) {
    this.isIcebergMode = isIcebergMode;
  }

  boolean getEnableChunkEncryption() {
    // When in Iceberg mode, chunk encryption is disabled. Otherwise, it is enabled. Since Iceberg
    // mode does not need client-side encryption.
    return !isIcebergMode;
  }

  boolean setAllDefaultValuesInEp() {
    // When in non-iceberg mode, we want to default the stats for all data types (int/real/string)
    // to 0 / to "".
    // However when in iceberg mode, we want to default only those stats that are
    // relevant to the current datatype.
    return !isIcebergMode;
  }

  boolean setIcebergSpecificFieldsInEp() {
    // When in Iceberg mode, we need to explicitly populate the major and minor version of parquet
    // in the EP metadata, createdOn, and extendedMetadataSize.
    return isIcebergMode;
  }

  boolean isEnableDistinctValuesCount() {
    // When in Iceberg mode, we enabled distinct values count in EP metadata.
    return isIcebergMode;
  }

  boolean isEnableValuesCount() {
    // When in Iceberg mode, we enabled values count in EP metadata for repeated group (e.g. map,
    // list).
    return isIcebergMode;
  }
}
