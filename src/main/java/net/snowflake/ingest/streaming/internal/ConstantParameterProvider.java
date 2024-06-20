/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

/** A class to provide non-configurable constants depends on Iceberg or non-Iceberg mode */
class ConstantParameterProvider {

  static class IcebergModeConstants {
    static final boolean ENABLE_CHUNK_ENCRYPTION = false;
  }

  static class NonIcebergModeConstants {
    static final boolean ENABLE_CHUNK_ENCRYPTION = true;
  }

  private final boolean enableChunkEncryption;

  ConstantParameterProvider(boolean isIcebergMode) {
    this.enableChunkEncryption =
        isIcebergMode
            ? IcebergModeConstants.ENABLE_CHUNK_ENCRYPTION
            : NonIcebergModeConstants.ENABLE_CHUNK_ENCRYPTION;
  }

  boolean getEnableChunkEncryption() {
    return this.enableChunkEncryption;
  }
}
