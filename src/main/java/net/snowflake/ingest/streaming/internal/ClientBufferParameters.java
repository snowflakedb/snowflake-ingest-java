/*
 * Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import net.snowflake.ingest.utils.ParameterProvider;

/** Channel's buffer relevant parameters that are set at the owning client level. */
public class ClientBufferParameters {

  private boolean enableParquetInternalBuffering;

  private long maxChunkSizeInBytes;

  private long maxAllowedRowSizeInBytes;

  /**
   * This is TEST-ONLY constructor
   *
   * @param enableParquetInternalBuffering flag whether buffering in internal Parquet buffers is
   *     enabled
   * @param maxChunkSizeInBytes maximum chunk size in bytes
   * @param maxAllowedRowSizeInBytes maximum row size in bytes
   */
  public ClientBufferParameters(
      boolean enableParquetInternalBuffering,
      long maxChunkSizeInBytes,
      long maxAllowedRowSizeInBytes) {
    this.enableParquetInternalBuffering = enableParquetInternalBuffering;
    this.maxChunkSizeInBytes = maxChunkSizeInBytes;
    this.maxAllowedRowSizeInBytes = maxAllowedRowSizeInBytes;
  }

  /**
   * @param clientInternal reference to the clientInternal object, where the relevant parameters are
   *     set
   */
  public ClientBufferParameters(SnowflakeStreamingIngestClientInternal clientInternal) {
    this.enableParquetInternalBuffering =
        clientInternal != null
            ? clientInternal.getParameterProvider().getEnableParquetInternalBuffering()
            : ParameterProvider.ENABLE_PARQUET_INTERNAL_BUFFERING_DEFAULT;
    this.maxChunkSizeInBytes =
        clientInternal != null
            ? clientInternal.getParameterProvider().getMaxChunkSizeInBytes()
            : ParameterProvider.MAX_CHUNK_SIZE_IN_BYTES_DEFAULT;
    this.maxAllowedRowSizeInBytes =
        clientInternal != null
            ? clientInternal.getParameterProvider().getMaxAllowedRowSizeInBytes()
            : ParameterProvider.MAX_ALLOWED_ROW_SIZE_IN_BYTES_DEFAULT;
  }

  public boolean getEnableParquetInternalBuffering() {
    return enableParquetInternalBuffering;
  }

  public long getMaxChunkSizeInBytes() {
    return maxChunkSizeInBytes;
  }

  public long getMaxAllowedRowSizeInBytes() {
    return maxAllowedRowSizeInBytes;
  }
}
