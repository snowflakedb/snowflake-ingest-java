/*
 * Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import net.snowflake.ingest.utils.Constants;
import net.snowflake.ingest.utils.ParameterProvider;

/** Channel's buffer relevant parameters that are set at the owning client level. */
public class ClientBufferParameters {

  private boolean enableParquetInternalBuffering;

  private long maxChunkSizeInBytes;

  private long maxAllowedRowSizeInBytes;

  private Constants.BdecParquetCompression bdecParquetCompression;

  /**
   * Private constructor used for test methods
   *
   * @param enableParquetInternalBuffering flag whether buffering in internal Parquet buffers is
   *     enabled
   * @param maxChunkSizeInBytes maximum chunk size in bytes
   * @param maxAllowedRowSizeInBytes maximum row size in bytes
   */
  private ClientBufferParameters(
      boolean enableParquetInternalBuffering,
      long maxChunkSizeInBytes,
      long maxAllowedRowSizeInBytes,
      Constants.BdecParquetCompression bdecParquetCompression) {
    this.enableParquetInternalBuffering = enableParquetInternalBuffering;
    this.maxChunkSizeInBytes = maxChunkSizeInBytes;
    this.maxAllowedRowSizeInBytes = maxAllowedRowSizeInBytes;
    this.bdecParquetCompression = bdecParquetCompression;
  }

  /** @param clientInternal reference to the client object where the relevant parameters are set */
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
    this.bdecParquetCompression =
        clientInternal != null
            ? clientInternal.getParameterProvider().getBdecParquetCompressionAlgorithm()
            : ParameterProvider.BDEC_PARQUET_COMPRESSION_ALGORITHM_DEFAULT;
  }

  /**
   * @param enableParquetInternalBuffering flag whether buffering in internal Parquet buffers is
   *     enabled
   * @param maxChunkSizeInBytes maximum chunk size in bytes
   * @param maxAllowedRowSizeInBytes maximum row size in bytes
   * @return ClientBufferParameters object
   */
  public static ClientBufferParameters test_createClientBufferParameters(
      boolean enableParquetInternalBuffering,
      long maxChunkSizeInBytes,
      long maxAllowedRowSizeInBytes,
      Constants.BdecParquetCompression bdecParquetCompression) {
    return new ClientBufferParameters(
        enableParquetInternalBuffering,
        maxChunkSizeInBytes,
        maxAllowedRowSizeInBytes,
        bdecParquetCompression);
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

  public Constants.BdecParquetCompression getBdecParquetCompression() {
    return bdecParquetCompression;
  }
}
