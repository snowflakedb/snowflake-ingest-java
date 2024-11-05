/*
 * Copyright (c) 2023-2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import java.util.Optional;
import net.snowflake.ingest.utils.Constants;
import net.snowflake.ingest.utils.ParameterProvider;
import org.apache.parquet.column.ParquetProperties;

/** Channel's buffer relevant parameters that are set at the owning client level. */
public class ClientBufferParameters {
  private static final String BDEC_PARQUET_MESSAGE_TYPE_NAME = "bdec";
  private static final String PARQUET_MESSAGE_TYPE_NAME = "schema";

  private long maxChunkSizeInBytes;

  private long maxAllowedRowSizeInBytes;

  private final boolean enableNewJsonParsingLogic;

  private Constants.BdecParquetCompression bdecParquetCompression;

  private final Optional<Integer> maxRowGroups;

  private boolean enableIcebergStreaming;

  private boolean enableDistinctValuesCount;

  private boolean enableValuesCount;

  private boolean enableDictionaryEncoding;

  /**
   * Private constructor used for test methods
   *
   * @param maxChunkSizeInBytes maximum chunk size in bytes
   * @param maxAllowedRowSizeInBytes maximum row size in bytes
   * @param enableIcebergStreaming
   */
  private ClientBufferParameters(
      long maxChunkSizeInBytes,
      long maxAllowedRowSizeInBytes,
      Constants.BdecParquetCompression bdecParquetCompression,
      boolean enableNewJsonParsingLogic,
      Optional<Integer> maxRowGroups,
      boolean enableIcebergStreaming,
      boolean enableDistinctValuesCount,
      boolean enableValuesCount) {
    this.maxChunkSizeInBytes = maxChunkSizeInBytes;
    this.maxAllowedRowSizeInBytes = maxAllowedRowSizeInBytes;
    this.bdecParquetCompression = bdecParquetCompression;
    this.enableNewJsonParsingLogic = enableNewJsonParsingLogic;
    this.maxRowGroups = maxRowGroups;
    this.enableIcebergStreaming = enableIcebergStreaming;
    this.enableDistinctValuesCount = enableDistinctValuesCount;
    this.enableValuesCount = enableValuesCount;
    this.enableDictionaryEncoding = enableIcebergStreaming;
  }

  /** @param clientInternal reference to the client object where the relevant parameters are set */
  public ClientBufferParameters(
      SnowflakeStreamingIngestClientInternal clientInternal,
      ParquetProperties.WriterVersion parquetWriterVersion) {
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
    this.enableNewJsonParsingLogic =
        clientInternal != null
            ? clientInternal.getParameterProvider().isEnableNewJsonParsingLogic()
            : ParameterProvider.ENABLE_NEW_JSON_PARSING_LOGIC_DEFAULT;
    this.enableIcebergStreaming =
        clientInternal != null
            ? clientInternal.getParameterProvider().isEnableIcebergStreaming()
            : ParameterProvider.ENABLE_ICEBERG_STREAMING_DEFAULT;
    this.maxRowGroups =
        enableIcebergStreaming
            ? Optional.of(InternalParameterProvider.MAX_ROW_GROUP_COUNT_ICEBERG_MODE_DEFAULT)
            : Optional.empty();
    this.enableDistinctValuesCount =
        clientInternal != null
            ? clientInternal.getInternalParameterProvider().isEnableDistinctValuesCount()
            : InternalParameterProvider.ENABLE_DISTINCT_VALUES_COUNT_DEFAULT;
    this.enableValuesCount =
        clientInternal != null
            ? clientInternal.getInternalParameterProvider().isEnableValuesCount()
            : InternalParameterProvider.ENABLE_VALUES_COUNT_DEFAULT;
    this.enableDictionaryEncoding =
        enableIcebergStreaming
            && parquetWriterVersion == ParquetProperties.WriterVersion.PARQUET_2_0;
  }

  /**
   * @param maxChunkSizeInBytes maximum chunk size in bytes
   * @param maxAllowedRowSizeInBytes maximum row size in bytes
   * @param enableIcebergStreaming
   * @return ClientBufferParameters object
   */
  public static ClientBufferParameters test_createClientBufferParameters(
      long maxChunkSizeInBytes,
      long maxAllowedRowSizeInBytes,
      Constants.BdecParquetCompression bdecParquetCompression,
      boolean enableNewJsonParsingLogic,
      Optional<Integer> maxRowGroups,
      boolean enableIcebergStreaming,
      boolean enableDistinctValuesCount,
      boolean enableValuesCount) {
    return new ClientBufferParameters(
        maxChunkSizeInBytes,
        maxAllowedRowSizeInBytes,
        bdecParquetCompression,
        enableNewJsonParsingLogic,
        maxRowGroups,
        enableIcebergStreaming,
        enableDistinctValuesCount,
        enableValuesCount);
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

  public boolean isEnableNewJsonParsingLogic() {
    return enableNewJsonParsingLogic;
  }

  public boolean isEnableIcebergStreaming() {
    return enableIcebergStreaming;
  }

  public Optional<Integer> getMaxRowGroups() {
    return maxRowGroups;
  }

  public String getParquetMessageTypeName() {
    return enableIcebergStreaming ? PARQUET_MESSAGE_TYPE_NAME : BDEC_PARQUET_MESSAGE_TYPE_NAME;
  }

  public boolean isEnableDistinctValuesCount() {
    return enableDistinctValuesCount;
  }

  public boolean isEnableValuesCount() {
    return enableValuesCount;
  }

  public boolean isEnableDictionaryEncoding() {
    return enableDictionaryEncoding;
  }
}
