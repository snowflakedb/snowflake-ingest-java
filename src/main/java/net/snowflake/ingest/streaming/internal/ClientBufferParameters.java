package net.snowflake.ingest.streaming.internal;

import net.snowflake.ingest.utils.ParameterProvider;

/** Channel's buffer relevant parameters that are set at the owning client level. */
public class ClientBufferParameters {
  private SnowflakeStreamingIngestClientInternal clientInternal;

  private boolean enableParquetInternalBuffering;

  private long maxChunkSizeInBytes;

  private long maxAllowedRowSizeInBytes;

  public ClientBufferParameters(
      boolean enableParquetInternalBuffering,
      long maxChunkSizeInBytes,
      long maxAllowedRowSizeInBytes) {
    this.enableParquetInternalBuffering = enableParquetInternalBuffering;
    this.maxChunkSizeInBytes = maxChunkSizeInBytes;
    this.maxAllowedRowSizeInBytes = maxAllowedRowSizeInBytes;
  }

  public ClientBufferParameters(SnowflakeStreamingIngestClientInternal clientInternal) {
    this.clientInternal = clientInternal;
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
