/*
 * Copyright (c) 2021-2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.utils;

import java.util.Arrays;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

/** Contains all the constants needed for Streaming Ingest */
public class Constants {

  // Client level constants
  public static final String USER = "user";
  public static final String ACCOUNT = "account";
  public static final String PORT = "port";
  public static final String SSL = "ssl";
  public static final String DATABASE = "database";
  public static final String SCHEMA = "schema";
  public static final String CONNECT_STRING = "connect_string";
  public static final String HOST = "host";
  public static final String WAREHOUSE = "warehouse";
  public static final String SCHEME = "scheme";
  public static final String ACCOUNT_URL = "url";
  public static final String ROLE = "role";
  public static final String AUTHORIZATION_TYPE = "authorization_type";
  public static final String JWT = "JWT";
  public static final String OAUTH = "OAuth";
  public static final String PRIVATE_KEY = "private_key";
  public static final String PRIVATE_KEY_PASSPHRASE = "private_key_passphrase";
  public static final String OAUTH_CLIENT_ID = "oauth_client_id";
  public static final String OAUTH_CLIENT_SECRET = "oauth_client_secret";
  public static final String OAUTH_REFRESH_TOKEN = "oauth_refresh_token";
  public static final String OAUTH_TOKEN_ENDPOINT = "oauth_token_endpoint";
  public static final String SNOWFLAKE_OAUTH_TOKEN_ENDPOINT = "/oauth/token-request";
  public static final String PRIMARY_FILE_ID_KEY =
      "primaryFileId"; // Don't change, should match Parquet Scanner
  public static final String ASSIGNED_FULL_FILE_NAME_KEY = "assignedFullFileName";
  public static final String SDK_VERSION_KEY = "sdkVersion";
  public static final long RESPONSE_SUCCESS = 0L; // Don't change, should match server side
  public static final long RESPONSE_ERR_GENERAL_EXCEPTION_RETRY_REQUEST =
      10L; // Don't change, should match server side
  public static final long RESPONSE_ERR_ENQUEUE_TABLE_CHUNK_QUEUE_FULL =
      7L; // Don't change, should match server side
  public static final int BLOB_UPLOAD_TIMEOUT_IN_SEC = 5;
  public static final int INSERT_THROTTLE_MAX_RETRY_COUNT = 60;
  public static final long MAX_BLOB_SIZE_IN_BYTES = 1024 * 1024 * 1024;
  public static final int BLOB_TAG_SIZE_IN_BYTES = 4;
  public static final int BLOB_VERSION_SIZE_IN_BYTES = 1;
  public static final int BLOB_FILE_SIZE_SIZE_IN_BYTES = 8;
  public static final int BLOB_CHECKSUM_SIZE_IN_BYTES = 8;
  public static final int BLOB_CHUNK_METADATA_LENGTH_SIZE_IN_BYTES = 4;
  public static final long THREAD_SHUTDOWN_TIMEOUT_IN_SEC = 300L;
  public static final String BLOB_EXTENSION_TYPE = "bdec";
  public static final int MAX_THREAD_COUNT = Integer.MAX_VALUE;
  public static final String CLIENT_CONFIGURE_ENDPOINT = "/v1/streaming/client/configure/";
  public static final String GENERATE_PRESIGNED_URLS_ENDPOINT =
      "/v1/streaming/presignedurls/generate/";
  public static final String REFRESH_TABLE_INFORMATION_ENDPOINT =
      "/v1/streaming/tableinformation/refresh/";
  public static final int COMMIT_MAX_RETRY_COUNT = 60;
  public static final int COMMIT_RETRY_INTERVAL_IN_MS = 1000;
  public static final String ENCRYPTION_ALGORITHM = "AES/CTR/NoPadding";
  public static final int ENCRYPTION_ALGORITHM_BLOCK_SIZE_BYTES = 16;
  public static final int MAX_STREAMING_INGEST_API_CHANNEL_RETRY = 3;
  public static final int STREAMING_INGEST_TELEMETRY_UPLOAD_INTERVAL_IN_SEC = 10;
  public static final long EP_NDV_UNKNOWN = -1L;
  public static final long EP_NV_UNKNOWN = -1L;
  public static final int MAX_OAUTH_REFRESH_TOKEN_RETRY = 3;
  public static final int BINARY_COLUMN_MAX_SIZE = 8 * 1024 * 1024;
  public static final int VARCHAR_COLUMN_MAX_SIZE = 16 * 1024 * 1024;

  // Channel level constants
  public static final String CHANNEL_STATUS_ENDPOINT = "/v1/streaming/channels/status/";
  public static final String OPEN_CHANNEL_ENDPOINT = "/v1/streaming/channels/open/";
  public static final String DROP_CHANNEL_ENDPOINT = "/v1/streaming/channels/drop/";
  public static final String REGISTER_BLOB_ENDPOINT = "/v1/streaming/channels/write/blobs/";

  public static final int PARQUET_MAJOR_VERSION = 1;
  public static final int PARQUET_MINOR_VERSION = 0;

  /**
   * Iceberg table serialization policy. Use v2 parquet writer for optimized serialization,
   * otherwise v1.
   */
  public enum IcebergSerializationPolicy {
    COMPATIBLE,
    OPTIMIZED;

    public ParquetProperties.WriterVersion toParquetWriterVersion() {
      switch (this) {
        case COMPATIBLE:
          return ParquetProperties.WriterVersion.PARQUET_1_0;
        case OPTIMIZED:
          return ParquetProperties.WriterVersion.PARQUET_2_0;
        default:
          throw new IllegalArgumentException(
              String.format(
                  "Unsupported ICEBERG_SERIALIZATION_POLICY = '%s', allowed values are %s",
                  this.name(), Arrays.asList(IcebergSerializationPolicy.values())));
      }
    }
  }

  public enum WriteMode {
    CLOUD_STORAGE,
    REST_API,
  }

  /** The write mode to generate BDEC file. */
  public enum BdecVersion {
    // Unused (previously Arrow)
    // ONE(1),

    // Unused (previously Arrow with per column compression.
    // TWO(2),

    /**
     * Uses Parquet to generate BDEC chunks with {@link
     * net.snowflake.ingest.streaming.internal.ParquetRowBuffer} (page-level compression). This
     * version is experimental and WIP at the moment.
     */
    THREE(3);

    private final byte version;

    BdecVersion(int version) {
      if (version > Byte.MAX_VALUE || version < Byte.MIN_VALUE) {
        throw new IllegalArgumentException("Version does not fit into the byte data type");
      }
      this.version = (byte) version;
    }

    public byte toByte() {
      return version;
    }

    public static BdecVersion fromInt(int val) {
      if (val > Byte.MAX_VALUE || val < Byte.MIN_VALUE) {
        throw new IllegalArgumentException("Version does not fit into the byte data type");
      }
      byte version = (byte) val;
      for (BdecVersion eversion : BdecVersion.values()) {
        if (eversion.version == version) {
          return eversion;
        }
      }
      throw new IllegalArgumentException(
          String.format(
              "Unsupported BLOB_FORMAT_VERSION = '%d', allowed values are %s",
              version, Arrays.asList(BdecVersion.values())));
    }
  }

  /**
   * Compression algorithm supported by BDEC Parquet Writer. It is a wrapper around Parquet's lib
   * CompressionCodecName, but we want to control and allow only specific values of that.
   */
  public enum BdecParquetCompression {
    GZIP,
    ZSTD;

    public CompressionCodecName getCompressionCodec() {
      return CompressionCodecName.fromConf(this.name());
    }

    public static BdecParquetCompression fromName(String name) {
      for (BdecParquetCompression e : BdecParquetCompression.values()) {
        if (e.name().toLowerCase().equals(name.toLowerCase())) {
          return e;
        }
      }
      throw new IllegalArgumentException(
          String.format(
              "Unsupported BDEC_PARQUET_COMPRESSION_ALGORITHM = '%s', allowed values are %s",
              name, Arrays.asList(BdecParquetCompression.values())));
    }
  }
  // Parameters
  public static final boolean DISABLE_BACKGROUND_FLUSH = false;
  public static final boolean COMPRESS_BLOB_TWICE = false;
  public static final boolean BLOB_NO_HEADER = true;
  public static final boolean ENABLE_TELEMETRY_TO_SF = true;

  // Metrics
  public static final String SNOWPIPE_STREAMING_JMX_METRIC_PREFIX = "snowflake.ingest.sdk";
  public static final String SNOWPIPE_STREAMING_SHARED_METRICS_REGISTRY =
      "SnowpipeStreamingMetrics";
  public static final String SNOWPIPE_STREAMING_JVM_MEMORY_AND_THREAD_METRICS_REGISTRY =
      "SnowpipeStreamingJvmMemoryAndThreadMetrics";
}
