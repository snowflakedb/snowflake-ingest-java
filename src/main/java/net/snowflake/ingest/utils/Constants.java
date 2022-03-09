/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.utils;

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
  public static final String PRIVATE_KEY = "private_key";
  public static final String PRIVATE_KEY_PASSPHRASE = "private_key_passphrase";
  public static final String JDBC_PRIVATE_KEY = "privateKey";
  public static final long RESPONSE_SUCCESS = 0L;
  public static final long BLOB_UPLOAD_TIMEOUT_IN_SEC = 5L;
  public static final int BLOB_UPLOAD_MAX_RETRY_COUNT = 12;
  public static final int INSERT_THROTTLE_MAX_RETRY_COUNT = 10;
  public static final long MAX_BLOB_SIZE_IN_BYTES = 512000000L;
  public static final long MAX_CHUNK_SIZE_IN_BYTES = 32000000L;
  public static final byte BLOB_FORMAT_VERSION = 0;
  public static final int BLOB_TAG_SIZE_IN_BYTES = 4;
  public static final int BLOB_VERSION_SIZE_IN_BYTES = 1;
  public static final int BLOB_FILE_SIZE_SIZE_IN_BYTES = 8;
  public static final int BLOB_CHECKSUM_SIZE_IN_BYTES = 8;
  public static final int BLOB_CHUNK_METADATA_LENGTH_SIZE_IN_BYTES = 4;
  public static final long THREAD_SHUTDOWN_TIMEOUT_IN_SEC = 300L;
  public static final String BLOB_EXTENSION_TYPE = "bdec";
  public static final int MAX_THREAD_COUNT = Integer.MAX_VALUE;
  public static final int CPU_IO_TIME_RATIO = 1;
  public static final String CLIENT_CONFIGURE_ENDPOINT = "/v1/streaming/client/configure/";
  public static final int COMMIT_MAX_RETRY_COUNT = 10;
  public static final int COMMIT_RETRY_INTERVAL_IN_MS = 500;
  public static final int ROW_SEQUENCER_IS_COMMITTED = 26;
  public static final String ENCRYPTION_ALGORITHM = "AES/CTR/NoPadding";

  // Channel level constants
  public static final String CHANNEL_STATUS_ENDPOINT = "/v1/streaming/channels/status/";
  public static final String OPEN_CHANNEL_ENDPOINT = "/v1/streaming/channels/open/";
  public static final String REGISTER_BLOB_ENDPOINT = "/v1/streaming/channels/write/blobs/";

  public static enum WriteMode {
    CLOUD_STORAGE,
    REST_API,
  }

  // Parameters
  public static final boolean DISABLE_BACKGROUND_FLUSH = false;
  public static final boolean COMPRESS_BLOB_TWICE = false;
  public static final boolean BLOB_NO_HEADER = true;

  // Metrics
  public static final String STREAMING_JMX_METRIC_PREFIX = "snowflake.ingest.sdk";

  public static final String STREAMING_SHARED_METRICS_REGISTRY = "StreamingSnowpipeMetrics";
}
