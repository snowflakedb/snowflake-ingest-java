/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

/** Contains all the constants needed for Streaming Ingest */
public class Constants {

  // Client level constants
  public static final String USER_NAME = "snowflake.user.name";
  public static final String ACCOUNT_URL = "snowflake.url.name";
  public static final String PRIVATE_KEY = "snowflake.private.key";
  public static final String PRIVATE_KEY_PASSPHRASE = "snowflake.private.key.passphrase";
  public static final long MAX_CHUNK_SIZE_IN_BYTES = 16000000L;
  public static final long RESPONSE_SUCCESS = 0L;
  public static final long BLOB_UPLOAD_TIMEOUT_IN_SEC = 10L;

  // Channel level constants
  public static final String OPEN_CHANNEL_ENDPOINT = "/v1/streaming/channels/open/";
}
