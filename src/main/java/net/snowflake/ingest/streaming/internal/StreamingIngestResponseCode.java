/*
 * Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import com.google.common.annotations.VisibleForTesting;

public enum StreamingIngestResponseCode {
  SUCCESS(0, "Success"),
  ERR_TABLE_DOES_NOT_EXIST_NOT_AUTHORIZED(
      4, "The supplied table does not exist or is not authorized"),
  ERR_CHANNEL_LIMIT_EXCEEDED_FOR_TABLE(
      5,
      "Channel limit exceeded for the given table, please contact Snowflake support to raise this"
          + " limit."),
  ERR_TABLE_OR_COLUMN_TYPE_NOT_SUPPORTED(
      6,
      "Snowpipe Streaming is not supported on the type of table resolved, or the table contains"
          + " columns that are either AUTOINCREMENT or IDENTITY columns or a column with a default"
          + " value"),
  ERR_ENQUEUE_TABLE_CHUNK_QUEUE_FULL(
      7,
      "The table has exceeded its limit of outstanding registration requests, please wait before"
          + " calling the insertRow or insertRows API"),
  ERR_DATABASE_DOES_NOT_EXIST_OR_NOT_AUTHORIZED(
      8, "The supplied database does not exist or is not authorized"),
  ERR_SCHEMA_DOES_NOT_EXIST_OR_NOT_AUTHORIZED(
      9, "The supplied schema does not exist or is not authorized"),
  ERR_GENERAL_EXCEPTION_RETRY_REQUEST(
      10, "Snowflake experienced a transient exception, please retry the request"),
  ERR_MALFORMED_REQUEST_MISSING_BLOBS_IN_REQUEST(
      11, StreamingIngestResponseCode.MALFORMED_REQUEST_STATUS_MESSAGE),
  ERR_MALFORMED_REQUEST_MISSING_CHUNKS_FOR_BLOB_IN_REQUEST(
      12, StreamingIngestResponseCode.MALFORMED_REQUEST_STATUS_MESSAGE),
  ERR_MALFORMED_REQUEST_MISSING_CHANNELS_FOR_CHUNK_FOR_BLOB_IN_REQUEST(
      13, StreamingIngestResponseCode.MALFORMED_REQUEST_STATUS_MESSAGE),
  ERR_MALFORMED_REQUEST_MISSING_BLOB_PATH(
      14, StreamingIngestResponseCode.MALFORMED_REQUEST_STATUS_MESSAGE),
  ERR_MALFORMED_REQUEST_MISSING_DATABASE_IN_REQUEST(
      15, StreamingIngestResponseCode.MALFORMED_REQUEST_STATUS_MESSAGE),
  ERR_MALFORMED_REQUEST_MISSING_SCHEMA_IN_REQUEST(
      16, StreamingIngestResponseCode.MALFORMED_REQUEST_STATUS_MESSAGE),
  ERR_MALFORMED_REQUEST_MISSING_TABLE_IN_REQUEST(
      17, StreamingIngestResponseCode.MALFORMED_REQUEST_STATUS_MESSAGE),
  ERR_MALFORMED_REQUEST_MISSING_CHANNEL_NAME_IN_REQUEST(
      18, StreamingIngestResponseCode.MALFORMED_REQUEST_STATUS_MESSAGE),
  ERR_CHANNEL_NO_LONGER_EXISTS(
      19,
      "The requested channel no longer exists, most likely due to inactivity. Please re-open the"
          + " channel"),
  ERR_CHANNEL_HAS_INVALID_CLIENT_SEQUENCER(
      20,
      "The channel is owned by another writer at this time. Either re-open the channel to gain"
          + " exclusive write ownership or close the channel and let the other writer continue"),
  ERR_CHANNEL_HAS_INVALID_ROW_SEQUENCER(
      21, "The client provided an out-of-order message, please reopen the channel"),
  ERR_OTHER_CHANNEL_IN_CHUNK_HAS_ISSUE(
      22,
      "Another channel managed by this Client had an issue which is preventing the current channel"
          + " from ingesting data. Please re-open the channel"),
  ERR_MALFORMED_REQUEST_DUPLICATE_CHANNEL_IN_CHUNK(
      23, StreamingIngestResponseCode.MALFORMED_REQUEST_STATUS_MESSAGE),
  ERR_MALFORMED_REQUEST_MISSING_CLIENT_SEQUENCER_IN_REQUEST(
      24, StreamingIngestResponseCode.MALFORMED_REQUEST_STATUS_MESSAGE),
  ERR_CHANNEL_DOES_NOT_EXIST_OR_IS_NOT_AUTHORIZED(
      25, "The channel does not exist or is not authorized"),
  ROW_SEQUENCER_IS_COMMITTED(26, "The requested row sequencer is committed"),
  ROW_SEQUENCER_IS_NOT_COMMITTED(27, "The requested row sequencer is not committed"),

  ERR_MALFORMED_REQUEST_MISSING_CHUNK_MD5_IN_REQUEST(
      28, StreamingIngestResponseCode.MALFORMED_REQUEST_STATUS_MESSAGE),
  ERR_MALFORMED_REQUEST_MISSING_EP_INFO_IN_REQUEST(
      29, StreamingIngestResponseCode.MALFORMED_REQUEST_STATUS_MESSAGE),
  ERR_MALFORMED_REQUEST_INVALID_CHUNK_LENGTH(
      30, StreamingIngestResponseCode.MALFORMED_REQUEST_STATUS_MESSAGE),
  ERR_MALFORMED_REQUEST_INVALID_ROW_COUNT_IN_EP_INFO(
      31, StreamingIngestResponseCode.MALFORMED_REQUEST_STATUS_MESSAGE),
  ERR_MALFORMED_REQUEST_INVALID_COLUMN_IN_EP_INFO(
      32, StreamingIngestResponseCode.MALFORMED_REQUEST_STATUS_MESSAGE),
  ERR_MALFORMED_REQUEST_INVALID_EP_INFO_GENERIC(
      33, StreamingIngestResponseCode.MALFORMED_REQUEST_STATUS_MESSAGE),
  ERR_MALFORMED_REQUEST_INVALID_BLOB_NAME(
      34, StreamingIngestResponseCode.MALFORMED_REQUEST_STATUS_MESSAGE),

  ERR_CHANNEL_MUST_BE_REOPENED(36, "The channel must be reopened"),
  ERR_MALFORMED_REQUEST_MISSING_ROLE_IN_REQUEST(
      37, StreamingIngestResponseCode.MALFORMED_REQUEST_STATUS_MESSAGE),
  ERR_MALFORMED_REQUEST_BLOB_HAS_WRONG_FORMAT_OR_EXTENSION(
      38, StreamingIngestResponseCode.MALFORMED_REQUEST_STATUS_MESSAGE),
  ERR_MALFORMED_REQUEST_BLOB_MISSING_MD5(
      39, StreamingIngestResponseCode.MALFORMED_REQUEST_STATUS_MESSAGE),
  ERR_MISSING_COLUMN_IN_EP_INFO(
      40,
      "A schema change occurred on the table, please re-open the channel and supply the missing"
          + " data"),
  ERR_DUPLICATE_BLOB_IN_REQUEST(41, "Duplicated blob in request"),
  ERR_UNSUPPORTED_BLOB_FILE_VERSION(42, "Unsupported blob file version"),
  ERR_OUTDATED_CLIENT_SDK_VERSION(
      43, "Outdated Client SDK. Please update your SDK to the latest version"),
  ERR_MALFORMED_REQUEST_UNDEFINED_USER_AGENT_HEADER(
      44, StreamingIngestResponseCode.MALFORMED_REQUEST_STATUS_MESSAGE),
  ERR_MALFORMED_REQUEST_MALFORMED_USER_AGENT_HEADER(
      45, StreamingIngestResponseCode.MALFORMED_REQUEST_STATUS_MESSAGE),
  ERR_INTERLEAVING_NOT_SUPPORTED_AT_THIS_TIME(
      46, "Interleaving among tables is not supported at this time"),
  ERR_RESOLVED_TABLE_IS_READ_ONLY(47, "Table is read-only"),
  ERR_MALFORMED_REQUEST_INVALID_VAL_IN_EP_INFO(
      48, "The request contains invalid column metadata, please contact Snowflake support"),
  ERR_INGESTION_ON_TABLE_NOT_ALLOWED(
      49,
      "Ingestion into this table is not allowed at this time, please contact Snowflake support"),
  ERR_DEPLOYMENT_ID_MISMATCH(58, "Deployment ID mismatch detected, client must be reconfigured"),
  ERR_INVALID_ENCRYPTION_KEY(
      59, "The encryption key is invalid or has changed, client must be reconfigured");

  public static final String UNKNOWN_STATUS_MESSAGE =
      "Unknown status message. Please contact Snowflake support for further assistance";

  public static final String MALFORMED_REQUEST_STATUS_MESSAGE =
      "The SDK generated a malformed request. Please contact Snowflake support for further "
          + " assistance";

  private final long statusCode;

  private final String message;

  StreamingIngestResponseCode(int statusCode, String message) {
    this.statusCode = statusCode;
    this.message = message;
  }

  @VisibleForTesting
  public long getStatusCode() {
    return statusCode;
  }

  @VisibleForTesting
  public String getMessage() {
    return message;
  }

  public static String getMessageByCode(Long statusCode) {
    if (statusCode != null) {
      for (StreamingIngestResponseCode code : StreamingIngestResponseCode.values()) {
        if (code.statusCode == statusCode) {
          return code.message;
        }
      }
    }
    return UNKNOWN_STATUS_MESSAGE;
  }
}
