/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.utils;

/** Ingest SDK internal error codes */
public enum ErrorCode {
  INTERNAL_ERROR("0001"),
  NULL_VALUE("0002"),
  NULL_OR_EMPTY_STRING("0003"),
  INVALID_ROW("0004"),
  UNKNOWN_DATA_TYPE("0005"),
  REGISTER_BLOB_FAILURE("0006"),
  OPEN_CHANNEL_FAILURE("0007"),
  BUILD_REQUEST_FAILURE("0008"),
  CLIENT_CONFIGURE_FAILURE("0009"),
  MISSING_CONFIG("0010"),
  BLOB_UPLOAD_FAILURE("0011"),
  RESOURCE_CLEANUP_FAILURE("0012"),
  INVALID_CHANNEL("0013"),
  CLOSED_CHANNEL("0014"),
  INVALID_URL("0015"),
  CLOSED_CLIENT("0016"),
  INVALID_PRIVATE_KEY("0017"),
  INVALID_ENCRYPTED_KEY("0018"),
  INVALID_DATA_IN_CHUNK("0019"),
  IO_ERROR("0020"),
  UNABLE_TO_CONNECT_TO_STAGE("0021"),
  KEYPAIR_CREATION_FAILURE("0022"),
  MD5_HASHING_NOT_AVAILABLE("0023"),
  CHANNEL_STATUS_FAILURE("0024"),
  CHANNEL_WITH_UNCOMMITTED_ROWS("0025"),
  INVALID_COLLATION_STRING("0026"),
  ENCRYPTION_FAILURE("0027");

  public static final String errorMessageResource = "net.snowflake.ingest.ingest_error_messages";

  /** Snowflake internal message associated to the error. */
  private final String messageCode;

  /**
   * Construct a new error code specification given Snowflake internal error code.
   *
   * @param messageCode Snowflake internal error code
   */
  ErrorCode(String messageCode) {
    this.messageCode = messageCode;
  }

  public String getMessageCode() {
    return messageCode;
  }

  @Override
  public String toString() {
    return "ErrorCode{" + "name=" + this.name() + ", messageCode=" + messageCode + "}";
  }
}
