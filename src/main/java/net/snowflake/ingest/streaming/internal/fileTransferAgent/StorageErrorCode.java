/*
 * Replicated from snowflake-jdbc: net.snowflake.client.jdbc.ErrorCode
 * Tag: v3.25.1
 *
 * Only the error codes used by the ingest storage clients are included.
 * Named StorageErrorCode to avoid collision with the existing ingest ErrorCode.
 */

package net.snowflake.ingest.streaming.internal.fileTransferAgent;

public enum StorageErrorCode {
  INTERNAL_ERROR(200001, "XX000"),
  S3_OPERATION_ERROR(200013, "58000"),
  IO_ERROR(200016, "58030"),
  AWS_CLIENT_ERROR(200020, "58000"),
  AZURE_SERVICE_ERROR(200044, "58000"),
  INVALID_PROXY_PROPERTIES(200051, "08000"),
  GCP_SERVICE_ERROR(200061, "58000");

  /** Value of CLOUD_STORAGE_CREDENTIALS_EXPIRED from JDBC Constants class. */
  public static final int CLOUD_STORAGE_CREDENTIALS_EXPIRED = 240001;

  private final Integer messageCode;
  private final String sqlState;

  StorageErrorCode(Integer messageCode, String sqlState) {
    this.messageCode = messageCode;
    this.sqlState = sqlState;
  }

  public Integer getMessageCode() {
    return this.messageCode;
  }

  public String getSqlState() {
    return this.sqlState;
  }

  @Override
  public String toString() {
    return "StorageErrorCode{messageCode=" + this.messageCode + ", sqlState=" + this.sqlState + '}';
  }
}
