/*
 * Replicated from snowflake-jdbc (v3.25.1)
 * Source: https://github.com/snowflakedb/snowflake-jdbc/blob/v3.25.1/src/main/java/net/snowflake/client/jdbc/cloud/storage/QueryIdHelper.java
 */
package net.snowflake.ingest.streaming.internal.fileTransferAgent;

class QueryIdHelper {
  static String queryIdFromEncMatOr(RemoteStoreFileEncryptionMaterial encMat, String queryId) {
    return encMat != null && encMat.getQueryId() != null ? encMat.getQueryId() : queryId;
  }
}
