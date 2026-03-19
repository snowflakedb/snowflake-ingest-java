/*
 * Replicated from snowflake-jdbc:
 *   net.snowflake.client.jdbc.cloud.storage.StorageObjectMetadata
 * Tag: v3.25.1
 * Source: https://github.com/snowflakedb/snowflake-jdbc/blob/v3.25.1/src/main/java/net/snowflake/client/jdbc/cloud/storage/StorageObjectMetadata.java
 */

package net.snowflake.ingest.streaming.internal.fileTransferAgent;

import java.util.Map;

public interface StorageObjectMetadata {
  Map<String, String> getUserMetadata();

  long getContentLength();

  void setContentLength(long contentLength);

  void addUserMetadata(String key, String value);

  void setContentEncoding(String encoding);

  String getContentEncoding();
}
