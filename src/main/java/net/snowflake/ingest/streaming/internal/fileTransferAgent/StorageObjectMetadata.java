/*
 * Replicated from snowflake-jdbc:
 *   net.snowflake.client.jdbc.cloud.storage.StorageObjectMetadata
 * Tag: v3.25.1
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
