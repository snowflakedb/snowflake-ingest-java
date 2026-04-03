/*
 * Replicated from snowflake-jdbc (v3.25.1)
 * Source: https://github.com/snowflakedb/snowflake-jdbc/blob/v3.25.1/src/main/java/net/snowflake/client/jdbc/cloud/storage/GCSAccessStrategy.java
 *
 * Permitted differences: package, SFSession removed (always null from callers),
 * SFPair uses ingest version, all storage types use ingest versions (same package).
 */
package net.snowflake.ingest.streaming.internal.fileTransferAgent;

import java.io.File;
import java.io.InputStream;
import java.util.Map;
import net.snowflake.ingest.utils.SFPair;

interface GCSAccessStrategy {
  StorageObjectSummaryCollection listObjects(String remoteStorageLocation, String prefix);

  StorageObjectMetadata getObjectMetadata(String remoteStorageLocation, String prefix);

  Map<String, String> download(
      int parallelism, String remoteStorageLocation, String stageFilePath, File localFile)
      throws InterruptedException;

  SFPair<InputStream, Map<String, String>> downloadToStream(
      String remoteStorageLocation, String stageFilePath, boolean isEncrypting);

  void uploadWithDownScopedToken(
      int parallelism,
      String remoteStorageLocation,
      String destFileName,
      String contentEncoding,
      Map<String, String> metadata,
      long contentLength,
      InputStream content,
      String queryId)
      throws InterruptedException;

  boolean handleStorageException(
      Exception ex,
      int retryCount,
      String operation,
      String command,
      String queryId,
      SnowflakeGCSClient gcsClient)
      throws SnowflakeSQLException;

  void shutdown();
}
