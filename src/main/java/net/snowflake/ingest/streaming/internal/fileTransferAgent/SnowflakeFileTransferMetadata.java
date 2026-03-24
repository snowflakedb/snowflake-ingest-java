/*
 * Replicated from snowflake-jdbc (v3.25.1)
 * Source: https://github.com/snowflakedb/snowflake-jdbc/blob/v3.25.1/src/main/java/net/snowflake/client/jdbc/SnowflakeFileTransferMetadata.java
 */
package net.snowflake.ingest.streaming.internal.fileTransferAgent;

public interface SnowflakeFileTransferMetadata {
  /**
   * Determine this metadata is for transferring one or multiple files.
   *
   * @return return true if it is for transferring one file.
   */
  boolean isForOneFile();
}
