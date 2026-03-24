/*
 * Replicated from snowflake-jdbc (v3.25.1)
 * Source: https://github.com/snowflakedb/snowflake-jdbc/blob/v3.25.1/src/main/java/net/snowflake/client/jdbc/SFBaseFileTransferAgent.java
 *
 * Originally SFBaseFileTransferAgent.CommandType inner enum. Extracted to top-level
 * for use by replicated getFileTransferMetadatas.
 */
package net.snowflake.ingest.streaming.internal.fileTransferAgent;

public enum CommandType {
  UPLOAD,
  DOWNLOAD
}
