/*
 * Replicated from snowflake-jdbc (v3.25.1)
 * Source: https://github.com/snowflakedb/snowflake-jdbc/blob/v3.25.1/src/main/java/net/snowflake/client/jdbc/SFBaseFileTransferAgent.java
 *
 * Only the CommandType enum is replicated. The rest of the class (FixedView
 * infrastructure, abstract methods, upload/download facades) is omitted —
 * those depend on JDBC-internal types (SnowflakeFixedView, FixedViewColumn,
 * ClassUtil, SnowflakeColumnMetadata) that are not used by the replicated
 * SnowflakeFileTransferAgent methods.
 */
package net.snowflake.ingest.streaming.internal.fileTransferAgent;

/** Base class for file transfers. */
public abstract class SFBaseFileTransferAgent {

  /** The types of file transfer: upload and download. */
  public enum CommandType {
    UPLOAD,
    DOWNLOAD
  }
}
