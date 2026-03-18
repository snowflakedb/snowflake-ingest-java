/*
 * Replicated from snowflake-jdbc:
 *   net.snowflake.client.jdbc.internal.snowflake.common.core.SqlState
 * Tag: v3.25.1
 *
 * Only the SQL state constants used by the ingest storage clients are included.
 */

package net.snowflake.ingest.streaming.internal.fileTransferAgent;

public final class SqlState {
  public static final String SYSTEM_ERROR = "58000";
  public static final String INTERNAL_ERROR = "XX000";

  private SqlState() {}
}
