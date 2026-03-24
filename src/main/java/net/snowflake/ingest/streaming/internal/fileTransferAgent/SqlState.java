/*
 * Replicated from snowflake-jdbc-thin (v3.25.1):
 *   net.snowflake.client.jdbc.internal.snowflake.common.core.SqlState
 * Originally from net.snowflake:snowflake-common
 *
 * Contains the SQL state constants used by replicated ErrorCode and
 * SnowflakeFileTransferAgent classes.
 */

package net.snowflake.ingest.streaming.internal.fileTransferAgent;

public final class SqlState {
  public static final String WARNING = "01000";
  public static final String NO_DATA = "02000";
  public static final String SQL_STATEMENT_NOT_YET_COMPLETE = "03000";
  public static final String CONNECTION_EXCEPTION = "08000";
  public static final String CONNECTION_DOES_NOT_EXIST = "08003";
  public static final String SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION = "08001";
  public static final String FEATURE_NOT_SUPPORTED = "0A000";
  public static final String DATA_EXCEPTION = "22000";
  public static final String INVALID_PARAMETER_VALUE = "22023";
  public static final String NUMERIC_VALUE_OUT_OF_RANGE = "22003";
  public static final String INVALID_AUTHORIZATION_SPECIFICATION = "28000";
  public static final String SYNTAX_ERROR = "42601";
  public static final String PROGRAM_LIMIT_EXCEEDED = "54000";
  public static final String QUERY_CANCELED = "57014";
  public static final String SYSTEM_ERROR = "58000";
  public static final String IO_ERROR = "58030";
  public static final String INTERNAL_ERROR = "XX000";

  private SqlState() {}
}
