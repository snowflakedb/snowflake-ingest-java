/*
 * Replicated from snowflake-jdbc: net.snowflake.client.core.IncidentUtil
 * Tag: v3.25.1
 * Source: https://github.com/snowflakedb/snowflake-jdbc/blob/v3.25.1/src/main/java/net/snowflake/client/core/IncidentUtil.java
 *
 * Only the constants used by EventHandler are included. The full class depends on
 * com.yammer.metrics.reporting.MetricsServlet which requires javax.servlet (not on
 * the ingest classpath). The incident dump functionality is JDBC-internal and not
 * used by the ingest SDK.
 */

package net.snowflake.ingest.streaming.internal.fileTransferAgent.log;

final class IncidentUtil {
  static final String INC_DUMP_FILE_NAME = "sf_incident_";
  static final String INC_DUMP_FILE_EXT = ".dmp";

  private IncidentUtil() {}
}
