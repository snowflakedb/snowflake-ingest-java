/*
 * Replicated from snowflake-jdbc (v3.25.1)
 * Source: https://github.com/snowflakedb/snowflake-jdbc/blob/v3.25.1/src/main/java/net/snowflake/client/jdbc/SnowflakeSQLLoggedException.java
 *
 * Permitted differences: package, SFLogger/isNullOrEmpty/ErrorCode/SnowflakeSQLException use
 * ingest versions. All telemetry imports swapped to ingest replicated versions.
 * SFSession/SFBaseSession removed (always null from callers). IB telemetry code removed
 * (dead code — session was always null so ibInstance was always null, only OOB path executed).
 */
package net.snowflake.ingest.streaming.internal.fileTransferAgent;

import static net.snowflake.ingest.streaming.internal.fileTransferAgent.StorageClientUtil.isNullOrEmpty;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import net.minidev.json.JSONObject;
import net.snowflake.ingest.connection.telemetry.TelemetryField;
import net.snowflake.ingest.streaming.internal.fileTransferAgent.log.SFLogger;
import net.snowflake.ingest.streaming.internal.fileTransferAgent.log.SFLoggerFactory;

/**
 * This SnowflakeSQLLoggedException class extends the SnowflakeSQLException class to add OOB
 * telemetry data for sql exceptions. Not all sql exceptions require OOB telemetry logging so the
 * exceptions in this class should only be thrown if there is a need for logging the exception with
 * OOB telemetry.
 */
public class SnowflakeSQLLoggedException extends SnowflakeSQLException {
  private static final SFLogger logger =
      SFLoggerFactory.getLogger(SnowflakeSQLLoggedException.class);
  private static final int NO_VENDOR_CODE = -1;

  public SnowflakeSQLLoggedException(
      String queryID, String sqlState, String message, Exception cause) {
    super(queryID, cause, sqlState, NO_VENDOR_CODE, message);
    sendTelemetryData(queryID, sqlState, NO_VENDOR_CODE, this);
  }

  /**
   * Function to create a TelemetryEvent log from the JSONObject and exception and send it via OOB
   * telemetry
   *
   * @param value JSONnode containing relevant information specific to the exception constructor
   *     that should be included in the telemetry data, such as sqlState or vendorCode
   * @param ex The exception being thrown
   * @param oobInstance Out of band telemetry instance through which log will be passed
   */
  private static void sendOutOfBandTelemetryMessage(
      JSONObject value, SQLException ex, TelemetryService oobInstance) {
    TelemetryEvent.LogBuilder logBuilder = new TelemetryEvent.LogBuilder();
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    ex.printStackTrace(pw);
    String stackTrace = maskStacktrace(sw.toString());
    value.put("Stacktrace", stackTrace);
    value.put("Exception", ex.getClass().getSimpleName());
    TelemetryEvent log =
        logBuilder.withName("Exception: " + ex.getClass().getSimpleName()).withValue(value).build();
    oobInstance.report(log);
  }

  /**
   * Helper function to remove sensitive data (error message, reason) from the stacktrace.
   *
   * @param stackTrace original stacktrace
   * @return stack trace with sensitive data removed
   */
  static String maskStacktrace(String stackTrace) {
    Pattern STACKTRACE_BEGINNING =
        Pattern.compile(
            "(com|net)(\\.snowflake\\.client\\.jdbc\\.Snowflake)(SQLLogged|LoggedFeatureNotSupported|SQL)(Exception)([\\s\\S]*?)(\\n"
                + "\\t?at\\snet|com\\.)",
            Pattern.MULTILINE | Pattern.CASE_INSENSITIVE);
    Matcher matcher = STACKTRACE_BEGINNING.matcher(stackTrace);
    // Remove the reason from after the stack trace (in group #5 of regex pattern)
    if (matcher.find()) {
      return matcher.replaceAll("$1$2$3$4$6");
    }
    return stackTrace;
  }

  /**
   * Helper function to create JSONObject node for OOB telemetry log
   *
   * @param queryId query ID
   * @param SQLState the SQL state
   * @param vendorCode the vendor code
   * @return JSONObject with data about SQLException
   */
  static JSONObject createOOBValue(String queryId, String SQLState, int vendorCode) {
    JSONObject oobValue = new JSONObject();
    oobValue.put("type", TelemetryField.SQL_EXCEPTION.toString());
    oobValue.put("DriverType", SnowflakeDriverConstants.SF_JDBC_APP_ID);
    oobValue.put("DriverVersion", SnowflakeDriverConstants.implementVersion);
    if (!isNullOrEmpty(queryId)) {
      oobValue.put("QueryID", queryId);
    }
    if (!isNullOrEmpty(SQLState)) {
      oobValue.put("SQLState", SQLState);
    }
    if (vendorCode != NO_VENDOR_CODE) {
      oobValue.put("ErrorNumber", vendorCode);
    }
    return oobValue;
  }

  /**
   * Function to construct log data and send via OOB telemetry.
   *
   * @param queryId query ID if exists
   * @param SQLState SQLState
   * @param vendorCode vendor code
   * @param ex Exception object
   */
  public static void sendTelemetryData(
      String queryId, String SQLState, int vendorCode, SQLException ex) {
    JSONObject oobValue = createOOBValue(queryId, SQLState, vendorCode);
    sendOutOfBandTelemetryMessage(oobValue, ex, TelemetryService.getInstance());
  }

  /**
   * @param reason exception reason
   * @param SQLState the SQL state
   * @param vendorCode the vendor code
   * @param queryId the query ID
   */
  public SnowflakeSQLLoggedException(
      String reason, String SQLState, int vendorCode, String queryId) {
    super(queryId, reason, SQLState, vendorCode);
    sendTelemetryData(queryId, SQLState, vendorCode, this);
  }

  /**
   * @param vendorCode the vendor code
   * @param SQLState the SQL state
   */
  public SnowflakeSQLLoggedException(int vendorCode, String SQLState) {
    super(SQLState, vendorCode);
    sendTelemetryData(null, SQLState, vendorCode, this);
  }

  /**
   * @param queryId the query ID
   * @param vendorCode the vendor code
   * @param SQLState the SQL state
   */
  public SnowflakeSQLLoggedException(String queryId, int vendorCode, String SQLState) {
    super(queryId, SQLState, vendorCode);
    sendTelemetryData(queryId, SQLState, vendorCode, this);
  }

  /**
   * @param queryId the query ID
   * @param SQLState the SQL state
   * @param reason the exception reason
   */
  public SnowflakeSQLLoggedException(String queryId, String SQLState, String reason) {
    super(reason, SQLState);
    sendTelemetryData(queryId, SQLState, NO_VENDOR_CODE, this);
  }

  /**
   * @param vendorCode the vendor code
   * @param SQLState the SQL state
   * @param params additional parameters
   */
  public SnowflakeSQLLoggedException(int vendorCode, String SQLState, Object... params) {
    this(null, vendorCode, SQLState, params);
  }

  /**
   * @param queryId the query ID
   * @param vendorCode the vendor code
   * @param SQLState the SQL state
   * @param params additional parameters
   */
  public SnowflakeSQLLoggedException(
      String queryId, int vendorCode, String SQLState, Object... params) {
    super(queryId, SQLState, vendorCode, params);
    sendTelemetryData(queryId, SQLState, vendorCode, this);
  }

  /**
   * @param errorCode the error code
   * @param ex Throwable exception
   * @param params additional parameters
   */
  public SnowflakeSQLLoggedException(ErrorCode errorCode, Throwable ex, Object... params) {
    super(ex, errorCode, params);
    sendTelemetryData(null, errorCode.getSqlState(), errorCode.getMessageCode(), this);
  }

  /**
   * @param SQLState the SQL state
   * @param vendorCode the vendor code
   * @param ex Throwable exception
   * @param params additional parameters
   */
  public SnowflakeSQLLoggedException(
      String SQLState, int vendorCode, Throwable ex, Object... params) {
    super(ex, SQLState, vendorCode, params);
    sendTelemetryData(null, SQLState, vendorCode, this);
  }

  /**
   * @param queryId the query ID
   * @param SQLState the SQL state
   * @param vendorCode the vendor code
   * @param ex Throwable exception
   * @param params additional parameters
   */
  public SnowflakeSQLLoggedException(
      String queryId, String SQLState, int vendorCode, Throwable ex, Object... params) {
    super(queryId, ex, SQLState, vendorCode, params);
    sendTelemetryData(queryId, SQLState, vendorCode, this);
  }

  /**
   * @param errorCode the error code
   * @param params additional parameters
   */
  @Deprecated
  public SnowflakeSQLLoggedException(ErrorCode errorCode, Object... params) {
    this(null, errorCode, params);
  }

  /**
   * @param queryId the query ID
   * @param errorCode the error code
   * @param params additional parameters
   */
  public SnowflakeSQLLoggedException(String queryId, ErrorCode errorCode, Object... params) {
    super(queryId, errorCode, params);
    sendTelemetryData(queryId, null, NO_VENDOR_CODE, this);
  }

  /**
   * @param e throwable exception
   */
  public SnowflakeSQLLoggedException(SFException e) {
    super(e);
    sendTelemetryData(null, null, NO_VENDOR_CODE, this);
  }

  /**
   * @param reason exception reason
   */
  @Deprecated
  public SnowflakeSQLLoggedException(String reason) {
    this(null, (String) null, reason);
  }

  /**
   * @param queryId the query ID
   * @param reason exception reason
   */
  public SnowflakeSQLLoggedException(String queryId, String reason) {
    super(queryId, reason, null);
    sendTelemetryData(queryId, null, NO_VENDOR_CODE, this);
  }
}
