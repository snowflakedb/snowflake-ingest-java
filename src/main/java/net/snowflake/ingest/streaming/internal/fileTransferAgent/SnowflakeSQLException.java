/*
 * Replicated from snowflake-jdbc: net.snowflake.client.jdbc.SnowflakeSQLException
 * Tag: v3.25.1
 *
 * Only the constructors used by the ingest storage clients are included.
 * The ResourceBundleManager look-up is preserved to maintain message-format parity.
 */

package net.snowflake.ingest.streaming.internal.fileTransferAgent;

import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.MissingResourceException;
import java.util.ResourceBundle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnowflakeSQLException extends SQLException {
  private static final Logger logger = LoggerFactory.getLogger(SnowflakeSQLException.class);
  private static final long serialVersionUID = 1L;

  private static final String ERROR_MESSAGE_RESOURCE =
      "net.snowflake.ingest.streaming.internal.fileTransferAgent.storage_error_messages";

  private String queryId = "unknown";

  /** (Throwable, String sqlState, int vendorCode, Object... params) */
  public SnowflakeSQLException(
      Throwable ex, String sqlState, int vendorCode, Object... params) {
    super(localizedMessage(String.valueOf(vendorCode), params), sqlState, vendorCode, ex);
    logger.debug(
        "Snowflake exception: " + localizedMessage(String.valueOf(vendorCode), params), ex);
  }

  /** (String sqlState, int vendorCode, Object... params) */
  public SnowflakeSQLException(String sqlState, int vendorCode, Object... params) {
    super(localizedMessage(String.valueOf(vendorCode), params), sqlState, vendorCode);
    logger.debug(
        "Snowflake exception: {}, sqlState: {}, vendorCode: {}",
        localizedMessage(String.valueOf(vendorCode), params),
        sqlState,
        vendorCode);
  }

  /** (StorageErrorCode errorCode, Object... params) */
  public SnowflakeSQLException(StorageErrorCode errorCode, Object... params) {
    super(
        localizedMessage(String.valueOf(errorCode.getMessageCode()), params),
        errorCode.getSqlState(),
        errorCode.getMessageCode());
  }

  /** (String reason, String sqlState, int vendorCode) — used by SnowflakeSQLLoggedException */
  public SnowflakeSQLException(String reason, String sqlState, int vendorCode) {
    super(reason, sqlState, vendorCode);
  }

  /** (String queryId, Throwable, String sqlState, int vendorCode, Object... params) */
  public SnowflakeSQLException(
      String queryId, Throwable ex, String sqlState, int vendorCode, Object... params) {
    super(localizedMessage(String.valueOf(vendorCode), params), sqlState, vendorCode, ex);
    this.queryId = queryId;
  }

  /** (String queryId, String sqlState, int vendorCode, Object... params) */
  public SnowflakeSQLException(
      String queryId, String sqlState, int vendorCode, Object... params) {
    super(localizedMessage(String.valueOf(vendorCode), params), sqlState, vendorCode);
    this.queryId = queryId;
  }

  public String getQueryId() {
    return this.queryId;
  }

  private static String localizedMessage(String key, Object... args) {
    try {
      ResourceBundle bundle = ResourceBundle.getBundle(ERROR_MESSAGE_RESOURCE);
      String template = bundle.getString(key);
      if (args != null && args.length > 0) {
        return MessageFormat.format(template, args);
      }
      return template;
    } catch (MissingResourceException e) {
      return '!' + key + '!';
    }
  }
}
