/*
 * Replicated from snowflake-jdbc (v3.25.1)
 * Source: https://github.com/snowflakedb/snowflake-jdbc/blob/v3.25.1/src/main/java/net/snowflake/client/core/SFOCSPException.java
 *
 * Permitted differences: package declaration, import swaps for already-replicated classes.
 */
package net.snowflake.ingest.streaming.internal.fileTransferAgent;

import net.snowflake.ingest.streaming.internal.fileTransferAgent.log.SFLogger;
import net.snowflake.ingest.streaming.internal.fileTransferAgent.log.SFLoggerFactory;

public class SFOCSPException extends Throwable {
  private static final SFLogger logger = SFLoggerFactory.getLogger(SFOCSPException.class);

  private static final long serialVersionUID = 1L;

  private final OCSPErrorCode errorCode;

  public SFOCSPException(OCSPErrorCode errorCode, String errorMsg) {
    this(errorCode, errorMsg, null);
  }

  public SFOCSPException(OCSPErrorCode errorCode, String errorMsg, Throwable cause) {
    super(errorMsg);
    this.errorCode = errorCode;
    if (cause != null) {
      this.initCause(cause);
    }
  }

  public OCSPErrorCode getErrorCode() {
    return errorCode;
  }

  @Override
  public String toString() {
    return super.toString()
        + (getErrorCode() != null ? ", errorCode = " + getErrorCode() : "")
        + (getMessage() != null ? ", errorMsg = " + getMessage() : "");
  }
}
