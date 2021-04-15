/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.utils;

import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Base Logging Utility */
public class Logging {
  private final Logger log = LoggerFactory.getLogger(getClass().getName());

  // only message
  protected void logInfo(String msg) {
    if (log.isInfoEnabled()) {
      log.info(logMessage(msg));
    }
  }

  protected void logTrace(String msg) {
    if (log.isTraceEnabled()) {
      log.trace(logMessage(msg));
    }
  }

  protected void logDebug(String msg) {
    if (log.isDebugEnabled()) {
      log.debug(logMessage(msg));
    }
  }

  protected void logWarn(String msg) {
    if (log.isWarnEnabled()) {
      log.warn(logMessage(msg));
    }
  }

  protected void logError(String msg) {
    if (log.isErrorEnabled()) {
      log.error(logMessage(msg));
    }
  }

  // format and variables
  protected void logInfo(String format, Object... vars) {
    if (log.isInfoEnabled()) {
      log.info(logMessage(format, vars));
    }
  }

  protected void logTrace(String format, Object... vars) {
    if (log.isTraceEnabled()) {
      log.trace(logMessage(format, vars));
    }
  }

  protected void logDebug(String format, Object... vars) {
    if (log.isDebugEnabled()) {
      log.debug(logMessage(format, vars));
    }
  }

  protected void logWarn(String format, Object... vars) {
    if (log.isWarnEnabled()) {
      log.warn(format, vars);
    }
  }

  protected void logError(String format, Object... vars) {
    if (log.isErrorEnabled()) {
      log.error(logMessage(format, vars));
    }
  }

  // static elements

  // log message tag
  static final String SF_LOG_TAG = "[SF_INGEST]";

  /*
   * the following methods wrap log message with Snowflake tag. For example,
   *
   * [SF_INGEST] this is a log message
   * [SF_INGEST] this is the second line
   *
   * All log messages should be wrapped by Snowflake tag.
   */

  /**
   * wrap a message without variable
   *
   * @param msg log message
   * @return log message wrapped by snowflake tag
   */
  private static String logMessage(String msg) {
    return "\n".concat(msg).replaceAll("\n", "\n" + SF_LOG_TAG + " ");
  }

  /**
   * wrap a message contains multiple variables, each {} will be replaced with the input variable
   *
   * @param format log message format string
   * @param vars variable list
   * @return log message wrapped by snowflake tag
   */
  private static String logMessage(String format, Object... vars) {
    for (int i = 0; i < vars.length; i++) {
      format = format.replaceFirst("\\{}", Objects.toString(vars[i]).replaceAll("\\$", "\\\\\\$"));
    }
    return logMessage(format);
  }
}
