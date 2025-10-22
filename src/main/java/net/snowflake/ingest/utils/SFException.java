/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.utils;

import java.text.MessageFormat;
import java.util.ResourceBundle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Snowflake exception in the Ingest SDK */
public class SFException extends RuntimeException {
  static final Logger logger = LoggerFactory.getLogger(SFException.class);
  static final ResourceBundle errorMessageBundle =
      ResourceBundle.getBundle(ErrorCode.errorMessageResource);

  private Throwable cause;
  private String vendorCode;
  private Object[] params;

  private static String getErrorMessage(final ErrorCode errorCode, final Object... params) {
    final String messageTemplate = errorMessageBundle.getString(errorCode.getMessageCode());
    return MessageFormat.format(messageTemplate, params);
  }

  /**
   * Construct a Snowflake exception from a cause, an error code and message parameters
   *
   * @param cause
   * @param errorCode
   * @param params
   */
  public SFException(Throwable cause, ErrorCode errorCode, Object... params) {
    super(getErrorMessage(errorCode, params), cause);

    this.vendorCode = errorCode.getMessageCode();
    this.params = params;
    this.cause = cause;
  }

  /**
   * Construct a Snowflake exception from an error code and message parameters
   *
   * @param errorCode
   * @param params
   */
  public SFException(ErrorCode errorCode, Object... params) {
    this(null, errorCode, params);
  }

  public String getVendorCode() {
    return vendorCode;
  }

  public Object[] getParams() {
    return params;
  }

  public Throwable getCause() {
    return cause;
  }

  /**
   * Checks if this exception has the specified error code
   *
   * @param errorCode the error code to check
   * @return true if this exception's vendor code matches the given error code
   */
  public boolean isErrorCode(ErrorCode errorCode) {
    return errorCode != null && errorCode.getMessageCode().equals(this.vendorCode);
  }

  /**
   * Extracts SFException from an exception or its cause
   *
   * @param e the exception to check
   * @return SFException if found, null otherwise
   */
  public static SFException extractSFException(Exception e) {
    if (e instanceof SFException) {
      return (SFException) e;
    }
    Throwable cause = e.getCause();
    if (cause instanceof SFException) {
      return (SFException) cause;
    }
    return null;
  }
}
