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
}
