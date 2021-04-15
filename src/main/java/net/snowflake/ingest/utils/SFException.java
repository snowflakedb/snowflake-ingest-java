/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.utils;

import net.snowflake.client.jdbc.internal.snowflake.common.core.ResourceBundleManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Snowflake exception in the Ingest SDK */
public class SFException extends Throwable {
  static final Logger logger = LoggerFactory.getLogger(SFException.class);

  static final ResourceBundleManager errorResourceBundleManager =
      ResourceBundleManager.getSingleton(ErrorCode.errorMessageResource);

  private Throwable cause;
  private String vendorCode;
  private Object[] params;

  /**
   * Construct a Snowflake exception from a cause, an error code and message parameters
   *
   * @param cause
   * @param errorCode
   * @param params
   */
  public SFException(Throwable cause, ErrorCode errorCode, Object... params) {
    super(
        errorResourceBundleManager.getLocalizedMessage(
            String.valueOf(errorCode.getMessageCode()), params),
        cause);

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
