/*
 * Replicated from snowflake-jdbc (v3.25.1)
 * Source: https://github.com/snowflakedb/snowflake-jdbc/blob/v3.25.1/src/main/java/net/snowflake/client/core/AttributeEnhancingHttpRequestRetryHandler.java
 *
 * Permitted differences: package declaration.
 */
package net.snowflake.ingest.streaming.internal.fileTransferAgent;

import java.io.IOException;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.protocol.HttpContext;

/**
 * Extends {@link DefaultHttpRequestRetryHandler} to store the current execution count (attempt
 * number) in the {@link HttpContext}. This allows interceptors to identify retry attempts.
 *
 * <p>The execution count is stored using the key defined by {@link #EXECUTION_COUNT_ATTRIBUTE}.
 */
class AttributeEnhancingHttpRequestRetryHandler extends DefaultHttpRequestRetryHandler {
  /**
   * The key used to store the current execution count (attempt number) in the {@link HttpContext}.
   * Interceptors can use this key to retrieve the count. The value stored will be an {@link
   * Integer}.
   */
  static final String EXECUTION_COUNT_ATTRIBUTE = "net.snowflake.client.core.execution-count";

  @Override
  public boolean retryRequest(IOException exception, int executionCount, HttpContext context) {
    context.setAttribute(EXECUTION_COUNT_ATTRIBUTE, executionCount);
    return super.retryRequest(exception, executionCount, context);
  }
}
