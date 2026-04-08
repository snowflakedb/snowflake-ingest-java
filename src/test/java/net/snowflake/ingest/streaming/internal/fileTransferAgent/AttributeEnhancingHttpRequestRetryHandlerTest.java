// Ported from snowflake-jdbc:
// net.snowflake.client.core.AttributeEnhancingHttpRequestRetryHandlerTest
package net.snowflake.ingest.streaming.internal.fileTransferAgent;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;
import org.junit.Test;

public class AttributeEnhancingHttpRequestRetryHandlerTest {
  private final IOException dummyException = new IOException("Test exception");

  @Test
  public void testAttributeSetCount0() {
    verifyAttributeSet(0);
  }

  @Test
  public void testAttributeSetCount3() {
    verifyAttributeSet(3);
  }

  @Test
  public void testAttributeSetCount5() {
    verifyAttributeSet(5);
  }

  @Test
  public void testAttributeSetCount10() {
    verifyAttributeSet(10);
  }

  private void verifyAttributeSet(int executionCount) {
    HttpContext context = new BasicHttpContext();
    AttributeEnhancingHttpRequestRetryHandler handler =
        new AttributeEnhancingHttpRequestRetryHandler();

    handler.retryRequest(dummyException, executionCount, context);

    assertEquals(
        "Attribute should be set to the provided executionCount: " + executionCount,
        executionCount,
        context.getAttribute(AttributeEnhancingHttpRequestRetryHandler.EXECUTION_COUNT_ATTRIBUTE));
  }
}
