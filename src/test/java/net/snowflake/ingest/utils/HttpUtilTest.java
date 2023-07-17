package net.snowflake.ingest.utils;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;

import java.io.IOException;
import net.snowflake.client.jdbc.internal.apache.http.HttpRequest;
import net.snowflake.client.jdbc.internal.apache.http.RequestLine;
import net.snowflake.client.jdbc.internal.apache.http.client.HttpRequestRetryHandler;
import net.snowflake.client.jdbc.internal.apache.http.client.protocol.HttpClientContext;
import org.junit.Test;
import org.mockito.Mockito;

public class HttpUtilTest {
  @Test
  public void testRequestRetryHandler() {
    HttpRequestRetryHandler httpRequestRetryHandler = HttpUtil.getHttpRequestRetryHandler();

    HttpClientContext httpContextMock = Mockito.mock(HttpClientContext.class);
    RequestLine requestLine = Mockito.mock(RequestLine.class);
    HttpRequest httpRequest = Mockito.mock(HttpRequest.class);

    doReturn(httpRequest).when(httpContextMock).getRequest();
    doReturn(requestLine).when(httpRequest).getRequestLine();
    doReturn("/api/v1/status").when(requestLine).getUri();

    assertTrue(
        httpRequestRetryHandler.retryRequest(
            new IOException("Test exception"), 0, httpContextMock));
    assertTrue(
        httpRequestRetryHandler.retryRequest(
            new IOException("Test exception"), 30, httpContextMock));
    assertFalse(
        httpRequestRetryHandler.retryRequest(
            new IOException("Test exception"), 31, httpContextMock));
  }
}
