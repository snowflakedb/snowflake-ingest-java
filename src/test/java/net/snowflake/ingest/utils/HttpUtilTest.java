package net.snowflake.ingest.utils;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;

import java.io.IOException;
import java.net.SocketException;
import java.net.UnknownHostException;
import javax.net.ssl.SSLException;
import org.apache.http.HttpRequest;
import org.apache.http.NoHttpResponseException;
import org.apache.http.RequestLine;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.protocol.HttpClientContext;
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
            new NoHttpResponseException("Test exception"), 1, httpContextMock));
    assertTrue(
        httpRequestRetryHandler.retryRequest(
            new SSLException("Test exception"), 1, httpContextMock));
    assertTrue(
        httpRequestRetryHandler.retryRequest(
            new SocketException("Test exception"), 1, httpContextMock));
    assertTrue(
        httpRequestRetryHandler.retryRequest(
            new UnknownHostException("Test exception"), 1, httpContextMock));
    assertFalse(
        httpRequestRetryHandler.retryRequest(
            new SSLException("Test exception"), 11, httpContextMock));
    assertFalse(httpRequestRetryHandler.retryRequest(new IOException(), 1, httpContextMock));
  }
}
