package net.snowflake.ingest.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;

import java.io.IOException;
import java.net.SocketException;
import java.net.UnknownHostException;
import javax.net.ssl.SSLException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.NoHttpResponseException;
import org.apache.http.RequestLine;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.ServiceUnavailableRetryStrategy;
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

  @Test
  public void testServiceUnavailableRetryStrategy() {
    ServiceUnavailableRetryStrategy retryStrategy = HttpUtil.getServiceUnavailableRetryStrategy();

    HttpClientContext context = Mockito.mock(HttpClientContext.class);
    HttpResponse response = Mockito.mock(HttpResponse.class);
    HttpRequest request = Mockito.mock(HttpRequest.class);
    RequestLine requestLine = Mockito.mock(RequestLine.class);
    StatusLine statusLine = Mockito.mock(StatusLine.class);

    doReturn(request).when(context).getRequest();
    doReturn(requestLine).when(request).getRequestLine();
    doReturn(statusLine).when(response).getStatusLine();

    // Expect retry
    doReturn(408).when(statusLine).getStatusCode();
    assertTrue("Expected retry on 408 Request Timeout", retryStrategy.retryRequest(response, 1, context));

    doReturn(429).when(statusLine).getStatusCode();
    assertTrue("Expected retry on 429 Too Many Requests", retryStrategy.retryRequest(response, 1, context));

    doReturn(500).when(statusLine).getStatusCode();
    assertTrue("Expected retry on 500 Internal Server Error", retryStrategy.retryRequest(response, 1, context));

    doReturn(503).when(statusLine).getStatusCode();
    assertTrue("Expected retry on 503 Service Unavailable", retryStrategy.retryRequest(response, 1, context));

    // Expect no retry
    doReturn(200).when(statusLine).getStatusCode();
    assertFalse("Expected no retry on 200 Success", retryStrategy.retryRequest(response, 1, context));

    doReturn(400).when(statusLine).getStatusCode();
    assertFalse("Expected no retry on 400 Bad Request", retryStrategy.retryRequest(response, 1, context));

    doReturn(404).when(statusLine).getStatusCode();
    assertFalse("Expected no retry on 404 Not Found", retryStrategy.retryRequest(response, 1, context));
  }
}
