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

    final String message = "Please retry";
    IOException[] retryExceptions = {
        new NoHttpResponseException(message),
        new SSLException(message),
        new SocketException(message),
        new UnknownHostException(message)
    };

    for (IOException exception : retryExceptions) {
      assertTrue(
          httpRequestRetryHandler.retryRequest(exception, 1, httpContextMock));
    }

    // Verify generic IOException is not retried
    assertFalse(httpRequestRetryHandler.retryRequest(new IOException(), 1, httpContextMock));

    // Verify retry is disabled when retry count exceeds the limit
    assertFalse(
        httpRequestRetryHandler.retryRequest(
            new SSLException(message), HttpUtil.MAX_RETRIES + 1, httpContextMock));
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

    int[] retryStatusCodes = {408, 429, 500, 503};
    for (int statusCode : retryStatusCodes) {
      doReturn(statusCode).when(statusLine).getStatusCode();
      assertTrue("Expected retry on " + statusCode, retryStrategy.retryRequest(response, 1, context));
    }

    int[] noRetryStatusCodes = {200, 400, 404};
    for (int statusCode : noRetryStatusCodes) {
      doReturn(statusCode).when(statusLine).getStatusCode();
      assertFalse("Expected no retry on " + statusCode, retryStrategy.retryRequest(response, 1, context));
    }
  }
}
