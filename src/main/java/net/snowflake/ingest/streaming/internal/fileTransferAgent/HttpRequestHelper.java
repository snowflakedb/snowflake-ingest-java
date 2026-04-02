/*
 * Utility class replacing JDBC's RestRequest.executeWithRetries and HttpUtil.executeGeneralRequest.
 * Provides HTTP request execution with exponential backoff retry logic.
 */
package net.snowflake.ingest.streaming.internal.fileTransferAgent;

import java.io.IOException;
import net.snowflake.ingest.streaming.internal.fileTransferAgent.log.SFLogger;
import net.snowflake.ingest.streaming.internal.fileTransferAgent.log.SFLoggerFactory;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.client.CloseableHttpClient;

/**
 * A simple HTTP request executor with retry logic, replacing JDBC's RestRequest.executeWithRetries
 * and HttpUtil.executeGeneralRequest.
 */
public final class HttpRequestHelper {
  private static final SFLogger logger = SFLoggerFactory.getLogger(HttpRequestHelper.class);

  private static final int DEFAULT_MAX_RETRIES = 7;
  private static final long BASE_BACKOFF_MILLIS = 1000L;
  private static final long MAX_BACKOFF_MILLIS = 16000L;

  private HttpRequestHelper() {}

  /**
   * Execute an HTTP request with default retry settings (7 retries, no 403 retry).
   *
   * @param httpClient the HTTP client to use
   * @param httpRequest the request to execute
   * @return the HTTP response
   * @throws IOException if all retries are exhausted
   */
  public static HttpResponse execute(CloseableHttpClient httpClient, HttpRequestBase httpRequest)
      throws IOException {
    return execute(httpClient, httpRequest, DEFAULT_MAX_RETRIES, false);
  }

  /**
   * Execute an HTTP request with retry logic using exponential backoff with jitter.
   *
   * <p>Retries on:
   *
   * <ul>
   *   <li>IOException (connection failures, timeouts)
   *   <li>HTTP 5xx (server errors)
   *   <li>HTTP 408 (request timeout)
   *   <li>HTTP 429 (too many requests)
   *   <li>Optionally HTTP 403 (forbidden, useful for presigned URL retries)
   * </ul>
   *
   * @param httpClient the HTTP client to use
   * @param httpRequest the request to execute
   * @param maxRetries maximum number of retry attempts
   * @param retryOnHTTP403 whether to retry on HTTP 403 responses
   * @return the HTTP response
   * @throws IOException if all retries are exhausted
   */
  public static HttpResponse execute(
      CloseableHttpClient httpClient,
      HttpRequestBase httpRequest,
      int maxRetries,
      boolean retryOnHTTP403)
      throws IOException {
    IOException lastException = null;

    for (int attempt = 0; attempt <= maxRetries; attempt++) {
      try {
        HttpResponse response = httpClient.execute(httpRequest);
        int statusCode = response.getStatusLine().getStatusCode();

        if (isRetryableStatusCode(statusCode, retryOnHTTP403)) {
          if (attempt < maxRetries) {
            logger.debug(
                "Retryable HTTP status {} on attempt {}/{}", statusCode, attempt + 1, maxRetries);
            sleep(backoffMillis(attempt));
            continue;
          }
          // Last attempt, return whatever we got
        }
        return response;
      } catch (IOException e) {
        lastException = e;
        if (attempt < maxRetries) {
          logger.debug("IOException on attempt {}/{}: {}", attempt + 1, maxRetries, e.getMessage());
          sleep(backoffMillis(attempt));
        }
      }
    }

    throw new IOException(
        "HTTP request failed after " + (maxRetries + 1) + " attempts", lastException);
  }

  private static boolean isRetryableStatusCode(int statusCode, boolean retryOnHTTP403) {
    if (statusCode >= 500) {
      return true;
    }
    if (statusCode == 408 || statusCode == 429) {
      return true;
    }
    if (retryOnHTTP403 && statusCode == 403) {
      return true;
    }
    return false;
  }

  /**
   * Compute backoff with jitter. Base is 1s, doubles each attempt, capped at 16s. Jitter is +/-
   * 25%.
   */
  private static long backoffMillis(int attempt) {
    long backoff = Math.min(BASE_BACKOFF_MILLIS << attempt, MAX_BACKOFF_MILLIS);
    // Add jitter: +/- 25%
    long jitter = (long) (backoff * 0.25 * (2 * Math.random() - 1));
    return Math.max(0, backoff + jitter);
  }

  private static void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
