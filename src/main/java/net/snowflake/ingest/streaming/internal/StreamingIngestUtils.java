/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.utils.Constants.MAX_STREAMING_INGEST_API_CHANNEL_RETRY;
import static net.snowflake.ingest.utils.Constants.RESPONSE_ERR_GENERAL_EXCEPTION_RETRY_REQUEST;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.function.Function;
import net.snowflake.client.jdbc.internal.apache.http.client.methods.CloseableHttpResponse;
import net.snowflake.client.jdbc.internal.apache.http.client.methods.HttpUriRequest;
import net.snowflake.client.jdbc.internal.apache.http.impl.client.CloseableHttpClient;
import net.snowflake.ingest.connection.IngestResponseException;
import net.snowflake.ingest.connection.RequestBuilder;
import net.snowflake.ingest.connection.ServiceResponseHandler;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.Logging;
import net.snowflake.ingest.utils.SFException;

public class StreamingIngestUtils {

  private static class DefaultStatusGetter<T extends StreamingIngestResponse>
      implements Function<T, Long> {
    public DefaultStatusGetter() {}

    public Long apply(T input) {
      return input.getStatusCode();
    }
  }

  private static final DefaultStatusGetter defaultStatusGetter = new DefaultStatusGetter();

  private static final Logging LOGGER = new Logging(StreamingIngestUtils.class);

  private static final ObjectMapper objectMapper = new ObjectMapper();

  /**
   * How many milliseconds of exponential backoff to sleep before retrying the request again:
   *
   * <ul>
   *   <li>0 or 1 failure => no sleep
   *   <li>2 failures => 1s
   *   <li>3 failures => 2s
   *   <li>4 or more failures => 4s
   * </ul>
   *
   * @param executionCount How many unsuccessful attempts have been attempted
   * @return Sleep time in ms
   */
  static long getSleepForRetryMs(int executionCount) {
    if (executionCount < 0) {
      throw new IllegalArgumentException(
          String.format(
              "executionCount must be a non-negative integer, passed: %d", executionCount));
    } else if (executionCount < 2) {
      return 0;
    } else {
      final int effectiveExecutionCount = Math.min(executionCount, 4);
      return (1 << (effectiveExecutionCount - 2)) * 1000L;
    }
  }

  public static void sleepForRetry(int executionCount) {
    long sleepForRetryMs = getSleepForRetryMs(executionCount);
    if (sleepForRetryMs == 0) {
      return;
    }

    try {
      Thread.sleep(sleepForRetryMs);
    } catch (InterruptedException e) {
      throw new SFException(ErrorCode.INTERNAL_ERROR, e.getMessage());
    }
  }

  static <T extends StreamingIngestResponse> T executeWithRetries(
      Class<T> targetClass,
      String endpoint,
      IStreamingIngestRequest payload,
      String message,
      ServiceResponseHandler.ApiName apiName,
      CloseableHttpClient httpClient,
      RequestBuilder requestBuilder)
      throws IOException, IngestResponseException {
    String payloadInString;
    try {
      payloadInString = objectMapper.writeValueAsString(payload);
    } catch (JsonProcessingException e) {
      throw new SFException(e, ErrorCode.BUILD_REQUEST_FAILURE, message);
    }
    return executeWithRetries(
        targetClass, endpoint, payloadInString, message, apiName, httpClient, requestBuilder);
  }

  static <T extends StreamingIngestResponse> T executeWithRetries(
      Class<T> targetClass,
      String endpoint,
      String payload,
      String message,
      ServiceResponseHandler.ApiName apiName,
      CloseableHttpClient httpClient,
      RequestBuilder requestBuilder)
      throws IOException, IngestResponseException {
    return (T)
        executeWithRetries(
            targetClass,
            endpoint,
            payload,
            message,
            apiName,
            httpClient,
            requestBuilder,
            defaultStatusGetter);
  }

  static <T> T executeWithRetries(
      Class<T> targetClass,
      String endpoint,
      String payload,
      String message,
      ServiceResponseHandler.ApiName apiName,
      CloseableHttpClient httpClient,
      RequestBuilder requestBuilder,
      Function<T, Long> statusGetter)
      throws IOException, IngestResponseException {
    int retries = 0;
    T response;
    HttpUriRequest request =
        requestBuilder.generateStreamingIngestPostRequest(payload, endpoint, message);
    do {
      try (CloseableHttpResponse httpResponse = httpClient.execute(request)) {
        response =
            ServiceResponseHandler.unmarshallStreamingIngestResponse(
                httpResponse, targetClass, apiName, httpClient, request, requestBuilder);
      }

      if (statusGetter.apply(response) == RESPONSE_ERR_GENERAL_EXCEPTION_RETRY_REQUEST) {
        LOGGER.logDebug(
            "Retrying request for streaming ingest, endpoint={}, retryCount={}, responseCode={}",
            endpoint,
            retries,
            statusGetter.apply(response));
        retries++;
        sleepForRetry(retries);
      } else {
        return response;
      }
    } while (retries <= MAX_STREAMING_INGEST_API_CHANNEL_RETRY);
    return response;
  }

  public static String getShortname(final String fullname) {
    final String[] parts = fullname.split("/");
    return parts[parts.length - 1];
  }
}
