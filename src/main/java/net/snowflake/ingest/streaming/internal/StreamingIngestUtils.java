package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.utils.Constants.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Map;
import java.util.function.Function;
import net.snowflake.client.jdbc.internal.apache.http.client.methods.CloseableHttpResponse;
import net.snowflake.client.jdbc.internal.apache.http.impl.client.CloseableHttpClient;
import net.snowflake.client.jdbc.internal.google.api.client.http.HttpStatusCodes;
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

  static void sleepForRetry(int executionCount) {
    try {
      Thread.sleep((1 << (executionCount + 1)) * 1000);
    } catch (InterruptedException e) {
      throw new SFException(ErrorCode.INTERNAL_ERROR, e.getMessage());
    }
  }

  static <T extends StreamingIngestResponse> T executeWithRetries(
      Class<T> targetClass,
      String endpoint,
      Map<Object, Object> payload,
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
    T response = null;
    do {
      try (CloseableHttpResponse httpResponse =
          httpClient.execute(
              requestBuilder.generateStreamingIngestPostRequest(payload, endpoint, message))) {

        // Refresh OAuth token if needed, do not count as retry
        if (httpResponse.getStatusLine().getStatusCode() == HttpStatusCodes.STATUS_CODE_UNAUTHORIZED
            && requestBuilder.authType.equals(OAUTH)) {
          requestBuilder.refreshToken();
          continue;
        }

        response =
            ServiceResponseHandler.unmarshallStreamingIngestResponse(
                httpResponse, targetClass, apiName);
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
