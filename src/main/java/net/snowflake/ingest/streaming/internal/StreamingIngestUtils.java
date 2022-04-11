package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.utils.Constants.MAX_API_RETRY;
import static net.snowflake.ingest.utils.Constants.RESPONSE_ERR_GENERAL_EXCEPTION_RETRY_REQUEST;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Map;
import java.util.function.Function;
import net.snowflake.ingest.connection.IngestResponseException;
import net.snowflake.ingest.connection.RequestBuilder;
import net.snowflake.ingest.connection.ServiceResponseHandler;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;
import org.apache.http.client.HttpClient;

public class StreamingIngestUtils {

  private static class DefaultStatusGetter<T extends StreamingIngestResponse>
      implements Function<T, Long> {
    public DefaultStatusGetter() {}

    public Long apply(T input) {
      return input.getStatusCode();
    }
  }

  private static final DefaultStatusGetter defaultStatusGetter = new DefaultStatusGetter();

  private static final ObjectMapper objectMapper = new ObjectMapper();

  static <T extends StreamingIngestResponse> T executeWithRetries(
      Class<T> targetClass,
      String endpoint,
      Map<Object, Object> payload,
      String message,
      ServiceResponseHandler.ApiName apiName,
      HttpClient httpClient,
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
      HttpClient httpClient,
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
      HttpClient httpClient,
      RequestBuilder requestBuilder,
      Function<T, Long> statusGetter)
      throws IOException, IngestResponseException {
    T response =
        ServiceResponseHandler.unmarshallStreamingIngestResponse(
            httpClient.execute(
                requestBuilder.generateStreamingIngestPostRequest(payload, endpoint, message)),
            targetClass,
            apiName);

    // Check for Snowflake specific response code
    if (statusGetter.apply(response) == RESPONSE_ERR_GENERAL_EXCEPTION_RETRY_REQUEST) {
      int retries = 0;
      while (retries < MAX_API_RETRY) {
        response =
            ServiceResponseHandler.unmarshallStreamingIngestResponse(
                httpClient.execute(
                    requestBuilder.generateStreamingIngestPostRequest(payload, endpoint, message)),
                targetClass,
                apiName);
        if (statusGetter.apply(response) == RESPONSE_ERR_GENERAL_EXCEPTION_RETRY_REQUEST) {
          retries++;
          try {
            Thread.sleep((1 << retries) * 1000);
          } catch (InterruptedException e) {
            throw new SFException(ErrorCode.INTERNAL_ERROR, e.getMessage());
          }
        } else {
          break;
        }
      }
    }
    return response;
  }
}
