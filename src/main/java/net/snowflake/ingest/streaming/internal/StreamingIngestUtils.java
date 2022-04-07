package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.utils.Constants.RESPONSE_ERR_ENQUEUE_TABLE_CHUNK_QUEUE_FULL;
import static net.snowflake.ingest.utils.Constants.RESPONSE_ERR_GENERAL_EXCEPTION_RETRY_REQUEST;
import static net.snowflake.ingest.utils.Constants.RESPONSE_SUCCESS;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Map;
import net.snowflake.ingest.connection.IngestResponseException;
import net.snowflake.ingest.connection.RequestBuilder;
import net.snowflake.ingest.connection.ServiceResponseHandler;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;
import org.apache.http.client.HttpClient;

public class StreamingIngestUtils {

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
    T response =
        ServiceResponseHandler.unmarshallStreamingIngestResponse(
            httpClient.execute(
                requestBuilder.generateStreamingIngestPostRequest(
                    payload, endpoint, "open channel")),
            targetClass,
            apiName);

    // Check for Snowflake specific response code
    if (response.getStatusCode() != RESPONSE_SUCCESS) {
      if (response.getStatusCode() == RESPONSE_ERR_GENERAL_EXCEPTION_RETRY_REQUEST
          || response.getStatusCode() == RESPONSE_ERR_ENQUEUE_TABLE_CHUNK_QUEUE_FULL) {
        int retries = 0;
        while (retries < 3) {
          response =
              ServiceResponseHandler.unmarshallStreamingIngestResponse(
                  httpClient.execute(
                      requestBuilder.generateStreamingIngestPostRequest(
                          payload, endpoint, message)),
                  targetClass,
                  apiName);
          if (response.getStatusCode() == RESPONSE_ERR_GENERAL_EXCEPTION_RETRY_REQUEST
              || response.getStatusCode() == RESPONSE_ERR_ENQUEUE_TABLE_CHUNK_QUEUE_FULL) {
            retries++;
            try {
              Thread.sleep(500 * retries);
            } catch (InterruptedException e) {
              throw new SFException(ErrorCode.INTERNAL_ERROR, e.getMessage());
            }
          } else {
            break;
          }
        }
      }
    }
    return response;
  }
}
