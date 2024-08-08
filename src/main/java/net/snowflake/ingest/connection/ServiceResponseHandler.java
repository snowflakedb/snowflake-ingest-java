/*
 * Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.connection;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.UUID;
import net.snowflake.client.jdbc.internal.apache.http.HttpEntity;
import net.snowflake.client.jdbc.internal.apache.http.HttpResponse;
import net.snowflake.client.jdbc.internal.apache.http.HttpStatus;
import net.snowflake.client.jdbc.internal.apache.http.StatusLine;
import net.snowflake.client.jdbc.internal.apache.http.client.methods.HttpGet;
import net.snowflake.client.jdbc.internal.apache.http.client.methods.HttpPost;
import net.snowflake.client.jdbc.internal.apache.http.client.methods.HttpUriRequest;
import net.snowflake.client.jdbc.internal.apache.http.impl.client.CloseableHttpClient;
import net.snowflake.client.jdbc.internal.apache.http.util.EntityUtils;
import net.snowflake.ingest.utils.BackOffException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class handles taking the HttpResponses we've gotten back, and producing an appropriate
 * response object for usage
 */
public final class ServiceResponseHandler {
  // Create a logger for this class
  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceResponseHandler.class);

  /**
   * Enums for Snowpipe REST API:
   * https://docs.snowflake.com/en/user-guide/data-load-snowpipe-rest-apis.html Used in
   * handleExceptionalStatus for logging purpose
   */
  public enum ApiName {
    INSERT_FILES("POST"),
    INSERT_REPORT("GET"),
    LOAD_HISTORY_SCAN("GET"),
    STREAMING_OPEN_CHANNEL("POST"),
    STREAMING_DROP_CHANNEL("POST"),
    STREAMING_CHANNEL_STATUS("POST"),
    STREAMING_REGISTER_BLOB("POST"),
    STREAMING_CLIENT_CONFIGURE("POST"),
    STREAMING_CHANNEL_CONFIGURE("POST");
    private final String httpMethod;

    private ApiName(String httpMethod) {
      this.httpMethod = httpMethod;
    }

    public String getHttpMethod() {
      return httpMethod;
    }
  }
  // the object mapper we use for deserialization
  static ObjectMapper mapper = new ObjectMapper();

  // If there are additional properties in the JSON, do NOT fail
  static {
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  }

  /**
   * isStatusOK - Checks if we have a status in the 2xx range
   *
   * @param statusLine the status line containing the code
   * @return whether the status x is in the range [200, 300)
   */
  private static boolean isStatusOK(StatusLine statusLine) {
    // If the status is 200 (OK) or greater but less than 300 (Multiple Choices) we're good
    return statusLine.getStatusCode() >= HttpStatus.SC_OK
        && statusLine.getStatusCode() < HttpStatus.SC_MULTIPLE_CHOICES;
  }

  /**
   * unmarshallIngestResponse Given an HttpResponse object - attempts to deserialize it into an
   * IngestResponse object
   *
   * @param response the HTTPResponse we want to distill into an IngestResponse
   * @param requestId
   * @param httpClient HttpClient for retries
   * @param httpPostForIngestFile HttpRequest for retries
   * @param builder RequestBuilder for retries
   * @return An IngestResponse with all of the parsed out information
   * @throws IOException if our entity is somehow corrupt or we can't get it
   * @throws IngestResponseException - if we have an uncategorized network issue
   * @throws BackOffException - if we have a 503 issue
   */
  public static IngestResponse unmarshallIngestResponse(
      HttpResponse response,
      UUID requestId,
      CloseableHttpClient httpClient,
      HttpPost httpPostForIngestFile,
      RequestBuilder builder)
      throws IOException, IngestResponseException, BackOffException {
    // we can't unmarshall a null response
    if (response == null) {
      LOGGER.warn("Null argument passed to unmarshallIngestResponse");
      throw new IllegalArgumentException();
    }

    // handle the exceptional status code
    response =
        handleExceptionalStatus(
            response, requestId, ApiName.INSERT_FILES, httpClient, httpPostForIngestFile, builder);

    String blob = consumeAndReturnResponseEntityAsString(response.getEntity());

    // Read out the blob entity into a class
    return mapper.readValue(blob, IngestResponse.class);
  }

  /**
   * unmarshallHistoryResponse Given an HttpResponse object - attempts to deserialize it into a
   * HistoryResponse object
   *
   * @param response the HttpResponse object we are trying to deserialize
   * @param requestId
   * @param httpClient HttpClient for retries
   * @param httpGetHistory HttpRequest for retries
   * @param builder RequestBuilder for retries
   * @return a HistoryResponse with all the parsed out information
   * @throws IOException if our entity is somehow corrupt or we can't get it
   * @throws IngestResponseException - if we have an uncategorized network issue
   * @throws BackOffException - if we have a 503 issue
   */
  public static HistoryResponse unmarshallHistoryResponse(
      HttpResponse response,
      UUID requestId,
      CloseableHttpClient httpClient,
      HttpGet httpGetHistory,
      RequestBuilder builder)
      throws IOException, IngestResponseException, BackOffException {
    // we can't unmarshall a null response
    if (response == null) {
      LOGGER.warn("Null response passed to unmarshallHistoryResponse");
      throw new IllegalArgumentException();
    }

    // handle the exceptional status code
    response =
        handleExceptionalStatus(
            response, requestId, ApiName.INSERT_REPORT, httpClient, httpGetHistory, builder);

    String blob = consumeAndReturnResponseEntityAsString(response.getEntity());

    // read out our blob into a pojo
    return mapper.readValue(blob, HistoryResponse.class);
  }

  /**
   * Given an HttpResponse object - attempts to deserialize it into a HistoryRangeResponse
   *
   * @param response the HttpResponse object we are trying to deserialize
   * @param requestId
   * @param httpClient HttpClient for retries
   * @param request HttpRequest for retries
   * @param builder HttpBuilder for retries
   * @return HistoryRangeResponse
   * @throws IOException if our entity is somehow corrupt or we can't get it
   * @throws IngestResponseException - if we have an uncategorized network issue
   * @throws BackOffException - if we have a 503 issue
   */
  public static HistoryRangeResponse unmarshallHistoryRangeResponse(
      HttpResponse response,
      UUID requestId,
      CloseableHttpClient httpClient,
      HttpGet request,
      RequestBuilder builder)
      throws IOException, IngestResponseException, BackOffException {

    // we can't unmarshall a null response
    if (response == null) {
      LOGGER.warn("Null response passed to unmarshallHistoryRangeResponse");
      throw new IllegalArgumentException();
    }

    // handle the exceptional status code
    response =
        handleExceptionalStatus(
            response, requestId, ApiName.LOAD_HISTORY_SCAN, httpClient, request, builder);

    String blob = consumeAndReturnResponseEntityAsString(response.getEntity());
    // read out our blob into a pojo
    return mapper.readValue(blob, HistoryRangeResponse.class);
  }

  /**
   * unmarshallStreamingIngestResponse Given an HttpResponse object - attempts to deserialize it
   * into an Object based on input type
   *
   * @param response http response from server
   * @param valueType the class type
   * @param apiName enum to represent the corresponding api name
   * @return the corresponding response object based on input class type
   * @throws IOException if a low-level I/O problem
   * @throws IngestResponseException if received an exceptional status code
   */
  public static <T> T unmarshallStreamingIngestResponse(
      HttpResponse response,
      Class<T> valueType,
      ApiName apiName,
      CloseableHttpClient httpClient,
      HttpUriRequest request,
      RequestBuilder requestBuilder)
      throws IOException, IngestResponseException {
    // We can't unmarshall a null response
    if (response == null) {
      LOGGER.warn("Null response passed to {}", valueType.getName());
      throw new IllegalArgumentException();
    }

    // Handle the exceptional status code
    response =
        handleExceptionalStatus(response, null, apiName, httpClient, request, requestBuilder);

    // Grab the string version of the response entity
    String blob = consumeAndReturnResponseEntityAsString(response.getEntity());

    // Read out our blob into a pojo
    return mapper.readValue(blob, valueType);
  }

  /**
   * handleExceptionStatusCode - throws the correct error when response status is not OK
   *
   * @param response HttpResponse
   * @param requestId
   * @param apiName enum to represent the corresponding api name
   * @param httpClient HttpClient for retries
   * @param request HttpRequest for retries
   * @param requestBuilder RequestBuilder for retries
   * @return modified HttpResponse
   * @throws IOException if our entity is somehow corrupt or we can't get it
   * @throws IngestResponseException - for all other non OK status
   * @throws BackOffException - if we have a 503 issue
   */
  private static HttpResponse handleExceptionalStatus(
      HttpResponse response,
      UUID requestId,
      ApiName apiName,
      CloseableHttpClient httpClient,
      HttpUriRequest request,
      RequestBuilder requestBuilder)
      throws IOException, IngestResponseException, BackOffException {
    if (!isStatusOK(response.getStatusLine())) {
      StatusLine statusLine = response.getStatusLine();
      LOGGER.warn(
          "{} Status hit from {}, requestId:{}",
          statusLine.getStatusCode(),
          apiName,
          requestId == null ? "" : requestId.toString());

      // Consume the response to release the connection
      String blob = consumeAndReturnResponseEntityAsString(response.getEntity());

      // if we have a 503 exception throw a backoff
      switch (statusLine.getStatusCode()) {
          // If we have a 503, BACKOFF
        case HttpStatus.SC_SERVICE_UNAVAILABLE:
          throw new BackOffException();
        case HttpStatus.SC_UNAUTHORIZED:
          LOGGER.warn("Authorization failed, refreshing Token succeeded, retry");
          requestBuilder.refreshToken();
          requestBuilder.addToken(request);
          response = httpClient.execute(request);
          if (!isStatusOK(response.getStatusLine())) {
            // Consume the response to release the connection
            consumeAndReturnResponseEntityAsString(response.getEntity());
            throw new SecurityException("Authorization failed after retry");
          }
          break;
        default:
          throw new IngestResponseException(
              statusLine.getStatusCode(),
              IngestResponseException.IngestExceptionBody.parseBody(blob));
      }
    }
    return response;
  }

  /**
   * Consumes the HttpEntity as mentioned in <a
   * href="https://hc.apache.org/httpcomponents-client-4.5.x/quickstart.html">HttpClient Docs</a>
   *
   * <p>Also returns the string version of this entity.
   *
   * @param httpResponseEntity the response entity obtained after successfully calling associated
   *     Rest APIs
   * @return String version of this http response which will be later used to deserialize into
   *     respective Response Object
   * @throws IOException if parsing error
   */
  private static String consumeAndReturnResponseEntityAsString(HttpEntity httpResponseEntity)
      throws IOException {
    // grab the string version of the response entity
    String responseEntityAsString = EntityUtils.toString(httpResponseEntity);

    EntityUtils.consumeQuietly(httpResponseEntity);
    return responseEntityAsString;
  }
}
