/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.connection;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.UUID;
import net.snowflake.ingest.utils.BackOffException;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class handles taking the HttpResponses we've gotten back, and producing an appropriate
 * response object for usage
 */
public final class ServiceResponseHandler {
  // Create a logger for this class
  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceResponseHandler.class);

  /** Enum for every public api, used in handleExceptionalStatus for logging purpose */
  private enum ApiName {
    INSERT_FILES("POST"),
    INSERT_REPORT("GET"),
    LOAD_HISTORY_SCAN("GET"),
    CLIENT_CONFIGURE("POST"),
    CLIENT_STATUS("GET");
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
   * @return An IngestResponse with all of the parsed out information
   * @throws IOException if our entity is somehow corrupt or we can't get it
   * @throws IngestResponseException - if we have an uncategorized network issue
   * @throws BackOffException - if we have a 503 issue
   */
  public static IngestResponse unmarshallIngestResponse(HttpResponse response, UUID requestId)
      throws IOException, IngestResponseException, BackOffException {
    // we can't unmarshall a null response
    if (response == null) {
      LOGGER.warn("Null argument passed to unmarshallIngestResponse");
      throw new IllegalArgumentException();
    }

    // handle the exceptional status code
    handleExceptionalStatus(response, requestId, ApiName.INSERT_FILES);

    // grab the response entity
    String blob = EntityUtils.toString(response.getEntity());

    // Read out the blob entity into a class
    return mapper.readValue(blob, IngestResponse.class);
  }

  /**
   * unmarshallHistoryResponse Given an HttpResponse object - attempts to deserialize it into a
   * HistoryResponse object
   *
   * @param response the HttpResponse object we are trying to deserialize
   * @param requestId
   * @return a HistoryResponse with all the parsed out information
   * @throws IOException if our entity is somehow corrupt or we can't get it
   * @throws IngestResponseException - if we have an uncategorized network issue
   * @throws BackOffException - if we have a 503 issue
   */
  public static HistoryResponse unmarshallHistoryResponse(HttpResponse response, UUID requestId)
      throws IOException, IngestResponseException, BackOffException {
    // we can't unmarshall a null response
    if (response == null) {
      LOGGER.warn("Null response passed to unmarshallHistoryResponse");
      throw new IllegalArgumentException();
    }

    // handle the exceptional status code
    handleExceptionalStatus(response, requestId, ApiName.INSERT_REPORT);

    // grab the string version of the response entity
    String blob = EntityUtils.toString(response.getEntity());

    // read out our blob into a pojo
    return mapper.readValue(blob, HistoryResponse.class);
  }

  /**
   * Given an HttpResponse object - attempts to deserialize it into a HistoryRangeResponse
   *
   * @param response the HttpResponse object we are trying to deserialize
   * @param requestId
   * @return HistoryRangeResponse
   * @throws IOException if our entity is somehow corrupt or we can't get it
   * @throws IngestResponseException - if we have an uncategorized network issue
   * @throws BackOffException - if we have a 503 issue
   */
  public static HistoryRangeResponse unmarshallHistoryRangeResponse(
      HttpResponse response, UUID requestId)
      throws IOException, IngestResponseException, BackOffException {

    // we can't unmarshall a null response
    if (response == null) {
      LOGGER.warn("Null response passed to unmarshallHistoryRangeResponse");
      throw new IllegalArgumentException();
    }

    // handle the exceptional status code
    handleExceptionalStatus(response, requestId, ApiName.LOAD_HISTORY_SCAN);

    // grab the string version of the response entity
    String blob = EntityUtils.toString(response.getEntity());

    // read out our blob into a pojo
    return mapper.readValue(blob, HistoryRangeResponse.class);
  }

  /**
   * unmarshallConfigureClientResponse - Given an HttpResponse object, attempts to deserialize it
   * into a ConfigureClientResponse
   *
   * @param response HttpResponse
   * @param requestId
   * @return ConfigureClientResponse
   * @throws IOException if our entity is somehow corrupt or we can't get it
   * @throws IngestResponseException - if we have an uncategorized network issue
   * @throws BackOffException - if we have a 503 issue
   */
  public static ConfigureClientResponse unmarshallConfigureClientResponse(
      HttpResponse response, UUID requestId)
      throws IOException, IngestResponseException, BackOffException {
    if (response == null) {
      LOGGER.warn("Null response passed to unmarshallConfigureClientResponse");
      throw new IllegalArgumentException();
    }

    // handle the exceptional status code
    handleExceptionalStatus(response, requestId, ApiName.CLIENT_CONFIGURE);

    // grab the string version of the response entity
    String blob = EntityUtils.toString(response.getEntity());

    // read out our blob into a pojo
    return mapper.readValue(blob, ConfigureClientResponse.class);
  }

  /**
   * unmarshallGetClientStatus - Given an HttpResponse object, attempts to deserialize it into a
   * ClientStatusResponse
   *
   * @param response HttpResponse
   * @param requestId
   * @return ClientStatusResponse
   * @throws IOException if our entity is somehow corrupt or we can't get it
   * @throws IngestResponseException - if we have an uncategorized network issue
   * @throws BackOffException - if we have a 503 issue
   */
  public static ClientStatusResponse unmarshallGetClientStatus(
      HttpResponse response, UUID requestId)
      throws IOException, IngestResponseException, BackOffException {
    if (response == null) {
      LOGGER.warn("Null response passed to unmarshallClientStatusResponse");
      throw new IllegalArgumentException();
    }

    // handle the exceptional status code
    handleExceptionalStatus(response, requestId, ApiName.CLIENT_STATUS);

    // grab the string version of the response entity
    String blob = EntityUtils.toString(response.getEntity());

    // read out our blob into a pojo
    return mapper.readValue(blob, ClientStatusResponse.class);
  }

  /**
   * handleExceptionStatusCode - throws the correct error when response status is not OK
   *
   * @param response HttpResponse
   * @param requestId
   * @throws IOException if our entity is somehow corrupt or we can't get it
   * @throws IngestResponseException - for all other non OK status
   * @throws BackOffException - if we have a 503 issue
   */
  private static void handleExceptionalStatus(
      HttpResponse response, UUID requestId, ApiName apiName)
      throws IOException, IngestResponseException, BackOffException {
    StatusLine statusLine = response.getStatusLine();
    if (!isStatusOK(statusLine)) {
      LOGGER.error(
          "Exceptional Status Code from {}: {}, requestId:{}",
          apiName,
          statusLine.getStatusCode(),
          requestId.toString());
      // if we have a 503 exception throw a backoff
      switch (statusLine.getStatusCode()) {
          // If we have a 503, BACKOFF
        case HttpStatus.SC_SERVICE_UNAVAILABLE:
          LOGGER.warn("503 Status hit, backoff");
          throw new BackOffException();

        default:
          String blob = EntityUtils.toString(response.getEntity());
          throw new IngestResponseException(
              statusLine.getStatusCode(),
              IngestResponseException.IngestExceptionBody.parseBody(blob));
      }
    }
  }
}
