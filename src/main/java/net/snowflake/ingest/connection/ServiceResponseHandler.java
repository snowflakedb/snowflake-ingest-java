package net.snowflake.ingest.connection;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.snowflake.ingest.utils.BackOffException;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * This class handles taking the HttpResponses we've gotten
 * back, and producing an appropriate response object for usage
 */
public final class ServiceResponseHandler
{
  //Create a logger for this class
  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceResponseHandler.class);

  //the object mapper we use for deserialization
  static ObjectMapper mapper = new ObjectMapper();


  //If there are additional properties in the JSON, do NOT fail
  static
  {
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  }


  /**
   * isStatusOK - Checks if we have a status in the 2xx range
   *
   * @param statusLine - the status line containing the code
   * @return whether the status x is in the range [200, 300)
   */

  private static boolean isStatusOK(StatusLine statusLine)
  {
    //If the status is 200 (OK) or greater but less than 300 (Multiple Choices) we're good
    return statusLine.getStatusCode() >= HttpStatus.SC_OK
        && statusLine.getStatusCode() < HttpStatus.SC_MULTIPLE_CHOICES;
  }

  /**
   * unmarshallIngestResponse
   * Given an HttpResponse object - attempts to deserialize it into
   * an IngestResponse object
   *
   * @param response the HTTPResponse we want to distill into an IngestResponse
   * @return An IngestResponse with all of the parsed out information
   * @throws IOException      - if our entity is somehow corrupt or we can't get it
   * @throws BackOffException if we have a 503 response
   */
  public static IngestResponse unmarshallIngestResponse(HttpResponse response)
      throws IOException, IngestResponseException
  {
    //we can't unmarshall a null response
    if (response == null)
    {
      LOGGER.warn("Null argument passed to unmarshallIngestResponse");
      throw new IllegalArgumentException();
    }

    //Grab the status line from the response
    StatusLine statusLine = response.getStatusLine();

    //If we didn't get a good status code, handle it
    if (!isStatusOK(statusLine))
    {

      //Exception status
      LOGGER.warn("Exceptional Status Code found in unmarshallInsert Response  - {}",
          statusLine.getStatusCode());

      handleExceptionalStatus(statusLine, response);
      return null;
    }

    //grab the response entity
    String blob = EntityUtils.toString(response.getEntity());

    //Read out the blob entity into a class
    return mapper.readValue(blob, IngestResponse.class);
  }


  /**
   * unmarshallHistoryResponse
   * Given an HttpResponse object - attempts to deserialize it into
   * a HistoryResponse object
   *
   * @param response the HttpResponse object we are trying to deserialize
   * @return a HistoryResponse with all the parsed out information
   * @throws IOException      - if we have an uncategorized network issue
   * @throws BackOffException - if have a 503 issue
   */
  public static HistoryResponse unmarshallHistoryResponse(HttpResponse response)
      throws IOException, IngestResponseException
  {
    //we can't unmarshall a null response
    if (response == null)
    {
      LOGGER.warn("Null response passed to unmarshallHistoryResponse");
      throw new IllegalArgumentException();
    }

    //Grab the status line
    StatusLine line = response.getStatusLine();

    if (!isStatusOK(line))
    {
      //A network issue occurred!
      LOGGER.warn("Exceptional Status Code found in unmarshallHistoryResponse - {}",
          line.getStatusCode());

      //handle the exceptional status code
      handleExceptionalStatus(line, response);
      return null;
    }

    //grab the string version of the response entity
    String blob = EntityUtils.toString(response.getEntity());


    //read out our blob into a pojo
    return mapper.readValue(blob, HistoryResponse.class);

  }

  public static HistoryRangeResponse unmarshallHistoryRangeResponse(
          HttpResponse response)
          throws IOException, IngestResponseException
  {

    //we can't unmarshall a null response
    if (response == null)
    {
      LOGGER.warn("Null response passed to unmarshallHistoryRangeResponse");
      throw new IllegalArgumentException();
    }

    //Grab the status line
    StatusLine line = response.getStatusLine();
    if (!isStatusOK(line))
    {
      //A network issue occurred!
      LOGGER.warn("Exceptional Status Code found in " +
                          "unmarshallHistoryRangeResponse - {}",
                  line.getStatusCode());

      //handle the exceptional status code
      handleExceptionalStatus(line, response);
      return null;
    }

    //grab the string version of the response entity
    String blob = EntityUtils.toString(response.getEntity());

    //read out our blob into a pojo
    return mapper.readValue(blob, HistoryRangeResponse.class);
  }


  /**
   * handleExceptionStatusCode - throws the correct error for a status
   *
   * @param statusLine the status line we want to check
   * @throws BackOffException -- if we have a 503 exception
   * @throws IOException      - if we don't know what it is
   */
  private static void handleExceptionalStatus(StatusLine statusLine,
                                              HttpResponse response)
                      throws IOException, IngestResponseException
  {
    //if we have a 503 exception throw a backoff
    switch (statusLine.getStatusCode())
    {
      //If we have a 503, BACKOFF
      case HttpStatus.SC_SERVICE_UNAVAILABLE:
        LOGGER.warn("503 Status hit, backoff");
        throw new BackOffException();

        //We don't know how to respond now...
      default:
        LOGGER.error("Status code {} found in response from service",
                     statusLine.getStatusCode());
        String blob = EntityUtils.toString(response.getEntity());
        throw new IngestResponseException(statusLine.getStatusCode(),
                                          IngestResponseException
                                          .IngestExceptionBody.parseBody(blob));
    }

  }
}
