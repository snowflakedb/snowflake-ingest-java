package net.snowflake.ingest.connection;

import java.util.UUID;

/**
 * IngestResponse - an object which contains a successful
 * response from the service for the insert request
 *
 * @author obabarinsa
 */
public class IngestResponse
{
  /**
   * Response - this represents the different responses
   * that the service can return to us
   *
   * @author obabarinsa
   */
  public static enum Response
  {
    SUCCESS, //the files have been succesfully registered to load
  }

  //the response we got back from the service
  //NOTE: This is NOT the HTTP response code

  //the requestId given to us by the user
  public String requestId;

  /**
   * getRequestUUID - the requestId as a UUID
   *
   * @return UUID version of the requestId
   */
  public UUID getRequestUUID()
  {
    return UUID.fromString(requestId);
  }
  @Override
  public String toString()
  {
    return String.valueOf(responseCode) + " " + requestId;
  }

  //the response code we got back from the service
  public Response responseCode;
}
