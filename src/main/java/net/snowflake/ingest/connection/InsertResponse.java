package net.snowflake.ingest.connection;

import java.util.UUID;

/**
 * InsertResponse - an object which contains a successful
 * response from the service for the insert request
 * @author obabarinsa
 */
public class InsertResponse
{
  /**
   * Status - this represents the different responses
   * that the service can return to us
   * @author
   */
  public static enum Response
  {
    SUCCESS, //the files have been succesfully registered to load
    ALREADY_SENT, //we've already sent all of these files
    TABLE_NOT_FOUND, //there is no table into which to load
    STAGE_NOT_FOUND, //there is no stage from which to load
    INTERNAL_ERROR //the service is borked!
  }

  //the response we got back from the service
  //NOTE: This is NOT the HTTP response code


  //the requestId given to us by the user
  public String requestId;

  /**
   * getRequestUUID - the requestId as a UUID
   * @returns UUID version of the requestId
   */
  public UUID getRequestUUID()
  {
    return UUID.fromString(requestId);
  }

  //the response code we got back from the service
  public Response responseCode;
}
