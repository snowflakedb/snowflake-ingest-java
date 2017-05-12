package net.snowflake.ingest.connection;

import java.io.Serializable;
import java.util.UUID;

/**
 * IngestResponse - an object which contains a successful
 * response from the service for the insert request
 *
 * @author obabarinsa
 */
public class IngestResponse implements Serializable
{
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
    return requestId;
  }
}
