/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.connection;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.UUID;

/**
 * IngestResponse - an object which contains a successful
 * response from the service for the insert request
 *
 * @author obabarinsa
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class IngestResponse
{
  //the requestId given to us by the user
  private String requestId;

  // response from the API
  private String responseCode;

  @Override
  public String toString()
  {
    return "IngestResponse{" +
        "requestId='" + requestId + '\'' +
        ", responseCode='" + responseCode + '\'' +
        '}';
  }

  /**
   * getRequestUUID - the requestId as a UUID
   *
   * @return UUID version of the requestId
   */
  public UUID getRequestUUID()
  {
    return UUID.fromString(requestId);
  }

  public String getRequestId()
  {
    return requestId;
  }

  public String getResponseCode()
  {
    return responseCode;
  }
}
