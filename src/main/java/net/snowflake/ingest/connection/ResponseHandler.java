package net.snowflake.ingest.connection;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * This class handles taking the HttpResponses we've gotten
 * back, and producing an appropriate response object for usage
 */
public class ResponseHandler
{
  //the object mapper we use for deserialization
  protected static ObjectMapper mapper = new ObjectMapper();

  static
  {
    //If there are additional properties in the JSON, do NOT fail
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  }
}
