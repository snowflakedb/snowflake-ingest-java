package net.snowflake.ingest.connection;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/**
 * Exception will capture error message when Snowflake encounters
 * an error during ingest or if trying to retrieve history report/
 * Created by vganesh on 5/20/17.
 */
public class IngestResponseException extends java.lang.Exception
{
  // HTTP error code sent back from Snowflake
  private int errorCode;
  private IngestExceptionBody errorBody;

  public IngestResponseException(int errorCode, IngestExceptionBody body)
  {
    this.errorCode = errorCode;
    this.errorBody = body;
  }

  @Override
  public String toString()
  {
    StringBuilder result = new StringBuilder();
    result.append("\nHTTP Status: ").append(errorCode).append("\n").
            append(errorBody.toString());

    return result.toString();
  }

  /**
   * Response exception REST message body sent back from Snowflake
   */
  public static class IngestExceptionBody
  {
    // Detailed object based information, if available
    public Object data;

    // Error message string sent back from Snowflake
    public String message;

    // Snowflake internal error code
    public String code;

    // Was the operation successful? In most exceptions, this will be false
    public boolean success;

    // If valid json was not received in exception, we will store message as
    // a plain text blob.
    public boolean validJson = true;
    public String messageBlob;

    // POJO constructor for mapper
    public IngestExceptionBody(){}
    // When exception JSON does not match, store message as blob
    public IngestExceptionBody(String blob)
    {
      messageBlob = blob;
      validJson = false;
    }

    //the object mapper we use for deserialization
    private static ObjectMapper mapper = new ObjectMapper();
    public static IngestExceptionBody parseBody(String blob)
            throws IOException
    {
      IngestExceptionBody body;
      try
      {
        body = mapper.readValue(blob,
                            IngestResponseException.IngestExceptionBody.class);
      }
      catch (JsonParseException | JsonMappingException e)
      {
        body = new IngestExceptionBody(blob);
      }

      return body;
    }

    @Override
    public String toString()
    {
      StringBuilder result = new StringBuilder();
      if (validJson)
      {
        result.append("{\n").append("Message: ").append(message).append(",\n")
                .append("Data: ").append(data).append("\n}\n");
      }
      else
      {
        result.append(messageBlob);
      }

      return result.toString();
    }
  }
}
