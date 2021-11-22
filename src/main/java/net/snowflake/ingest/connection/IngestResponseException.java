/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.connection;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;

/**
 * Exception will capture error message when Snowflake encounters an error during ingest or if
 * trying to retrieve history report/ Created by vganesh on 5/20/17.
 */
public class IngestResponseException extends Exception {
  // HTTP error code sent back from Snowflake
  private int errorCode;
  private IngestExceptionBody errorBody;

  IngestResponseException(int errorCode, IngestExceptionBody body) {
    super("HTTP Status: " + errorCode + " ErrorBody: " + body.toString());
    this.errorCode = errorCode;
    this.errorBody = body;
  }

  /**
   * Getter for error code
   *
   * @return int errorCode
   */
  public int getErrorCode() {
    return errorCode;
  }

  /**
   * Getter for error body
   *
   * @return IngestExceptionBody errorBody
   */
  public IngestExceptionBody getErrorBody() {
    return errorBody;
  }

  @Override
  public String toString() {
    return "\nHTTP Status: " + errorCode + "\n" + errorBody.toString();
  }

  /** Response exception REST message body sent back from Snowflake */
  public static class IngestExceptionBody {
    // Detailed object based information, if available
    private Object data;

    // Error message string sent back from Snowflake
    private String message;

    // Snowflake internal error code
    private String code;

    // Was the operation successful? In most exceptions, this will be false
    private boolean success;

    // If valid json was not received in exception, we will store message as
    // a plain text blob.
    boolean validJson = true;
    String messageBlob;

    // Snowflake message headers
    private String headers;

    // POJO constructor for mapper
    public IngestExceptionBody() {}
    // When exception JSON does not match, store message as blob
    IngestExceptionBody(String blob) {
      messageBlob = blob;
      validJson = false;
    }

    // the object mapper we use for deserialization
    private static ObjectMapper mapper = new ObjectMapper();

    static IngestExceptionBody parseBody(String blob) throws IOException {
      IngestExceptionBody body;
      try {
        body = mapper.readValue(blob, IngestExceptionBody.class);
      } catch (JsonParseException | JsonMappingException e) {
        body = new IngestExceptionBody(blob);
      }

      return body;
    }

    @Override
    public String toString() {
      StringBuilder result = new StringBuilder();
      if (validJson) {
        result
            .append("{\n")
            .append("Message: ")
            .append(message)
            .append(",\n")
            .append("Data: ")
            .append(data)
            .append("\n}\n");
      } else {
        result.append(messageBlob);
      }

      return result.toString();
    }

    public Object getData() {
      return data;
    }

    public void setData(Object data) {
      this.data = data;
    }

    public String getMessage() {
      return message;
    }

    public void setMessage(String message) {
      this.message = message;
    }

    public String getCode() {
      return code;
    }

    public void setCode(String code) {
      this.code = code;
    }

    public boolean isSuccess() {
      return success;
    }

    public void setSuccess(boolean success) {
      this.success = success;
    }

    public String getHeaders() {
      return headers;
    }

    public void setHeaders(String headers) {
      this.headers = headers;
    }
  }
}
