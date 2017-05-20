package net.snowflake.ingest.connection;

/**
 * Exception will capture error message when Snowflake encounters
 * an error during ingest or if trying to retrieve history report/
 * Created by vganesh on 5/20/17.
 */
public class IngestResponseException extends java.lang.RuntimeException
{
  /**
   * Http Status code seen from the Ingest REST endpoints
   */
  public final int errorCode;

  /**
   * Http body received when an error status code is received
   */
  public final String errorCodeStr;

  public IngestResponseException(int statusCode, String message)
  {
    errorCode = statusCode;
    errorCodeStr = message;
  }
}
