package net.snowflake.ingest.connection;

/**
 * Given a users credentials, this class handles submitting and updating
 * @author obabarinsa
 */
public class RequestManager
{

  //the maximum number of files we'll submit in one request
  public static int MAX_FILES_PER_REQUEST = 1024;
}
