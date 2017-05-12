package net.snowflake.ingest;

import java.io.IOException;
import net.snowflake.ingest.SnowIngestClient;
import net.snowflake.ingest.SnowConfig;

/**
 * This sample demonstrates how to make basic requests to the Snowflake
 * Ingest service for Java.
 * <p>
 * <b>Prerequisites:</b>
 * You must have a valid set of Snowflake credentials
 * <p>
 * Fill in your Snowflake access config in the provided config file
 * template, and be sure to move the file to the default location
 * (~/.snow/config) where the sample code will load the config from.
 * <p>
 */
public class SnowSample
{

  public static void main(String[] args) throws IOException
  {
    System.out.println("Starting snowflake ingest client");
    SnowConfig config = null;
    try
    {
      config = new SnowConfig();
    }
    catch (Exception e)
    {
      throw new RuntimeException(
              "Could not load snowflake config from the config file. " +
                      "Please make sure that ~/.snow/config file is valid", e);
    }

    System.out.println("Connecting to " + config.Scheme() + "://" +
                      config.Host() + ":" + config.Port() +
                      "\n with Account:" + config.Account() +
                      ", User: " + config.User());

  //the name of our target DB
  final String DATABASE = "testdb";

  //the name of our target schema
  final String SCHEMA = "public";

  //the name of our stage
  final String STAGE = "ingest_stage";

  //the name of our target table
  final String TABLE = "ingest_table";

  //the name of our pipe
  final String PIPE = "ingest_pipe";
  //base file name we want to load
  final String BASE_FILENAME = "letters.csv";


    try
    {
      SnowIngestClient client = new SnowIngestClient(config, DATABASE, SCHEMA,
              STAGE, TABLE, PIPE);
      client.loadSingle(BASE_FILENAME);
    }
    catch(Exception e)
    {

    }
  }
}
