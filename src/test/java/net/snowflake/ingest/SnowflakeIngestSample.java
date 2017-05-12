package net.snowflake.ingest;

import net.snowflake.ingest.connection.HistoryResponse;
import net.snowflake.ingest.connection.IngestResponse;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

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
public class SnowflakeIngestSample
{

  /**
   * Create directories if they don't exist under directoryPath.
   * @param directoryPath
   * @throws IOException
   */
  private static void makeLocalDirectory(String directoryPath)
          throws IOException
  {
    Files.createDirectories(Paths.get(directoryPath));
  }

  private static Path createTempJson(String directoryPath, String filename, int rows)
          throws IOException
  {
    final ThreadLocalRandom rnd = ThreadLocalRandom.current();

    int content_cnt = 2;

    final Path json = Files.createTempFile(Paths.get(directoryPath),
                                           filename, ".json");

    try (Writer w = Files.newBufferedWriter(json, StandardCharsets.UTF_8))
    {
      for (int i = 0; i < rows; i++)
      {

        String[] contents = new String[content_cnt];

        for (int j = 0; j < content_cnt; j++)
        {
          contents[j] = "\"row-" + String.valueOf(i*content_cnt + j) + "\":\""
            + String.valueOf(rnd.nextLong()) + "\"";
        }

        w.write("{" + String.join(",",contents) + "}");
        w.write("\n");
      }
    }
    return json;
  }

  private static Path createTempCsv(String directoryPath,
                                    String filename,
                                    int rows) throws IOException
  {
    final ThreadLocalRandom rnd = ThreadLocalRandom.current();

    final Path csv = Files.createTempFile(Paths.get(directoryPath),
                                          filename, ".csv");
    final String fileName = csv.getFileName().toString();

    try (Writer w = Files.newBufferedWriter(csv, StandardCharsets.UTF_8))
    {
      for (int i = 0; i < rows; i++)
      {
        w.write(String.valueOf(i));
        w.write(",");
        w.write("row-");
        w.write(String.valueOf(i));
        w.write(",");
        w.write(String.valueOf(rnd.nextLong()));
        w.write(",");
        w.write(fileName);
        w.write("\n");
      }
    }
    return csv;
  }


  /**
   * Create file under directoryPath with fileName and
   * populate it with test data
   * @param directoryPath
   * @param filename
   * @throws IOException
   */
  private static void makeSampleFile(String directoryPath, String filename)
          throws IOException
  {
    File file = new File(directoryPath, filename);

    //if our file doesn't already exist
    if (!file.exists())
    {
      //create it
      file.createNewFile();

      //populate it with some data
      FileWriter fw = new FileWriter(file.getAbsoluteFile());
      BufferedWriter bw = new BufferedWriter(fw);
      for (char letter = 'a'; letter <= 'z'; letter++)
      {
        bw.write(letter + "\n");
      }
      //close it back up
      bw.close();
    }

    return;
  }
    /**
   * Creates the stages and files we'll use for this test
   */
  private static void setup(SnowflakeIngestClient client,
                            SnowflakeConfig config,
                            SnowflakePipeConfig pipeConfig)
          throws Exception
  {
    //use the right database
    client.doQuery("use database " + pipeConfig.Database());

    //use the right schema
    client.doQuery("use schema " + pipeConfig.Schema());

    //create the target stage
    client.doQuery("create or replace stage "
                           + client.quote(pipeConfig.Stage())
                           + " url='file:///tmp/data/'");

    //create the target
    client.doQuery("create or replace table "
                           + client.quote(pipeConfig.Table())
                           + " (row_id int, row_str string, num int, src string)");

    // Create the pipe for subsequently ingesting files to.
    client.doQuery("create or replace pipe "
                           + client.quote(pipeConfig.Name()) +
                           " as copy into " + client.quote(pipeConfig.Table())
                           + " from @" + client.quote(pipeConfig.Stage())
                           + " file_format=(type='csv')");

    String pk = client.getPublicKeyString();

    //assume the necessary privileges
    client.doQuery("use role accountadmin");

    //set the public key
    client.doQuery("alter user " + config.User() +
            " set RSA_PUBLIC_KEY='" + pk + "'");

    client.doQuery("use role sysadmin");
  }

  public static HistoryResponse sleepAndFetchHistory(SnowflakeIngestClient client)
  {
    try
    {
      Thread.sleep(500);
      return client.insertHistory();
    } catch (Exception e)
    {
      return null;
    }
  }

  /**
   * Given a set of files, keep querying history until all of files that are
   * in the set have been seen in the history. Times out after 2 minutes
   * @param client
   * @param files
   * @return HistoryResponse containing only the FileEntries corresponding to
   *          requested input
   * @throws Exception
   */
  public static HistoryResponse waitForFilesHistory(SnowflakeIngestClient client,
                                             Set<String> files)
                                          throws Exception
  {
    ExecutorService service = Executors.newSingleThreadExecutor();

    class GetHistory implements
            Callable<HistoryResponse>
    {
      private Set<String> filesWatchList;
      private final SnowflakeIngestClient client;
      GetHistory(Set<String> files, SnowflakeIngestClient client)
      {
        this.filesWatchList = files;
        this.client = client;
      }

      public HistoryResponse call()
              throws Exception
      {
        HistoryResponse filesHistory = new HistoryResponse();
        while (true)
        {
          HistoryResponse response = sleepAndFetchHistory(client);

          if (response != null && response.files != null)
          {
            for (HistoryResponse.FileEntry entry : response.files)
            {
              String filename = entry.path;
              //if we have a complete file that we've
              // loaded with the same name..
              if (entry.path != null && entry.complete &&
                      filesWatchList.contains(filename))
              {
                filesHistory.files.add(entry);
                filesWatchList.remove(filename);
                //we can return true!
                if (filesWatchList.isEmpty()) {
                  return filesHistory;
                }
              }
            }
          }
        }
      }
    }

    GetHistory historyCaller = new GetHistory(files, client);
    //fork off waiting for a load to the service
    Future<HistoryResponse> result = service.submit(historyCaller);

    HistoryResponse response = result.get(2, TimeUnit.MINUTES);
    return response;
  }

  public static void main(String[] args) throws IOException
  {
    //base file name we want to load
    final String fileWithWrongCSVFormat = "letters.csv";
    final String jsonFileName = "sample";

    makeLocalDirectory("/tmp/data");
    makeSampleFile("/tmp/data", fileWithWrongCSVFormat);
    Set<String> files = new TreeSet<>();
    files.add(createTempCsv("/tmp/data", "sample", 10).getFileName().toString());
    files.add(createTempCsv("/tmp/data", "sample", 15).getFileName().toString());
    files.add(createTempCsv("/tmp/data", "sample", 20).getFileName().toString());
    System.out.println("Starting snowflake ingest client");
    SnowflakeConfig config = null;
    SnowflakePipeConfig pipeConfig = null;

    // All the required parameters needed to talk to the ingest service
    {
      String account = "testaccount";
      String user = "snowman";
      String host = "localhost";
      String scheme = "http";
      String port = "8080";
      String password = "test";
      config = new SnowflakeConfig(account, user, host, scheme, port, password);

      //the name of our target DB
      String database = "testdb";
      //the name of our target schema
      String schema = "public";
      //the name of our stage
      String stage = "ingest_stage";
      //the name of our target table
      String table = "ingest_table";
      //the name of our pipe
      String pipe = "ingest_pipe";
      pipeConfig = new SnowflakePipeConfig(database, schema, stage,
                                           table, pipe);
    }

    System.out.println("Connecting to " + config.Scheme() + "://" +
                      config.Host() + ":" + config.Port() +
                      "\n with Account:" + config.Account() +
                      ", User: " + config.User());


    try
    {
      SnowflakeIngestClient client = new SnowflakeIngestClient(
                                                config, pipeConfig, false);
      setup(client, config, pipeConfig);
      IngestResponse response = client.insertFiles(files);
      System.out.println("Received ingest response: " + response.toString());
      response = client.insertFile(fileWithWrongCSVFormat);
      System.out.println("Received ingest response: " + response.toString());
      // This will show up as a file that was not loaded in history
      files.add(fileWithWrongCSVFormat);
      HistoryResponse history = waitForFilesHistory(client, files);
      System.out.println("Received history response: " + history.toString());
    }
    catch(Exception e)
    {
      System.out.println(e);

    }
  }
}
