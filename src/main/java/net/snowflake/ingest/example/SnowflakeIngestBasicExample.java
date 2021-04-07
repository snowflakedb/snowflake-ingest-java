/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.example;

import java.io.IOException;
import java.security.KeyPair;
import java.sql.Connection;
import java.time.Instant;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import net.snowflake.ingest.SimpleIngestManager;
import net.snowflake.ingest.connection.HistoryRangeResponse;
import net.snowflake.ingest.connection.HistoryResponse;
import net.snowflake.ingest.connection.IngestResponse;
import net.snowflake.ingest.connection.IngestResponseException;
import net.snowflake.ingest.utils.StagedFileWrapper;

/**
 * This sample demonstrates how to make basic requests to the Snowflake Ingest service for Java.
 *
 * <p><b>Prerequisites:</b> You must have a valid set of Snowflake credentials
 */
public class SnowflakeIngestBasicExample {
  // Details required to connect to Snowflake over jdbc and establish
  // pre-requesites
  private static String account = "s3testaccount";
  private static String user = "snowman";
  private static String host = "s3testaccount.snowflakecomputing.com";
  private static String scheme = "https";
  private static String password = "****";
  private static int port = 8080;

  // Details for the pipe which we are going to use
  // the name of our target DB
  private static String database = "testdb";
  // the name of our target schema
  private static String schema = "public";
  // the name of our stage
  private static String stage = "ingest_stage";
  // the name of our target table
  private static String table = "ingest_table";
  // the name of our pipe
  private static String pipe = "ingest_pipe";

  // the connection we will use for queries
  private static Connection conn;

  // the Administrative connection
  // the fully qualified pipe name
  private static String fqPipe = database + "." + schema + "." + pipe;

  // the actual ingest manager
  private static SimpleIngestManager manager;

  // our keypair
  private static KeyPair keypair;

  /** Creates the stages and files we'll use for this test */
  private static void setup(Set<String> files, String filesLocation) throws Exception {
    // use the right database
    IngestExampleHelper.doQuery(conn, "use database " + database);

    // use the right schema
    IngestExampleHelper.doQuery(conn, "use schema " + schema);

    // create the target stage
    IngestExampleHelper.doQuery(
        conn, "create or replace stage " + stage + " FILE_FORMAT=(type='csv' COMPRESSION=NONE)");

    // create the target
    IngestExampleHelper.doQuery(
        conn,
        "create or replace table " + table + " (row_id int, row_str string, num int, src string)");
    // Create the pipe for subsequently ingesting files to.
    IngestExampleHelper.doQuery(
        conn,
        "create or replace pipe "
            + pipe
            + " as copy into "
            + table
            + " from @"
            + stage
            + " file_format=(type='csv')");

    String pk = IngestExampleHelper.getPublicKeyString(keypair);

    // assume the necessary privileges
    IngestExampleHelper.doQuery(conn, "use role accountadmin");

    // set the public key
    IngestExampleHelper.doQuery(conn, "alter user " + user + " set RSA_PUBLIC_KEY='" + pk + "'");
    files.forEach(
        file -> {
          IngestExampleHelper.doQuery(
              conn, "PUT " + filesLocation + file + " @" + stage + " AUTO_COMPRESS=FALSE");
        });

    IngestExampleHelper.doQuery(conn, "use role sysadmin");
  }

  /**
   * Ingest a single file from the staging are using ingest service
   *
   * @param filename
   * @return
   * @throws Exception
   */
  private static IngestResponse insertFile(String filename) throws Exception {
    // create a file wrapper
    StagedFileWrapper myFile = new StagedFileWrapper(filename, null);
    return manager.ingestFile(myFile, null);
  }

  /**
   * Ingest a set of files from the staging area through the ingest service
   *
   * @param files
   * @return
   * @throws Exception
   */
  private static IngestResponse insertFiles(Set<String> files) throws Exception {
    return manager.ingestFiles(manager.wrapFilepaths(files), null);
  }

  /**
   * Given a set of files, keep querying history until all of files that are in the set have been
   * seen in the history. Times out after 2 minutes
   *
   * @param files
   * @return HistoryResponse containing only the FileEntries corresponding to requested input
   * @throws Exception
   */
  private static HistoryResponse waitForFilesHistory(Set<String> files) throws Exception {
    ExecutorService service = Executors.newSingleThreadExecutor();

    class GetHistory implements Callable<HistoryResponse> {
      private Set<String> filesWatchList;

      GetHistory(Set<String> files) {
        this.filesWatchList = files;
      }

      String beginMark = null;

      public HistoryResponse call() throws Exception {
        HistoryResponse filesHistory = null;
        while (true) {
          Thread.sleep(500);
          HistoryResponse response = manager.getHistory(null, null, beginMark);
          if (response.getNextBeginMark() != null) {
            beginMark = response.getNextBeginMark();
          }
          if (response != null && response.files != null) {
            for (HistoryResponse.FileEntry entry : response.files) {
              // if we have a complete file that we've
              // loaded with the same name..
              String filename = entry.getPath();
              if (entry.getPath() != null
                  && entry.isComplete()
                  && filesWatchList.contains(filename)) {
                if (filesHistory == null) {
                  filesHistory = new HistoryResponse();
                  filesHistory.setPipe(response.getPipe());
                }
                filesHistory.files.add(entry);
                filesWatchList.remove(filename);
                // we can return true!
                if (filesWatchList.isEmpty()) {
                  return filesHistory;
                }
              }
            }
          }
        }
      }
    }

    GetHistory historyCaller = new GetHistory(files);
    // fork off waiting for a load to the service
    Future<HistoryResponse> result = service.submit(historyCaller);

    HistoryResponse response = result.get(2, TimeUnit.MINUTES);
    return response;
  }

  public static void main(String[] args) throws IOException {
    final String filesDirectory = "/tmp/data/";
    final String filesLocationUrl = "file:///tmp/data/";
    // The first few steps simulate example files that we will write to the
    // ingest service.
    // base file name we want to load
    final String fileWithWrongCSVFormat = "letters.csv";
    IngestExampleHelper.makeLocalDirectory(filesDirectory);
    IngestExampleHelper.makeSampleFile(filesDirectory, fileWithWrongCSVFormat);
    Set<String> files = new TreeSet<>();
    files.add(
        IngestExampleHelper.createTempCsv(filesDirectory, "sample", 10).getFileName().toString());
    files.add(
        IngestExampleHelper.createTempCsv(filesDirectory, "sample", 15).getFileName().toString());
    files.add(
        IngestExampleHelper.createTempCsv(filesDirectory, "sample", 20).getFileName().toString());
    System.out.println("Starting snowflake ingest client");
    System.out.println(
        "Connecting to "
            + scheme
            + "://"
            + host
            + ":"
            + port
            + "\n with Account:"
            + account
            + ", User: "
            + user);

    try {
      final long oneHourMillis = 1000 * 3600l;
      String startTime =
          Instant.ofEpochMilli(System.currentTimeMillis() - 4 * oneHourMillis).toString();
      conn = IngestExampleHelper.getConnection(user, password, account, host, port);
      keypair = IngestExampleHelper.generateKeyPair();
      manager = new SimpleIngestManager(account, user, fqPipe, keypair, scheme, host, port);
      files.add(fileWithWrongCSVFormat);
      setup(files, filesLocationUrl);
      IngestResponse response = insertFiles(files);
      System.out.println("Received ingest response: " + response.toString());
      response = insertFile(fileWithWrongCSVFormat);
      System.out.println("Received ingest response: " + response.toString());
      // This will show up as a file that was not loaded in history
      HistoryResponse history = waitForFilesHistory(files);
      System.out.println("Received history response: " + history.toString());
      String endTime = Instant.ofEpochMilli(System.currentTimeMillis()).toString();

      HistoryRangeResponse historyRangeResponse = manager.getHistoryRange(null, startTime, endTime);

      System.out.println("Received history range response: " + historyRangeResponse.toString());
    } catch (IngestResponseException e) {
      System.out.println("Service exception: " + e.toString());
    } catch (Exception e) {
      System.out.println(e);
    }
  }
}
