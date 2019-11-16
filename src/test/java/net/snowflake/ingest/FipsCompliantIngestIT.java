package net.snowflake.ingest;

import net.snowflake.ingest.connection.HistoryRangeResponse;
import net.snowflake.ingest.connection.HistoryResponse;
import net.snowflake.ingest.connection.IngestResponse;
import net.snowflake.ingest.connection.IngestResponseException;
import net.snowflake.ingest.connection.IngestStatus;
import net.snowflake.ingest.example.IngestExampleHelper;
import net.snowflake.ingest.fips.BouncyCastleFIPSInit;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.security.InvalidAlgorithmParameterException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.spec.RSAKeyGenParameterSpec;
import java.sql.Connection;
import java.time.Instant;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * This test is specifically testing REST apis with FIPS compliance mode
 * enabled.
 * <p>
 * <b>Prerequisites:</b>
 * You must have a valid set of Snowflake credentials
 * We use encrypted key pair so follow the instructions written on the
 * documentation and also in readme of this repo.
 */
public class FipsCompliantIngestIT
{
  // Details required to connect to Snowflake over jdbc and establish
  // pre-requesites
  private static final String ACCOUNT = "s3testaccount";
  private static final String USER = "username";
  private static final String HOST = "s3testaccount.snowflake.com";
  private static final String SCHEME = "http";
  private static final String PASSWORD = "***";
  private static final String ALGORITHM = "RSA";
  private static final int PORT = 8082;

  // Details for the pipe which we are going to use
  //the name of our target DB
  private static final String DATABASE = "testdb";
  //the name of our target schema
  private static final String SCHEMA = "public";
  //the name of our stage
  private static final String STAGE = "ingest_stage";
  //the name of our target table
  private static final String TABLE = "ingest_table";
  //the name of our pipe
  private static final String PIPE = "ingest_pipe";

  //the connection we will use for queries
  private static Connection conn;

  //the Administrative connection
  //the fully qualified pipe name
  private static final String FQ_PIPE = DATABASE + "." + SCHEMA + "." + PIPE;

  //the actual ingest manager
  private static SimpleIngestManager manager;

  //our keypair
  private static KeyPair keypair;

  // Set of files to ingest
  private static Set<String> files = new TreeSet<>();

  /**
   * Creates the stages and files we'll use for this test
   */
  private static void setup(Set<String> files, String filesLocation)
      throws Exception
  {
    //use the right database
    IngestExampleHelper.doQuery(conn, "use database " + DATABASE);

    //use the right schema
    IngestExampleHelper.doQuery(conn, "use schema " + SCHEMA);

    //create the target stage
    IngestExampleHelper.doQuery(conn, "create or replace stage " + STAGE +
        " FILE_FORMAT=(type='csv' COMPRESSION=NONE)");

    //create the target
    IngestExampleHelper.doQuery(conn, "create or replace table "
        + TABLE
        + " (row_id int, row_str string, num int, src string)");
    // Create the pipe for subsequently ingesting files to.
    IngestExampleHelper.doQuery(conn, "create or replace pipe "
        + PIPE + " as copy into " + TABLE
        + " from @" + STAGE
        + " file_format=(type='csv')");

    String publicKey = IngestExampleHelper.getPublicKeyString(keypair);

    //assume the necessary privileges
    IngestExampleHelper.doQuery(conn, "use role accountadmin");

    //set the public key
    IngestExampleHelper.doQuery(conn, "alter user " + USER +
        " set RSA_PUBLIC_KEY='" + publicKey + "'");
    files.forEach(file->
                  { IngestExampleHelper.doQuery(conn, "PUT " + filesLocation
                      + file + " @" + STAGE +
                      " AUTO_COMPRESS=FALSE");});

    IngestExampleHelper.doQuery(conn, "use role sysadmin");
  }

  /**
   * Generates an RSA keypair for use in this test, with fips provider.
   *
   * @return a valid RSA keypair
   * @throws NoSuchAlgorithmException if we don't have an RSA algo
   * @throws NoSuchProviderException  if we can't use SHA1PRNG for randomization
   */
  public static KeyPair generateKeyPairWithBCFipsProvider()
      throws NoSuchProviderException, NoSuchAlgorithmException,
             InvalidAlgorithmParameterException
  {
    KeyPairGenerator keyPair = KeyPairGenerator.getInstance(ALGORITHM, "BCFIPS");
    keyPair.initialize(new RSAKeyGenParameterSpec(2048,
                                                  RSAKeyGenParameterSpec.F4));
    return keyPair.generateKeyPair();
  }

  /**
   * Ingest a set of files from the staging area through the ingest service
   * @param files
   * @return
   * @throws Exception
   */
  private static IngestResponse insertFiles(Set<String> files)
      throws Exception
  {
    return manager.ingestFiles(manager.wrapFilepaths(files), null);
  }

  /**
   * Given a set of files, keep querying history until all of files that are
   * in the set have been seen in the history. Times out after 2 minutes
   * @param files
   * @return HistoryResponse containing only the FileEntries corresponding to
   *          requested input
   * @throws Exception
   */
  private static HistoryResponse waitForFilesHistory(Set<String> files)
      throws Exception
  {
    ExecutorService service = Executors.newSingleThreadExecutor();

    class GetHistory implements
                     Callable<HistoryResponse>
    {
      private Set<String> filesWatchList;
      GetHistory(Set<String> files)
      {
        this.filesWatchList = files;
      }
      String beginMark = null;

      public HistoryResponse call()
          throws Exception
      {
        HistoryResponse filesHistory = null;
        while (true)
        {
          Thread.sleep(500);
          HistoryResponse response = manager.getHistory(null, null, beginMark);
          if (response.getNextBeginMark() != null)
          {
            beginMark = response.getNextBeginMark();
          }
          if (response != null && response.files != null)
          {
            for (HistoryResponse.FileEntry entry : response.files)
            {
              //if we have a complete file that we've
              // loaded with the same name..
              String filename = entry.getPath();
              if (entry.getPath() != null && entry.isComplete() &&
                  filesWatchList.contains(filename))
              {
                if (filesHistory == null)
                {
                  filesHistory = new HistoryResponse();
                  filesHistory.setPipe(response.getPipe());
                }
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

    GetHistory historyCaller = new GetHistory(files);
    //fork off waiting for a load to the service
    Future<HistoryResponse> result = service.submit(historyCaller);

    HistoryResponse response = result.get(2, TimeUnit.MINUTES);
    return response;
  }

  @Before
  public void beforeAll() throws Exception
  {
    BouncyCastleFIPSInit.init();

    final String filesDirectory = "/tmp/data/";
    final String filesLocationUrl = "file:///tmp/data/";
    // The first few steps simulate example files that we will write to the
    // ingest service.
    //base file name we want to load
    final String fileWithWrongCSVFormat = "letters.csv";
    IngestExampleHelper.makeLocalDirectory(filesDirectory);
    IngestExampleHelper.makeSampleFile(filesDirectory, fileWithWrongCSVFormat);
    files.add(IngestExampleHelper.createTempCsv(filesDirectory, "sample", 10).
        getFileName().toString());
    files.add(IngestExampleHelper.createTempCsv(filesDirectory, "sample", 15).
        getFileName().toString());
    files.add(IngestExampleHelper.createTempCsv(filesDirectory, "sample", 20).
        getFileName().toString());
    files.add(fileWithWrongCSVFormat);
    conn = IngestExampleHelper.getConnection(USER, PASSWORD,
                                             ACCOUNT, HOST, PORT);
    keypair = generateKeyPairWithBCFipsProvider();
    manager = new SimpleIngestManager(ACCOUNT, USER,
                                      FQ_PIPE, keypair, SCHEME,
                                      HOST, PORT);
    setup(files, filesLocationUrl);
  }

  /**
   * Remove test table and pipe
   */
  @After
  public void afterAll()
  {
    IngestExampleHelper.doQuery(conn, "drop pipe if exists " + FQ_PIPE);
    IngestExampleHelper.doQuery(conn, "drop stage if exists " + STAGE);
    IngestExampleHelper.doQuery(conn, "drop table if exists " + TABLE);
  }

  @Test
  public void testIngestWithFipsComplianceMode() throws Exception
  {
    System.out.println("Starting snowflake ingest client");
    System.out.println("Connecting to " + SCHEME + "://" +
                           HOST + ":" + PORT +
                           "\n with Account:" + ACCOUNT +
                           ", User: " + USER);

    try
    {
      final long oneHourMillis = 1000 * 3600l;
      String startTime = Instant
          .ofEpochMilli(System.currentTimeMillis() - 4 * oneHourMillis).toString();

      IngestResponse insertFileResponse = insertFiles(files);
      System.out.println("Received ingest insertFileResponse: " + insertFileResponse.toString());

      Assert.assertEquals("SUCCESS", insertFileResponse.getResponseCode());

      HistoryResponse history = waitForFilesHistory(files);
      System.out.println("Received history response: " + history.toString());
      String endTime = Instant
          .ofEpochMilli(System.currentTimeMillis()).toString();

      HistoryRangeResponse historyRangeResponse =
          manager.getHistoryRange(null,
                                  startTime,
                                  endTime);

      System.out.println("Received history range response: " +
                             historyRangeResponse.toString());
      // Lets iterate over the historyResponse and add all rowsInserted

      final long actualSumOfRowsInserted = historyRangeResponse.files
          .stream()
          .map((HistoryResponse.FileEntry::getRowsInserted))
          .mapToLong(Long::longValue)
          .sum();

      final Set<String> loadedStatus = historyRangeResponse.files
          .stream()
          .map(HistoryResponse.FileEntry::getStatus)
          .map(IngestStatus::toString)
          .collect(Collectors.toSet());

      // Sum of all rows = 45
      Assert.assertEquals(45, actualSumOfRowsInserted);

      // One of the files was failed to load, letters.csv
      Assert.assertTrue(loadedStatus.contains("LOAD_FAILED"));

    }
    catch(IngestResponseException e)
    {
      System.out.println("Service exception: "+ e.toString());
      throw e;
    }
    catch(Exception e)
    {
      System.out.println(e);
      throw e;
    }
  }
}
