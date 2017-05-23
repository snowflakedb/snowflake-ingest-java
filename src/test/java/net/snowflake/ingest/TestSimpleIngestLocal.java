package net.snowflake.ingest;

import net.snowflake.ingest.connection.HistoryResponse;
import net.snowflake.ingest.connection.IngestResponse;
import net.snowflake.ingest.utils.StagedFileWrapper;
import org.apache.commons.codec.binary.Base64;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;


/**
 * TestSimpleIngestLocal - this class tests whether or not we are
 * successfully able to create a local file, push it to snowflake,
 */
public class TestSimpleIngestLocal
{
  //The encryption algorithm we will use to generate keys
  private final static String ALGORITHM = "RSA";

  //the name of the file we want to push
  private static final String FILENAME = "/tmp/data/letters.csv";

  //base file name we want to load
  private static final String BASE_FILENAME = "letters.csv";

  //the account for which we are pushing
  private static final String ACCOUNT = "testaccount";

  //the user who is going to be ingesting these files
  private static final String USER = "snowman";

  //the password of this test user
  private static final String PASSWORD = "test";

  //the connecting port
  private static final int PORT = 8082;

  //the host name
  private static final String HOST = "localhost";

  //the scheme name
  private static final String SCHEME = "http";

  //the actual connection string
  private static final String CONNECT_STRING =
      "jdbc:snowflake://" + HOST + ":" + PORT;

  //Do we actually want to use SSL
  private static final String SSL = "off";

  //the connection we will use for queries
  private final Connection conn;

  //the Administrative connection

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

  //the fully qualified table name
  private final String FQ_TABLE =
      DATABASE + "." + SCHEMA + "." + quote(TABLE);

  //the fully qualified stage name
  private final String FQ_STAGE =
      DATABASE + "." + SCHEMA + "." + quote(STAGE);

  //the fully qualified pipe name
  private final String FQ_PIPE =
      DATABASE + "." + SCHEMA + "." + quote(PIPE);

  //the actual ingest manager
  private final SimpleIngestManager manager;

  //our keypair
  private final KeyPair keypair;

  /**
   * TestSimpleIngestLocal - makes a new instance of
   * this test class by creating a sql connection to the database
   */
  public TestSimpleIngestLocal()
      throws ClassNotFoundException, SQLException,
      NoSuchAlgorithmException, NoSuchProviderException
  {
    //create a connection
    conn = getConnection(USER);


    //generate a keypair
    keypair = generateKeyPair();
    //make an ingest manager
    manager = new SimpleIngestManager(ACCOUNT, USER,
        FQ_PIPE, keypair, SCHEME, HOST, PORT);
  }

  /**
   * Generates an RSA keypair for use in this test
   *
   * @return a valid RSA keypair
   * @throws NoSuchAlgorithmException if we don't have an RSA algo
   * @throws NoSuchProviderException  if we can't use SHA1PRNG for randomization
   */
  private KeyPair generateKeyPair()
      throws NoSuchProviderException, NoSuchAlgorithmException
  {
    KeyPairGenerator keyGen = KeyPairGenerator.getInstance(ALGORITHM);
    SecureRandom random = SecureRandom.getInstance("SHA1PRNG", "SUN");
    keyGen.initialize(2048, random);
    return keyGen.generateKeyPair();
  }

  /**
   * Gets a JDBC connection to the service
   *
   * @param user user name
   * @return a valid JDBC connection
   */
  private Connection getConnection(String user)
      throws ClassNotFoundException, SQLException
  {
    //check first to see if we have the Snowflake JDBC
    Class.forName("net.snowflake.client.jdbc.SnowflakeDriver");

    //build our properties
    Properties props = new Properties();
    props.put("user", user);
    props.put("password", PASSWORD);
    props.put("account", ACCOUNT);
    props.put("ssl", SSL);

    //fire off the connection
    return DriverManager.getConnection(CONNECT_STRING, props);
  }

  /**
   * Creates a local file for loading into our table
   *
   * @return URI of this file
   * @throws IOException If we can't write the file
   */
  private URI makeLocalFile()
      throws IOException
  {
    File file = new File(FILENAME);

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

    return file.toURI();
  }

  /**
   * Attempts to create a directory in which we can store
   * our local files
   */
  private void createTempStageDir()
  {
    final String base = "/tmp/data";
    try
    {
      Files.createDirectories(Paths.get(base));
    } catch (IOException e)
    {
      throw new IllegalStateException("create temp dir failed", e);
    }
  }


  /**
   * Try to execute a query and throw if we fail
   *
   * @param query the query in question
   */
  private void doQuery(String query)
  {
    try (Statement statement = conn.createStatement())
    {
      statement.executeQuery(query);
    }
    //if ANY exceptions occur, an illegal state has been reached
    catch (Exception e)
    {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Generate the public key as a string
   *
   * @return the public key as a string
   */
  private String getPublicKeyString()
      throws NoSuchAlgorithmException, InvalidKeySpecException
  {
    KeyFactory keyFactory = KeyFactory.getInstance(ALGORITHM);
    final PublicKey pk = keypair.getPublic();
    X509EncodedKeySpec spec =
        keyFactory.getKeySpec(pk, X509EncodedKeySpec.class);
    return Base64.encodeBase64String(spec.getEncoded());
  }


  /**
   * Simple helper method to escape a string via quotes
   *
   * @return quoted string
   */
  private static String quote(String arg)
  {
    return '"' + arg + '"';
  }

  /**
   * Creates the stages and files we'll use for this test
   */
  @Before
  public void setup()
      throws Exception
  {
    //create the temporary directory and local file
    createTempStageDir();
    makeLocalFile();


    //use the right database
    doQuery("use database " + DATABASE);

    //use the right schema
    doQuery("use schema " + SCHEMA);

    //create the target stage
    doQuery("create or replace stage " + quote(STAGE) +
        " url='file:///tmp/data/'");

    //create the target
    doQuery("create or replace table " + quote(TABLE) +
        " (c1 string)");

    doQuery("grant insert on table " + quote(TABLE) + " to accountadmin");

    doQuery("create or replace pipe " + quote(PIPE) +
        " as copy into " + quote(TABLE) + " from @" + quote(STAGE) +
        " file_format=(type='csv')");

    String pk = getPublicKeyString();

    //assume the necessary privileges
    doQuery("use role accountadmin");

    //set the public key
    doQuery("alter user " + USER +
        " set RSA_PUBLIC_KEY='" + pk + "'");

    doQuery("use role sysadmin");
  }

  /**
   * Attempts to sleep and fetch the history afterwards
   *
   * @return the history object or null if an error happened
   */
  private HistoryResponse sleepAndFetchHistory()
  {
    try
    {

      Thread.sleep(500);
      return manager.getHistory(null);
    } catch (Exception e)
    {
      return null;
    }
  }

  /**
   * testLoadSingle -- succeeds if we load a single file
   */
  @Test
  public void testLoadSingle()
      throws Exception
  {

    //keeps track of whether we've loaded the file
    boolean loaded = false;

    //create a file wrapper
    StagedFileWrapper myFile = new StagedFileWrapper(BASE_FILENAME, null);

    //get an insert response after we submit
    IngestResponse insertResponse = manager.ingestFile(myFile, null);

    //create a new thread
    ExecutorService service = Executors.newSingleThreadExecutor();

    //fork off waiting for a load to the service
    Future<?> result = service.submit(() ->
        {
          //we spin here forever
          while (true)
          {
            HistoryResponse response = sleepAndFetchHistory();

            if (response != null && response.files != null)
            {
              for (HistoryResponse.FileEntry entry : response.files)
              {
                //if we have a complete file that we've loaded with the same name..
                if (entry.path != null && entry.complete && entry.path.contains(BASE_FILENAME))
                {
                  //we can return true!
                  return;
                }
              }
            }
          }
        }
    );

    //try to wait until the future is done
    try
    {
      //wait up to 1 minutes to load
      result.get(1, TimeUnit.MINUTES);
      loaded = true;
    } finally
    {
      assertTrue(loaded);
    }
  }

}
