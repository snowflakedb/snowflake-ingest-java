package net.snowflake.ingest;

import junit.framework.TestCase;
import net.snowflake.ingest.connection.HistoryResponse;
import net.snowflake.ingest.connection.InsertResponse;
import net.snowflake.ingest.utils.FileWrapper;
import org.apache.commons.codec.binary.Base64;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.*;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.sql.*;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * TestSimpleIngestLocal - this class tests whether or not we are
 * successfully able to create a local file, push it to snowflake,
 */
public class TestSimpleIngestLocal extends TestCase
{
  //The encryption algorithm we will use to generate keys
  private final static String ALGORITHM = "RSA";

  //the base

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

  //the actual connection string
  private static final String CONNECT_STRING =
      "jdbc:snowflake://" + HOST + ":" + PORT;

  //Do we actually want to use SSL
  private static final String SSL = "off";

  //the connection we will use for queries
  private final Connection conn;

  //the name of our target DB
  private static final String DATABASE = "testdb";

  //the name of our target schema
  private static final String SCHEMA = "public";

  //the name of our stage
  private static final String STAGE = "ingest_stage";

  //the name of our target table
  private static final String TABLE = "ingest_table";


  //the fully qualified table name
  private final String FQ_TABLE =
      DATABASE + "." + SCHEMA + "." + quote(TABLE);

  //the fully qualfied stage name
  private final String FQ_STAGE =
      DATABASE + "." + SCHEMA + "." + quote(STAGE);

  //the actual ingest manager
  private final SimpleIngestManager manager;

  //our keypair
  private final KeyPair keypair ;

  /**
   * TestSimpleIngestLocal - makes a new instance of
   * this test class by creating a sql connection to the database
   */
  TestSimpleIngestLocal()
  throws ClassNotFoundException, SQLException,
      NoSuchAlgorithmException, NoSuchProviderException
  {
    //create a connection
    conn = getConnection();
    //generate a keypair
    keypair = generateKeyPair();
    //make an ingest manager
    manager = new SimpleIngestManager(ACCOUNT, USER,
        FQ_TABLE,FQ_STAGE, keypair);
  }

  /**
   * Generates an RSA keypair for use in this test
   * @return a valid RSA keypair
   * @throws NoSuchAlgorithmException if we don't have an RSA algo
   * @throws NoSuchProviderException if we can't use SHA1PRNG for randomization
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
   * @return a valid JDBC connection
   */
  private Connection getConnection()
  throws ClassNotFoundException, SQLException
  {
    //check first to see if we have the Snowflake JDBC
    Class.forName("net.snowflake.client.driver.SnowflakeDriver");

    //build our properties
    Properties props = new Properties();
    props.put("user", USER);
    props.put("password", PASSWORD);
    props.put("account", ACCOUNT);
    props.put("ssl", SSL);

    //fire off the connection
    return DriverManager.getConnection(CONNECT_STRING, props);
  }

  /**
   * Creates a local file for loading into our table
   * @return URI of this file
   * @throws IOException
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
      for(char letter = 'a'; letter <= 'z'; letter++ )
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
    final Path basedir = Paths.get(base);
    try
    {
      Files.createDirectories(Paths.get(base));
    }
    catch (IOException e)
    {
      throw new IllegalStateException("create temp dir failed", e);
    }
  }


  /**
   * Try to execute a query and throw if we fail
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
   * @return the public key as a string
   */
  private String getPublicKeyString()
  throws NoSuchAlgorithmException, InvalidKeySpecException
  {
    KeyFactory keyFactory  = KeyFactory.getInstance(ALGORITHM);
    final PublicKey pk = keypair.getPublic();
    X509EncodedKeySpec spec =
        keyFactory.getKeySpec(pk, X509EncodedKeySpec.class);
    return Base64.encodeBase64String(spec.getEncoded());
  }




  /**
   * Simple helper method to escape a string via quotes
   * @return quoted string
   */
  private static String quote(String arg)
  {
    return '"' + arg + '"';
  }

  /**
   * Creates the stages and files we'll use for this test
   */
  protected void setUp()
  throws Exception
  {
    //create the temporary directory and local file
    createTempStageDir();
    makeLocalFile();

    //create the target stage
    doQuery("create or replace stage " + quote(STAGE) +
            " url='file://tmp/data'");

    //create the target
    doQuery("create or replace table " + quote(TABLE) +
            " (c1 string)");
    String pk = getPublicKeyString();

    doQuery("alter user " + USER +
            " set RSA_PUBLIC_KEY='" + pk + "'");
  }

  /**
   * testLoadSingle -- succeeds if we load a single file
   */
  protected void testLoadSingle()
  throws Exception
  {
    //keeps track of whether we've loaded the file
    boolean loaded = false;

    //create a file wrapper
    FileWrapper myFile = new FileWrapper(BASE_FILENAME, null);

    //get an insert response after we submit
    InsertResponse insertResponse = manager.ingestFile(myFile, null);

    //assert that we successfully enqueued
    assertTrue(insertResponse.responseCode == InsertResponse.Response.SUCCESS);

    ExecutorService service = Executors.newSingleThreadExecutor();

    Future<Boolean> result = service.submit(() ->
        {
          while(true)
          {
            Thread.sleep(1000);
            HistoryResponse response = manager.get
          }
        }
    );
  }

}
