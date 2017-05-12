package net.snowflake.ingest;

import net.snowflake.ingest.connection.HistoryResponse;
import net.snowflake.ingest.connection.IngestResponse;
import net.snowflake.ingest.utils.StagedFileWrapper;
import org.apache.commons.codec.binary.Base64;

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
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * @author vganesh
 */
public class SnowflakeIngestClient
{
  //The encryption algorithm we will use to generate keys
  private final static String ALGORITHM = "RSA";

  //Do we actually want to use SSL
  private final String ssl;

  //the connection we will use for queries
  private final Connection conn;

  //the Administrative connection
 //the fully qualified pipe name
  private final String fqPipe;

  //the actual ingest manager
  private final SimpleIngestManager manager;

  //our keypair
  private final KeyPair keypair;


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
   * @param config A valid snowflake config
   * @return a valid JDBC connection
   */
  private Connection getConnection(SnowflakeConfig config)
          throws ClassNotFoundException, SQLException
  {
    //check first to see if we have the Snowflake JDBC
    Class.forName("net.snowflake.client.jdbc.SnowflakeDriver");

    //build our properties
    Properties props = new Properties();
    props.put("user", config.User());
    props.put("password", config.Password());
    props.put("account", config.Account());
    props.put("ssl", ssl);

  //the actual connection string
    String connectString =
          "jdbc:snowflake://" + config.Host() + ":" + config.Port();

    //fire off the connection
    return DriverManager.getConnection(connectString, props);
  }

  /**
   * Wrapper interface to connect with the Snowflake ingest service endpoints
   * @param config Snowflake config providing account, user info
   * @param pipeConfig Snowflake pipe config which this client will use
   * @param useSSL
   * @throws Exception
   * @throws ClassNotFoundException if Snowflake JDBC not loaded
   * @throws SQLException
   * @throws NoSuchAlgorithmException
   * @throws NoSuchProviderException
   */
  public SnowflakeIngestClient(SnowflakeConfig config,
                               SnowflakePipeConfig pipeConfig,
                               boolean useSSL)
          throws Exception, ClassNotFoundException, SQLException,
          NoSuchAlgorithmException, NoSuchProviderException
  {
    fqPipe = pipeConfig.Database() + "." + pipeConfig.Schema() +
            "." + quote(pipeConfig.Name());
    ssl = (useSSL)?"on":"off";
    conn = getConnection(config);

    keypair = generateKeyPair();
    manager = new SimpleIngestManager(config.Account(), config.User(),
                                      fqPipe, keypair, config.Scheme(),
                                      config.Host(), config.Port());
  }

  /**
   * Try to execute a SQL query and throw if it fails
   *
   * @param query the query in question
   */
  public void doQuery(String query)
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
  public String getPublicKeyString()
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
  public static String quote(String arg)
  {
    return '"' + arg + '"';
  }

  /**
   * Ingest a single file from the staging are using ingest service
   * @param filename
   * @return
   * @throws Exception
   */
  public IngestResponse insertFile(String filename)
          throws Exception
  {
    //create a file wrapper
    StagedFileWrapper myFile = new StagedFileWrapper(filename, null);
    return manager.ingestFile(myFile, null);
  }

  /**
   * Ingest a set of files from the staging area through the ingest service
   * @param files
   * @return
   * @throws Exception
   */
  public IngestResponse insertFiles(Set<String> files)
          throws Exception
  {
    List<StagedFileWrapper> ingestFiles = new ArrayList<StagedFileWrapper>(files.size());
    for(String file: files)
    {
      ingestFiles.add(new StagedFileWrapper(file, null));
    }

    return manager.ingestFiles(ingestFiles, null);
  }

  /**
   * Request for history of ingests
   * @return
   * @throws Exception
   */
  public HistoryResponse insertHistory() throws Exception
  {
    return manager.getHistory(null);
  }
}
