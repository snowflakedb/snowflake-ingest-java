package net.snowflake.ingest;

import net.snowflake.ingest.connection.HistoryResponse;
import net.snowflake.ingest.connection.IngestResponse;
import net.snowflake.ingest.connection.IngestResponseException;
import net.snowflake.ingest.connection.RequestBuilder;
import net.snowflake.ingest.connection.ServiceResponseHandler;
import net.snowflake.ingest.utils.BackOffException;
import net.snowflake.ingest.utils.StagedFileWrapper;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyPair;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * This class provides a basic, low-level abstraction over
 * the Snowflake Ingest Service REST api
 * <p>
 * Usage of this class delegates all exception and state handling to the developer
 *
 * @author obabarinsa
 */
public class SimpleIngestManager
{

  /**
   * This Builder allows someone to configure a SimpleIngestManager
   * prior to instantiating the manager
   *
   * @author obabarinsa
   */

  public static class Builder
  {

    //the account name we want to use
    private String account;

    //the user who will be loading data
    private String user;

    //the fully qualified pipe name
    private String pipe;

    //the key pair we want to use to authenticate
    private KeyPair keypair;

    /**
     * getAccount - returns the name of the account this builder will inject into the
     * IngestManager
     *
     * @return account name
     */
    public String getAccount()
    {
      return account;
    }

    /**
     * setAccount - set the account for the ingest manager and return this builder
     *
     * @param account the account which will be loading into this table
     * @return this builder object
     */
    public Builder setAccount(String account)
    {
      this.account = account;
      return this;
    }

    /**
     * getUser - get the user who will be loading using the ingest service
     *
     * @return the user name
     */
    public String getUser()
    {
      return user;
    }

    /**
     * setUser - sets the user who will be loading with the ingest manager
     *
     * @param user the user who will be loading
     * @return the current builder with the user set
     */
    public Builder setUser(String user)
    {
      this.user = user;
      return this;
    }


    /**
     * getPipe - get the pipe for the ingest manager this builder will create
     *
     * @return the target pipe for this ingest manager
     */
    public String getPipe()
    {
      return pipe;
    }


    /**
     * setTable - sets the pipe which the SimpleIngestManager will be using
     *
     * @param pipe the target pipe for the ingest manager
     * @return the current builder with the target pipe
     */
    public Builder setPipe(String pipe)
    {
      this.pipe = pipe;
      return this;
    }


    /**
     * getKeyPair - returns the key-pair we're using for authentication
     *
     * @return the RSA 2048 key-pair we use to sign tokens
     */
    public KeyPair getKeypair()
    {
      return keypair;
    }


    /**
     * setKeypair - sets the RSA 2048 bit keypair we'll be using for token signing
     *
     * @param keypair the keypair we'll be using for auth
     * @return the current builder with the key set
     */
    public Builder setKeypair(KeyPair keypair)
    {
      this.keypair = keypair;
      return this;
    }

    /**
     * build - returns a new instance of SimpleIngestManager using the information
     * set in this builder object
     */
    public SimpleIngestManager build()
    {
      return new SimpleIngestManager(account, user, pipe, keypair);
    }

  }


  //logger object for this class
  private static final Logger LOGGER =
          LoggerFactory.getLogger(SimpleIngestManager.class);
  //HTTP Client that we use for sending requests to the service
  private HttpClient httpClient;

  //the account in which the user lives
  private String account;

  //the username of the user
  private String user;

  //the fully qualified name of the pipe
  private String pipe;

  //the keypair we're using for authentication
  private KeyPair keyPair;

  //the request builder who handles building the HttpRequests we send
  private final RequestBuilder builder;


  /**
   * init - Does the basic work of constructing a SimpleIngestManager that
   * is common across all constructors
   *
   * @param account The account into which we're loading
   * @param user the user performing this load
   * @param pipe the fully qualified name of pipe
   * @param keyPair the KeyPair we'll use to sign JWT tokens
   */
  private void init(String account, String user, String pipe,
                    KeyPair keyPair)
  {
    //set up our reference variables
    this.account = account;
    this.user = user;
    this.pipe = pipe;
    this.keyPair = keyPair;

    //make our client for sending requests
    httpClient = HttpClients.createDefault();
    //make the request builder we'll use to build messages to the service
  }

  /**
   * Constructs a SimpleIngestManager for a given user in a specific account
   * In addition, this also takes takes the target table and source stage
   * Finally, it also requires a valid KeyPair object registered with
   * Snowflake DB
   *
   * @param account The account into which we're loading
   * @param user the user performing this load
   * @param pipe the fully qualified name of the pipe
   * @param keyPair the KeyPair we'll use to sign JWT tokens
   */
  public SimpleIngestManager(String account, String user, String pipe,
                             KeyPair keyPair)
  {
    //call our initializer method
    init(account, user, pipe, keyPair);

    //create the request builder
    this.builder = new RequestBuilder(account, user, keyPair);
  }


  /**
   * Constructs a SimpleIngestManager for a given user in a specific account
   * In addition, this also takes takes the target table and source stage
   * Finally, it also requires a valid KeyPair object registered with
   * Snowflake DB
   *
   * @param account the account into which we're loading
   * @param user the user performing this load
   * @param pipe the fully qualified name of the pipe
   * @param keyPair the KeyPair we'll use to sign JWT tokens
   * @param schemeName http or https
   * @param hostName the hostname
   * @param port the port number
   */
  public SimpleIngestManager(String account, String user, String pipe,
                      KeyPair keyPair, String schemeName,
                      String hostName, int port)
  {
    //call our initializer method
    init(account, user, pipe, keyPair);

    //make the request builder we'll use to build messages to the service
    builder = new RequestBuilder(account, user, keyPair,
                                 schemeName, hostName, port);
  }


  /**
   * getAccount - Gives back the name of the account
   * that this IngestManager is targeting
   *
   * @return the name of the account
   */
  public String getAccount()
  {
    return account;
  }


  /**
   * getUser - gives back the user on behalf of
   * which this ingest manager is loading
   *
   * @return the user name
   */
  public String getUser()
  {
    return user;
  }

  /**
   * getPipe - gives back the pipe which we are using
   *
   * @return the pipe name
   */
  public String getPipe()
  {
    return pipe;
  }


  /**
   * wrapFilepaths - convenience method to take a list of filenames and
   * produce a list of FileWrappers with unset size
   *
   * @param filenames the filenames you want to wrap up
   * @return a corresponding list of StagedFileWrapper objects
   */
  public static List<StagedFileWrapper> wrapFilepaths(Set<String> filenames)
  {
    //if we get a null, throw
    if (filenames == null)
    {
      throw new IllegalArgumentException();
    }

    return filenames.parallelStream()
        .map(fname -> new StagedFileWrapper(fname, null)).
                            collect(Collectors.toList());

  }

  /**
   * ingestFile - ingest a single file
   *
   * @param file - a wrapper around a filename and size
   * @param requestId - a requestId that we'll use to label - if null,
   *                    we generate one for the user
   * @return an insert response from the server
   * @throws BackOffException   - if we have a 503 response
   * @throws IOException        - if we have some other network failure
   * @throws URISyntaxException - if the provided account name was illegal and
   *                            caused a URI construction failure
   */
  public IngestResponse ingestFile(StagedFileWrapper file, UUID requestId)
      throws URISyntaxException, IOException
  {
    return ingestFiles(Collections.singletonList(file), requestId);
  }

  /**
   * ingestFiles - synchronously sends a request to the ingest
   * service to enqueue these files
   *
   * @param files - list of wrappers around filenames and sizes
   * @param requestId - a requestId that we'll use to label - if null,
   *                  we generate one for the user
   * @return an insert response from the server
   * @throws BackOffException   - if we have a 503 response
   * @throws IOException        - if we have some other network failure
   * @throws IngestResponseException - if snowflake encountered
   *                                    error during ingest
   * @throws URISyntaxException - if the provided account name was illegal and
   *                            caused a URI construction failure
   */

  public IngestResponse ingestFiles(List<StagedFileWrapper> files,
                                    UUID requestId)
      throws URISyntaxException, IOException, IngestResponseException
  {

    //the request id we want to send with this payload
    UUID request = requestId == null ? UUID.randomUUID() : requestId;

    //We're about to send this request number
    LOGGER.info("Sending Request UUID - ", request.toString());

    //send the request and get a response....
    HttpResponse response = httpClient.execute(
        builder.generateInsertRequest(request,
             pipe, files));

    LOGGER.info("Attempting to unmarshall insert response - {}", response);
    return ServiceResponseHandler.unmarshallIngestResponse(response);
  }


  /**
   * getHistory - pings the service to see the current ingest history for this table
   *
   * @param requestId a UUID we use to label the request, if null, one is generated for the user
   * @return a response showing the available ingest history from the service
   * @throws BackOffException   - if we have a 503 response
   * @throws IOException        - if we have some other network failure
   * @throws IngestResponseException - if snowflake encountered error during ingest
   * @throws URISyntaxException - if the provided account name was illegal and caused a URI construction failure
   */
  public HistoryResponse getHistory(UUID requestId)
      throws URISyntaxException, IOException, IngestResponseException
  {
    //if we have no requestId generate one
    if (requestId == null)
    {
      requestId = UUID.randomUUID();
    }

    //send the request and get a response...
    HttpResponse response = httpClient.execute(
        builder.generateHistoryRequest(requestId, pipe)
    );


    LOGGER.info("Attempting to unmarshall history response - {}", response);
    return ServiceResponseHandler.unmarshallHistoryResponse(response);
  }


}
