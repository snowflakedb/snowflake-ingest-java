package net.snowflake.ingest;

import net.snowflake.ingest.connection.HistoryResponse;
import net.snowflake.ingest.connection.InsertResponse;
import net.snowflake.ingest.connection.RequestBuilder;
import net.snowflake.ingest.connection.ServiceResponseHandler;
import net.snowflake.ingest.utils.BackOffException;
import net.snowflake.ingest.utils.FileWrapper;
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
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * This class provides a basic, low-level abstraction over
 * the Snowflake Ingest Service REST api
 *
 * Usage of this class delegates all exception and state handling to the developer
 * @author obabarinsa
 */
public class SimpleIngestManager
{

  //logger object for this class
  private static final Logger LOGGER = LoggerFactory.getLogger(SimpleIngestManager.class);
  //HTTP Client that we use for sending requests to the service
  private HttpClient httpClient;

  //the account in which the user lives
  private String account;

  //the username of the user
  private String user;

  //the fully qualfied name of the table
  private String table;

  //the fully qualified name of the stage
  private String stage;

  //the keypair we're using for authentication
  private KeyPair keyPair;

  //the request builder who handles building the HttpRequests we send
  private final RequestBuilder builder;


  /**
   * init - Does the basic work of constructing a SimpleIngestManager that
   * is common across all constructors
   * @param account The account into which we're loading
   * @param user the user performing this load
   * @param table the fully qualified name of the table
   * @param stage the fully qualfied name of the stage
   * @param keyPair the KeyPair we'll use to sign JWT tokens
   */
  private void init(String account, String user, String table,
                             String stage, KeyPair keyPair)
  {
    //set up our reference variables
    this.account = account;
    this.user = user;
    this.table = table;
    this.stage = stage;
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
   * @param table the fully qualified name of the table
   * @param stage the fully qualfied name of the stage
   * @param keyPair the KeyPair we'll use to sign JWT tokens
   */
  public SimpleIngestManager(String account, String user, String table,
                             String stage, KeyPair keyPair)
  {
    LOGGER.info("Entering SimpleIngestManger Constructor");

    //call our initializer method
    init(account, user, table, stage, keyPair);

    //create the request builder
    this.builder = new RequestBuilder(account, user, keyPair);

    LOGGER.info("Exiting SimpleIngestManger Constructor");
  }


  /**
   * Constructs a SimpleIngestManager for a given user in a specific account
   * In addition, this also takes takes the target table and source stage
   * Finally, it also requires a valid KeyPair object registered with
   * Snowflake DB
   * @param account the account into which we're loading
   * @param user the user performing this load
   * @param table the fully qualified name of the table
   * @param stage the fully qualfied name of the stage
   * @param
   * @param keyPair the KeyPair we'll use to sign JWT tokens
   * @param schemeName http or https
   * @param hostName the hostname
   * @param port the port number
   */
  public SimpleIngestManager(String account, String user, String table,
                             String stage, KeyPair keyPair, String schemeName,
                             String hostName, int port)
  {
    LOGGER.info("Entering SimpleIngestManger Constructor");

    //call our initializer method
    init(account, user, table, stage, keyPair);

    //make the request builder we'll use to build messages to the service
    builder = new RequestBuilder(account, user, keyPair, schemeName, hostName, port);

    LOGGER.info("Exiting SimpleIngestManager Constructor");
  }


  /**
   * getAccount - Gives back the name of the accont
   * that this IngestManager is targeting
   * @return the name of the account
   */
  public String getAccount()
  {
    return account;
  }


  /**
   * getUser - gives back the user on behalf of
   * which this ingest manager is loading
   * @return the user name
   */
  public String getUser()
  {
    return user;
  }

  /**
   * getTable - gives back the table into which we are loading
   * @return the table name
   */
  public String getTable()
  {
    return table;
  }

  /**
   * getStage - gives back the name of the stage into
   * which we are loading
   * @return the stage name
   */
  public String getStage()
  {
    return stage;
  }


  /**
   * wrapFilepaths - convenience method to take a list of filenames and
   * produce a list of FileWrappers with unset size
   * @param filenames the filenames you want to wrap up
   * @return a corresponding list of FileWrapper objects
   */
  public static List<FileWrapper> wrapFilepaths(List<String> filenames)
  {
    //if we get a null, throw
    if(filenames == null)
    {
      throw new IllegalArgumentException();
    }

    return filenames.parallelStream()
        .map(fname -> new FileWrapper(fname, null)).collect(Collectors.toList());

  }

  /**
   * ingestFile - ingest a single file
   * @param file - a wrapper around a filename and size
   * @param requestId - a requestId that we'll use to label
   * - if null, we generate one for the user
   * @return an insert response from the server
   * @throws BackOffException - if we have a 503 response
   * @throws IOException - if we have some other network failure
   * @throws URISyntaxException - if the provided account name
   * was illegal and caused a URI construction failure
   */
  public InsertResponse ingestFile(FileWrapper file, UUID requestId)
  throws URISyntaxException, IOException
  {
    return ingestFiles(Collections.singletonList(file), requestId);
  }

  /**
   * ingestFiles - synchronously sends a request to the ingest
   * service to enqueue these files
   * @param files - list of wrappers around filenames and sizes
   * @param requestId - a requestId that we'll use to label
   * - if null, we generate one for the user
   * @return an insert response from the server
   * @throws BackOffException - if we have a 503 response
   * @throws IOException - if we have some other network failure
   * @throws URISyntaxException - if the provided account name
   * was illegal and caused a URI construction failure
   */

  public InsertResponse ingestFiles(List<FileWrapper> files, UUID requestId)
  throws URISyntaxException, IOException
  {

    //the request id we want to send with this payload
    UUID request =  requestId == null? UUID.randomUUID() : requestId;

    //We're about to send this request number
    LOGGER.info("Sending Request UUID - ", request.toString());

    //send the request and get a response....
    HttpResponse response =  httpClient.execute(
        builder.generateInsertRequest(request,
            table, stage, files));

    LOGGER.info("Attempting to unmarshall insert response - {}", response);
    return ServiceResponseHandler.unmarshallInsertResponse(response);
  }


  /**
   * getHistory - pings the service to see the current ingest history for this table
   * @param requestId a UUID we use to label the request, if null, one is generated for the user
   * @return a response showing the available ingest history from the service
   * @throws BackOffException - if we have a 503 response
   * @throws IOException - if we have some other network failure
   * @throws URISyntaxException - if the provided account name
   * was illegal and caused a URI construction failure
   */
  public HistoryResponse getHistory(UUID requestId)
  throws URISyntaxException, IOException
  {
    //if we have no requestId generate one
    if(requestId == null)
    {
      requestId = UUID.randomUUID();
    }

    //send the request and get a response...
    HttpResponse response = httpClient.execute(
      builder.generateHistoryRequest(requestId, table)
    );


    LOGGER.info("Attempting to unmarshall history response - {}", response);
    return ServiceResponseHandler.unmarshallHistoryResponse(response);
  }


}
