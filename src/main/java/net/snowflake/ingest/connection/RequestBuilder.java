package net.snowflake.ingest.connection;

import net.snowflake.ingest.utils.FileWrapper;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.utils.URIBuilder;

import java.net.URI;
import java.net.URISyntaxException;
import java.security.KeyPair;
import java.text.MessageFormat;
import java.util.List;
import java.util.UUID;


import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class handles constructing the URIs for our
 * requests as well as putting together the payloads we'll
 * be sending
 * @author obabarinsa
 */
public final class RequestBuilder
{

  //a logger for all of our needs in this class
  private static final Logger LOGGER = LoggerFactory.getLogger(RequestBuilder.class.getName());

  //the name of the target account
  private String account;

  //the name of the active user
  private String user;

  //the security manager who will handle token generation
  private SecurityManager securityManager;

  //the default connection scheme is HTTPS
  private static final String DEFAULT_SCHEME = "https";

  //whatever the actual scheme is
  private String scheme;

  //the default port is 443
  private static final int DEFAULT_PORT = 443;

  //the actual port number
  private final int port;

  //the default host is snowflakecomputing.com
  private static final String DEFAULT_HOST = "snowflakecomputing.com";

  //whatever the actual host is
  private final String host;

  //the endpoint format string for inserting files
  private static final String INSERT_ENDPOINT_FORMAT = "/v1/data/tables/%s/insertFiles";

  //the endpoint for history queries
  private static final String HISTORY_ENDPOINT_FORMAT = "/v1/data/tables/%s/insertReport";

  //the request id parameter name
  private static final String REQUEST_ID = "requestId";

  //the name parameter name
  private static final String STAGE_PARAMETER = "stage";

  //the string name for the HTTP auth bearer
  private static final String BEARER_PARAMETER = "Bearer ";

  //and object mapper for all marshalling and unmarshalling
  private static final ObjectMapper objectMapper = new ObjectMapper();


  /**
   * A simple POJO for generating our POST body to the insert endpoint
   * @author obabarinsa
   */
  static class InsertRequest
  {
    //the list of files we're loading
    public List<FileWrapper> files;
  }

  /**
   * RequestBuilder - general usage constructor
   * @param accountName - the name of the Snowflake account to which we're connecting
   * @param userName - the username of the entity loading files
   * @param keyPair - the Public/Private key pair we'll use to authenticate
   */
  public RequestBuilder(String accountName, String userName, KeyPair keyPair)
  {
    this(accountName, userName, keyPair,
        DEFAULT_SCHEME, DEFAULT_HOST, DEFAULT_PORT);
  }

  /**
   * RequestBuilder - this constructor is for testing purposes only
   * @param accountName - the account name to which we're connecting
   * @param userName - for whom are we connecting?
   * @param keyPair - our auth credentials
   * @param schemeName - are we HTTP or HTTPS?
   * @param hostName - the host for this snowflake instance
   * @param portNum - the port number
   */
   RequestBuilder(String accountName,
                 String userName,
                 KeyPair keyPair,
                 String schemeName,
                 String hostName,
                 int portNum)
  {
     //none of these arguments should be null
    if(accountName == null || userName == null || keyPair == null)
    {
      throw new IllegalArgumentException();
    }

    //create our security/token manager
    securityManager = new SecurityManager(accountName, userName, keyPair);

    //stash references to the account and user name as well
    this.account = accountName.toUpperCase();
    this.user =  userName.toUpperCase();

    //save our host, scheme and port info
    port = portNum;
    scheme = schemeName;
    host = hostName;

    LOGGER.info("Creating a RequestBuilder with arguments : " +
        "Account : {}, User : {}, Scheme : {}, Host : {}, Port : {}", account,
        user, scheme, host, port);
  }


  /**
   * Given a request UUID, construct a URIBuilder for the common parts
   * of any Ingest Service request
   * @param requestId the UUID with which we want to label this request
   * @return a URI builder we can use to finish build the URI
   */
  private URIBuilder makeBaseURI(UUID requestId)
  {
    //We can't make a request with no id
    if(requestId == null)
    {
      throw new IllegalArgumentException();
    }

    //construct the builder object
    URIBuilder builder = new URIBuilder();

    //set the scheme
    builder.setScheme(scheme);

    //set the host name
    builder.setHost(host);

    //set the port name
    builder.setPort(port);

    //set the request id
    builder.addParameter(REQUEST_ID, requestId.toString());

    //Log the base url
    LOGGER.info(MessageFormat.format("Base URL  as generated so far : {}", builder.toString()));

    return builder;
  }

  /**
   * makeInsertURI - Given a request UUID, and a fully qualified table name
   * make a URI for the Ingest Service inserting
   * @param requestId the UUID we'll use as the label
   * @param table the table name
   * @param stage the stage name
   * @return URI for the insert request
   */
  private URI makeInsertURI(UUID requestId, String table, String stage) throws URISyntaxException
  {
    //if the table name is null, we have to abort
    if(table == null)
    {
      throw new IllegalArgumentException();
    }

    //get the base endpoint uri
    URIBuilder builder = makeBaseURI(requestId);

    //set the stage parameter
    builder.setParameter(STAGE_PARAMETER, stage);

    //add the path for the URI
    builder.setPath(String.format(INSERT_ENDPOINT_FORMAT, table));

    //build the final URI
    return builder.build();
  }


  /**
   * makeHistoryURI - Given a request UUID, and a fully qualified table name
   * make a URI for the history reporting
   * @param requestId the label for this request
   * @param table the table name
   * @return URI for the insert request
   */
  private URI makeHistoryURI(UUID requestId, String table) throws URISyntaxException
  {
    //if the table name is null, we have to abort
    if(table == null)
    {
      throw new IllegalArgumentException();
    }

    //get the base endpoint UIR
    URIBuilder builder = makeBaseURI(requestId);

    //set the path for the URI
    builder.setPath(String.format(HISTORY_ENDPOINT_FORMAT, table));

    LOGGER.info("Final History URIBuilder - {}", builder.toString());
    //build the final URI
    return builder.build();
  }

  /**
   * generateFilesJSON - Given a list of files, make some json to represent it
   * @param files the list of files we want to send
   * @return the string json blob
   */
  private String generateFilesJSON(List<FileWrapper> files)
  {
    //if the files argument is null, throw
    if(files == null)
    {
      LOGGER.info("Null files argument in RequestBuilder");
      throw new IllegalArgumentException();
    }

    //create pojo
    InsertRequest pojo = new InsertRequest();
    pojo.files = files;

    //serialize to a string
    try
    {
      return objectMapper.writeValueAsString(pojo);
    }
    //if we have an exception we need to log and throw
    catch (Exception e)
    {
      LOGGER.error("Unable to Generate JSON Body for Insert request");
      throw new RuntimeException();
    }
  }


  /**
   * addToken - adds a the JWT token to a request
   * @param request the URI request
   * @param token the token to add
   */
  private static void addToken(HttpUriRequest request, String token)
  {
    request.setHeader(HttpHeaders.AUTHORIZATION, BEARER_PARAMETER + token);
  }

  /**
   * generateInsertRequest - given a table, stage and list of files, make a request for the
   * insert endpoint
   * @param requestId a UUID we will use to label this request
   * @param table a fully qualified table name
   * @param stage a fully qualified stage name
   * @param files a list of files
   * @return a post request with all the data we need
   * @throws URISyntaxException if the URI components provided are improper
   */
  public HttpPost generateInsertRequest(UUID requestId, String table, String stage, List<FileWrapper> files)
  throws URISyntaxException
  {
    //make the insert URI
    URI insertURI = makeInsertURI(requestId, table, stage);

    //Make the post request
    HttpPost post = new HttpPost(insertURI);

    //add the auth token
    addToken(post, securityManager.getToken());

    //the entity for the containing the json
    final StringEntity entity = new StringEntity(generateFilesJSON(files), ContentType.APPLICATION_JSON);
    post.setEntity(entity);

    return post;
  }

  /**
   * generateHistoryRequest - given a requestId and a table, make a history request
   * @param requestId a UUID we will use to label this request
   * @param table a fully qualified table name
   * @return a get request with all the data we need
   * @throws URISyntaxException - If the URI components provided are improper
   */
  public HttpGet generateHistoryRequest(UUID requestId, String table)
  throws URISyntaxException
  {
    //make the history URI
    URI historyURI = makeHistoryURI(requestId, table);

    //make the get request
    HttpGet get = new HttpGet(historyURI);

    //add the auth token
    addToken(get, securityManager.getToken());

    return get;
  }
}
