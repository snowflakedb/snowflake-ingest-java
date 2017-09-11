/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.connection;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.snowflake.ingest.SimpleIngestManager;
import net.snowflake.ingest.utils.StagedFileWrapper;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

/**
 * This class handles constructing the URIs for our
 * requests as well as putting together the payloads we'll
 * be sending
 *
 * @author obabarinsa
 */
public final class RequestBuilder
{

  //a logger for all of our needs in this class
  private static final Logger LOGGER =
          LoggerFactory.getLogger(RequestBuilder.class.getName());

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
  private static final String INGEST_ENDPOINT_FORMAT =
                            "/v1/data/pipes/%s/insertFiles";

  //the endpoint for history queries
  private static final String HISTORY_ENDPOINT_FORMAT =
                            "/v1/data/pipes/%s/insertReport";

  // the endpoint for history time range queries
  private static final String HISTORY_RANGE_ENDPOINT_FORMAT =
                            "/v1/data/pipes/%s/loadHistoryScan";

  //optional number of max seconds of items to fetch(eg. in the last hour)
  private static final String RECENT_HISTORY_IN_SECONDS =
                                                            "recentSeconds";

  //optional. if not null, tells us where to start the next request
  private static final String HISTORY_BEGIN_MARK = "beginMark";

  // Start time for the history range
  private static final String HISTORY_RANGE_START_INCLUSIVE = "startTimeInclusive";

  // End time for the history range; is optional
  private static final String HISTORY_RANGE_END_EXCLUSIVE = "endTimeExclusive";

  //the request id parameter name
  private static final String REQUEST_ID = "requestId";

  //the string name for the HTTP auth bearer
  private static final String BEARER_PARAMETER = "Bearer ";

  //and object mapper for all marshalling and unmarshalling
  private static final ObjectMapper objectMapper = new ObjectMapper();

  // Don't change!
  private static final String CLIENT_NAME = "SnowpipeJavaSDK";

  private static final String DEFAULT_VERSION = "0.1.0";
  private static final String RESOURCES_FILE = "project.properties";
  private static final Properties PROPERTIES = loadProperties();

  private static final String USER_AGENT = getUserAgent();

  private static Properties loadProperties()
  {
    Properties properties = new Properties();
    properties.put("version", DEFAULT_VERSION);

    try
    {
      URL res = SimpleIngestManager.class.getClassLoader().
                                          getResource(RESOURCES_FILE);
      if (res == null)
      {
        throw new UncheckedIOException(new FileNotFoundException(RESOURCES_FILE));
      }

      URI uri;
      try
      {
        uri = res.toURI();
      }
      catch (URISyntaxException ex)
      {
        throw new IllegalArgumentException(ex);
      }

      try (InputStream is = Files.newInputStream(Paths.get(uri)))
      {
        properties.load(is);
      }
      catch (IOException ex)
      {
        throw new UncheckedIOException("Failed to load resource", ex);
      }
    }
    catch(Exception e)
    {
      LOGGER.warn("Could not read version info: " + e.toString());
    }

    return properties;
  }

  private static String getUserAgent()
  {
    final String clientVersion = PROPERTIES.getProperty("version");
    final String javaVersion = System.getProperty("java.version");
    final String platform = System.getProperty("os.name") +
                            System.getProperty("os.version") +
                            System.getProperty("os.arch");

    // {client-name}/{version}/{java-version}/{platform}
    final String userAgentFormat = "%s/%s/%s/%s";
    return String.format(userAgentFormat,
                                    CLIENT_NAME,
                                    clientVersion,
                                    javaVersion,
                                    platform);
  }
  /**
   * A simple POJO for generating our POST body to the insert endpoint
   *
   * @author obabarinsa
   */
  private static class IngestRequest
  {
    //the list of files we're loading
    public List<StagedFileWrapper> files;
  }

  /**
   * RequestBuilder - general usage constructor
   *
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
   *
   * @param accountName - the account name to which we're connecting
   * @param userName - for whom are we connecting?
   * @param keyPair - our auth credentials
   * @param schemeName - are we HTTP or HTTPS?
   * @param hostName - the host for this snowflake instance
   * @param portNum - the port number
   */
  public RequestBuilder(String accountName,
                        String userName,
                        KeyPair keyPair,
                        String schemeName,
                        String hostName,
                        int portNum)
  {
    //none of these arguments should be null
    if (accountName == null || userName == null || keyPair == null)
    {
      throw new IllegalArgumentException();
    }

    //create our security/token manager
    securityManager = new SecurityManager(accountName, userName, keyPair);

    //stash references to the account and user name as well
    String account = accountName.toUpperCase();
    String user = userName.toUpperCase();

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
   *
   * @param requestId the UUID with which we want to label this request
   * @return a URI builder we can use to finish build the URI
   */
  private URIBuilder makeBaseURI(UUID requestId)
  {
    //We can't make a request with no id
    if (requestId == null)
    {
      LOGGER.error("RequestId is null!");
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
    builder.setParameter(REQUEST_ID, requestId.toString());

    return builder;
  }

  /**
   * makeInsertURI - Given a request UUID, and a fully qualified pipe name
   * make a URI for the Ingest Service inserting
   *
   * @param requestId the UUID we'll use as the label
   * @param pipe the pipe name
   * @return URI for the insert request
   */
  private URI makeInsertURI(UUID requestId, String pipe)
          throws URISyntaxException
  {
    //if the pipe name is null, we have to abort
    if (pipe == null)
    {
      LOGGER.error("Table argument is null");
      throw new IllegalArgumentException();
    }

    //get the base endpoint uri
    URIBuilder builder = makeBaseURI(requestId);

    //add the path for the URI
    builder.setPath(String.format(INGEST_ENDPOINT_FORMAT, pipe));

    //build the final URI
    return builder.build();
  }


  /**
   * makeHistoryURI - Given a request UUID, and a fully qualified pipe name
   * make a URI for the history reporting
   *
   * @param requestId the label for this request
   * @param pipe the pipe name
   * @param recentSeconds history only for items in the recentSeconds window
   * @param beginMark mark from which history should be fetched
   * @return URI for the insert request
   */
  private URI makeHistoryURI(UUID requestId,
                             String pipe,
                             Integer recentSeconds,
                             String beginMark)
                throws URISyntaxException
  {
    //if the table name is null, we have to abort
    if (pipe == null)
    {
      throw new IllegalArgumentException();
    }

    //get the base endpoint UIR
    URIBuilder builder = makeBaseURI(requestId);

    //set the path for the URI
    builder.setPath(String.format(HISTORY_ENDPOINT_FORMAT, pipe));

    if (recentSeconds != null)
    {
      builder.setParameter(RECENT_HISTORY_IN_SECONDS,
                           String.valueOf(recentSeconds));
    }

    if (beginMark != null)
    {
      builder.setParameter(HISTORY_BEGIN_MARK, beginMark);
    }

    LOGGER.info("Final History URIBuilder - {}", builder.toString());
    //build the final URI
    return builder.build();
  }

  /**
   * makeHistoryURI - Given a request UUID, and a fully qualified pipe name
   * make a URI for the history reporting
   *
   * @param requestId the label for this request
   * @param pipe the pipe name
   * @param startTimeInclusive Start time inclusive of scan range, in ISO-8601 format.
   *                  Missing millisecond part in string will lead to a zero
   *                  milliseconds. This is a required query parameter, and a
   *                  400 will be returned if this query parameter is missing
   * @param endTimeExclusive End time exclusive of scan range. If this query parameter
   *             is missing or user provided value is later than current millis,
   *             then current millis is used.
   * @return URI for the insert request
   */
  private URI makeHistoryRangeURI(UUID requestId,
                             String pipe,
                             String startTimeInclusive,
                             String endTimeExclusive)
          throws URISyntaxException
  {
    //if the table name is null, we have to abort
    if (pipe == null)
    {
      throw new IllegalArgumentException();
    }

    //get the base endpoint UIR
    URIBuilder builder = makeBaseURI(requestId);

    //set the path for the URI
    builder.setPath(String.format(HISTORY_RANGE_ENDPOINT_FORMAT, pipe));

    if (startTimeInclusive != null)
    {
      builder.setParameter(HISTORY_RANGE_START_INCLUSIVE, startTimeInclusive);
    }

    if (endTimeExclusive != null)
    {
      builder.setParameter(HISTORY_RANGE_END_EXCLUSIVE, endTimeExclusive);
    }

    LOGGER.info("Final History URIBuilder - {}", builder.toString());
    //build the final URI
    return builder.build();
  }
  /**
   * generateFilesJSON - Given a list of files, make some json to represent it
   *
   * @param files the list of files we want to send
   * @return the string json blob
   */
  private String generateFilesJSON(List<StagedFileWrapper> files)
  {
    //if the files argument is null, throw
    if (files == null)
    {
      LOGGER.info("Null files argument in RequestBuilder");
      throw new IllegalArgumentException();
    }

    //create pojo
    IngestRequest pojo = new IngestRequest();
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
   * addUserAgent - adds the user agent header to a request
   *
   * @param request the URI request
   */
  private static void addUserAgent(HttpUriRequest request)
  {
    request.setHeader(HttpHeaders.USER_AGENT, USER_AGENT);
  }

  /**
   * addToken - adds a the JWT token to a request
   *
   * @param request the URI request
   * @param token the token to add
   */
  private static void addToken(HttpUriRequest request, String token)
  {
    request.setHeader(HttpHeaders.AUTHORIZATION, BEARER_PARAMETER + token);
  }

  private static void addHeaders(HttpUriRequest request,
                                 String token)
  {
    addUserAgent(request);
    //add the auth token
    addToken(request, token);
  }

  /**
   * generateInsertRequest - given a table, stage and list of files,
   * make a request for the insert endpoint
   *
   * @param requestId a UUID we will use to label this request
   * @param pipe a fully qualified pipe name
   * @param files a list of files
   * @return a post request with all the data we need
   * @throws URISyntaxException if the URI components provided are improper
   */
  public HttpPost generateInsertRequest(UUID requestId, String pipe,
                                        List<StagedFileWrapper> files)
      throws URISyntaxException
  {
    //make the insert URI
    URI insertURI = makeInsertURI(requestId, pipe);
    LOGGER.info("Created Insert Request : {} ", insertURI);

    //Make the post request
    HttpPost post = new HttpPost(insertURI);

    addHeaders(post, securityManager.getToken());

    //the entity for the containing the json
    final StringEntity entity = new StringEntity(generateFilesJSON(files),
                                                 ContentType.APPLICATION_JSON);
    post.setEntity(entity);

    return post;
  }

  /**
   * generateHistoryRequest - given a requestId and a pipe, make a history request
   *
   * @param requestId a UUID we will use to label this request
   * @param pipe a fully qualified pipe name
   * @param recentSeconds history only for items in the recentSeconds window
   * @param beginMark mark from which history should be fetched
   * @return a get request with all the data we need
   * @throws URISyntaxException - If the URI components provided are improper
   */
  public HttpGet generateHistoryRequest(UUID requestId,
                                        String pipe,
                                        Integer recentSeconds,
                                        String beginMark)
      throws URISyntaxException
  {
    //make the history URI
    URI historyURI = makeHistoryURI(requestId, pipe, recentSeconds, beginMark);

    //make the get request
    HttpGet get = new HttpGet(historyURI);

    addHeaders(get, securityManager.getToken());

    return get;
  }

  /**
  * generateHistoryRangeRequest -
   * given a requestId and a pipe, get history for all ingests between
   *  time ranges start-end
  *
  * @param requestId a UUID we will use to label this request
  * @param pipe a fully qualified pipe name
  * @param startTimeInclusive Start time inclusive of scan range, in ISO-8601 format.
  *                  Missing millisecond part in string will lead to a zero
  *                  milliseconds. This is a required query parameter, and a
  *                  400 will be returned if this query parameter is missing
  * @param endTimeExclusive End time exclusive of scan range. If this query parameter
  *             is missing or user provided value is later than current millis,
  *                           then current millis is used.
  * @return URI for the insert request
  */
  public HttpGet generateHistoryRangeRequest(UUID requestId,
                                             String pipe,
                                             String startTimeInclusive,
                                             String endTimeExclusive)
      throws URISyntaxException
  {
    URI historyRangeURI = makeHistoryRangeURI(requestId,
                                              pipe,
                                              startTimeInclusive,
                                              endTimeExclusive);

    HttpGet get = new HttpGet(historyRangeURI);

    addHeaders(get, securityManager.getToken());

    return get;
  }
}
