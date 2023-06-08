/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.connection;

import static net.snowflake.ingest.utils.Constants.ENABLE_TELEMETRY_TO_SF;
import static net.snowflake.ingest.utils.Utils.isNullOrEmpty;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.KeyPair;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import net.snowflake.client.jdbc.internal.apache.http.HttpHeaders;
import net.snowflake.client.jdbc.internal.apache.http.client.methods.HttpGet;
import net.snowflake.client.jdbc.internal.apache.http.client.methods.HttpPost;
import net.snowflake.client.jdbc.internal.apache.http.client.methods.HttpUriRequest;
import net.snowflake.client.jdbc.internal.apache.http.client.utils.URIBuilder;
import net.snowflake.client.jdbc.internal.apache.http.entity.ContentType;
import net.snowflake.client.jdbc.internal.apache.http.entity.StringEntity;
import net.snowflake.client.jdbc.internal.apache.http.impl.client.CloseableHttpClient;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;
import net.snowflake.ingest.utils.SnowflakeURL;
import net.snowflake.ingest.utils.StagedFileWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class handles constructing the URIs for our requests as well as putting together the
 * payloads we'll be sending
 *
 * @author obabarinsa
 */
public class RequestBuilder {
  // a logger for all of our needs in this class
  private static final Logger LOGGER = LoggerFactory.getLogger(RequestBuilder.class.getName());

  /* Member variables Begin */

  // the security manager which will handle token generation
  private final SecurityManager securityManager;

  // whatever the actual scheme is
  private String scheme;

  // the actual port number
  private final int port;

  // whatever the actual host is
  private final String host;

  private final String userAgentSuffix;

  // Reference to the telemetry service
  private final TelemetryService telemetryService;

  /* Member variables End */

  /* Static constants Begin */

  // the default host is snowflakecomputing.com
  public static final String DEFAULT_HOST_SUFFIX = "snowflakecomputing.com";

  // the default connection scheme is HTTPS
  private static final String DEFAULT_SCHEME = "https";

  // the default port is 443
  private static final int DEFAULT_PORT = 443;

  // the endpoint format string for inserting files
  private static final String INGEST_ENDPOINT_FORMAT = "/v1/data/pipes/%s/insertFiles";

  // the endpoint for history queries
  private static final String HISTORY_ENDPOINT_FORMAT = "/v1/data/pipes/%s/insertReport";

  // the endpoint for history time range queries
  private static final String HISTORY_RANGE_ENDPOINT_FORMAT = "/v1/data/pipes/%s/loadHistoryScan";

  // the endpoint for configure snowpipe client
  private static final String CONFIGURE_CLIENT_ENDPOINT_FORMAT =
      "/v1/data/pipes/%s/client/configure";

  // the endpoint for get snowpipe client status
  private static final String CLIENT_STATUS_ENDPOINT_FORMAT = "/v1/data/pipes/%s/client/status";

  // optional number of max seconds of items to fetch(eg. in the last hour)
  private static final String RECENT_HISTORY_IN_SECONDS = "recentSeconds";

  // optional. if not null, tells us where to start the next request
  private static final String HISTORY_BEGIN_MARK = "beginMark";

  // Start time for the history range
  private static final String HISTORY_RANGE_START_INCLUSIVE = "startTimeInclusive";

  // End time for the history range; is optional
  private static final String HISTORY_RANGE_END_EXCLUSIVE = "endTimeExclusive";

  // the request id parameter name
  private static final String REQUEST_ID = "requestId";

  // the show skipped files parameter name
  private static final String SHOW_SKIPPED_FILES = "showSkippedFiles";

  // the string name for the HTTP auth bearer
  private static final String BEARER_PARAMETER = "Bearer ";

  // and object mapper for all marshalling and unmarshalling
  private static final ObjectMapper objectMapper = new ObjectMapper();

  private static final String USER_AGENT = getDefaultUserAgent();

  // Don't change!
  public static final String CLIENT_NAME = "SnowpipeJavaSDK";

  public static final String DEFAULT_VERSION = "1.1.5";

  public static final String JAVA_USER_AGENT = "JAVA";

  public static final String OS_INFO_USER_AGENT_FORMAT = "(%s %s %s)";

  public static final String SF_HEADER_AUTHORIZATION_TOKEN_TYPE =
      "X-Snowflake-Authorization-Token-Type";

  public static final String JWT_TOKEN_TYPE = "KEYPAIR_JWT";

  public static final String HTTP_HEADER_CONTENT_TYPE_JSON = "application/json";

  /**
   * RequestBuilder - general usage constructor
   *
   * @param accountName - the name of the Snowflake account to which we're connecting
   * @param userName - the username of the entity loading files
   * @param keyPair - the Public/Private key pair we'll use to authenticate
   */
  public RequestBuilder(String accountName, String userName, KeyPair keyPair) {
    this(accountName, userName, keyPair, DEFAULT_SCHEME, DEFAULT_HOST_SUFFIX, DEFAULT_PORT, null);
  }

  /**
   * RequestBuilder constructor which uses default schemes, host and port.
   *
   * @param accountName - the name of the Snowflake account to which we're connecting
   * @param userName - the username of the entity loading files
   * @param keyPair - the Public/Private key pair we'll use to authenticate
   * @param userAgentSuffix - The suffix part of HTTP Header User-Agent
   */
  public RequestBuilder(
      String accountName,
      String userName,
      String hostName,
      KeyPair keyPair,
      String userAgentSuffix) {
    this(accountName, userName, keyPair, DEFAULT_SCHEME, hostName, DEFAULT_PORT, userAgentSuffix);
  }

  /**
   * Constructor to use if not intended to use userAgentSuffix. i.e. User-Agent HTTP header suffix
   * part is null, (The default one is still valid, check out #defaultUserAgent)
   *
   * @param accountName - the account name to which we're connecting
   * @param userName - for whom are we connecting?
   * @param keyPair - our auth credentials
   * @param schemeName - are we HTTP or HTTPS?
   * @param hostName - the host for this snowflake instance
   * @param portNum - the port number
   */
  public RequestBuilder(
      String accountName,
      String userName,
      KeyPair keyPair,
      String schemeName,
      String hostName,
      int portNum) {
    this(accountName, userName, keyPair, schemeName, hostName, portNum, null);
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
   * @param userAgentSuffix - The suffix part of HTTP Header User-Agent
   */
  public RequestBuilder(
      String accountName,
      String userName,
      KeyPair keyPair,
      String schemeName,
      String hostName,
      int portNum,
      String userAgentSuffix) {
    this(
        accountName, userName, keyPair, schemeName, hostName, portNum, userAgentSuffix, null, null);
  }

  /**
   * RequestBuilder - constructor used by streaming ingest
   *
   * @param url - the Snowflake account to which we're connecting
   * @param userName - the username of the entity loading files
   * @param keyPair - the Public/Private key pair we'll use to authenticate
   * @param httpClient - reference to the http client
   * @param clientName - name of the client, used to uniquely identify a client if used
   */
  public RequestBuilder(
      SnowflakeURL url,
      String userName,
      KeyPair keyPair,
      CloseableHttpClient httpClient,
      String clientName) {
    this(
        url.getAccount(),
        userName,
        keyPair,
        url.getScheme(),
        url.getUrlWithoutPort(),
        url.getPort(),
        null,
        httpClient,
        clientName);
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
   * @param userAgentSuffix - The suffix part of HTTP Header User-Agent
   * @param httpClient - reference to the http client
   * @param clientName - name of the client, used to uniquely identify a client if used
   */
  public RequestBuilder(
      String accountName,
      String userName,
      KeyPair keyPair,
      String schemeName,
      String hostName,
      int portNum,
      String userAgentSuffix,
      CloseableHttpClient httpClient,
      String clientName) {
    // none of these arguments should be null
    if (accountName == null || userName == null || keyPair == null) {
      throw new IllegalArgumentException();
    }

    // Set up the telemetry service if needed
    this.telemetryService =
        ENABLE_TELEMETRY_TO_SF
            ? new TelemetryService(
                httpClient, clientName, schemeName + "://" + hostName + ":" + portNum)
            : null;

    // create our security/token manager
    securityManager = new SecurityManager(accountName, userName, keyPair, telemetryService);

    // stash references to the account and user name as well
    String account = accountName.toUpperCase();
    String user = userName.toUpperCase();

    // save our host, scheme and port info
    this.port = portNum;
    this.scheme = schemeName;
    this.host = hostName;
    this.userAgentSuffix = userAgentSuffix;

    LOGGER.info(
        "Creating a RequestBuilder with arguments : "
            + "Account : {}, User : {}, Scheme : {}, Host : {}, Port : {}, userAgentSuffix: {}",
        account,
        user,
        this.scheme,
        this.host,
        this.port,
        this.userAgentSuffix);
  }

  /**
   * Creates a string for user agent which should always be present in all requests to Snowflake
   * (Snowpipe APIs)
   *
   * <p>Here is the format we will use:
   *
   * <p>SnowpipeJavaSDK/version (platform details) JAVA/<java-version>
   *
   * @return the default agent string
   */
  private static String getDefaultUserAgent() {
    StringBuilder defaultUserAgent = new StringBuilder(CLIENT_NAME + "/" + DEFAULT_VERSION);

    final String osInformation =
        String.format(
            OS_INFO_USER_AGENT_FORMAT,
            System.getProperty("os.name"),
            System.getProperty("os.version"),
            System.getProperty("os.arch"));

    // append osInformation string to user agent
    defaultUserAgent.append(" ");
    defaultUserAgent.append(osInformation);
    defaultUserAgent.append(" ");

    // Add Java Version
    final String javaVersion = System.getProperty("java.version");
    defaultUserAgent.append(JAVA_USER_AGENT + "/").append(javaVersion);
    String userAgent = defaultUserAgent.toString();

    LOGGER.info("Default user agent " + userAgent);
    return userAgent;
  }

  private static String buildCustomUserAgent(String additionalUserAgentInfo) {
    return USER_AGENT.trim() + " " + additionalUserAgentInfo;
  }
  /** A simple POJO for generating our POST body to the insert endpoint */
  private static class IngestRequest {
    // the list of files we're loading
    private final List<StagedFileWrapper> files;

    // additional info passed along with files which includes clientSequencer and offsetToken
    // can be null
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final InsertFilesClientInfo clientInfo;

    /** Constructor used when both files and clientInfo are passed in request */
    public IngestRequest(List<StagedFileWrapper> files, InsertFilesClientInfo clientInfo) {
      this.files = files;
      this.clientInfo = clientInfo;
    }

    /**
     * Ctor used when only files is used in request body.
     *
     * <p>clientInfo will be defaulted to null
     */
    public IngestRequest(List<StagedFileWrapper> files) {
      this(files, null);
    }

    /* Gets the list of files which were added in request */
    public List<StagedFileWrapper> getFiles() {
      return files;
    }

    /* Gets the clientInfo associated with files which was added in request */
    public InsertFilesClientInfo getClientInfo() {
      return clientInfo;
    }
  }

  /**
   * Given a request UUID, construct a URIBuilder for the common parts of any Ingest Service request
   *
   * @param requestId the UUID with which we want to label this request
   * @return a URI builder we can use to finish build the URI
   */
  private URIBuilder makeBaseURI(UUID requestId) {
    // We can't make a request with no id
    if (requestId == null) {
      LOGGER.error("RequestId is null!");
      throw new IllegalArgumentException();
    }

    // construct the builder object
    URIBuilder builder = new URIBuilder();

    // set the scheme
    builder.setScheme(scheme);

    // set the host name
    builder.setHost(host);

    // set the port name
    builder.setPort(port);

    // set the request id
    builder.setParameter(REQUEST_ID, requestId.toString());

    return builder;
  }

  /**
   * makeInsertURI - Given a request UUID, and a fully qualified pipe name make a URI for the Ingest
   * Service inserting
   *
   * @param requestId the UUID we'll use as the label
   * @param pipe the pipe name
   * @param showSkippedFiles a boolean which returns skipped files when set to true
   * @return URI for the insert request
   */
  private URI makeInsertURI(UUID requestId, String pipe, boolean showSkippedFiles)
      throws URISyntaxException {
    // if the pipe name is null, we have to abort
    if (pipe == null) {
      LOGGER.error("Table argument is null");
      throw new IllegalArgumentException();
    }

    // get the base endpoint uri
    URIBuilder builder = makeBaseURI(requestId);

    // set the query parameter to showSkippedFiles
    builder.setParameter(SHOW_SKIPPED_FILES, String.valueOf(showSkippedFiles));

    // add the path for the URI
    builder.setPath(String.format(INGEST_ENDPOINT_FORMAT, pipe));

    // build the final URI
    return builder.build();
  }

  /**
   * makeHistoryURI - Given a request UUID, and a fully qualified pipe name make a URI for the
   * history reporting
   *
   * @param requestId the label for this request
   * @param pipe the pipe name
   * @param recentSeconds history only for items in the recentSeconds window
   * @param beginMark mark from which history should be fetched
   * @return URI for the insert request
   */
  private URI makeHistoryURI(UUID requestId, String pipe, Integer recentSeconds, String beginMark)
      throws URISyntaxException {
    // if the table name is null, we have to abort
    if (pipe == null) {
      throw new IllegalArgumentException();
    }

    // get the base endpoint UIR
    URIBuilder builder = makeBaseURI(requestId);

    // set the path for the URI
    builder.setPath(String.format(HISTORY_ENDPOINT_FORMAT, pipe));

    if (recentSeconds != null) {
      builder.setParameter(RECENT_HISTORY_IN_SECONDS, String.valueOf(recentSeconds));
    }

    if (beginMark != null) {
      builder.setParameter(HISTORY_BEGIN_MARK, beginMark);
    }

    LOGGER.info("Final History URIBuilder - {}", builder.toString());
    // build the final URI
    return builder.build();
  }

  /**
   * makeHistoryURI - Given a request UUID, and a fully qualified pipe name make a URI for the
   * history reporting
   *
   * @param requestId the label for this request
   * @param pipe the pipe name
   * @param startTimeInclusive Start time inclusive of scan range, in ISO-8601 format. Missing
   *     millisecond part in string will lead to a zero milliseconds. This is a required query
   *     parameter, and a 400 will be returned if this query parameter is missing
   * @param endTimeExclusive End time exclusive of scan range. If this query parameter is missing or
   *     user provided value is later than current millis, then current millis is used.
   * @return URI for the insert request
   */
  private URI makeHistoryRangeURI(
      UUID requestId, String pipe, String startTimeInclusive, String endTimeExclusive)
      throws URISyntaxException {
    // if the table name is null, we have to abort
    if (pipe == null) {
      throw new IllegalArgumentException();
    }

    // get the base endpoint UIR
    URIBuilder builder = makeBaseURI(requestId);

    // set the path for the URI
    builder.setPath(String.format(HISTORY_RANGE_ENDPOINT_FORMAT, pipe));

    if (startTimeInclusive != null) {
      builder.setParameter(HISTORY_RANGE_START_INCLUSIVE, startTimeInclusive);
    }

    if (endTimeExclusive != null) {
      builder.setParameter(HISTORY_RANGE_END_EXCLUSIVE, endTimeExclusive);
    }

    LOGGER.info("Final History URIBuilder - {}", builder.toString());
    // build the final URI
    return builder.build();
  }

  /**
   * Given a request UUID, and a fully qualified pipe name make a URI for configure snowpipe client
   * http://snowflakeURL{:PORT}/v1/data/pipes/{pipeName}/client/configure
   *
   * <p>Where snowflake URL can be an old url or new regionless URL.
   *
   * <p>Request Body is Empty
   *
   * <p>And our response looks like:
   *
   * <pre>
   *   {
   *      'clientSequencer': LONG
   *   }
   * </pre>
   *
   * @param requestId the label for this request
   * @param pipe the pipe name
   * @return configure snowpipe client URI
   * @throws URISyntaxException
   */
  private URI makeConfigureClientURI(UUID requestId, String pipe) throws URISyntaxException {
    if (pipe == null) {
      throw new IllegalArgumentException();
    }
    URIBuilder builder = makeBaseURI(requestId);
    builder.setPath(String.format(CONFIGURE_CLIENT_ENDPOINT_FORMAT, pipe));
    LOGGER.info("Final Configure Client URIBuilder - {}", builder);
    return builder.build();
  }

  /**
   * makeGetClientURI - Given a request UUID, and a fully qualified pipe name make a URI for getting
   * snowpipe client http://snowflakeURL{:PORT}/v1/data/pipes/{pipeName}/client/status
   *
   * <p>Where snowflake URL can be an old url or new regionless URL.
   *
   * <p>Request Body is Empty
   *
   * <p>And our response looks like:
   *
   * <pre>
   *   {
   *      'offsetToken': STRING
   *      'clientSequencer': LONG
   *   }
   * </pre>
   *
   * @param requestId the label for this request
   * @param pipe the pipe name
   * @return get client URI
   * @throws URISyntaxException
   */
  private URI makeGetClientURI(UUID requestId, String pipe) throws URISyntaxException {
    if (pipe == null) {
      throw new IllegalArgumentException();
    }
    URIBuilder builder = makeBaseURI(requestId);
    builder.setPath(String.format(CLIENT_STATUS_ENDPOINT_FORMAT, pipe));
    LOGGER.info("Final Get Client URIBuilder - {}", builder);
    return builder.build();
  }

  /**
   * Given a list of files, and an optional clientInfo generate json string which later can be
   * passed in request body of insertFiles API
   *
   * @param files the list of files we want to send
   * @param clientInfo optional clientInfo which can be empty.
   * @return the string json blob
   * @throws IllegalArgumentException if files passed in is null
   */
  private String serializeInsertFilesRequest(
      List<StagedFileWrapper> files, Optional<InsertFilesClientInfo> clientInfo) {
    // if the files argument is null, throw
    if (files == null) {
      LOGGER.info("Null files argument in RequestBuilder");
      throw new IllegalArgumentException();
    }

    // create pojo
    IngestRequest ingestRequest =
        clientInfo
            .map(insertFilesClientInfo -> new IngestRequest(files, insertFilesClientInfo))
            .orElseGet(() -> new IngestRequest(files));

    // serialize to a string
    try {
      return objectMapper.writeValueAsString(ingestRequest);
    }
    // if we have an exception we need to log and throw
    catch (Exception e) {
      LOGGER.error("Unable to Generate JSON Body for Insert request");
      throw new RuntimeException();
    }
  }

  /**
   * Add user agent to the request Header for passed Http request
   *
   * @param request the URI request
   * @param userAgentSuffix adds the user agent header to a request(As a suffix) along with the
   *     default one. If it is null or empty, only default one is used.
   */
  private static void addUserAgent(HttpUriRequest request, String userAgentSuffix) {
    if (!isNullOrEmpty(userAgentSuffix)) {
      final String userAgent = buildCustomUserAgent(userAgentSuffix);
      request.setHeader(HttpHeaders.USER_AGENT, userAgent);
      return;
    }
    request.setHeader(HttpHeaders.USER_AGENT, USER_AGENT);
  }

  /**
   * addToken - adds a the JWT token to a request
   *
   * @param request the URI request
   * @param token the token to add
   */
  private static void addToken(HttpUriRequest request, String token) {
    request.setHeader(HttpHeaders.AUTHORIZATION, BEARER_PARAMETER + token);
    request.setHeader(SF_HEADER_AUTHORIZATION_TOKEN_TYPE, JWT_TOKEN_TYPE);
  }

  private static void addHeaders(HttpUriRequest request, String token, String userAgentSuffix) {
    addUserAgent(request, userAgentSuffix);

    // Add the auth token
    addToken(request, token);

    // Add Accept header
    request.setHeader(HttpHeaders.ACCEPT, HTTP_HEADER_CONTENT_TYPE_JSON);
  }

  /**
   * generateInsertRequest - given a table, stage and list of files, make a request for the insert
   * endpoint
   *
   * @param requestId a UUID we will use to label this request
   * @param pipe a fully qualified pipe name
   * @param files a list of files
   * @param showSkippedFiles a boolean which returns skipped files when set to true
   * @return a post request with all the data we need
   * @throws URISyntaxException if the URI components provided are improper
   */
  public HttpPost generateInsertRequest(
      UUID requestId, String pipe, List<StagedFileWrapper> files, boolean showSkippedFiles)
      throws URISyntaxException {
    return generateInsertRequest(requestId, pipe, files, showSkippedFiles, Optional.empty());
  }

  /**
   * generateInsertRequest - given a pipe, list of files and clientInfo, make a request for the
   * insert endpoint
   *
   * @param requestId a UUID we will use to label this request
   * @param pipe a fully qualified pipe name
   * @param files a list of files
   * @param showSkippedFiles a boolean which returns skipped files when set to true
   * @param clientInfo
   * @return a post request with all the data we need
   * @throws URISyntaxException if the URI components provided are improper
   */
  public HttpPost generateInsertRequest(
      UUID requestId,
      String pipe,
      List<StagedFileWrapper> files,
      boolean showSkippedFiles,
      Optional<InsertFilesClientInfo> clientInfo)
      throws URISyntaxException {
    // make the insert URI
    URI insertURI = makeInsertURI(requestId, pipe, showSkippedFiles);
    LOGGER.info("Created Insert Request : {} ", insertURI);

    // Make the post request
    HttpPost post = new HttpPost(insertURI);

    addHeaders(post, securityManager.getToken(), this.userAgentSuffix);

    // the entity for the containing the json
    final StringEntity entity =
        new StringEntity(
            serializeInsertFilesRequest(files, clientInfo), ContentType.APPLICATION_JSON);
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
   * @throws URISyntaxException - If the URI components provided are improper
   */
  public HttpGet generateHistoryRequest(
      UUID requestId, String pipe, Integer recentSeconds, String beginMark)
      throws URISyntaxException {
    // make the history URI
    URI historyURI = makeHistoryURI(requestId, pipe, recentSeconds, beginMark);

    // make the get request
    HttpGet get = new HttpGet(historyURI);

    addHeaders(get, securityManager.getToken(), this.userAgentSuffix);

    return get;
  }

  /**
   * generateHistoryRangeRequest - given a requestId and a pipe, get history for all ingests between
   * time ranges start-end
   *
   * @param requestId a UUID we will use to label this request
   * @param pipe a fully qualified pipe name
   * @param startTimeInclusive Start time inclusive of scan range, in ISO-8601 format. Missing
   *     millisecond part in string will lead to a zero milliseconds. This is a required query
   *     parameter, and a 400 will be returned if this query parameter is missing
   * @param endTimeExclusive End time exclusive of scan range. If this query parameter is missing or
   *     user provided value is later than current millis, then current millis is used.
   * @return URI for the insert request
   */
  public HttpGet generateHistoryRangeRequest(
      UUID requestId, String pipe, String startTimeInclusive, String endTimeExclusive)
      throws URISyntaxException {
    URI historyRangeURI =
        makeHistoryRangeURI(requestId, pipe, startTimeInclusive, endTimeExclusive);

    HttpGet get = new HttpGet(historyRangeURI);

    addHeaders(get, securityManager.getToken(), this.userAgentSuffix /*User agent information*/);

    return get;
  }

  /**
   * Generate post request for streaming ingest related APIs
   *
   * @param payload POST request payload as string
   * @param endPoint REST API endpoint
   * @param message error message if there are failures during HTTP building
   * @return URI for the POST request
   */
  public HttpPost generateStreamingIngestPostRequest(
      String payload, String endPoint, String message) {
    LOGGER.debug("Generate Snowpipe streaming request: endpoint={}, payload={}", endPoint, payload);
    // Make the corresponding URI
    URI uri = null;
    try {
      uri =
          new URIBuilder().setScheme(scheme).setHost(host).setPort(port).setPath(endPoint).build();
    } catch (URISyntaxException e) {
      throw new SFException(e, ErrorCode.BUILD_REQUEST_FAILURE, message);
    }

    // Make the post request
    HttpPost post = new HttpPost(uri);

    addHeaders(post, securityManager.getToken(), this.userAgentSuffix /*User agent information*/);

    // The entity for the containing the json
    final StringEntity entity = new StringEntity(payload, ContentType.APPLICATION_JSON);
    post.setEntity(entity);

    return post;
  }

  /**
   * Given a requestId and a pipe, make a configure client request
   *
   * @param requestID a UUID we will use to label this request
   * @param pipe a fully qualified pipe name
   * @return configure client request
   * @throws URISyntaxException
   */
  public HttpPost generateConfigureClientRequest(UUID requestID, String pipe)
      throws URISyntaxException {
    URI configureClientURI = makeConfigureClientURI(requestID, pipe);
    HttpPost post = new HttpPost(configureClientURI);
    addHeaders(post, securityManager.getToken(), this.userAgentSuffix);
    return post;
  }

  /**
   * Generate post request for streaming ingest related APIs
   *
   * @param payload POST request payload
   * @param endPoint REST API endpoint
   * @param message error message if there are failures during HTTP building
   * @return URI for the POST request
   */
  public HttpPost generateStreamingIngestPostRequest(
      Map<Object, Object> payload, String endPoint, String message) {
    // Convert the payload to string
    String payloadInString = null;
    try {
      payloadInString = objectMapper.writeValueAsString(payload);
    } catch (JsonProcessingException e) {
      throw new SFException(e, ErrorCode.BUILD_REQUEST_FAILURE, message);
    }

    return this.generateStreamingIngestPostRequest(payloadInString, endPoint, message);
  }

  /**
   * Given a requestId and a pipe, make a get client status request
   *
   * @param requestID UUID
   * @param pipe a fully qualified pipe name
   * @return get client status request
   * @throws URISyntaxException
   */
  public HttpGet generateGetClientStatusRequest(UUID requestID, String pipe)
      throws URISyntaxException {
    URI getClientStatusURI = makeGetClientURI(requestID, pipe);
    HttpGet get = new HttpGet(getClientStatusURI);
    addHeaders(get, securityManager.getToken(), this.userAgentSuffix);
    return get;
  }

  /**
   * Closes the resources being used by RequestBuilder object. {@link SecurityManager} is one such
   * resource which uses a threadpool which needs to be shutdown once SimpleIngestManager is done
   * interacting with Snowpipe Service (Rest APIs)
   *
   * @throws Exception
   */
  public void closeResources() {
    securityManager.close();
    telemetryService.close();
  }

  /** Get the telemetry service */
  public TelemetryService getTelemetryService() {
    return telemetryService;
  }
}
