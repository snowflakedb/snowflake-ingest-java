/*
 * Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.connection;

import static net.snowflake.ingest.utils.Constants.ENABLE_TELEMETRY_TO_SF;
import static net.snowflake.ingest.utils.Utils.isNullOrEmpty;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.KeyPair;
import java.util.List;
import java.util.Map;
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

  public static final String DEFAULT_VERSION = "3.0.1";

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
   * @param credential - the credential we'll use to authenticate
   */
  @Deprecated
  public RequestBuilder(String accountName, String userName, Object credential) {
    this(
        accountName, userName, credential, DEFAULT_SCHEME, DEFAULT_HOST_SUFFIX, DEFAULT_PORT, null);
  }

  /**
   * RequestBuilder constructor which uses default schemes, host and port.
   *
   * @param accountName - the name of the Snowflake account to which we're connecting
   * @param userName - the username of the entity loading files
   * @param credential - the credential we'll use to authenticate
   * @param userAgentSuffix - The suffix part of HTTP Header User-Agent
   */
  @Deprecated
  public RequestBuilder(
      String accountName,
      String userName,
      String hostName,
      Object credential,
      String userAgentSuffix) {
    this(
        accountName, userName, credential, DEFAULT_SCHEME, hostName, DEFAULT_PORT, userAgentSuffix);
  }

  /**
   * Constructor to use if not intended to use userAgentSuffix. i.e. User-Agent HTTP header suffix
   * part is null, (The default one is still valid, check out #defaultUserAgent)
   *
   * @param accountName - the account name to which we're connecting
   * @param userName - for whom are we connecting?
   * @param credential - the credential we'll use to authenticate
   * @param schemeName - are we HTTP or HTTPS?
   * @param hostName - the host for this snowflake instance
   * @param portNum - the port number
   */
  @Deprecated
  public RequestBuilder(
      String accountName,
      String userName,
      Object credential,
      String schemeName,
      String hostName,
      int portNum) {
    this(accountName, userName, credential, schemeName, hostName, portNum, null);
  }

  /**
   * RequestBuilder - this constructor is for testing purposes only
   *
   * @param accountName - the account name to which we're connecting
   * @param userName - for whom are we connecting?
   * @param credential - the credential we'll use to authenticate
   * @param schemeName - are we HTTP or HTTPS?
   * @param hostName - the host for this snowflake instance
   * @param portNum - the port number
   * @param userAgentSuffix - The suffix part of HTTP Header User-Agent
   */
  @Deprecated
  public RequestBuilder(
      String accountName,
      String userName,
      Object credential,
      String schemeName,
      String hostName,
      int portNum,
      String userAgentSuffix) {
    this(
        accountName,
        userName,
        credential,
        schemeName,
        hostName,
        portNum,
        userAgentSuffix,
        null /* securityManager */,
        null /* httpClient */,
        false /* enableIcebergStreaming */,
        null /* clientName */);
  }

  /**
   * RequestBuilder - constructor used by streaming ingest
   *
   * @param url - the Snowflake account to which we're connecting
   * @param userName - the username of the entity loading files
   * @param credential - the credential we'll use to authenticate
   * @param httpClient - reference to the http client
   * @param enableIcebergStreaming whether the client is running in iceberg mode
   * @param clientName - name of the client, used to uniquely identify a client if used
   */
  public RequestBuilder(
      SnowflakeURL url,
      String userName,
      Object credential,
      CloseableHttpClient httpClient,
      boolean enableIcebergStreaming,
      String clientName) {
    this(
        url.getAccount(),
        userName,
        credential,
        url.getScheme(),
        url.getUrlWithoutPort(),
        url.getPort(),
        null /* userAgentSuffix */,
        null /* securityManager */,
        httpClient,
        enableIcebergStreaming,
        clientName);
  }

  /**
   * RequestBuilder - this constructor is for testing purposes only
   *
   * @param accountName - the account name to which we're connecting
   * @param userName - for whom are we connecting?
   * @param credential - our auth credentials, either JWT key pair or OAuth credential
   * @param schemeName - are we HTTP or HTTPS?
   * @param hostName - the host for this snowflake instance
   * @param portNum - the port number
   * @param userAgentSuffix - The suffix part of HTTP Header User-Agent
   * @param securityManager - The security manager for authentication
   * @param httpClient - reference to the http client
   * @param enableIcebergStreaming whether the client is running in iceberg mode
   * @param clientName - name of the client, used to uniquely identify a client if used
   */
  public RequestBuilder(
      String accountName,
      String userName,
      Object credential,
      String schemeName,
      String hostName,
      int portNum,
      String userAgentSuffix,
      SecurityManager securityManager,
      CloseableHttpClient httpClient,
      boolean enableIcebergStreaming,
      String clientName) {
    // none of these arguments should be null
    if (accountName == null || userName == null || credential == null) {
      throw new IllegalArgumentException();
    }

    // Set up the telemetry service if needed
    // TODO: SNOW-854272 Support telemetry service when using OAuth authentication
    this.telemetryService =
        ENABLE_TELEMETRY_TO_SF && credential instanceof KeyPair
            ? new TelemetryService(
                httpClient,
                enableIcebergStreaming,
                clientName,
                schemeName + "://" + hostName + ":" + portNum)
            : null;

    // stash references to the account and username as well
    String account = accountName.toUpperCase();
    String user = userName.toUpperCase();

    // save our host, scheme and port info
    this.port = portNum;
    this.scheme = schemeName;
    this.host = hostName;
    this.userAgentSuffix = userAgentSuffix;

    // create our security/token manager
    if (securityManager == null) {
      if (credential instanceof KeyPair) {
        this.securityManager =
            new JWTManager(accountName, userName, (KeyPair) credential, telemetryService);
      } else if (credential instanceof OAuthCredential) {
        this.securityManager =
            new OAuthManager(
                accountName,
                userName,
                (OAuthCredential) credential,
                makeBaseURI(),
                telemetryService);
      } else {
        throw new IllegalArgumentException(
            "Credential should be instance of either KeyPair or OAuthCredential");
      }
    } else {
      this.securityManager = securityManager;
    }

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

    /**
     * Ctor used when only files is used in request body.
     *
     * <p>clientInfo will be defaulted to null
     */
    public IngestRequest(List<StagedFileWrapper> files) {
      this.files = files;
    }

    /* Gets the list of files which were added in request */
    public List<StagedFileWrapper> getFiles() {
      return files;
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

    // construct the builder object without param
    URIBuilder builder = makeBaseURI();

    // set the request id
    builder.setParameter(REQUEST_ID, requestId.toString());

    return builder;
  }

  /** Construct a URIBuilder for common parts of request without any parameter */
  private URIBuilder makeBaseURI() {
    // construct the builder object
    URIBuilder builder = new URIBuilder();

    // set the scheme
    builder.setScheme(scheme);

    // set the host name
    builder.setHost(host);

    // set the port name
    builder.setPort(port);

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
   * Given a list of files, generate a json string which later can be passed in request body of
   * insertFiles API
   *
   * @param files the list of files we want to send
   * @return the string json blob
   * @throws IllegalArgumentException if files passed in is null
   */
  private String serializeInsertFilesRequest(List<StagedFileWrapper> files) {
    // if the files argument is null, throw
    if (files == null) {
      LOGGER.info("Null files argument in RequestBuilder");
      throw new IllegalArgumentException();
    }

    // create pojo
    IngestRequest ingestRequest = new IngestRequest(files);

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
   * addToken - adds a token to a request
   *
   * @param request the URI request
   */
  public void addToken(HttpUriRequest request) {
    request.setHeader(HttpHeaders.AUTHORIZATION, BEARER_PARAMETER + securityManager.getToken());
    request.setHeader(SF_HEADER_AUTHORIZATION_TOKEN_TYPE, this.securityManager.getTokenType());
  }

  /**
   * addHeader - adds necessary header to a request
   *
   * @param request the URI request
   * @param userAgentSuffix user agent suffix
   */
  private void addHeaders(HttpUriRequest request, String userAgentSuffix) {
    addUserAgent(request, userAgentSuffix);

    // Add the auth token
    addToken(request);

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
    // make the insert URI
    URI insertURI = makeInsertURI(requestId, pipe, showSkippedFiles);
    LOGGER.info("Created Insert Request : {} ", insertURI);

    // Make the post request
    HttpPost post = new HttpPost(insertURI);

    addHeaders(post, this.userAgentSuffix);

    // the entity for the containing the json
    final StringEntity entity =
        new StringEntity(serializeInsertFilesRequest(files), ContentType.APPLICATION_JSON);
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

    addHeaders(get, this.userAgentSuffix);

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

    addHeaders(get, this.userAgentSuffix /*User agent information*/);

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
    final String requestId = UUID.randomUUID().toString();
    LOGGER.debug(
        "Generate Snowpipe streaming request: endpoint={}, payload={}, requestId={}",
        endPoint,
        payload,
        requestId);
    // Make the corresponding URI
    URI uri = null;
    try {
      uri =
          new URIBuilder()
              .setScheme(scheme)
              .setHost(host)
              .setPort(port)
              .setPath(endPoint)
              .setParameter(REQUEST_ID, requestId)
              .build();
    } catch (URISyntaxException e) {
      throw new SFException(e, ErrorCode.BUILD_REQUEST_FAILURE, message);
    }

    // Make the post request
    HttpPost post = new HttpPost(uri);

    addHeaders(post, this.userAgentSuffix /*User agent information*/);

    // The entity for the containing the json
    final StringEntity entity = new StringEntity(payload, ContentType.APPLICATION_JSON);
    post.setEntity(entity);

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
   * Set refresh token, this method is for refresh token renewal without requiring to restart
   * client. This method only works when the authorization type is OAuth
   *
   * @param refreshToken the new refresh token
   */
  public void setRefreshToken(String refreshToken) {
    if (securityManager instanceof OAuthManager) {
      ((OAuthManager) securityManager).setRefreshToken(refreshToken);
    }
  }

  /** Get authorization type */
  public String getAuthType() {
    return securityManager.getTokenType();
  }

  /** Refresh token manually */
  public void refreshToken() {
    securityManager.refreshToken();
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
    if (telemetryService != null) {
      telemetryService.close();
    }
  }

  /** Get the telemetry service */
  public TelemetryService getTelemetryService() {
    return telemetryService;
  }
}
