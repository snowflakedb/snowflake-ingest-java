/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest;

import static net.snowflake.ingest.connection.RequestBuilder.DEFAULT_HOST_SUFFIX;
import static net.snowflake.ingest.utils.Utils.isNullOrEmpty;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.spec.InvalidKeySpecException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import net.snowflake.ingest.connection.ClientStatusResponse;
import net.snowflake.ingest.connection.ConfigureClientResponse;
import net.snowflake.ingest.connection.HistoryRangeResponse;
import net.snowflake.ingest.connection.HistoryResponse;
import net.snowflake.ingest.connection.IngestResponse;
import net.snowflake.ingest.connection.IngestResponseException;
import net.snowflake.ingest.connection.InsertFilesClientInfo;
import net.snowflake.ingest.connection.RequestBuilder;
import net.snowflake.ingest.connection.ServiceResponseHandler;
import net.snowflake.ingest.utils.BackOffException;
import net.snowflake.ingest.utils.HttpUtil;
import net.snowflake.ingest.utils.StagedFileWrapper;
import net.snowflake.ingest.utils.Utils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides a basic, low-level abstraction over the Snowflake Ingest Service REST api
 *
 * <p>Usage of this class delegates all exception and state handling to the developer
 *
 * @author obabarinsa
 */
public class SimpleIngestManager implements AutoCloseable {
  /**
   * This Builder allows someone to configure a SimpleIngestManager prior to instantiating the
   * manager
   *
   * @author obabarinsa
   */
  public static class Builder {

    // the account name we want to use
    private String account;

    // the user who will be loading data
    private String user;

    // the fully qualified pipe name
    private String pipe;

    // the key pair we want to use to authenticate
    private KeyPair keypair;

    // Used to add additional information to User-Agent(Http Header)
    private String userAgentSuffix;

    // Hostname to connect to, default will be RequestBuilder#DEFAULT_HOST
    private String hostName;

    // the role name we want to use to authenticate
    private String role;

    /**
     * getAccount - returns the name of the account this builder will inject into the IngestManager
     *
     * @return account name
     */
    public String getAccount() {
      return account;
    }

    /**
     * setAccount - set the account for the ingest manager and return this builder
     *
     * @param account the account which will be loading into this table
     * @return this builder object
     */
    public Builder setAccount(String account) {
      this.account = account;
      return this;
    }

    /**
     * getUser - get the user who will be loading using the ingest service
     *
     * @return the user name
     */
    public String getUser() {
      return user;
    }

    /**
     * setUser - sets the user who will be loading with the ingest manager
     *
     * @param user the user who will be loading
     * @return the current builder with the user set
     */
    public Builder setUser(String user) {
      this.user = user;
      return this;
    }

    /**
     * getPipe - get the pipe for the ingest manager this builder will create
     *
     * @return the target pipe for this ingest manager
     */
    public String getPipe() {
      return pipe;
    }

    /**
     * setTable - sets the pipe which the SimpleIngestManager will be using
     *
     * @param pipe the target pipe for the ingest manager
     * @return the current builder with the target pipe
     */
    public Builder setPipe(String pipe) {
      this.pipe = pipe;
      return this;
    }

    /**
     * getKeyPair - returns the key-pair we're using for authentication
     *
     * @return the RSA 2048 key-pair we use to sign tokens
     */
    public KeyPair getKeypair() {
      return keypair;
    }

    /**
     * setKeypair - sets the RSA 2048 bit keypair we'll be using for token signing
     *
     * @param keypair the keypair we'll be using for auth
     * @return the current builder with the key set
     */
    public Builder setKeypair(KeyPair keypair) {
      this.keypair = keypair;
      return this;
    }

    /**
     * Get the set user agent suffix. (This is in additional to the default one.)
     *
     * <p>It can be null or empty.
     */
    public String getUserAgentSuffix() {
      return userAgentSuffix;
    }

    /**
     * Sets the name of the role that will be used for authorization. If not set, the default role
     * assigned to the user will be used.
     *
     * @param role the role name we'll be using for auth
     * @return the current builder with the role name set
     */
    public Builder setRole(String role) {
      this.role = role;
      return this;
    }

    /**
     * Get the set role name.
     *
     * <p>It can be null or empty.
     */
    public String getRole() {
      return role;
    }

    /* Sets the user agent suffix as part of this SimpleIngestManager Instance */
    public Builder setUserAgentSuffix(String userAgentSuffix) {
      this.userAgentSuffix = userAgentSuffix;
      return this;
    }

    /* Gets the host name */
    public String getHostName() {
      return hostName;
    }

    /* Sets host name, if not explicitly used, a default will be used. */
    public Builder setHostName(String hostName) {
      this.hostName = hostName;
      return this;
    }

    /**
     * build - returns a new instance of SimpleIngestManager using the information set in this
     * builder object
     */
    public SimpleIngestManager build() {
      if (isNullOrEmpty(hostName)) {
        return new SimpleIngestManager(
            account, user, pipe, DEFAULT_HOST_SUFFIX, keypair, userAgentSuffix, role);
      }
      return new SimpleIngestManager(account, user, pipe, hostName, keypair, userAgentSuffix, role);
    }
  }

  // logger object for this class
  private static final Logger LOGGER = LoggerFactory.getLogger(SimpleIngestManager.class);
  // HTTP Client that we use for sending requests to the service
  private HttpClient httpClient;

  // the account in which the user lives
  private String account;

  // the username of the user
  private String user;

  // the fully qualified name of the pipe
  private String pipe;

  // the keypair we're using for authentication
  private KeyPair keyPair;

  // the request builder who handles building the HttpRequests we send
  private final RequestBuilder builder;

  // ========= Constructors Begin =========

  /**
   * Constructs a SimpleIngestManager for a given user in a specific account In addition, this also
   * takes takes the target table and source stage Finally, it also requires a valid KeyPair object
   * registered with Snowflake DB
   *
   * <p>This method is deprecated, please use the constructor that only requires PrivateKey instead
   * of KeyPair.
   *
   * @param account The account into which we're loading Note: account should not include region or
   *     cloud provider info. e.g. if host is testaccount.us-east-1.azure .snowflakecomputing.com,
   *     account should be testaccount
   * @param user the user performing this load
   * @param pipe the fully qualified name of the pipe
   * @param keyPair the KeyPair we'll use to sign JWT tokens
   */
  @Deprecated
  public SimpleIngestManager(String account, String user, String pipe, KeyPair keyPair) {
    // call our initializer method
    init(account, user, pipe, keyPair);

    // create the request builder
    this.builder = new RequestBuilder(account, user, keyPair);
  }

  /**
   * Constructs a SimpleIngestManager for a given user in a specific account In addition, this also
   * takes takes the target table and source stage Finally, it also requires a valid KeyPair object
   * registered with Snowflake DB
   *
   * <p>This method is deprecated, please use the constructor that only requires PrivateKey instead
   * of KeyPair.
   *
   * @param account the account into which we're loading Note: account should not include region or
   *     cloud provider info. e.g. if host is testaccount.us-east-1.azure .snowflakecomputing.com
   *     account should be testaccount If this is the case, you should use the constructor that
   *     accepts hostname as argument
   * @param user the user performing this load
   * @param pipe the fully qualified name of the pipe
   * @param keyPair the KeyPair we'll use to sign JWT tokens
   * @param schemeName http or https
   * @param hostName the hostname
   * @param port the port number
   */
  @Deprecated
  public SimpleIngestManager(
      String account,
      String user,
      String pipe,
      KeyPair keyPair,
      String schemeName,
      String hostName,
      int port) {
    // call our initializer method
    init(account, user, pipe, keyPair);

    // make the request builder we'll use to build messages to the service
    builder = new RequestBuilder(account, user, keyPair, schemeName, hostName, port);
  }

  /**
   * Constructs a SimpleIngestManager for a given user in a specific account In addition, this also
   * takes takes the target table and source stage Finally, it also requires a valid private key
   * registered with Snowflake DB
   *
   * <p>Note: this method only takes in account parameter and derive the hostname, i.e.
   * testaccount.snowfakecomputing.com. If your deployment is not aws us-west, please use the
   * constructor that accept hostname as argument
   *
   * @param account The account into which we're loading Note: account should not include region or
   *     cloud provider info. e.g. if host is testaccount.us-east-1.azure .snowflakecomputing.com,
   *     account should be testaccount. If this is the case, you should use the constructor that
   *     accepts hostname as argument
   * @param user the user performing this load
   * @param pipe the fully qualified name of the pipe
   * @param privateKey the private key we'll use to sign JWT tokens
   * @throws NoSuchAlgorithmException if can't create key factory by using RSA algorithm
   * @throws InvalidKeySpecException if private key or public key is invalid
   */
  public SimpleIngestManager(String account, String user, String pipe, PrivateKey privateKey)
      throws InvalidKeySpecException, NoSuchAlgorithmException {
    KeyPair keyPair = Utils.createKeyPairFromPrivateKey(privateKey);

    // call our initializer method
    init(account, user, pipe, keyPair);

    // create the request builder
    this.builder = new RequestBuilder(account, user, keyPair);
  }

  /**
   * Using this constructor for Builder pattern. KeyPair can be passed in now since we have
   * made @see {@link Utils#createKeyPairFromPrivateKey(PrivateKey)} public
   *
   * @param account The account into which we're loading Note: account should not include region or
   *     cloud provider info. e.g. if host is testaccount.us-east-1.azure .snowflakecomputing.com,
   *     account should be testaccount. If this is the case, you should use the constructor that
   *     accepts hostname as argument
   * @param user the user performing this load
   * @param pipe the fully qualified name of the pipe
   * @param hostName the hostname
   * @param keyPair keyPair associated with the private key used for authentication. See @see {@link
   *     Utils#createKeyPairFromPrivateKey} to generate KP from p8Key
   * @param userAgentSuffix user agent suffix we want to add.
   */
  public SimpleIngestManager(
      String account,
      String user,
      String pipe,
      String hostName,
      KeyPair keyPair,
      String userAgentSuffix) {
    // call our initializer method
    init(account, user, pipe, keyPair);

    // create the request builder
    this.builder = new RequestBuilder(account, user, hostName, keyPair, userAgentSuffix);
  }

  /**
   * Using this constructor for Builder pattern. KeyPair can be passed in now since we have
   * made @see {@link Utils#createKeyPairFromPrivateKey(PrivateKey)} public
   *
   * @param account The account into which we're loading Note: account should not include region or
   *     cloud provider info. e.g. if host is testaccount.us-east-1.azure .snowflakecomputing.com,
   *     account should be testaccount. If this is the case, you should use the constructor that
   *     accepts hostname as argument
   * @param user the user performing this load
   * @param pipe the fully qualified name of the pipe
   * @param hostName the hostname
   * @param keyPair keyPair associated with the private key used for authentication. See @see {@link
   *     Utils#createKeyPairFromPrivateKey} to generate KP from p8Key
   * @param userAgentSuffix user agent suffix we want to add.
   */
  public SimpleIngestManager(
      String account,
      String user,
      String pipe,
      String hostName,
      KeyPair keyPair,
      String userAgentSuffix,
      String role) {
    // call our initializer method
    init(account, user, pipe, keyPair);

    // create the request builder
    this.builder = new RequestBuilder(account, user, hostName, keyPair, userAgentSuffix, role);
  }

  /**
   * Constructs a SimpleIngestManager for a given user in a specific account In addition, this also
   * takes takes the target table and source stage Finally, it also requires a valid private key
   * registered with Snowflake DB
   *
   * @param account the account into which we're loading Note: account should not include region or
   *     cloud provider info. e.g. if host is testaccount.us-east-1.azure .snowflakecomputing.com,
   *     account should be testaccount
   * @param user the user performing this load
   * @param pipe the fully qualified name of the pipe
   * @param privateKey the private key we'll use to sign JWT tokens
   * @param schemeName http or https
   * @param hostName the hostname i.e. testaccount.us-east-1.azure .snowflakecomputing.com
   * @param port the port number
   * @throws NoSuchAlgorithmException if can't create key factory by using RSA algorithm
   * @throws InvalidKeySpecException if private key or public key is invalid
   */
  public SimpleIngestManager(
      String account,
      String user,
      String pipe,
      PrivateKey privateKey,
      String schemeName,
      String hostName,
      int port)
      throws NoSuchAlgorithmException, InvalidKeySpecException {
    KeyPair keyPair = Utils.createKeyPairFromPrivateKey(privateKey);
    // call our initializer method
    init(account, user, pipe, keyPair);

    // make the request builder we'll use to build messages to the service
    builder = new RequestBuilder(account, user, keyPair, schemeName, hostName, port);
  }

  /* Another flavor of constructor which supports userAgentSuffix */
  public SimpleIngestManager(
      String account,
      String user,
      String pipe,
      PrivateKey privateKey,
      String schemeName,
      String hostName,
      int port,
      String userAgentSuffix)
      throws NoSuchAlgorithmException, InvalidKeySpecException {
    KeyPair keyPair = Utils.createKeyPairFromPrivateKey(privateKey);
    // call our initializer method
    init(account, user, pipe, keyPair);

    // make the request builder we'll use to build messages to the service
    builder =
        new RequestBuilder(account, user, keyPair, schemeName, hostName, port, userAgentSuffix);
  }

  /* Another flavor of constructor which supports userAgentSuffix and role */
  public SimpleIngestManager(
      String account,
      String user,
      String pipe,
      PrivateKey privateKey,
      String schemeName,
      String hostName,
      int port,
      String userAgentSuffix,
      String role)
      throws NoSuchAlgorithmException, InvalidKeySpecException {
    KeyPair keyPair = Utils.createKeyPairFromPrivateKey(privateKey);
    // call our initializer method
    init(account, user, pipe, keyPair);

    // make the request builder we'll use to build messages to the service
    builder =
        new RequestBuilder(
            account, user, keyPair, schemeName, hostName, port, userAgentSuffix, role);
  }

  // ========= Constructors End =========

  /**
   * init - Does the basic work of constructing a SimpleIngestManager that is common across all
   * constructors
   *
   * @param account The account into which we're loading
   * @param user the user performing this load
   * @param pipe the fully qualified name of pipe
   * @param keyPair the KeyPair we'll use to sign JWT tokens
   */
  private void init(String account, String user, String pipe, KeyPair keyPair) {
    // set up our reference variables
    this.account = account;
    this.user = user;
    this.pipe = pipe;
    this.keyPair = keyPair;

    // make our client for sending requests
    httpClient = HttpUtil.getHttpClient();
    // make the request builder we'll use to build messages to the service
  }

  /**
   * getAccount - Gives back the name of the account that this IngestManager is targeting
   *
   * @return the name of the account
   */
  public String getAccount() {
    return account;
  }

  /**
   * getUser - gives back the user on behalf of which this ingest manager is loading
   *
   * @return the user name
   */
  public String getUser() {
    return user;
  }

  /**
   * getPipe - gives back the pipe which we are using
   *
   * @return the pipe name
   */
  public String getPipe() {
    return pipe;
  }

  /**
   * wrapFilepaths - convenience method to take a list of filenames and produce a list of
   * FileWrappers with unset size
   *
   * @param filenames the filenames you want to wrap up
   * @return a corresponding list of StagedFileWrapper objects
   */
  public static List<StagedFileWrapper> wrapFilepaths(Set<String> filenames) {
    // if we get a null, throw
    if (filenames == null) {
      throw new IllegalArgumentException();
    }

    return filenames.parallelStream()
        .map(fname -> new StagedFileWrapper(fname, null))
        .collect(Collectors.toList());
  }

  /**
   * ingestFile - ingest a single file
   *
   * @param file - a wrapper around a filename and size
   * @param requestId - a requestId that we'll use to label - if null, we generate one for the user
   * @return an insert response from the server
   * @throws URISyntaxException - if the provided account name was illegal and caused a URI
   *     construction failure
   * @throws IOException - if we have some other network failure
   * @throws IngestResponseException - if snowflake encountered error during ingest
   * @throws BackOffException - if we have a 503 response
   */
  public IngestResponse ingestFile(StagedFileWrapper file, UUID requestId)
      throws URISyntaxException, IOException, IngestResponseException, BackOffException {
    return ingestFiles(Collections.singletonList(file), requestId);
  }

  /**
   * ingestFile - ingest a single file
   *
   * @param file - a wrapper around a filename and size
   * @param requestId - a requestId that we'll use to label - if null, we generate one for the user
   * @param showSkippedFiles - a flag which returns the files that were skipped when set to true.
   * @return an insert response from the server
   * @throws URISyntaxException - if the provided account name was illegal and caused a URI
   *     construction failure
   * @throws IOException - if we have some other network failure
   * @throws IngestResponseException - if snowflake encountered error during ingest
   * @throws BackOffException - if we have a 503 response
   */
  public IngestResponse ingestFile(StagedFileWrapper file, UUID requestId, boolean showSkippedFiles)
      throws URISyntaxException, IOException, IngestResponseException, BackOffException {
    return ingestFiles(Collections.singletonList(file), requestId, showSkippedFiles);
  }

  /**
   * ingestFiles - synchronously sends a request to the ingest service to enqueue these files
   *
   * @param files - list of wrappers around filenames and sizes
   * @param requestId - a requestId that we'll use to label - if null, we generate one for the user
   * @return an insert response from the server
   * @throws URISyntaxException - if the provided account name was illegal and caused a URI
   *     construction failure
   * @throws IOException - if we have some other network failure
   * @throws IngestResponseException - if snowflake encountered error during ingest
   * @throws BackOffException - if we have a 503 response
   */
  public IngestResponse ingestFiles(List<StagedFileWrapper> files, UUID requestId)
      throws URISyntaxException, IOException, IngestResponseException, BackOffException {
    return ingestFiles(files, requestId, false);
  }

  /**
   * ingestFiles - synchronously sends a request to the ingest service to enqueue these files
   *
   * @param files - list of wrappers around filenames and sizes
   * @param requestId - a requestId that we'll use to label - if null, we generate one for the user
   * @param showSkippedFiles - a flag which returns the files that were skipped when set to true.
   * @return an insert response from the server
   * @throws URISyntaxException - if the provided account name was illegal and caused a URI
   *     construction failure
   * @throws IOException - if we have some other network failure
   * @throws IngestResponseException - if snowflake encountered error during ingest
   * @throws BackOffException - if we have a 503 response
   */
  public IngestResponse ingestFiles(
      List<StagedFileWrapper> files, UUID requestId, boolean showSkippedFiles)
      throws URISyntaxException, IOException, IngestResponseException, BackOffException {
    return ingestFiles(files, requestId, showSkippedFiles, null /* Client info is null */);
  }

  /**
   * ingestFiles With Client Info - synchronously sends a request to the ingest service to enqueue
   * these files along with clientSequencer and offSetToken.
   *
   * <p>OffsetToken will be atomically persisted on server(Snowflake) side along with files if the
   * clientSequencer added in this request matches with what Snowflake currently has.
   *
   * <p>If clientSequencers doesnt match, 400 response code is sent back and no files will be added.
   *
   * @param files - list of wrappers around filenames and sizes
   * @param requestId - a requestId that we'll use to label - if null, we generate one for the user
   * @param showSkippedFiles - a flag which returns the files that were skipped when set to true.
   * @param clientInfo - clientSequencer and offsetToken to pass along with files. Can be null.
   * @return an insert response from the server
   * @throws URISyntaxException - if the provided account name was illegal and caused a URI
   *     construction failure
   * @throws IOException - if we have some other network failure
   * @throws IngestResponseException - if snowflake encountered error during ingest
   * @throws BackOffException - if we have a 503 response
   */
  public IngestResponse ingestFiles(
      List<StagedFileWrapper> files,
      UUID requestId,
      boolean showSkippedFiles,
      InsertFilesClientInfo clientInfo)
      throws URISyntaxException, IOException, IngestResponseException, BackOffException {

    // the request id we want to send with this payload
    if (requestId == null || requestId.toString().isEmpty()) {
      requestId = UUID.randomUUID();
    }

    HttpPost httpPostForIngestFile =
        builder.generateInsertRequest(
            requestId, pipe, files, showSkippedFiles, Optional.ofNullable(clientInfo));

    // send the request and get a response....
    HttpResponse response = httpClient.execute(httpPostForIngestFile);

    LOGGER.info(
        "Attempting to unmarshall insert response - {}, with clientInfo - {}",
        response,
        clientInfo);
    return ServiceResponseHandler.unmarshallIngestResponse(response, requestId);
  }

  /**
   * Pings the service to see the current ingest history for this table
   *
   * @param requestId a UUID we use to label the request, if null, one is generated for the user
   * @param recentSeconds history only for items in the recentSeconds window
   * @param beginMark mark from which history should be fetched
   * @return a response showing the available ingest history from the service
   * @throws URISyntaxException - if the provided account name was illegal and caused a URI
   *     construction failure
   * @throws IOException - if we have some other network failure
   * @throws IngestResponseException - if snowflake encountered error during ingest
   * @throws BackOffException - if we have a 503 response
   */
  public HistoryResponse getHistory(UUID requestId, Integer recentSeconds, String beginMark)
      throws URISyntaxException, IOException, IngestResponseException, BackOffException {
    // if we have no requestId generate one
    if (requestId == null || requestId.toString().isEmpty()) {
      requestId = UUID.randomUUID();
    }

    HttpGet httpGetHistory =
        builder.generateHistoryRequest(requestId, pipe, recentSeconds, beginMark);

    // send the request and get a response...
    HttpResponse response = httpClient.execute(httpGetHistory);

    LOGGER.info("Attempting to unmarshall history response - {}", response);
    return ServiceResponseHandler.unmarshallHistoryResponse(response, requestId);
  }

  /**
   * Pings the service to see the current ingest history for this table
   *
   * @param requestId a UUID we use to label the request, if null, one is generated for the user
   * @param startTimeInclusive Start time inclusive of scan range, in ISO-8601 format. Missing
   *     millisecond part in string will lead to a zero milliseconds. This is a required query
   *     parameter, and a 400 will be returned if this query parameter is missing
   * @param endTimeExclusive End time exclusive of scan range. If this query parameter is missing or
   *     user provided value is later than current millis, then current millis is used.
   * @return a response showing the available ingest history from the service
   * @throws URISyntaxException if the provided account name was illegal and caused a URI
   *     construction failure
   * @throws IOException - if we have some other network failure
   * @throws IngestResponseException - if snowflake encountered error during ingest
   * @throws BackOffException if we have a 503 response
   */
  public HistoryRangeResponse getHistoryRange(
      UUID requestId, String startTimeInclusive, String endTimeExclusive)
      throws URISyntaxException, IOException, IngestResponseException, BackOffException {
    if (requestId == null || requestId.toString().isEmpty()) {
      requestId = UUID.randomUUID();
    }

    HttpResponse response =
        httpClient.execute(
            builder.generateHistoryRangeRequest(
                requestId, pipe, startTimeInclusive, endTimeExclusive));

    LOGGER.info("Attempting to unmarshall history range response - {}", response);
    return ServiceResponseHandler.unmarshallHistoryRangeResponse(response, requestId);
  }

  /**
   * Register a snowpipe client and returns the client sequencer
   *
   * @param requestId a UUID we use to label the request, if null, one is generated for the user
   * @return
   * @throws URISyntaxException - if the provided account name was illegal and caused a URI
   *     construction failure
   * @throws IOException - if we have some other network failure
   * @throws IngestResponseException - if snowflake encountered error during ingest
   * @throws BackOffException - if we have a 503 response
   */
  public ConfigureClientResponse configureClient(UUID requestId)
      throws URISyntaxException, IOException, IngestResponseException, BackOffException {
    if (requestId == null || requestId.toString().isEmpty()) {
      requestId = UUID.randomUUID();
    }
    HttpResponse response =
        httpClient.execute(builder.generateConfigureClientRequest(requestId, pipe));
    LOGGER.info("Attempting to unmarshall configure client response - {}", response);
    return ServiceResponseHandler.unmarshallConfigureClientResponse(response, requestId);
  }

  /**
   * Get client status for snowpipe which contains offset token and client sequencer
   *
   * @param requestId a UUID we use to label the request, if null, one is generated for the user
   * @return
   * @throws URISyntaxException - if the provided account name was illegal and caused a URI
   *     construction failure
   * @throws IOException - if we have some other network failure
   * @throws IngestResponseException - if snowflake encountered error during ingest
   * @throws BackOffException - if we have a 503 response
   */
  public ClientStatusResponse getClientStatus(UUID requestId)
      throws URISyntaxException, IOException, IngestResponseException, BackOffException {
    if (requestId == null || requestId.toString().isEmpty()) {
      requestId = UUID.randomUUID();
    }
    HttpResponse response =
        httpClient.execute(builder.generateGetClientStatusRequest(requestId, pipe));
    LOGGER.info("Attempting to unmarshall get client status response - {}", response);
    return ServiceResponseHandler.unmarshallGetClientStatus(response, requestId);
  }

  /**
   * Closes the resources associated with this object. Resources cannot be reopened, initialize new
   * instance of this class {@link SimpleIngestManager} to reopen and start ingesting/monitoring new
   * data.
   */
  @Override
  public void close() {
    builder.closeResources();
  }

  /* Used for testing */
  public RequestBuilder getRequestBuilder() {
    return this.builder;
  }
}
