/*
 * Copyright (c) 2021-2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.streaming.internal.StreamingIngestUtils.sleepForRetry;
import static net.snowflake.ingest.utils.Constants.COMMIT_MAX_RETRY_COUNT;
import static net.snowflake.ingest.utils.Constants.COMMIT_RETRY_INTERVAL_IN_MS;
import static net.snowflake.ingest.utils.Constants.ENABLE_TELEMETRY_TO_SF;
import static net.snowflake.ingest.utils.Constants.MAX_STREAMING_INGEST_API_CHANNEL_RETRY;
import static net.snowflake.ingest.utils.Constants.RESPONSE_ERR_ENQUEUE_TABLE_CHUNK_QUEUE_FULL;
import static net.snowflake.ingest.utils.Constants.RESPONSE_ERR_GENERAL_EXCEPTION_RETRY_REQUEST;
import static net.snowflake.ingest.utils.Constants.RESPONSE_SUCCESS;
import static net.snowflake.ingest.utils.Constants.SNOWPIPE_STREAMING_JMX_METRIC_PREFIX;
import static net.snowflake.ingest.utils.Constants.SNOWPIPE_STREAMING_JVM_MEMORY_AND_THREAD_METRICS_REGISTRY;
import static net.snowflake.ingest.utils.Constants.SNOWPIPE_STREAMING_SHARED_METRICS_REGISTRY;
import static net.snowflake.ingest.utils.Constants.STREAMING_INGEST_TELEMETRY_UPLOAD_INTERVAL_IN_SEC;
import static net.snowflake.ingest.utils.Constants.USER;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Slf4jReporter;
import com.codahale.metrics.Timer;
import com.codahale.metrics.jmx.JmxReporter;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import net.snowflake.client.core.SFSessionProperty;
import net.snowflake.client.jdbc.internal.apache.http.client.utils.URIBuilder;
import net.snowflake.client.jdbc.internal.apache.http.impl.client.CloseableHttpClient;
import net.snowflake.ingest.connection.IngestResponseException;
import net.snowflake.ingest.connection.OAuthCredential;
import net.snowflake.ingest.connection.RequestBuilder;
import net.snowflake.ingest.connection.TelemetryService;
import net.snowflake.ingest.streaming.DropChannelRequest;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.utils.Constants;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.HttpUtil;
import net.snowflake.ingest.utils.Logging;
import net.snowflake.ingest.utils.Pair;
import net.snowflake.ingest.utils.ParameterProvider;
import net.snowflake.ingest.utils.SFException;
import net.snowflake.ingest.utils.SnowflakeURL;
import net.snowflake.ingest.utils.Utils;

/**
 * The first version of implementation for SnowflakeStreamingIngestClient. The client internally
 * manages a few things:
 * <li>the channel cache, which contains all the channels that belong to this account
 * <li>the flush service, which schedules and coordinates the flush to Snowflake tables
 *
 * @param <T> type of column data ({@link ParquetChunkData})
 */
public class SnowflakeStreamingIngestClientInternal<T> implements SnowflakeStreamingIngestClient {

  private static final Logging logger = new Logging(SnowflakeStreamingIngestClientInternal.class);

  // Counter to generate unique request ids per client
  private final AtomicLong counter = new AtomicLong(0);

  // Provides constant values that can be set by constructor
  private final ParameterProvider parameterProvider;

  // Name of the client
  private final String name;

  // Snowflake role for the client to use
  private String role;

  // Provides constant values which is determined by the Iceberg or non-Iceberg mode
  private final InternalParameterProvider internalParameterProvider;

  // Http client to send HTTP requests to Snowflake
  private final CloseableHttpClient httpClient;

  // Reference to the channel cache
  private final ChannelCache<T> channelCache;

  // Reference to the flush service
  private final FlushService<T> flushService;

  // Reference to storage manager
  private final IStorageManager storageManager;

  // Indicates whether the client has closed
  private volatile boolean isClosed;

  // Indicates wheter the client is streaming to Iceberg tables
  private final boolean isIcebergMode;

  // Indicates whether the client is under test mode
  private final boolean isTestMode;

  // Performance testing related metrics
  MetricRegistry metrics;
  Histogram blobSizeHistogram; // Histogram for blob size after compression
  Histogram blobRowCountHistogram; // Histogram for blob row count
  Histogram cpuHistogram; // Histogram for jvm process cpu usage
  Timer flushLatency; // Latency for end to end flushing
  Timer buildLatency; // Latency for building a blob
  Timer uploadLatency; // Latency for uploading a blob
  Timer registerLatency; // Latency for registering a blob
  Meter uploadThroughput; // Throughput for uploading blobs
  Meter inputThroughput; // Throughput for inserting into the internal buffer

  // JVM and thread related metrics
  MetricRegistry jvmMemoryAndThreadMetrics;

  // The request builder who handles building the HttpRequests we send
  private RequestBuilder requestBuilder;

  // Background thread that uploads telemetry data periodically
  private ScheduledExecutorService telemetryWorker;

  // Snowflake service client to make API calls
  private SnowflakeServiceClient snowflakeServiceClient;

  /**
   * Constructor
   *
   * @param name the name of the client
   * @param accountURL Snowflake account url
   * @param prop connection properties
   * @param httpClient http client for sending request
   * @param isTestMode whether we're under test mode
   * @param isIcebergMode whether we're streaming to Iceberg tables
   * @param requestBuilder http request builder
   * @param parameterOverrides parameters we override in case we want to set different values
   */
  SnowflakeStreamingIngestClientInternal(
      String name,
      SnowflakeURL accountURL,
      Properties prop,
      CloseableHttpClient httpClient,
      boolean isIcebergMode,
      boolean isTestMode,
      RequestBuilder requestBuilder,
      Map<String, Object> parameterOverrides) {
    this.parameterProvider = new ParameterProvider(parameterOverrides, prop, isIcebergMode);
    this.internalParameterProvider = new InternalParameterProvider(isIcebergMode);

    this.name = name;
    String accountName = accountURL == null ? null : accountURL.getAccount();
    this.isIcebergMode = isIcebergMode;
    this.isTestMode = isTestMode;
    this.httpClient = httpClient == null ? HttpUtil.getHttpClient(accountName) : httpClient;
    this.channelCache = new ChannelCache<>();
    this.isClosed = false;
    this.requestBuilder = requestBuilder;

    if (!isTestMode) {
      // Setup request builder for communication with the server side
      this.role = prop.getProperty(Constants.ROLE);

      Object credential = null;

      // Authorization type will be set to jwt by default
      if (prop.getProperty(Constants.AUTHORIZATION_TYPE).equals(Constants.JWT)) {
        try {
          credential =
              Utils.createKeyPairFromPrivateKey(
                  (PrivateKey) prop.get(SFSessionProperty.PRIVATE_KEY.getPropertyKey()));
        } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
          throw new SFException(e, ErrorCode.KEYPAIR_CREATION_FAILURE);
        }
      } else {
        URI oAuthTokenEndpoint;
        try {
          if (prop.getProperty(Constants.OAUTH_TOKEN_ENDPOINT) == null) {
            // Set OAuth token endpoint to Snowflake OAuth by default
            oAuthTokenEndpoint =
                new URIBuilder()
                    .setScheme(accountURL.getScheme())
                    .setHost(accountURL.getUrlWithoutPort())
                    .setPort(accountURL.getPort())
                    .setPath(Constants.SNOWFLAKE_OAUTH_TOKEN_ENDPOINT)
                    .build();
          } else {
            oAuthTokenEndpoint = new URI(prop.getProperty(Constants.OAUTH_TOKEN_ENDPOINT));
          }
        } catch (URISyntaxException e) {
          throw new SFException(e, ErrorCode.INVALID_URL);
        }
        credential =
            new OAuthCredential(
                prop.getProperty(Constants.OAUTH_CLIENT_ID),
                prop.getProperty(Constants.OAUTH_CLIENT_SECRET),
                prop.getProperty(Constants.OAUTH_REFRESH_TOKEN),
                oAuthTokenEndpoint);
      }
      this.requestBuilder =
          new RequestBuilder(
              accountURL,
              prop.get(USER).toString(),
              credential,
              this.httpClient,
              String.format("%s_%s", this.name, System.currentTimeMillis()));

      logger.logInfo("Using {} for authorization", this.requestBuilder.getAuthType());
    }

    if (this.requestBuilder != null) {
      // Setup client telemetries if needed
      this.setupMetricsForClient();
    }

    this.snowflakeServiceClient = new SnowflakeServiceClient(this.httpClient, this.requestBuilder);

    this.storageManager =
        isIcebergMode
            ? new ExternalVolumeManager(
                isTestMode, this.role, this.name, this.snowflakeServiceClient)
            : new InternalStageManager<T>(
                isTestMode, this.role, this.name, this.snowflakeServiceClient);

    try {
      this.flushService =
          new FlushService<>(this, this.channelCache, this.storageManager, this.isTestMode);
    } catch (Exception e) {
      // Need to clean up the resources before throwing any exceptions
      cleanUpResources();
      throw e;
    }

    logger.logInfo(
        "Client created, name={}, account={}. isTestMode={}, parameters={}",
        name,
        accountURL == null ? "" : accountURL.getAccount(),
        isTestMode,
        parameterProvider);
  }

  /**
   * Default Constructor
   *
   * @param name the name of the client
   * @param accountURL Snowflake account url
   * @param prop connection properties
   * @param parameterOverrides map of parameters to override for this client
   * @param isIcebergMode whether we're streaming to Iceberg tables
   * @param isTestMode indicates whether it's under test mode
   */
  public SnowflakeStreamingIngestClientInternal(
      String name,
      SnowflakeURL accountURL,
      Properties prop,
      Map<String, Object> parameterOverrides,
      boolean isIcebergMode,
      boolean isTestMode) {
    this(name, accountURL, prop, null, isIcebergMode, isTestMode, null, parameterOverrides);
  }

  /*** Constructor for TEST ONLY
   *
   * @param name the name of the client
   */
  SnowflakeStreamingIngestClientInternal(String name, boolean isIcebergMode) {
    this(name, null, null, null, isIcebergMode, true, null, new HashMap<>());
  }

  // TESTING ONLY - inject the request builder
  @VisibleForTesting
  public void injectRequestBuilder(RequestBuilder requestBuilder) {
    this.requestBuilder = requestBuilder;
    this.snowflakeServiceClient = new SnowflakeServiceClient(this.httpClient, this.requestBuilder);
  }

  /**
   * Get the client name
   *
   * @return the client name
   */
  @Override
  public String getName() {
    return this.name;
  }

  /**
   * Get the role used by the client
   *
   * @return the client's role
   */
  String getRole() {
    return this.role;
  }

  /** @return a boolean to indicate whether the client is closed or not */
  @Override
  public boolean isClosed() {
    return isClosed;
  }

  /**
   * Open a channel against a Snowflake table
   *
   * @param request the open channel request
   * @return a SnowflakeStreamingIngestChannel object
   */
  @Override
  public SnowflakeStreamingIngestChannelInternal<?> openChannel(OpenChannelRequest request) {
    if (isClosed) {
      throw new SFException(ErrorCode.CLOSED_CLIENT);
    }

    logger.logDebug(
        "Open channel request start, channel={}, table={}, client={}",
        request.getChannelName(),
        request.getFullyQualifiedTableName(),
        getName());

    OpenChannelResponse response = null;
    try {
      OpenChannelRequestInternal openChannelRequest =
          new OpenChannelRequestInternal(
              this.storageManager.getClientPrefix() + "_" + counter.getAndIncrement(),
              this.role,
              request.getDBName(),
              request.getSchemaName(),
              request.getTableName(),
              request.getChannelName(),
              Constants.WriteMode.CLOUD_STORAGE,
              this.isIcebergMode,
              request.getOffsetToken());
      response = snowflakeServiceClient.openChannel(openChannelRequest);
    } catch (IOException | IngestResponseException e) {
      throw new SFException(e, ErrorCode.OPEN_CHANNEL_FAILURE, e.getMessage());
    }

    if (isIcebergMode
        && response.getTableColumns().stream()
            .anyMatch(c -> c.getSourceIcebergDataType() == null)) {
      throw new SFException(
          ErrorCode.INTERNAL_ERROR, "Iceberg table columns must have sourceIcebergDataType set");
    }

    logger.logInfo(
        "Open channel request succeeded, channel={}, table={}, clientSequencer={},"
            + " rowSequencer={}, client={}",
        request.getChannelName(),
        request.getFullyQualifiedTableName(),
        response.getClientSequencer(),
        response.getRowSequencer(),
        getName());

    // Channel is now registered, add it to the in-memory channel pool
    SnowflakeStreamingIngestChannelInternal<T> channel =
        SnowflakeStreamingIngestChannelFactory.<T>builder(response.getChannelName())
            .setDBName(response.getDBName())
            .setSchemaName(response.getSchemaName())
            .setTableName(response.getTableName())
            .setOffsetToken(response.getOffsetToken())
            .setRowSequencer(response.getRowSequencer())
            .setChannelSequencer(response.getClientSequencer())
            .setOwningClient(this)
            .setEncryptionKey(response.getEncryptionKey())
            .setEncryptionKeyId(response.getEncryptionKeyId())
            .setOnErrorOption(request.getOnErrorOption())
            .setDefaultTimezone(request.getDefaultTimezone())
            .setOffsetTokenVerificationFunction(request.getOffsetTokenVerificationFunction())
            .setParquetWriterVersion(
                response.getIcebergSerializationPolicy().getParquetWriterVersion())
            .build();

    // Setup the row buffer schema
    channel.setupSchema(response.getTableColumns());

    // Add channel to the channel cache
    this.channelCache.addChannel(channel);

    this.storageManager.registerTable(
        new TableRef(response.getDBName(), response.getSchemaName(), response.getTableName()),
        response.getIcebergLocationInfo());

    return channel;
  }

  @Override
  public void dropChannel(DropChannelRequest request) {
    if (isClosed) {
      throw new SFException(ErrorCode.CLOSED_CLIENT);
    }

    logger.logDebug(
        "Drop channel request start, channel={}, table={}, client={}",
        request.getChannelName(),
        request.getFullyQualifiedTableName(),
        getName());

    try {
      DropChannelRequestInternal dropChannelRequest =
          new DropChannelRequestInternal(
              this.storageManager.getClientPrefix() + "_" + counter.getAndIncrement(),
              this.role,
              request.getDBName(),
              request.getSchemaName(),
              request.getTableName(),
              request.getChannelName(),
              this.isIcebergMode,
              request instanceof DropChannelVersionRequest
                  ? ((DropChannelVersionRequest) request).getClientSequencer()
                  : null);
      snowflakeServiceClient.dropChannel(dropChannelRequest);

      logger.logInfo(
          "Drop channel request succeeded, channel={}, table={}, clientSequencer={} client={}",
          request.getChannelName(),
          request.getFullyQualifiedTableName(),
          request instanceof DropChannelVersionRequest
              ? ((DropChannelVersionRequest) request).getClientSequencer()
              : null,
          getName());
    } catch (IngestResponseException | IOException e) {
      throw new SFException(e, ErrorCode.DROP_CHANNEL_FAILURE, e.getMessage());
    }
  }

  /**
   * Return the latest committed/persisted offset token for all channels
   *
   * @return map of channel to the latest persisted offset token
   */
  @Override
  public Map<String, String> getLatestCommittedOffsetTokens(
      List<SnowflakeStreamingIngestChannel> channels) {
    List<SnowflakeStreamingIngestChannelInternal<?>> internalChannels =
        channels.stream()
            .map(c -> (SnowflakeStreamingIngestChannelInternal<?>) c)
            .collect(Collectors.toList());
    List<ChannelsStatusResponse.ChannelStatusResponseDTO> channelsStatus =
        getChannelsStatus(internalChannels).getChannels();
    Map<String, String> result = new HashMap<>();
    for (int idx = 0; idx < channels.size(); idx++) {
      result.put(
          channels.get(idx).getFullyQualifiedName(),
          channelsStatus.get(idx).getPersistedOffsetToken());
    }
    return result;
  }

  /**
   * Fetch channels status from Snowflake
   *
   * @param channels a list of channels that we want to get the status on
   * @return a ChannelsStatusResponse object
   */
  ChannelsStatusResponse getChannelsStatus(
      List<SnowflakeStreamingIngestChannelInternal<?>> channels) {
    try {
      ChannelsStatusRequest request = new ChannelsStatusRequest();
      List<ChannelsStatusRequest.ChannelStatusRequestDTO> requestDTOs =
          channels.stream()
              .map(ChannelsStatusRequest.ChannelStatusRequestDTO::new)
              .collect(Collectors.toList());
      request.setChannels(requestDTOs);
      request.setRole(this.role);

      ChannelsStatusResponse response = snowflakeServiceClient.getChannelStatus(request);

      for (int idx = 0; idx < channels.size(); idx++) {
        SnowflakeStreamingIngestChannelInternal<?> channel = channels.get(idx);
        ChannelsStatusResponse.ChannelStatusResponseDTO channelStatus =
            response.getChannels().get(idx);
        if (channelStatus.getStatusCode() != RESPONSE_SUCCESS) {
          String errorMessage =
              String.format(
                  "Channel has failure status_code, name=%s, channel_sequencer=%d, status_code=%d",
                  channel.getFullyQualifiedName(),
                  channel.getChannelSequencer(),
                  channelStatus.getStatusCode());
          logger.logWarn(errorMessage);
          if (getTelemetryService() != null) {
            getTelemetryService()
                .reportClientFailure(this.getClass().getSimpleName(), errorMessage);
          }
        }
      }

      return response;
    } catch (IOException | IngestResponseException e) {
      throw new SFException(e, ErrorCode.CHANNEL_STATUS_FAILURE, e.getMessage());
    }
  }

  /**
   * Register the uploaded blobs to a Snowflake table
   *
   * @param blobs list of uploaded blobs
   */
  void registerBlobs(List<BlobMetadata> blobs) {
    for (List<BlobMetadata> blobBatch : partitionBlobListForRegistrationRequest(blobs)) {
      this.registerBlobs(blobBatch, 0);
    }
  }

  /**
   * Partition the collection of blobs into sub-lists, so that the total number of chunks in each
   * sublist does not exceed the max allowed number of chunks in one registration request.
   */
  List<List<BlobMetadata>> partitionBlobListForRegistrationRequest(List<BlobMetadata> blobs) {
    List<List<BlobMetadata>> result = new ArrayList<>();
    List<BlobMetadata> currentBatch = new ArrayList<>();
    int chunksInCurrentBatch = 0;
    int maxChunksInRegistrationRequest = parameterProvider.getMaxChunksInRegistrationRequest();

    for (BlobMetadata blob : blobs) {
      if (blob.getChunks().size() > maxChunksInRegistrationRequest) {
        throw new SFException(
            ErrorCode.INTERNAL_ERROR,
            String.format(
                "Incorrectly generated blob detected - number of chunks in the blob is larger than"
                    + " the max allowed number of chunks. Please report this bug to Snowflake."
                    + " bdec=%s chunkCount=%d maxAllowedChunkCount=%d",
                blob.getPath(), blob.getChunks().size(), maxChunksInRegistrationRequest));
      }

      if (chunksInCurrentBatch + blob.getChunks().size() > maxChunksInRegistrationRequest) {
        // Newly added BDEC file would exceed the max number of chunks in a single registration
        // request. We put chunks collected so far into the result list and create a new batch with
        // the current blob
        result.add(currentBatch);
        currentBatch = new ArrayList<>();
        currentBatch.add(blob);
        chunksInCurrentBatch = blob.getChunks().size();
      } else {
        // Newly added BDEC can be added to the current batch because it does not exceed the max
        // number of chunks in a single registration request, yet.
        currentBatch.add(blob);
        chunksInCurrentBatch += blob.getChunks().size();
      }
    }

    if (!currentBatch.isEmpty()) {
      result.add(currentBatch);
    }
    return result;
  }

  /**
   * Register the uploaded blobs to a Snowflake table
   *
   * @param blobs list of uploaded blobs
   * @param executionCount Number of times this call has been attempted, used to track retries
   */
  void registerBlobs(List<BlobMetadata> blobs, final int executionCount) {
    logger.logInfo(
        "Register blob request preparing for blob={}, client={}, executionCount={}",
        blobs.stream().map(BlobMetadata::getPath).collect(Collectors.toList()),
        this.name,
        executionCount);

    RegisterBlobResponse response = null;
    try {
      RegisterBlobRequest request =
          new RegisterBlobRequest(
              this.storageManager.getClientPrefix() + "_" + counter.getAndIncrement(),
              this.role,
              blobs,
              this.isIcebergMode);
      response = snowflakeServiceClient.registerBlob(request, executionCount);
    } catch (IOException | IngestResponseException e) {
      throw new SFException(e, ErrorCode.REGISTER_BLOB_FAILURE, e.getMessage());
    }

    logger.logInfo(
        "Register blob request returned for blob={}, client={}, executionCount={}",
        blobs.stream().map(BlobMetadata::getPath).collect(Collectors.toList()),
        this.name,
        executionCount);

    // We will retry any blob chunks that were rejected because internal Snowflake queues are full
    Set<ChunkRegisterStatus> queueFullChunks = new HashSet<>();
    response
        .getBlobsStatus()
        .forEach(
            blobStatus ->
                blobStatus
                    .getChunksStatus()
                    .forEach(
                        chunkStatus ->
                            chunkStatus
                                .getChannelsStatus()
                                .forEach(
                                    channelStatus -> {
                                      if (channelStatus.getStatusCode() != RESPONSE_SUCCESS) {
                                        // If the chunk queue is full, we wait and retry the chunks
                                        if ((channelStatus.getStatusCode()
                                                    == RESPONSE_ERR_ENQUEUE_TABLE_CHUNK_QUEUE_FULL
                                                || channelStatus.getStatusCode()
                                                    == RESPONSE_ERR_GENERAL_EXCEPTION_RETRY_REQUEST)
                                            && executionCount
                                                < MAX_STREAMING_INGEST_API_CHANNEL_RETRY) {
                                          queueFullChunks.add(chunkStatus);
                                        } else {
                                          String errorMessage =
                                              String.format(
                                                  "Channel has been invalidated because of failure"
                                                      + " response, name=%s, channel_sequencer=%d,"
                                                      + " status_code=%d, message=%s,"
                                                      + " executionCount=%d",
                                                  channelStatus.getChannelName(),
                                                  channelStatus.getChannelSequencer(),
                                                  channelStatus.getStatusCode(),
                                                  channelStatus.getMessage(),
                                                  executionCount);
                                          logger.logWarn(errorMessage);
                                          if (getTelemetryService() != null) {
                                            getTelemetryService()
                                                .reportClientFailure(
                                                    this.getClass().getSimpleName(), errorMessage);
                                          }
                                          channelCache.invalidateChannelIfSequencersMatch(
                                              chunkStatus.getDBName(),
                                              chunkStatus.getSchemaName(),
                                              chunkStatus.getTableName(),
                                              channelStatus.getChannelName(),
                                              channelStatus.getChannelSequencer(),
                                              errorMessage);
                                        }
                                      }
                                    })));

    if (!queueFullChunks.isEmpty()) {
      logger.logInfo(
          "Retrying registerBlobs request, blobs={}, retried_chunks={}, executionCount={}",
          blobs,
          queueFullChunks,
          executionCount);
      List<BlobMetadata> retryBlobs = this.getRetryBlobs(queueFullChunks, blobs);
      if (retryBlobs.isEmpty()) {
        throw new SFException(ErrorCode.INTERNAL_ERROR, "Failed to retry queue full chunks");
      }
      sleepForRetry(executionCount);
      this.registerBlobs(retryBlobs, executionCount + 1);
    }
  }

  /**
   * Constructs a new register blobs payload consisting of chunks that were rejected by a prior
   * registration attempt
   *
   * @param queueFullChunks ChunkRegisterStatus values for the chunks that had been rejected
   * @param blobs List<BlobMetadata> from the prior registration call
   * @return a new List<BlobMetadata> for only chunks matching queueFullChunks
   */
  List<BlobMetadata> getRetryBlobs(
      Set<ChunkRegisterStatus> queueFullChunks, List<BlobMetadata> blobs) {
    /*
    If a channel returns a RESPONSE_ERR_ENQUEUE_TABLE_CHUNK_QUEUE_FULL statusCode then all channels in the same chunk
    will have that statusCode.  Here we collect all channels with RESPONSE_ERR_ENQUEUE_TABLE_CHUNK_QUEUE_FULL and use
    them to pull out the chunks to retry from blobs
     */
    Set<Pair<String, Long>> queueFullKeys =
        queueFullChunks.stream()
            .flatMap(
                chunkRegisterStatus -> {
                  return chunkRegisterStatus.getChannelsStatus().stream()
                      .map(
                          channelStatus ->
                              new Pair<String, Long>(
                                  channelStatus.getChannelName(),
                                  channelStatus.getChannelSequencer()));
                })
            .collect(Collectors.toSet());
    List<BlobMetadata> retryBlobs = new ArrayList<>();
    blobs.forEach(
        blobMetadata -> {
          List<ChunkMetadata> relevantChunks =
              blobMetadata.getChunks().stream()
                  .filter(
                      chunkMetadata ->
                          chunkMetadata.getChannels().stream()
                              .map(
                                  channelMetadata ->
                                      new Pair<>(
                                          channelMetadata.getChannelName(),
                                          channelMetadata.getClientSequencer()))
                              .anyMatch(queueFullKeys::contains))
                  .collect(Collectors.toList());
          if (!relevantChunks.isEmpty()) {
            retryBlobs.add(
                BlobMetadata.createBlobMetadata(
                    blobMetadata.getPath(),
                    blobMetadata.getMD5(),
                    blobMetadata.getVersion(),
                    relevantChunks,
                    blobMetadata.getBlobStats(),
                    // Important to not change the spansMixedTables value in case of retries. The
                    // correct value is the value that the already uploaded blob has.
                    blobMetadata.getSpansMixedTables()));
          }
        });

    return retryBlobs;
  }

  /** Close the client, which will flush first and then release all the resources */
  @Override
  public void close() throws Exception {
    if (isClosed) {
      return;
    }

    isClosed = true;
    this.channelCache.closeAllChannels();

    // Flush any remaining rows and cleanup all the resources
    try {
      this.flush(true).get();

      // Report telemetry if needed
      reportStreamingIngestTelemetryToSF();

      // Unregister jmx metrics
      if (this.metrics != null) {
        Slf4jReporter.forRegistry(metrics).outputTo(logger.getLogger()).build().report();
        removeMetricsFromRegistry();
      }

      // LOG jvm memory and thread metrics at the end
      if (this.jvmMemoryAndThreadMetrics != null) {
        Slf4jReporter.forRegistry(jvmMemoryAndThreadMetrics)
            .outputTo(logger.getLogger())
            .build()
            .report();
      }
    } catch (InterruptedException | ExecutionException e) {
      throw new SFException(e, ErrorCode.RESOURCE_CLEANUP_FAILURE, "client close");
    } finally {
      this.flushService.shutdown();
      cleanUpResources();
    }
  }

  /**
   * Flush all data in memory to persistent storage and register with a Snowflake table
   *
   * @param closing whether the flush is called as part of client closing
   * @return future which will be complete when the flush the data is registered
   */
  CompletableFuture<Void> flush(boolean closing) {
    if (isClosed && !closing) {
      throw new SFException(ErrorCode.CLOSED_CLIENT);
    }
    return this.flushService.flush(true);
  }

  /** Set the flag to indicate that a flush is needed */
  void setNeedFlush(String fullyQualifiedTableName) {
    this.flushService.setNeedFlush(fullyQualifiedTableName);
  }

  /** Remove the channel in the channel cache if the channel sequencer matches */
  void removeChannelIfSequencersMatch(SnowflakeStreamingIngestChannelInternal<T> channel) {
    this.channelCache.removeChannelIfSequencersMatch(channel);
  }

  /** Get whether we're running under test mode */
  boolean isTestMode() {
    return this.isTestMode;
  }

  /** Get the http client */
  CloseableHttpClient getHttpClient() {
    return this.httpClient;
  }

  /** Get the request builder */
  RequestBuilder getRequestBuilder() {
    return this.requestBuilder;
  }

  /** Get the channel cache */
  ChannelCache<T> getChannelCache() {
    return this.channelCache;
  }

  /** Get the flush service */
  FlushService<T> getFlushService() {
    return this.flushService;
  }

  /**
   * Check if any channels has uncommitted rows
   *
   * @param channels a list of channels we want to check against
   * @return a list of channels that has uncommitted rows
   */
  List<SnowflakeStreamingIngestChannelInternal<?>> verifyChannelsAreFullyCommitted(
      List<SnowflakeStreamingIngestChannelInternal<?>> channels) {
    if (channels.isEmpty()) {
      return channels;
    }

    // Start checking the status of all the channels in the list
    int retry = 0;
    boolean isTimeout = true;
    List<ChannelsStatusResponse.ChannelStatusResponseDTO> oldChannelsStatus = new ArrayList<>();
    List<SnowflakeStreamingIngestChannelInternal<?>> channelsWithError = new ArrayList<>();
    do {
      List<ChannelsStatusResponse.ChannelStatusResponseDTO> channelsStatus =
          getChannelsStatus(channels).getChannels();
      List<SnowflakeStreamingIngestChannelInternal<?>> tempChannels = new ArrayList<>();
      List<ChannelsStatusResponse.ChannelStatusResponseDTO> tempChannelsStatus = new ArrayList<>();

      for (int idx = 0; idx < channelsStatus.size(); idx++) {
        ChannelsStatusResponse.ChannelStatusResponseDTO channelStatus = channelsStatus.get(idx);
        SnowflakeStreamingIngestChannelInternal<?> channel = channels.get(idx);
        long rowSequencer = channel.getChannelState().getRowSequencer();
        logger.logInfo(
            "Get channel status name={}, status={}, clientSequencer={}, rowSequencer={},"
                + " startOffsetToken={}, endOffsetToken={}, persistedRowSequencer={},"
                + " persistedOffsetToken={}",
            channel.getName(),
            channelStatus.getStatusCode(),
            channel.getChannelSequencer(),
            rowSequencer,
            channel.getChannelState().getStartOffsetToken(),
            channel.getChannelState().getEndOffsetToken(),
            channelStatus.getPersistedRowSequencer(),
            channelStatus.getPersistedOffsetToken());
        if (channelStatus.getStatusCode() != RESPONSE_SUCCESS) {
          channelsWithError.add(channel);
        } else if (!channelStatus.getPersistedRowSequencer().equals(rowSequencer)) {
          tempChannels.add(channel);
          tempChannelsStatus.add(channelStatus);
        }
      }

      // Check whether the server side commit is making progress
      boolean isMakingProgress = tempChannels.size() != channels.size();
      if (!isMakingProgress) {
        for (int idx = 0; idx < channelsStatus.size(); idx++) {
          if (oldChannelsStatus.isEmpty()
              || !channelsStatus
                  .get(idx)
                  .getPersistedRowSequencer()
                  .equals(oldChannelsStatus.get(idx).getPersistedRowSequencer())) {
            isMakingProgress = true;
            break;
          }
        }
      }

      // Break if all the channels are fully committed, otherwise retry and check again
      oldChannelsStatus = tempChannelsStatus;
      channels = tempChannels;
      if (channels.isEmpty()) {
        isTimeout = false;
        break;
      }

      // If we know the commit is making progress, don't increase the retry count
      if (!isMakingProgress) {
        retry++;
      }

      try {
        Thread.sleep(COMMIT_RETRY_INTERVAL_IN_MS);
      } catch (InterruptedException e) {
        throw new SFException(ErrorCode.INTERNAL_ERROR, e.getMessage());
      }
    } while (retry < COMMIT_MAX_RETRY_COUNT);

    if (isTimeout) {
      logger.logWarn(
          "Commit service at server side is not making progress, stop retrying for client={}.",
          this.name);
    }

    channels.addAll(channelsWithError);
    return channels;
  }

  /**
   * Get ParameterProvider with configurable parameters
   *
   * @return ParameterProvider used by the client
   */
  ParameterProvider getParameterProvider() {
    return parameterProvider;
  }

  /**
   * Get InternalParameterProvider with internal parameters
   *
   * @return {@link InternalParameterProvider} used by the client
   */
  InternalParameterProvider getInternalParameterProvider() {
    return internalParameterProvider;
  }

  /**
   * Set refresh token, this method is for refresh token renewal without requiring to restart
   * client. This method only works when the authorization type is OAuth
   *
   * @param refreshToken the new refresh token
   */
  @Override
  public void setRefreshToken(String refreshToken) {
    if (requestBuilder != null) {
      requestBuilder.setRefreshToken(refreshToken);
    }
  }

  /** Return whether the client is streaming to Iceberg tables */
  boolean isIcebergMode() {
    return isIcebergMode;
  }

  /**
   * Registers the performance metrics along with JVM memory and Threads.
   *
   * <p>Latency and throughput metrics are emitted to JMX, jvm memory and thread metrics are logged
   * to Slf4JLogger
   */
  private void setupMetricsForClient() {
    // Start the telemetry background worker if needed
    if (ENABLE_TELEMETRY_TO_SF) {
      this.telemetryWorker = Executors.newSingleThreadScheduledExecutor();
      this.telemetryWorker.scheduleWithFixedDelay(
          this::reportStreamingIngestTelemetryToSF,
          STREAMING_INGEST_TELEMETRY_UPLOAD_INTERVAL_IN_SEC,
          STREAMING_INGEST_TELEMETRY_UPLOAD_INTERVAL_IN_SEC,
          TimeUnit.SECONDS);
    }

    // Register metrics if needed
    metrics = new MetricRegistry();

    if (ENABLE_TELEMETRY_TO_SF || this.parameterProvider.hasEnabledSnowpipeStreamingMetrics()) {
      // CPU usage metric
      cpuHistogram = metrics.histogram(MetricRegistry.name("cpu", "usage", "histogram"));

      // Latency metrics
      flushLatency = metrics.timer(MetricRegistry.name("latency", "flush"));
      buildLatency = metrics.timer(MetricRegistry.name("latency", "build"));
      uploadLatency = metrics.timer(MetricRegistry.name("latency", "upload"));
      registerLatency = metrics.timer(MetricRegistry.name("latency", "register"));

      // Throughput metrics
      uploadThroughput = metrics.meter(MetricRegistry.name("throughput", "upload"));
      inputThroughput = metrics.meter(MetricRegistry.name("throughput", "input"));

      // Blob histogram metrics
      blobSizeHistogram = metrics.histogram(MetricRegistry.name("blob", "size", "histogram"));
      blobRowCountHistogram =
          metrics.histogram(MetricRegistry.name("blob", "row", "count", "histogram"));
    }

    if (this.parameterProvider.hasEnabledSnowpipeStreamingMetrics()) {
      JmxReporter jmxReporter =
          JmxReporter.forRegistry(this.metrics)
              .inDomain(SNOWPIPE_STREAMING_JMX_METRIC_PREFIX)
              .convertDurationsTo(TimeUnit.SECONDS)
              .createsObjectNamesWith(
                  (ignoreMeterType, jmxDomain, metricName) ->
                      getObjectName(this.getName(), jmxDomain, metricName))
              .build();
      jmxReporter.start();

      // Add JVM and thread metrics too
      jvmMemoryAndThreadMetrics = new MetricRegistry();
      jvmMemoryAndThreadMetrics.register(
          MetricRegistry.name("jvm", "memory"), new MemoryUsageGaugeSet());
      jvmMemoryAndThreadMetrics.register(
          MetricRegistry.name("jvm", "threads"), new ThreadStatesGaugeSet());

      SharedMetricRegistries.add(
          SNOWPIPE_STREAMING_JVM_MEMORY_AND_THREAD_METRICS_REGISTRY, jvmMemoryAndThreadMetrics);
    }

    if (metrics.getMetrics().size() != 0) {
      SharedMetricRegistries.add(SNOWPIPE_STREAMING_SHARED_METRICS_REGISTRY, metrics);
    }
  }

  /**
   * This method is called to fetch an object name for all registered metrics. It can be called
   * during registration or unregistration. (Internal implementation of codehale)
   *
   * @param clientName name of the client. Passed in builder
   * @param jmxDomain JMX Domain
   * @param metricName metric name used while registering the metric.
   * @return Object Name constructed from above three args
   */
  private static ObjectName getObjectName(String clientName, String jmxDomain, String metricName) {
    try {
      String sb = jmxDomain + ":clientName=" + clientName + ",name=" + metricName;

      return new ObjectName(sb);
    } catch (MalformedObjectNameException e) {
      logger.logWarn("Could not create Object name for MetricName={}", metricName);
      throw new SFException(ErrorCode.INTERNAL_ERROR, "Invalid metric name");
    }
  }

  /** Unregister all streaming related metrics from registry */
  private void removeMetricsFromRegistry() {
    if (metrics.getMetrics().size() != 0) {
      logger.logDebug("Unregistering all metrics for client={}", this.getName());
      metrics.removeMatching(MetricFilter.startsWith(SNOWPIPE_STREAMING_JMX_METRIC_PREFIX));
      SharedMetricRegistries.remove(SNOWPIPE_STREAMING_SHARED_METRICS_REGISTRY);
    }
  }

  /**
   * Get the Telemetry Service for a given client
   *
   * @return TelemetryService used by the client
   */
  TelemetryService getTelemetryService() {
    return this.requestBuilder == null ? null : requestBuilder.getTelemetryService();
  }

  /** Report streaming ingest related telemetries to Snowflake */
  private void reportStreamingIngestTelemetryToSF() {
    TelemetryService telemetryService = getTelemetryService();
    if (telemetryService != null) {
      telemetryService.reportLatencyInSec(
          this.buildLatency, this.uploadLatency, this.registerLatency, this.flushLatency);
      telemetryService.reportThroughputBytesPerSecond(this.inputThroughput, this.uploadThroughput);
      telemetryService.reportCpuMemoryUsage(this.cpuHistogram);
    }
  }

  /** Cleanup any resource during client closing or failures */
  private void cleanUpResources() {
    if (this.telemetryWorker != null) {
      this.telemetryWorker.shutdown();
    }
    if (this.requestBuilder != null) {
      this.requestBuilder.closeResources();
    }
    if (!this.isTestMode) {
      HttpUtil.shutdownHttpConnectionManagerDaemonThread();
    }
  }
}
