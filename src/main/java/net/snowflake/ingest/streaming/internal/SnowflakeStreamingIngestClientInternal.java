/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.connection.ServiceResponseHandler.ApiName.STREAMING_CHANNEL_STATUS;
import static net.snowflake.ingest.connection.ServiceResponseHandler.ApiName.STREAMING_OPEN_CHANNEL;
import static net.snowflake.ingest.connection.ServiceResponseHandler.ApiName.STREAMING_REGISTER_BLOB;
import static net.snowflake.ingest.streaming.internal.StreamingIngestUtils.executeWithRetries;
import static net.snowflake.ingest.streaming.internal.StreamingIngestUtils.sleepForRetry;
import static net.snowflake.ingest.utils.Constants.CHANNEL_STATUS_ENDPOINT;
import static net.snowflake.ingest.utils.Constants.COMMIT_MAX_RETRY_COUNT;
import static net.snowflake.ingest.utils.Constants.COMMIT_RETRY_INTERVAL_IN_MS;
import static net.snowflake.ingest.utils.Constants.ENABLE_TELEMETRY_TO_SF;
import static net.snowflake.ingest.utils.Constants.JDBC_PRIVATE_KEY;
import static net.snowflake.ingest.utils.Constants.MAX_STREAMING_INGEST_API_CHANNEL_RETRY;
import static net.snowflake.ingest.utils.Constants.OPEN_CHANNEL_ENDPOINT;
import static net.snowflake.ingest.utils.Constants.REGISTER_BLOB_ENDPOINT;
import static net.snowflake.ingest.utils.Constants.RESPONSE_ERR_ENQUEUE_TABLE_CHUNK_QUEUE_FULL;
import static net.snowflake.ingest.utils.Constants.RESPONSE_ERR_GENERAL_EXCEPTION_RETRY_REQUEST;
import static net.snowflake.ingest.utils.Constants.RESPONSE_ROW_SEQUENCER_IS_COMMITTED;
import static net.snowflake.ingest.utils.Constants.RESPONSE_SUCCESS;
import static net.snowflake.ingest.utils.Constants.SNOWPIPE_STREAMING_JMX_METRIC_PREFIX;
import static net.snowflake.ingest.utils.Constants.SNOWPIPE_STREAMING_JVM_MEMORY_AND_THREAD_METRICS_REGISTRY;
import static net.snowflake.ingest.utils.Constants.SNOWPIPE_STREAMING_SHARED_METRICS_REGISTRY;
import static net.snowflake.ingest.utils.Constants.TELEMETRY_SERVICE_REPORT_INTERVAL_IN_SEC;
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
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.security.KeyPair;
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
import net.snowflake.client.jdbc.internal.apache.http.impl.client.CloseableHttpClient;
import net.snowflake.ingest.connection.IngestResponseException;
import net.snowflake.ingest.connection.RequestBuilder;
import net.snowflake.ingest.streaming.OpenChannelRequest;
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
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

/**
 * The first version of implementation for SnowflakeStreamingIngestClient. The client internally
 * manages a few things:
 * <li>the channel cache, which contains all the channels that belong to this account
 * <li>the flush service, which schedules and coordinates the flush to Snowflake tables
 */
public class SnowflakeStreamingIngestClientInternal implements SnowflakeStreamingIngestClient {

  private static final Logging logger = new Logging(SnowflakeStreamingIngestClientInternal.class);

  // Object mapper for all marshalling and unmarshalling
  private static final ObjectMapper objectMapper = new ObjectMapper();

  // Counter to generate unique request ids per client
  private final AtomicLong counter = new AtomicLong(0);

  // Provides constant values that can be set by constructor
  private final ParameterProvider parameterProvider;

  // Name of the client
  private final String name;

  // Snowflake role for the client to use
  private String role;

  // Http client to send HTTP requests to Snowflake
  private final CloseableHttpClient httpClient;

  // Reference to the channel cache
  private final ChannelCache channelCache;

  // Reference to the flush service
  private final FlushService flushService;

  // Memory allocator
  private final BufferAllocator allocator;

  // Indicates whether the client has closed
  private volatile boolean isClosed;

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
  Meter inputThroughput; // Throughput for inserting into the Arrow buffer

  // JVM and thread related metrics
  MetricRegistry jvmMemoryAndThreadMetrics;

  // The request builder who handles building the HttpRequests we send
  private RequestBuilder requestBuilder;

  // Reference to the telemetry service
  private TelemetryService telemetryService;

  private ScheduledExecutorService telemetryWorker;

  /**
   * Constructor
   *
   * @param name the name of the client
   * @param accountURL Snowflake account url
   * @param prop connection properties
   * @param httpClient http client for sending request
   * @param isTestMode whether we're under test mode
   * @param requestBuilder http request builder
   * @param parameterOverrides parameters we override in case we want to set different values
   */
  SnowflakeStreamingIngestClientInternal(
      String name,
      SnowflakeURL accountURL,
      Properties prop,
      CloseableHttpClient httpClient,
      boolean isTestMode,
      RequestBuilder requestBuilder,
      Map<String, Object> parameterOverrides) {
    this.parameterProvider = new ParameterProvider(parameterOverrides, prop);

    this.name = name;
    this.isTestMode = isTestMode;
    this.httpClient = httpClient == null ? HttpUtil.getHttpClient() : httpClient;
    this.channelCache = new ChannelCache();
    this.allocator = new RootAllocator();
    this.isClosed = false;
    this.requestBuilder = requestBuilder;

    if (!isTestMode) {
      // Setup request builder for communication with the server side
      this.role = prop.getProperty(Constants.ROLE);
      try {
        KeyPair keyPair =
            Utils.createKeyPairFromPrivateKey((PrivateKey) prop.get(JDBC_PRIVATE_KEY));
        this.requestBuilder = new RequestBuilder(accountURL, prop.get(USER).toString(), keyPair);
      } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
        throw new SFException(e, ErrorCode.KEYPAIR_CREATION_FAILURE);
      }
    }

    this.flushService = new FlushService(this, this.channelCache, this.isTestMode);

    if (!isTestMode) {
      // Set up the telemetry service if needed
      if (ENABLE_TELEMETRY_TO_SF) {
        this.telemetryService =
            new TelemetryService(
                this.httpClient,
                String.format("%s_%s", this.name, this.flushService.getClientPrefix()),
                accountURL.getFullUrl());
        // this.telemetryService.refreshJWTToken();

        this.telemetryWorker = Executors.newSingleThreadScheduledExecutor();
        this.telemetryWorker.scheduleWithFixedDelay(
            this::reportTelemetryToSF,
            10,
            TELEMETRY_SERVICE_REPORT_INTERVAL_IN_SEC,
            TimeUnit.SECONDS);
      }

      // Publish client telemetries if needed
      this.registerMetricsForClient();
    }

    logger.logDebug(
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
   */
  public SnowflakeStreamingIngestClientInternal(
      String name,
      SnowflakeURL accountURL,
      Properties prop,
      Map<String, Object> parameterOverrides) {
    this(name, accountURL, prop, null, false, null, parameterOverrides);
  }

  /**
   * Constructor for TEST ONLY
   *
   * @param name the name of the client
   */
  SnowflakeStreamingIngestClientInternal(String name) {
    this(name, null, null, null, true, null, new HashMap<>());
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
  public SnowflakeStreamingIngestChannelInternal openChannel(OpenChannelRequest request) {
    if (isClosed) {
      throw new SFException(ErrorCode.CLOSED_CLIENT);
    }

    logger.logDebug(
        "Open channel request start, channel={}, table={}",
        request.getChannelName(),
        request.getFullyQualifiedTableName());

    try {
      Map<Object, Object> payload = new HashMap<>();
      payload.put(
          "request_id", this.flushService.getClientPrefix() + "_" + counter.getAndIncrement());
      payload.put("channel", request.getChannelName());
      payload.put("table", request.getTableName());
      payload.put("database", request.getDBName());
      payload.put("schema", request.getSchemaName());
      payload.put("write_mode", Constants.WriteMode.CLOUD_STORAGE.name());
      payload.put("role", this.role);

      OpenChannelResponse response =
          executeWithRetries(
              OpenChannelResponse.class,
              OPEN_CHANNEL_ENDPOINT,
              payload,
              "open channel",
              STREAMING_OPEN_CHANNEL,
              httpClient,
              requestBuilder);

      // Check for Snowflake specific response code
      if (response.getStatusCode() != RESPONSE_SUCCESS) {
        logger.logDebug(
            "Open channel request failed, channel={}, table={}, message={}",
            request.getChannelName(),
            request.getFullyQualifiedTableName(),
            response.getMessage());
        throw new SFException(ErrorCode.OPEN_CHANNEL_FAILURE, response.getMessage());
      }

      logger.logDebug(
          "Open channel request succeeded, channel={}, table={}",
          request.getChannelName(),
          request.getFullyQualifiedTableName());

      // Channel is now registered, add it to the in-memory channel pool
      SnowflakeStreamingIngestChannelInternal channel =
          SnowflakeStreamingIngestChannelFactory.builder(response.getChannelName())
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
              .build();

      // Setup the row buffer schema
      channel.setupSchema(response.getTableColumns());

      // Add channel to the channel cache
      this.channelCache.addChannel(channel);

      return channel;
    } catch (IOException | IngestResponseException e) {
      throw new SFException(e, ErrorCode.OPEN_CHANNEL_FAILURE, e.getMessage());
    }
  }

  /**
   * Fetch channels status from Snowflake
   *
   * @param channels a list of channels that we want to get the status on
   * @return a ChannelsStatusResponse object
   */
  ChannelsStatusResponse getChannelsStatus(List<SnowflakeStreamingIngestChannelInternal> channels) {
    try {
      ChannelsStatusRequest request = new ChannelsStatusRequest();
      List<ChannelsStatusRequest.ChannelStatusRequestDTO> requestDTOs =
          channels.stream()
              .map(ChannelsStatusRequest.ChannelStatusRequestDTO::new)
              .collect(Collectors.toList());
      request.setChannels(requestDTOs);
      request.setRole(this.role);
      request.setRequestId(this.flushService.getClientPrefix() + "_" + counter.getAndIncrement());

      String payload = objectMapper.writeValueAsString(request);

      ChannelsStatusResponse response =
          executeWithRetries(
              ChannelsStatusResponse.class,
              CHANNEL_STATUS_ENDPOINT,
              payload,
              "channel status",
              STREAMING_CHANNEL_STATUS,
              httpClient,
              requestBuilder);

      // Check for Snowflake specific response code
      if (response.getStatusCode() != RESPONSE_SUCCESS) {
        throw new SFException(ErrorCode.CHANNEL_STATUS_FAILURE, response.getMessage());
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
    this.registerBlobs(blobs, 0);
  }

  /**
   * Register the uploaded blobs to a Snowflake table
   *
   * @param blobs list of uploaded blobs
   * @param executionCount Number of times this call has been attempted, used to track retries
   */
  void registerBlobs(List<BlobMetadata> blobs, final int executionCount) {
    logger.logDebug(
        "Register blob request start for blob={}, client={}, executionCount={}",
        blobs.stream().map(BlobMetadata::getPath).collect(Collectors.toList()),
        this.name,
        executionCount);

    RegisterBlobResponse response = null;
    try {
      Map<Object, Object> payload = new HashMap<>();
      payload.put(
          "request_id", this.flushService.getClientPrefix() + "_" + counter.getAndIncrement());
      payload.put("blobs", blobs);
      payload.put("role", this.role);

      response =
          executeWithRetries(
              RegisterBlobResponse.class,
              REGISTER_BLOB_ENDPOINT,
              payload,
              "register blob",
              STREAMING_REGISTER_BLOB,
              httpClient,
              requestBuilder);

      // Check for Snowflake specific response code
      if (response.getStatusCode() != RESPONSE_SUCCESS) {
        logger.logDebug(
            "Register blob request failed for blob={}, client={}, message={}, executionCount={}",
            blobs.stream().map(BlobMetadata::getPath).collect(Collectors.toList()),
            this.name,
            response.getMessage(),
            executionCount);
        throw new SFException(ErrorCode.REGISTER_BLOB_FAILURE, response.getMessage());
      }
    } catch (IOException | IngestResponseException e) {
      throw new SFException(e, ErrorCode.REGISTER_BLOB_FAILURE, e.getMessage());
    }

    logger.logDebug(
        "Register blob request succeeded for blob={}, client={}, executionCount={}",
        blobs.stream().map(BlobMetadata::getPath).collect(Collectors.toList()),
        this.name,
        executionCount);

    // We will retry any blob chunks that were rejected becuase internal Snowflake queues are full
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
                                          logger.logWarn(
                                              "Channel has been invalidated because of failure"
                                                  + " response, name={}, channel sequencer={},"
                                                  + " status code={}, executionCount={}",
                                              channelStatus.getChannelName(),
                                              channelStatus.getChannelSequencer(),
                                              channelStatus.getStatusCode(),
                                              executionCount);
                                          channelCache.invalidateChannelIfSequencersMatch(
                                              chunkStatus.getDBName(),
                                              chunkStatus.getSchemaName(),
                                              chunkStatus.getTableName(),
                                              channelStatus.getChannelName(),
                                              channelStatus.getChannelSequencer());
                                        }
                                      }
                                    })));

    if (!queueFullChunks.isEmpty()) {
      logger.logDebug(
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
                                      new Pair(
                                          channelMetadata.getChannelName(),
                                          channelMetadata.getClientSequencer()))
                              .anyMatch(channelKey -> queueFullKeys.contains(channelKey)))
                  .collect(Collectors.toList());
          if (!relevantChunks.isEmpty()) {
            retryBlobs.add(
                new BlobMetadata(blobMetadata.getPath(), blobMetadata.getMD5(), relevantChunks));
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

      // Unregister jmx metrics
      if (metrics != null) {
        Slf4jReporter.forRegistry(metrics).outputTo(logger.getLogger()).build().report();
        removeMetricsFromRegistry();
      }

      // LOG jvm memory and thread metrics at the end
      if (jvmMemoryAndThreadMetrics != null) {
        Slf4jReporter.forRegistry(jvmMemoryAndThreadMetrics)
            .outputTo(logger.getLogger())
            .build()
            .report();
      }
    } catch (InterruptedException | ExecutionException e) {
      throw new SFException(e, ErrorCode.RESOURCE_CLEANUP_FAILURE, "client close");
    } finally {
      if (this.telemetryWorker != null) {
        this.telemetryWorker.shutdown();
      }
      this.flushService.shutdown();
      Utils.closeAllocator(this.allocator);
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
  void setNeedFlush() {
    this.flushService.setNeedFlush();
  }

  /**
   * Get the buffer allocator
   *
   * @return the buffer allocator
   */
  BufferAllocator getAllocator() {
    return this.allocator;
  }

  /** Remove the channel in the channel cache if the channel sequencer matches */
  void removeChannelIfSequencersMatch(SnowflakeStreamingIngestChannelInternal channel) {
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
  ChannelCache getChannelCache() {
    return this.channelCache;
  }

  /** Get the flush service */
  FlushService getFlushService() {
    return this.flushService;
  }

  /**
   * Check if any channels has uncommitted rows
   *
   * @param channels a list of channels we want to check against
   * @return a list of channels that has uncommitted rows
   */
  List<SnowflakeStreamingIngestChannelInternal> verifyChannelsAreFullyCommitted(
      List<SnowflakeStreamingIngestChannelInternal> channels) {
    if (channels.isEmpty()) {
      return channels;
    }

    // Start checking the status of all the channels in the list
    int retry = 0;
    boolean isTimeout = true;
    List<ChannelsStatusResponse.ChannelStatusResponseDTO> oldChannelsStatus = new ArrayList<>();
    do {
      List<ChannelsStatusResponse.ChannelStatusResponseDTO> channelsStatus =
          getChannelsStatus(channels).getChannels();
      List<SnowflakeStreamingIngestChannelInternal> tempChannels = new ArrayList<>();
      List<ChannelsStatusResponse.ChannelStatusResponseDTO> tempChannelsStatus = new ArrayList<>();

      for (int idx = 0; idx < channelsStatus.size(); idx++) {
        if (channelsStatus.get(idx).getStatusCode() != RESPONSE_ROW_SEQUENCER_IS_COMMITTED) {
          tempChannels.add(channels.get(idx));
          tempChannelsStatus.add(channelsStatus.get(idx));
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

  /*
   * Registers the performance metrics along with JVM memory and Threads.
   *
   * Latency and throughput metrics are emitted to JMX, jvm memory and thread metrics are logged to Slf4JLogger
   */
  private void registerMetricsForClient() {
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
    }

    if (this.parameterProvider.hasEnabledSnowpipeStreamingMetrics()) {
      // Blob histogram metrics
      blobSizeHistogram = metrics.histogram(MetricRegistry.name("blob", "size", "histogram"));
      blobRowCountHistogram =
          metrics.histogram(MetricRegistry.name("blob", "row", "count", "histogram"));

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
      StringBuilder sb = new StringBuilder(jmxDomain).append(":clientName=").append(clientName);

      sb.append(",name=").append(metricName);

      return new ObjectName(sb.toString());
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
   * Get Telemetry Service for a given client
   *
   * @return TelemetryService used by the client
   */
  TelemetryService getTelemetryService() {
    return this.telemetryService;
  }

  void reportTelemetryToSF() {
    if (this.telemetryService != null) {
      this.telemetryService.reportLatencyInSec(
          this.buildLatency, this.uploadLatency, this.registerLatency, this.flushLatency);
      this.telemetryService.reportThroughputBytesPerSecond(
          this.inputThroughput, this.uploadThroughput);
      this.telemetryService.reportCpuMemoryUsage(this.cpuHistogram);
    }
  }
}
