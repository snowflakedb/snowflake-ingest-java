/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.connection.ServiceResponseHandler.ApiName.STREAMING_CHANNEL_STATUS;
import static net.snowflake.ingest.connection.ServiceResponseHandler.ApiName.STREAMING_OPEN_CHANNEL;
import static net.snowflake.ingest.connection.ServiceResponseHandler.ApiName.STREAMING_REGISTER_BLOB;
import static net.snowflake.ingest.utils.Constants.CHANNEL_STATUS_ENDPOINT;
import static net.snowflake.ingest.utils.Constants.COMMIT_MAX_RETRY_COUNT;
import static net.snowflake.ingest.utils.Constants.COMMIT_RETRY_INTERVAL_IN_MS;
import static net.snowflake.ingest.utils.Constants.ENABLE_PERF_MEASUREMENT;
import static net.snowflake.ingest.utils.Constants.JDBC_PRIVATE_KEY;
import static net.snowflake.ingest.utils.Constants.OPEN_CHANNEL_ENDPOINT;
import static net.snowflake.ingest.utils.Constants.REGISTER_BLOB_ENDPOINT;
import static net.snowflake.ingest.utils.Constants.RESPONSE_SUCCESS;
import static net.snowflake.ingest.utils.Constants.ROW_SEQUENCER_IS_COMMITTED;
import static net.snowflake.ingest.utils.Constants.USER;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Slf4jReporter;
import com.codahale.metrics.Timer;
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
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import net.snowflake.ingest.connection.IngestResponseException;
import net.snowflake.ingest.connection.RequestBuilder;
import net.snowflake.ingest.connection.ServiceResponseHandler;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.utils.Constants;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.HttpUtil;
import net.snowflake.ingest.utils.Logging;
import net.snowflake.ingest.utils.SFException;
import net.snowflake.ingest.utils.SnowflakeURL;
import net.snowflake.ingest.utils.Utils;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.http.client.HttpClient;

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

  // Provides constant values set by Snowflake server
  private static final ParameterProvider parameterProvider = new ParameterProvider();

  // Name of the client
  private final String name;

  // Snowflake role for the client to use
  private String role;

  // Http client to send HTTP request to Snowflake
  private final HttpClient httpClient;

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

  // The request builder who handles building the HttpRequests we send
  private RequestBuilder requestBuilder;

  /**
   * Constructor
   *
   * @param name the name of the client
   * @param accountURL Snowflake account url
   * @param prop connection properties
   * @param httpClient http client for sending request
   * @param isTestMode whether we're under test mode
   */
  SnowflakeStreamingIngestClientInternal(
      String name,
      SnowflakeURL accountURL,
      Properties prop,
      HttpClient httpClient,
      boolean isTestMode,
      RequestBuilder requestBuilder,
      Map<String, Object> parameterOverrides) {
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

    if (ENABLE_PERF_MEASUREMENT) {
      metrics = new MetricRegistry();
      blobSizeHistogram = metrics.histogram(MetricRegistry.name(name, "blob", "size", "histogram"));
      blobRowCountHistogram =
          metrics.histogram(MetricRegistry.name(name, "blob", "row", "count", "histogram"));
      cpuHistogram = metrics.histogram(MetricRegistry.name(name, "cpu", "usage", "histogram"));
      flushLatency = metrics.timer(MetricRegistry.name(name, "flush", "latency"));
      buildLatency = metrics.timer(MetricRegistry.name(name, "build", "latency"));
      uploadLatency = metrics.timer(MetricRegistry.name(name, "upload", "latency"));
      registerLatency = metrics.timer(MetricRegistry.name(name, "register", "latency"));
      uploadThroughput = metrics.meter(MetricRegistry.name(name, "upload", "throughput"));
      inputThroughput = metrics.meter(MetricRegistry.name(name, "input", "throughput"));
      metrics.register(MetricRegistry.name("jvm", "memory"), new MemoryUsageGaugeSet());
      metrics.register(MetricRegistry.name("jvm", "threads"), new ThreadStatesGaugeSet());
      SharedMetricRegistries.add("Metrics", metrics);
    }

    this.parameterProvider.setParameterMap(parameterOverrides, prop);

    logger.logDebug(
        "Client created, name={}, account={}. isTestMode={}",
        name,
        accountURL == null ? "" : accountURL.getAccount(),
        isTestMode);
  }

  /**
   * Default Constructor
   *
   * @param name the name of the client
   * @param accountURL Snowflake account url
   * @param prop connection properties
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
    this(name, null, null, null, true, null, null);
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

  /**
   * @return a boolean to indicate whether the client is closed or not
   */
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
          ServiceResponseHandler.unmarshallStreamingIngestResponse(
              httpClient.execute(
                  requestBuilder.generateStreamingIngestPostRequest(
                      payload, OPEN_CHANNEL_ENDPOINT, "open channel")),
              OpenChannelResponse.class,
              STREAMING_OPEN_CHANNEL);

      // Check for Snowflake specific response code
      if (response.getStatusCode() != RESPONSE_SUCCESS) {
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
      throw new SFException(e, ErrorCode.OPEN_CHANNEL_FAILURE);
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
          ServiceResponseHandler.unmarshallStreamingIngestResponse(
              httpClient.execute(
                  requestBuilder.generateStreamingIngestPostRequest(
                      payload, CHANNEL_STATUS_ENDPOINT, "channel status")),
              ChannelsStatusResponse.class,
              STREAMING_CHANNEL_STATUS);

      // Check for Snowflake specific response code
      if (response.getStatusCode() != RESPONSE_SUCCESS) {
        throw new SFException(ErrorCode.CHANNEL_STATUS_FAILURE, response.getMessage());
      }

      return response;
    } catch (IOException | IngestResponseException e) {
      throw new SFException(e, ErrorCode.CHANNEL_STATUS_FAILURE);
    }
  }

  /**
   * Register the uploaded blobs to a Snowflake table
   *
   * @param blobs list of uploaded blobs
   */
  void registerBlobs(List<BlobMetadata> blobs) {
    logger.logDebug(
        "Register blob request start for blob={}, client={}",
        blobs.stream().map(BlobMetadata::getPath).collect(Collectors.toList()),
        this.name);

    RegisterBlobResponse response = null;
    try {
      Map<Object, Object> payload = new HashMap<>();
      payload.put(
          "request_id", this.flushService.getClientPrefix() + "_" + counter.getAndIncrement());
      payload.put("blobs", blobs);
      payload.put("role", this.role);

      response =
          ServiceResponseHandler.unmarshallStreamingIngestResponse(
              httpClient.execute(
                  requestBuilder.generateStreamingIngestPostRequest(
                      payload, REGISTER_BLOB_ENDPOINT, "register blob")),
              RegisterBlobResponse.class,
              STREAMING_REGISTER_BLOB);

      // Check for Snowflake specific response code
      if (response.getStatusCode() != RESPONSE_SUCCESS) {
        throw new SFException(ErrorCode.REGISTER_BLOB_FAILURE, response.getMessage());
      }
    } catch (IOException | IngestResponseException e) {
      throw new SFException(e, ErrorCode.REGISTER_BLOB_FAILURE);
    }

    logger.logDebug(
        "Register blob request succeeded for blob={}, client={}",
        blobs.stream().map(BlobMetadata::getPath).collect(Collectors.toList()),
        this.name);

    // Invalidate any channels that returns a failure status code
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
                                        logger.logWarn(
                                            "Channel has been invalidated because of failure"
                                                + " response, name={}, channel sequencer={},"
                                                + " status code={}",
                                            channelStatus.getChannelName(),
                                            channelStatus.getChannelSequencer(),
                                            channelStatus.getStatusCode());
                                        channelCache.invalidateChannelIfSequencersMatch(
                                            chunkStatus.getDBName(),
                                            chunkStatus.getSchemaName(),
                                            chunkStatus.getTableName(),
                                            channelStatus.getChannelName(),
                                            channelStatus.getChannelSequencer());
                                      }
                                    })));
  }

  /** Close the client, which will flush first and then release all the resources */
  @Override
  public void close() throws Exception {
    if (isClosed) {
      return;
    }

    isClosed = true;
    this.channelCache.closeAllChannels();

    // Collect the perf metrics before closing if needed
    if (metrics != null) {
      Slf4jReporter.forRegistry(metrics).outputTo(logger.getLogger()).build().report();
    }

    // Flush any remaining rows and cleanup all the resources
    try {
      this.flush(true).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new SFException(e, ErrorCode.RESOURCE_CLEANUP_FAILURE, "client close");
    } finally {
      this.flushService.shutdown();

      for (BufferAllocator alloc : this.allocator.getChildAllocators()) {
        alloc.releaseBytes(alloc.getAllocatedMemory());
        alloc.close();
      }
      this.allocator.releaseBytes(this.allocator.getAllocatedMemory());
      this.allocator.close();
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
  HttpClient getHttpClient() {
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
        if (channelsStatus.get(idx).getStatusCode() != ROW_SEQUENCER_IS_COMMITTED) {
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

  ParameterProvider getParameterProvider() {
    return parameterProvider;
  }
}
