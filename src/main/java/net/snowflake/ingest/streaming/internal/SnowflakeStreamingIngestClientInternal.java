/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.utils.Constants.JDBC_PRIVATE_KEY;
import static net.snowflake.ingest.utils.Constants.JDBC_USER;
import static net.snowflake.ingest.utils.Constants.OPEN_CHANNEL_ENDPOINT;
import static net.snowflake.ingest.utils.Constants.REGISTER_BLOB_ENDPOINT;
import static net.snowflake.ingest.utils.Constants.RESPONSE_SUCCESS;

import java.io.IOException;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.spec.InvalidKeySpecException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import net.snowflake.client.jdbc.SnowflakeDriver;
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

  // Name of the client
  private final String name;

  // Connection to the Snowflake account
  private Connection connection;

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
      RequestBuilder requestBuilder) {
    this.name = name;
    this.isTestMode = isTestMode;
    this.httpClient = httpClient == null ? HttpUtil.getHttpClient() : httpClient;
    this.channelCache = new ChannelCache();
    this.allocator = new RootAllocator();
    this.isClosed = false;
    this.requestBuilder = requestBuilder;

    if (!isTestMode) {
      try {
        logger.logDebug("Trying to connect to Snowflake account={}", accountURL.getFullUrl());
        this.connection = new SnowflakeDriver().connect(accountURL.getJdbcUrl(), prop);
      } catch (SQLException e) {
        throw new SFException(e, ErrorCode.SF_CONNECTION_FAILURE);
      }

      // Setup request builder for communication with the server side
      try {
        KeyPair keyPair =
            Utils.createKeyPairFromPrivateKey((PrivateKey) prop.get(JDBC_PRIVATE_KEY));
        this.requestBuilder =
            new RequestBuilder(accountURL, prop.get(JDBC_USER).toString(), keyPair);
      } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
        throw new SFException(e, ErrorCode.KEYPAIR_CREATION_FAILURE);
      }
    }

    this.flushService = new FlushService(this, this.channelCache, this.connection, this.isTestMode);

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
      String name, SnowflakeURL accountURL, Properties prop) {
    this(name, accountURL, prop, null, false, null);
  }

  /**
   * Constructor for TEST ONLY
   *
   * @param name the name of the client
   */
  SnowflakeStreamingIngestClientInternal(String name) {
    this(name, null, null, null, true, null);
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
    if (isClosed()) {
      throw new SFException(ErrorCode.CLOSED_CLIENT);
    }

    logger.logDebug(
        "Open channel request start, channel={}, table={}",
        request.getChannelName(),
        request.getFullyQualifiedTableName());

    try {
      Map<Object, Object> payload = new HashMap<>();
      payload.put("channel", request.getChannelName());
      payload.put("table", request.getTableName());
      payload.put("database", request.getDBName());
      payload.put("schema", request.getSchemaName());
      payload.put("write_mode", Constants.WriteMode.CLOUD_STORAGE.name());

      OpenChannelResponse response =
          ServiceResponseHandler.unmarshallStreamingIngestResponse(
              httpClient.execute(
                  requestBuilder.generateStreamingIngestPostRequest(
                      payload, OPEN_CHANNEL_ENDPOINT, "open channel")),
              OpenChannelResponse.class);

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
          SnowflakeStreamingIngestChannelFactory.builder(request.getChannelName())
              .setDBName(request.getDBName())
              .setSchemaName(request.getSchemaName())
              .setTableName(request.getTableName())
              .setOffsetToken(response.getOffsetToken())
              .setRowSequencer(response.getRowSequencer())
              .setChannelSequencer(response.getClientSequencer())
              .setOwningClient(this)
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
   * Register the uploaded blobs to a Snowflake table
   *
   * @param blobs list of uploaded blobs
   */
  void registerBlobs(List<BlobMetadata> blobs) {
    logger.logDebug(
        "Register blob request start for blob={}, client={}",
        blobs.stream().map(BlobMetadata::getPath).collect(Collectors.toList()),
        this.name);

    try {
      Map<Object, Object> payload = new HashMap<>();
      payload.put("request_id", null);
      payload.put("blobs", blobs);

      RegisterBlobResponse response =
          ServiceResponseHandler.unmarshallStreamingIngestResponse(
              httpClient.execute(
                  requestBuilder.generateStreamingIngestPostRequest(
                      payload, REGISTER_BLOB_ENDPOINT, "register blob")),
              RegisterBlobResponse.class);

      // TODO: to fail fast, the channels could to be invalidated if register blob show failures
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
  }

  /**
   * Close the client, which will flush first and then release all the resources
   *
   * @return future which will be complete when the channel is closed
   */
  @Override
  public CompletableFuture<Void> close() {
    if (isClosed()) {
      return CompletableFuture.completedFuture(null);
    }

    isClosed = true;
    // First mark all the channels as closed, then flush any leftover rows in the buffer
    this.channelCache.closeAllChannels();
    return flush(true)
        .thenRun(
            () -> {
              try {
                this.flushService.shutdown();
                this.allocator.close();
                if (!isTestMode) {
                  this.connection.close();
                }
              } catch (SQLException | InterruptedException e) {
                throw new SFException(e, ErrorCode.RESOURCE_CLEANUP_FAILURE, "client close");
              }
            });
  }

  /**
   * Flush all data in memory to persistent storage and register with a Snowflake table
   *
   * @return future which will be complete when the flush the data is registered
   */
  @Override
  public CompletableFuture<Void> flush() {
    return flush(false);
  }

  private CompletableFuture<Void> flush(boolean closing) {
    if (isClosed() && !closing) {
      throw new SFException(ErrorCode.CLOSED_CLIENT);
    }
    return this.flushService.flush(closing);
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
}
