/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.connection.ServiceResponseHandler.ApiName.STREAMING_CHANNEL_CONFIGURE;
import static net.snowflake.ingest.connection.ServiceResponseHandler.ApiName.STREAMING_CHANNEL_STATUS;
import static net.snowflake.ingest.connection.ServiceResponseHandler.ApiName.STREAMING_CLIENT_CONFIGURE;
import static net.snowflake.ingest.connection.ServiceResponseHandler.ApiName.STREAMING_DROP_CHANNEL;
import static net.snowflake.ingest.connection.ServiceResponseHandler.ApiName.STREAMING_OPEN_CHANNEL;
import static net.snowflake.ingest.connection.ServiceResponseHandler.ApiName.STREAMING_REGISTER_BLOB;
import static net.snowflake.ingest.streaming.internal.StreamingIngestUtils.executeWithRetries;
import static net.snowflake.ingest.utils.Constants.CHANNEL_CONFIGURE_ENDPOINT;
import static net.snowflake.ingest.utils.Constants.CHANNEL_STATUS_ENDPOINT;
import static net.snowflake.ingest.utils.Constants.CLIENT_CONFIGURE_ENDPOINT;
import static net.snowflake.ingest.utils.Constants.DROP_CHANNEL_ENDPOINT;
import static net.snowflake.ingest.utils.Constants.OPEN_CHANNEL_ENDPOINT;
import static net.snowflake.ingest.utils.Constants.REGISTER_BLOB_ENDPOINT;
import static net.snowflake.ingest.utils.Constants.RESPONSE_SUCCESS;

import java.io.IOException;
import java.util.stream.Collectors;
import net.snowflake.client.jdbc.internal.apache.http.impl.client.CloseableHttpClient;
import net.snowflake.ingest.connection.IngestResponseException;
import net.snowflake.ingest.connection.RequestBuilder;
import net.snowflake.ingest.connection.ServiceResponseHandler;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.Logging;
import net.snowflake.ingest.utils.SFException;

/**
 * The SnowflakeServiceClient class is responsible for making API requests to the Snowflake service.
 */
class SnowflakeServiceClient {
  private static final Logging logger = new Logging(SnowflakeServiceClient.class);

  // HTTP client used for making requests
  private final CloseableHttpClient httpClient;

  // Request builder for building streaming API request
  private final RequestBuilder requestBuilder;

  /**
   * Default constructor
   *
   * @param httpClient the HTTP client used for making requests
   * @param requestBuilder the request builder for building streaming API requests
   */
  SnowflakeServiceClient(CloseableHttpClient httpClient, RequestBuilder requestBuilder) {
    this.httpClient = httpClient;
    this.requestBuilder = requestBuilder;
  }

  /**
   * Configures the client given a {@link ConfigureRequest}.
   *
   * @param request the client configuration request
   * @return the response from the configuration request
   */
  ConfigureResponse clientConfigure(ConfigureRequest request) {
    ConfigureResponse response =
        executeApiRequestWithRetries(
            request,
            ConfigureResponse.class,
            CLIENT_CONFIGURE_ENDPOINT,
            "client configure",
            STREAMING_CLIENT_CONFIGURE,
            ErrorCode.CONFIGURE_FAILURE);
    if (response.getStatusCode() != RESPONSE_SUCCESS) {
      logger.logDebug("Client configure request failed, message={}", response.getMessage());
      throw new SFException(ErrorCode.CONFIGURE_FAILURE, response.getMessage());
    }
    return response;
  }

  /**
   * Configures the channel given a {@link ConfigureRequest}.
   *
   * @param request the channel configuration request
   * @return the response from the configuration request
   */
  ConfigureResponse channelConfigure(ConfigureRequest request) {
    ConfigureResponse response =
        executeApiRequestWithRetries(
            request,
            ConfigureResponse.class,
            CHANNEL_CONFIGURE_ENDPOINT,
            "channel configure",
            STREAMING_CHANNEL_CONFIGURE,
            ErrorCode.CONFIGURE_FAILURE);

    if (response.getStatusCode() != RESPONSE_SUCCESS) {
      logger.logDebug(
          "Channel configure request failed, table={}, message={}",
          request.getFullyQualifiedTableName(),
          response.getMessage());
      throw new SFException(ErrorCode.CONFIGURE_FAILURE, response.getMessage());
    }
    return response;
  }

  /**
   * Opens a channel given a {@link OpenChannelRequestInternal}.
   *
   * @param request the open channel request
   * @return the response from the open channel request
   */
  OpenChannelResponse openChannel(OpenChannelRequestInternal request) {
    OpenChannelResponse response =
        executeApiRequestWithRetries(
            request,
            OpenChannelResponse.class,
            OPEN_CHANNEL_ENDPOINT,
            "open channel",
            STREAMING_OPEN_CHANNEL,
            ErrorCode.OPEN_CHANNEL_FAILURE);

    if (response.getStatusCode() != RESPONSE_SUCCESS) {
      logger.logDebug(
          "Open channel request failed, table={}, message={}",
          request.getFullyQualifiedTableName(),
          response.getMessage());
      throw new SFException(ErrorCode.OPEN_CHANNEL_FAILURE, response.getMessage());
    }
    return response;
  }

  /**
   * Drops a channel given a {@link DropChannelRequestInternal}.
   *
   * @param request the drop channel request
   * @return the response from the drop channel request
   */
  DropChannelResponse dropChannel(DropChannelRequestInternal request) {
    DropChannelResponse response =
        executeApiRequestWithRetries(
            request,
            DropChannelResponse.class,
            DROP_CHANNEL_ENDPOINT,
            "drop channel",
            STREAMING_DROP_CHANNEL,
            ErrorCode.DROP_CHANNEL_FAILURE);

    if (response.getStatusCode() != RESPONSE_SUCCESS) {
      logger.logDebug(
          "Drop channel request failed, table={}, message={}",
          request.getFullyQualifiedTableName(),
          response.getMessage());
      throw new SFException(ErrorCode.DROP_CHANNEL_FAILURE, response.getMessage());
    }
    return response;
  }

  /**
   * Gets the status of a channel given a {@link ChannelsStatusRequest}.
   *
   * @param request the channel status request
   * @return the response from the channel status request
   */
  ChannelsStatusResponse channelStatus(ChannelsStatusRequest request) {
    ChannelsStatusResponse response =
        executeApiRequestWithRetries(
            request,
            ChannelsStatusResponse.class,
            CHANNEL_STATUS_ENDPOINT,
            "channel status",
            STREAMING_CHANNEL_STATUS,
            ErrorCode.CHANNEL_STATUS_FAILURE);

    if (response.getStatusCode() != RESPONSE_SUCCESS) {
      logger.logDebug("Channel status request failed, message={}", response.getMessage());
      throw new SFException(ErrorCode.CHANNEL_STATUS_FAILURE, response.getMessage());
    }
    return response;
  }

  /**
   * Registers a blob given a {@link RegisterBlobRequest}.
   *
   * @param request the register blob request
   * @param executionCount the number of times the request has been executed, used for logging
   * @return the response from the register blob request
   */
  RegisterBlobResponse registerBlob(RegisterBlobRequest request, final int executionCount) {
    RegisterBlobResponse response =
        executeApiRequestWithRetries(
            request,
            RegisterBlobResponse.class,
            REGISTER_BLOB_ENDPOINT,
            "register blob",
            STREAMING_REGISTER_BLOB,
            ErrorCode.REGISTER_BLOB_FAILURE);

    if (response.getStatusCode() != RESPONSE_SUCCESS) {
      logger.logDebug(
          "Register blob request failed for blob={}, message={}, executionCount={}",
          request.getBlobs().stream().map(BlobMetadata::getPath).collect(Collectors.toList()),
          response.getMessage(),
          executionCount);
      throw new SFException(ErrorCode.REGISTER_BLOB_FAILURE, response.getMessage());
    }
    return response;
  }

  private <T extends StreamingIngestResponse> T executeApiRequestWithRetries(
      StreamingIngestRequest request,
      Class<T> responseClass,
      String endpoint,
      String operation,
      ServiceResponseHandler.ApiName apiName,
      ErrorCode errorCode) {
    try {
      return executeWithRetries(
          responseClass,
          endpoint,
          request,
          operation,
          apiName,
          this.httpClient,
          this.requestBuilder);
    } catch (IngestResponseException | IOException e) {
      throw new SFException(e, errorCode, e.getMessage());
    }
  }
}
