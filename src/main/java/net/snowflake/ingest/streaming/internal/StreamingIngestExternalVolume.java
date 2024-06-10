/*
 * Copyright (c) 2021-2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.utils.Constants.CHANNEL_CONFIGURE_ENDPOINT;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import net.snowflake.client.jdbc.SnowflakeFileTransferAgent;
import net.snowflake.client.jdbc.SnowflakeFileTransferMetadataV1;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.jdbc.internal.apache.http.impl.client.CloseableHttpClient;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.JsonNode;
import net.snowflake.ingest.connection.RequestBuilder;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;

/** Handles uploading files to an external volume */
class StreamingIngestExternalVolume extends AbstractCloudStorage {

  // Any valid channel flush context that points to the external volume used for making channel
  // configure calls
  private volatile ChannelFlushContext arbitraryValidChannelFlushContext;

  // Hash of the volume
  private final String volumeHash;

  /**
   * General constructor
   *
   * @param role Snowflake role used by the Client
   * @param httpClient http client reference
   * @param requestBuilder request builder to build the HTTP request
   * @param clientName the client name
   * @param stageLocation external volume information received from open channel
   * @param maxUploadRetries maximum time of upload retries
   */
  StreamingIngestExternalVolume(
      String role,
      CloseableHttpClient httpClient,
      RequestBuilder requestBuilder,
      String clientName,
      JsonNode stageLocation,
      int maxUploadRetries)
      throws SnowflakeSQLException {
    super(
        role, httpClient, requestBuilder, clientName, CHANNEL_CONFIGURE_ENDPOINT, maxUploadRetries);
    this.fileTransferMetadataWithAge =
        new SnowflakeFileTransferMetadataWithAge(
            (SnowflakeFileTransferMetadataV1)
                SnowflakeFileTransferAgent.getFileTransferMetadatas(
                        parseStageLocation(stageLocation))
                    .get(0),
            Optional.of(System.currentTimeMillis()));
    this.volumeHash = stageLocation.get("volume_hash").asText();
  }

  /**
   * Constructor for TESTING that takes SnowflakeFileTransferMetadataWithAge as input
   *
   * @param isTestMode must be true
   * @param role Snowflake role used by the Client
   * @param httpClient http client reference
   * @param requestBuilder request builder to build the HTTP request
   * @param clientName the client name
   * @param stageLocation external volume information received from open channel
   * @param testMetadata SnowflakeFileTransferMetadataWithAge to test with
   */
  StreamingIngestExternalVolume(
      boolean isTestMode,
      String role,
      CloseableHttpClient httpClient,
      RequestBuilder requestBuilder,
      String clientName,
      JsonNode stageLocation,
      AbstractCloudStorage.SnowflakeFileTransferMetadataWithAge testMetadata,
      int maxRetryCount)
      throws SnowflakeSQLException {
    this(role, httpClient, requestBuilder, clientName, stageLocation, maxRetryCount);
    if (!isTestMode) {
      throw new SFException(ErrorCode.INTERNAL_ERROR);
    }
    this.fileTransferMetadataWithAge = testMetadata;
  }

  public void setArbitraryValidChannelFlushContext(
      ChannelFlushContext arbitraryValidChannelFlushContext) {
    this.arbitraryValidChannelFlushContext = arbitraryValidChannelFlushContext;
  }

  @Override
  protected Map<Object, Object> getConfigurePayload() {
    Map<Object, Object> payload = new HashMap<>();
    payload.put("role", this.role);
    Map<Object, Object> channelPayload = new HashMap<>();
    channelPayload.put("database", this.arbitraryValidChannelFlushContext.getDbName());
    channelPayload.put("schema", this.arbitraryValidChannelFlushContext.getSchemaName());
    channelPayload.put("table", this.arbitraryValidChannelFlushContext.getTableName());
    channelPayload.put("channel_name", this.arbitraryValidChannelFlushContext.getName());
    payload.put("channel", channelPayload);
    return payload;
  }

  /**
   * Upload file to external volume
   *
   * @param filePath
   * @param blob
   */
  @Override
  void put(String filePath, byte[] blob) {
    try {
      putRemote(filePath, blob);
    } catch (SnowflakeSQLException | IOException e) {
      throw new SFException(e, ErrorCode.BLOB_UPLOAD_FAILURE);
    }
  }

  public String getVolumeHash() {
    return volumeHash;
  }
}
