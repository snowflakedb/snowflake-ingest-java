/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.connection.ServiceResponseHandler.ApiName.STREAMING_CHANNEL_CONFIGURE;
import static net.snowflake.ingest.utils.Constants.CHANNEL_CONFIGURE_ENDPOINT;

import java.io.IOException;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;

/** Class to manage multiple external volumes */
class ExternalVolumeManager<T> implements StorageManager<T> {

  // Reference to the external volume per table
  private final Map<String, StreamingIngestStorage> externalVolumeMap;
  private final SnowflakeStreamingIngestClientInternal<T> owningClient;

  private final boolean isTestMode;

  /**
   * Constructor for ExternalVolumeManager
   *
   * @param isTestMode whether the manager in test mode
   * @param client the owning client
   */
  ExternalVolumeManager(boolean isTestMode, SnowflakeStreamingIngestClientInternal<T> client) {
    this.owningClient = client;
    this.isTestMode = isTestMode;
    this.externalVolumeMap = new ConcurrentHashMap<>();
  }

  /**
   * Given a blob, return the target storage by looking up the table name from the channel context
   *
   * @param blobData the blob to upload
   * @return target storage
   */
  @Override
  public StreamingIngestStorage getStorage(List<List<ChannelData<T>>> blobData) {
    // Only one chunk per blob in Iceberg mode.
    ChannelFlushContext channelContext = blobData.get(0).get(0).getChannelContext();
    StreamingIngestStorage stage =
        this.externalVolumeMap.get(channelContext.getFullyQualifiedTableName());

    if (stage == null) {
      throw new SFException(
          ErrorCode.INTERNAL_ERROR,
          String.format(
              "No storage found for table %s", channelContext.getFullyQualifiedTableName()));
    }

    return stage;
  }

  /**
   * Add a storage to the manager by looking up the table name from the open channel response
   *
   * @param openChannelResponse response from open channel
   */
  @Override
  public void addStorage(OpenChannelResponse openChannelResponse) {
    String fullyQualifiedTableName =
        String.format(
            "%s.%s.%s",
            openChannelResponse.getDBName(),
            openChannelResponse.getSchemaName(),
            openChannelResponse.getTableName());
    if (!this.externalVolumeMap.containsKey(fullyQualifiedTableName)) {
      try {
        ConfigureCallHandler configureCallHandler =
            ConfigureCallHandler.builder(
                    this.owningClient.getHttpClient(),
                    this.owningClient.getRequestBuilder(),
                    STREAMING_CHANNEL_CONFIGURE,
                    CHANNEL_CONFIGURE_ENDPOINT)
                .setRole(this.owningClient.getRole())
                .setDatabase(openChannelResponse.getDBName())
                .setSchema(openChannelResponse.getSchemaName())
                .setTable(openChannelResponse.getTableName())
                .build();
        this.externalVolumeMap.put(
            fullyQualifiedTableName,
            new StreamingIngestStorage(
                isTestMode,
                configureCallHandler,
                this.owningClient.getName(),
                DEFAULT_MAX_UPLOAD_RETRIES));
      } catch (SnowflakeSQLException | IOException err) {
        throw new SFException(err, ErrorCode.UNABLE_TO_CONNECT_TO_STAGE);
      }
    }
  }

  // TODO: SNOW-1502887 Blob path generation for iceberg table
  @Override
  public String generateBlobPath() {
    return "snow_dummy_file_name";
  }

  // TODO: SNOW-1502887 Blob path generation for iceberg table
  @Override
  public String getBlobPath(Calendar calendar, String clientPrefix) {
    return "";
  }

  /**
   * Get the client prefix from first external volume in the map
   *
   * @return the client prefix
   */
  @Override
  public String getClientPrefix() {
    if (this.externalVolumeMap.isEmpty()) {
      return null;
    }
    return this.externalVolumeMap.values().iterator().next().getClientPrefix();
  }
}
