/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import java.io.IOException;
import java.util.Calendar;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.ingest.connection.IngestResponseException;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;
import net.snowflake.ingest.utils.Utils;

class ExternalVolumeLocation {
  public final String dbName;
  public final String schemaName;
  public final String tableName;

  public ExternalVolumeLocation(String dbName, String schemaName, String tableName) {
    this.dbName = dbName;
    this.schemaName = schemaName;
    this.tableName = tableName;
  }
}

/** Class to manage multiple external volumes */
class ExternalVolumeManager<T> implements IStorageManager<T, ExternalVolumeLocation> {
  // Reference to the external volume per table
  private final Map<String, StreamingIngestStorage<T, ExternalVolumeLocation>> externalVolumeMap;

  // name of the owning client
  private final String clientName;

  // role of the owning client
  private final String role;

  // Reference to the Snowflake service client used for configure calls
  private final SnowflakeServiceClient snowflakeServiceClient;

  // Client prefix generated by the Snowflake server
  private final String clientPrefix;

  /**
   * Constructor for ExternalVolumeManager
   *
   * @param isTestMode whether the manager in test mode
   * @param role the role of the client
   * @param clientName the name of the client
   * @param snowflakeServiceClient the Snowflake service client used for configure calls
   */
  ExternalVolumeManager(
      boolean isTestMode,
      String role,
      String clientName,
      SnowflakeServiceClient snowflakeServiceClient) {
    this.role = role;
    this.clientName = clientName;
    this.snowflakeServiceClient = snowflakeServiceClient;
    this.externalVolumeMap = new ConcurrentHashMap<>();
    try {
      this.clientPrefix =
          isTestMode
              ? "testPrefix"
              : this.snowflakeServiceClient
                  .clientConfigure(new ClientConfigureRequest(role))
                  .getClientPrefix();
    } catch (IngestResponseException | IOException e) {
      throw new SFException(e, ErrorCode.CLIENT_CONFIGURE_FAILURE, e.getMessage());
    }
  }

  /**
   * Given a fully qualified table name, return the target storage by looking up the table name
   *
   * @param fullyQualifiedTableName the target fully qualified table name
   * @return target storage
   */
  @Override
  public StreamingIngestStorage<T, ExternalVolumeLocation> getStorage(
      String fullyQualifiedTableName) {
    // Only one chunk per blob in Iceberg mode.
    StreamingIngestStorage<T, ExternalVolumeLocation> stage =
        this.externalVolumeMap.get(fullyQualifiedTableName);

    if (stage == null) {
      throw new SFException(
          ErrorCode.INTERNAL_ERROR,
          String.format("No external volume found for table %s", fullyQualifiedTableName));
    }

    return stage;
  }

  /**
   * Add a storage to the manager by looking up the table name from the open channel response
   *
   * @param dbName the database name
   * @param schemaName the schema name
   * @param tableName the table name
   * @param fileLocationInfo response from open channel
   */
  @Override
  public void addStorage(
      String dbName, String schemaName, String tableName, FileLocationInfo fileLocationInfo) {
    String fullyQualifiedTableName =
        Utils.getFullyQualifiedTableName(dbName, schemaName, tableName);

    try {
      this.externalVolumeMap.put(
          fullyQualifiedTableName,
          new StreamingIngestStorage<T, ExternalVolumeLocation>(
              this,
              this.clientName,
              fileLocationInfo,
              new ExternalVolumeLocation(dbName, schemaName, tableName),
              DEFAULT_MAX_UPLOAD_RETRIES));
    } catch (SnowflakeSQLException | IOException err) {
      throw new SFException(err, ErrorCode.UNABLE_TO_CONNECT_TO_STAGE);
    }
  }

  /**
   * Gets the latest file location info (with a renewed short-lived access token) for the specified
   * location
   *
   * @param location A reference to the target location
   * @param fileName optional filename for single-file signed URL fetch from server
   * @return the new location information
   */
  @Override
  public FileLocationInfo getRefreshedLocation(
      ExternalVolumeLocation location, Optional<String> fileName) {
    try {
      ChannelConfigureRequest request =
          new ChannelConfigureRequest(
              this.role, location.dbName, location.schemaName, location.tableName);
      fileName.ifPresent(request::setFileName);
      ChannelConfigureResponse response = this.snowflakeServiceClient.channelConfigure(request);
      return response.getStageLocation();
    } catch (IngestResponseException | IOException e) {
      throw new SFException(e, ErrorCode.CLIENT_CONFIGURE_FAILURE, e.getMessage());
    }
  }

  // TODO: SNOW-1502887 Blob path generation for iceberg table
  @Override
  public String generateBlobPath() {
    return "snow_dummy_file_name.parquet";
  }

  // TODO: SNOW-1502887 Blob path generation for iceberg table
  @Override
  public void decrementBlobSequencer() {}

  // TODO: SNOW-1502887 Blob path generation for iceberg table
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
    return this.clientPrefix;
  }
}
