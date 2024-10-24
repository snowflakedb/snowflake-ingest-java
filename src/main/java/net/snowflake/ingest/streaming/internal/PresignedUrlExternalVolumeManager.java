/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.streaming.internal.GeneratePresignedUrlsResponse.PresignedUrlInfo;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import net.snowflake.ingest.connection.IngestResponseException;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.Logging;
import net.snowflake.ingest.utils.SFException;

/** Class to manage multiple external volumes */
class PresignedUrlExternalVolumeManager implements IStorageManager {
  // TODO: Rename all logger members to LOGGER and checkin code formatting rules
  private static final Logging logger = new Logging(PresignedUrlExternalVolumeManager.class);
  // Reference to the external volume per table
  private final Map<String, PresignedUrlExternalVolume> externalVolumeMap;

  // name of the owning client
  private final String clientName;

  private final String role;

  // Reference to the Snowflake service client used for configure calls
  private final SnowflakeServiceClient serviceClient;

  // Client prefix generated by the Snowflake server
  private final String clientPrefix;

  // Deployment ID returned by the Snowflake server
  private final Long deploymentId;

  // concurrency control to avoid creating multiple ExternalVolume objects for the same table (if
  // openChannel is called
  // multiple times concurrently)
  private final Object registerTableLock = new Object();

  /**
   * Constructor for ExternalVolumeManager
   *
   * @param isTestMode whether the manager in test mode
   * @param role the role of the client
   * @param clientName the name of the client
   * @param snowflakeServiceClient the Snowflake service client used for configure calls
   */
  PresignedUrlExternalVolumeManager(
      boolean isTestMode,
      String role,
      String clientName,
      SnowflakeServiceClient snowflakeServiceClient) {
    this.clientName = clientName;
    this.role = role;
    this.serviceClient = snowflakeServiceClient;
    this.externalVolumeMap = new ConcurrentHashMap<>();
    try {
      ClientConfigureResponse response =
          this.serviceClient.clientConfigure(new ClientConfigureRequest(role));
      this.clientPrefix = isTestMode ? "testPrefix" : response.getClientPrefix();
      this.deploymentId = response.getDeploymentId();
    } catch (IngestResponseException | IOException e) {
      throw new SFException(e, ErrorCode.CLIENT_CONFIGURE_FAILURE, e.getMessage());
    }
    logger.logDebug(
        "Created PresignedUrlExternalVolumeManager with clientName=%s and clientPrefix=%s",
        clientName, clientPrefix);
  }

  /**
   * Given a fully qualified table name, return the target storage by looking up the table name
   *
   * @param fullyQualifiedTableName the target fully qualified table name
   * @return target storage
   */
  @Override
  public PresignedUrlExternalVolume getStorage(String fullyQualifiedTableName) {
    // Only one chunk per blob in Iceberg mode.
    return getVolumeSafe(fullyQualifiedTableName);
  }

  /** Informs the storage manager about a new table that's being ingested into by the client. */
  @Override
  public void registerTable(TableRef tableRef, FileLocationInfo locationInfo) {
    if (this.externalVolumeMap.containsKey(tableRef.fullyQualifiedName)) {
      logger.logInfo(
          "Skip registering table since its already been registered with the VolumeManager."
              + " tableRef=%s",
          tableRef);
      return;
    }

    // future enhancement - per table locks instead of per-client lock
    synchronized (registerTableLock) {
      if (this.externalVolumeMap.containsKey(tableRef.fullyQualifiedName)) {
        logger.logInfo(
            "Skip registering table since its already been registered with the VolumeManager."
                + " tableRef=%s",
            tableRef);
        return;
      }

      try {
        PresignedUrlExternalVolume externalVolume =
            new PresignedUrlExternalVolume(
                clientName,
                getClientPrefix(),
                deploymentId,
                role,
                tableRef,
                locationInfo,
                serviceClient);
        this.externalVolumeMap.put(tableRef.fullyQualifiedName, externalVolume);
      } catch (SFException ex) {
        logger.logError(
            "ExtVolManager.registerTable for tableRef=% failed with exception=%s", tableRef, ex);
        // allow external volume ctor's SFExceptions to bubble up directly
        throw ex;
      } catch (Exception err) {
        logger.logError(
            "ExtVolManager.registerTable for tableRef=% failed with exception=%s", tableRef, err);
        throw new SFException(
            err,
            ErrorCode.UNABLE_TO_CONNECT_TO_STAGE,
            String.format("fullyQualifiedTableName=%s", tableRef));
      }
    }
  }

  @Override
  public BlobPath generateBlobPath(String fullyQualifiedTableName) {
    PresignedUrlExternalVolume volume = getVolumeSafe(fullyQualifiedTableName);
    PresignedUrlInfo urlInfo = volume.dequeueUrlInfo();
    return new BlobPath(urlInfo.url /* uploadPath */, urlInfo.fileName /* fileRegistrationPath */);
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

  private PresignedUrlExternalVolume getVolumeSafe(String fullyQualifiedTableName) {
    PresignedUrlExternalVolume volume = this.externalVolumeMap.get(fullyQualifiedTableName);

    if (volume == null) {
      throw new SFException(
          ErrorCode.INTERNAL_ERROR,
          String.format("No external volume found for tableRef=%s", fullyQualifiedTableName));
    }

    return volume;
  }
}
