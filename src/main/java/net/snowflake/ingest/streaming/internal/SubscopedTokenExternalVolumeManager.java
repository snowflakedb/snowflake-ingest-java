/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import net.snowflake.ingest.connection.IngestResponseException;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.Logging;
import net.snowflake.ingest.utils.SFException;
import net.snowflake.ingest.utils.Utils;

/** Class to manage multiple external volumes */
class SubscopedTokenExternalVolumeManager implements IStorageManager {
  private static final Logging logger = new Logging(SubscopedTokenExternalVolumeManager.class);
  // Reference to the external volume per table
  private final ConcurrentHashMap<String, InternalStage> externalVolumeMap;

  /** Increasing counter to generate a unique blob name */
  private final AtomicLong counter;

  // name of the owning client
  private final String clientName;

  private final String role;

  // Reference to the Snowflake service client used for configure calls
  private final SnowflakeServiceClient serviceClient;

  // Client prefix generated by the Snowflake server
  private final String clientPrefix;

  /**
   * Constructor for ExternalVolumeManager
   *
   * @param role the role of the client
   * @param clientName the name of the client
   * @param snowflakeServiceClient the Snowflake service client used for configure calls
   */
  SubscopedTokenExternalVolumeManager(
      String role, String clientName, SnowflakeServiceClient snowflakeServiceClient) {
    this.clientName = clientName;
    this.role = role;
    this.counter = new AtomicLong(0);
    this.serviceClient = snowflakeServiceClient;
    this.externalVolumeMap = new ConcurrentHashMap<>();
    try {
      ClientConfigureResponse response =
          this.serviceClient.clientConfigure(new ClientConfigureRequest(role));
      this.clientPrefix = response.getClientPrefix();
    } catch (IngestResponseException | IOException e) {
      throw new SFException(e, ErrorCode.CLIENT_CONFIGURE_FAILURE, e.getMessage());
    }
    logger.logDebug(
        "Created SubscopedTokenExternalVolumeManager with clientName=%s and clientPrefix=%s",
        clientName, clientPrefix);
  }

  /**
   * Given a fully qualified table name, return the target storage by looking up the table name
   *
   * @param fullyQualifiedTableName the target fully qualified table name
   * @return target storage
   */
  @Override
  public InternalStage getStorage(String fullyQualifiedTableName) {
    // Only one chunk per blob in Iceberg mode.
    return getVolumeSafe(fullyQualifiedTableName);
  }

  /** Informs the storage manager about a new table that's being ingested into by the client. */
  @Override
  public void registerTable(TableRef tableRef) {
    this.externalVolumeMap.computeIfAbsent(
        tableRef.fullyQualifiedName, fqn -> createStageForTable(tableRef));
  }

  private InternalStage createStageForTable(TableRef tableRef) {
    // Get the locationInfo when we know this is the first register call for a given table. This is
    // done to reduce the
    // unnecessary overload on token generation if a client opens up a hundred channels at the same
    // time.
    FileLocationInfo locationInfo = getRefreshedLocation(tableRef, Optional.empty());

    try {
      return new InternalStage(
          this, clientName, getClientPrefix(), tableRef, locationInfo, DEFAULT_MAX_UPLOAD_RETRIES);
    } catch (SFException ex) {
      logger.logError(
          "ExtVolManager.registerTable for tableRef={} failed with exception={}", tableRef, ex);
      // allow external volume ctor's SFExceptions to bubble up directly
      throw ex;
    } catch (Exception err) {
      logger.logError(
          "ExtVolManager.registerTable for tableRef={} failed with exception={}", tableRef, err);
      throw new SFException(
          err,
          ErrorCode.UNABLE_TO_CONNECT_TO_STAGE,
          String.format("fullyQualifiedTableName=%s", tableRef));
    }
  }

  @Override
  public BlobPath generateBlobPath(String fullyQualifiedTableName) {
    InternalStage volume = getVolumeSafe(fullyQualifiedTableName);

    // {nullableTableBasePath}/data/streaming_ingest/{figsId}/snow_{volumeHash}_{figsId}_{workerRank}_1_
    return generateBlobPathFromLocationInfoPath(
        fullyQualifiedTableName,
        volume.getFileLocationInfo().getPath(),
        Utils.getTwoHexChars(),
        this.counter.getAndIncrement());
  }

  @VisibleForTesting
  static BlobPath generateBlobPathFromLocationInfoPath(
      String fullyQualifiedTableName,
      String filePathRelativeToVolume,
      String twoHexChars,
      long counterValue) {
    String[] parts = filePathRelativeToVolume.split("/");
    if (parts.length < 5) {
      logger.logError(
          "Invalid file path returned by server. Table={} FilePathRelativeToVolume={}",
          fullyQualifiedTableName,
          filePathRelativeToVolume);
      throw new SFException(ErrorCode.INTERNAL_ERROR, "File path returned by server is invalid");
    }

    // add twoHexChars as a prefix to the fileName (the last part of fileLocationInfo.getPath)
    String fileNameRelativeToCredentialedPath = parts[parts.length - 1];
    fileNameRelativeToCredentialedPath =
        String.join("/", twoHexChars, fileNameRelativeToCredentialedPath);

    // set this new fileName (with the prefix) back on the parts array so the full path can be
    // reconstructed
    parts[parts.length - 1] = fileNameRelativeToCredentialedPath;
    filePathRelativeToVolume = String.join("/", parts);

    // add a monotonically increasing counter at the end and the file extension
    String suffix = counterValue + ".parquet";

    return new BlobPath(
        fileNameRelativeToCredentialedPath + suffix /* uploadPath */,
        filePathRelativeToVolume + suffix /* fileRegistrationPath */);
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

  @Override
  public FileLocationInfo getRefreshedLocation(TableRef tableRef, Optional<String> fileName) {
    try {
      RefreshTableInformationResponse response =
          this.serviceClient.refreshTableInformation(
              new RefreshTableInformationRequest(tableRef, this.role, true));
      logger.logDebug("Refreshed tokens for table={}", tableRef);
      if (response.getIcebergLocationInfo() == null) {
        logger.logError(
            "Did not receive location info, this will cause ingestion to grind to a halt."
                + " TableRef={}",
            tableRef);
      } else {
        Map<String, String> creds = response.getIcebergLocationInfo().getCredentials();
        if (creds == null || creds.isEmpty()) {
          logger.logError(
              "Did not receive creds in location info, this will cause ingestion to grind to a"
                  + " halt. TableRef={}",
              tableRef);
        }
      }

      return response.getIcebergLocationInfo();
    } catch (IngestResponseException | IOException e) {
      throw new SFException(e, ErrorCode.REFRESH_TABLE_INFORMATION_FAILURE, e.getMessage());
    }
  }

  private InternalStage getVolumeSafe(String fullyQualifiedTableName) {
    InternalStage volume = this.externalVolumeMap.get(fullyQualifiedTableName);

    if (volume == null) {
      throw new SFException(
          ErrorCode.INTERNAL_ERROR,
          String.format("No external volume found for tableRef=%s", fullyQualifiedTableName));
    }

    return volume;
  }
}
