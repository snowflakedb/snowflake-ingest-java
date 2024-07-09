/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.utils.Constants.BLOB_EXTENSION_TYPE;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.Calendar;
import java.util.Optional;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.ingest.connection.IngestResponseException;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;
import net.snowflake.ingest.utils.Utils;

class InternalStageLocation {
  public InternalStageLocation() {}
}

/** Class to manage single Snowflake internal stage */
class InternalStageManager<T> implements StorageManager<T, InternalStageLocation> {
  // Target stage for the client
  private final StreamingIngestStorage<T, InternalStageLocation> targetStage;

  // Increasing counter to generate a unique blob name per client
  private final AtomicLong counter;

  // Whether the manager in test mode
  private final boolean isTestMode;

  // Snowflake service client used for configure calls
  private final SnowflakeServiceClient snowflakeServiceClient;

  // The role of the client
  private final String role;

  // Client prefix generated by the Snowflake server
  private final String clientPrefix;

  /**
   * Constructor for InternalStageManager
   *
   * @param isTestMode whether the manager in test mode
   * @param role the role of the client
   * @param clientName the name of the client
   * @param snowflakeServiceClient the Snowflake service client to use for configure calls
   */
  InternalStageManager(
      boolean isTestMode,
      String role,
      String clientName,
      SnowflakeServiceClient snowflakeServiceClient) {
    this.snowflakeServiceClient = snowflakeServiceClient;
    this.isTestMode = isTestMode;
    this.role = role;
    this.counter = new AtomicLong(0);
    try {
      if (!isTestMode) {
        ClientConfigureResponse response =
            this.snowflakeServiceClient.clientConfigure(new ClientConfigureRequest(role));
        this.clientPrefix = response.getClientPrefix();
        this.targetStage =
            new StreamingIngestStorage<T, InternalStageLocation>(
                this,
                clientName,
                response.getStageLocation(),
                new InternalStageLocation(),
                DEFAULT_MAX_UPLOAD_RETRIES);
      } else {
        this.clientPrefix = "testPrefix";
        this.targetStage =
            new StreamingIngestStorage<T, InternalStageLocation>(
                this,
                "testClient",
                (StreamingIngestStorage.SnowflakeFileTransferMetadataWithAge) null,
                new InternalStageLocation(),
                DEFAULT_MAX_UPLOAD_RETRIES);
      }
    } catch (IngestResponseException | IOException e) {
      throw new SFException(e, ErrorCode.CLIENT_CONFIGURE_FAILURE, e.getMessage());
    } catch (SnowflakeSQLException e) {
      throw new SFException(e, ErrorCode.UNABLE_TO_CONNECT_TO_STORAGE, e.getMessage());
    }
  }

  /**
   * Get the storage. In this case, the storage is always the target stage as there's only one stage
   * in non-iceberg mode.
   *
   * @param fullyQualifiedTableName the target fully qualified table name
   * @return the target storage
   */
  @Override
  @SuppressWarnings("unused")
  public StreamingIngestStorage<T, InternalStageLocation> getStorage(
      String fullyQualifiedTableName) {
    // There's always only one stage for the client in non-iceberg mode
    return targetStage;
  }

  /** Add storage to the manager. Do nothing as there's only one stage in non-Iceberg mode. */
  @Override
  public void addStorage(
      String dbName, String schemaName, String tableName, FileLocationInfo fileLocationInfo) {}

  /** Remove storage from the manager. Do nothing as there's only one stage in non-Iceberg mode. */
  @Override
  public void removeStorage(String dbName, String schemaName, String tableName) {}

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
      InternalStageLocation location, Optional<String> fileName) {
    try {
      ClientConfigureRequest request = new ClientConfigureRequest(this.role);
      fileName.ifPresent(request::setFileName);
      ClientConfigureResponse response = snowflakeServiceClient.clientConfigure(request);
      return response.getStageLocation();
    } catch (IngestResponseException | IOException e) {
      throw new SFException(e, ErrorCode.CLIENT_CONFIGURE_FAILURE, e.getMessage());
    }
  }

  /**
   * Generate a blob path, which is: "YEAR/MONTH/DAY_OF_MONTH/HOUR_OF_DAY/MINUTE/<current utc
   * timestamp + client unique prefix + thread id + counter>.BDEC"
   *
   * @return the generated blob file path
   */
  @Override
  public String generateBlobPath() {
    Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    return getBlobPath(calendar, this.clientPrefix);
  }

  @Override
  public void decrementBlobSequencer() {
    this.counter.decrementAndGet();
  }

  /** For TESTING */
  @VisibleForTesting
  public String getBlobPath(Calendar calendar, String clientPrefix) {
    if (this.isTestMode && clientPrefix == null) {
      clientPrefix = "testPrefix";
    }

    Utils.assertStringNotNullOrEmpty("client prefix", clientPrefix);
    int year = calendar.get(Calendar.YEAR);
    int month = calendar.get(Calendar.MONTH) + 1; // Gregorian calendar starts from 0
    int day = calendar.get(Calendar.DAY_OF_MONTH);
    int hour = calendar.get(Calendar.HOUR_OF_DAY);
    int minute = calendar.get(Calendar.MINUTE);
    long time = TimeUnit.MILLISECONDS.toSeconds(calendar.getTimeInMillis());
    long threadId = Thread.currentThread().getId();
    // Create the blob short name, the clientPrefix contains the deployment id
    String blobShortName =
        Long.toString(time, 36)
            + "_"
            + clientPrefix
            + "_"
            + threadId
            + "_"
            + this.counter.getAndIncrement()
            + "."
            + BLOB_EXTENSION_TYPE;
    return year + "/" + month + "/" + day + "/" + hour + "/" + minute + "/" + blobShortName;
  }

  /**
   * Get the unique client prefix generated by the Snowflake server
   *
   * @return the client prefix
   */
  @Override
  public String getClientPrefix() {
    return this.clientPrefix;
  }
}
