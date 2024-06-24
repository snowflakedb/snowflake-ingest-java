/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.connection.ServiceResponseHandler.ApiName.STREAMING_CLIENT_CONFIGURE;
import static net.snowflake.ingest.utils.Constants.BLOB_EXTENSION_TYPE;
import static net.snowflake.ingest.utils.Constants.CLIENT_CONFIGURE_ENDPOINT;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;
import net.snowflake.ingest.utils.Utils;

/** Class to manage single Snowflake internal stage */
class InternalStageManager<T> implements StorageManager<T> {
  // Target stage for the client
  private final StreamingIngestStorage targetStage;

  // Increasing counter to generate a unique blob name per client
  private final AtomicLong counter;

  // Whether the manager in test mode
  private final boolean isTestMode;

  /**
   * Constructor for InternalStageManager
   *
   * @param isTestMode whether the manager in test mode
   * @param client the owning client
   */
  InternalStageManager(boolean isTestMode, SnowflakeStreamingIngestClientInternal<T> client) {
    ConfigureCallHandler configureCallHandler =
        ConfigureCallHandler.builder(
                client.getHttpClient(),
                client.getRequestBuilder(),
                STREAMING_CLIENT_CONFIGURE,
                CLIENT_CONFIGURE_ENDPOINT)
            .setRole(client.getRole())
            .setIsTestMode(isTestMode)
            .build();
    this.isTestMode = isTestMode;
    this.counter = new AtomicLong(0);
    try {
      targetStage =
          new StreamingIngestStorage(
              isTestMode, configureCallHandler, client.getName(), DEFAULT_MAX_UPLOAD_RETRIES);
    } catch (SnowflakeSQLException | IOException err) {
      throw new SFException(err, ErrorCode.UNABLE_TO_CONNECT_TO_STAGE);
    }
  }

  /**
   * Get the storage. In this case, the storage is always the target stage as there's only one stage
   * in non-iceberg mode.
   *
   * @param blobData this parameter does not affect the method outcome
   * @return the target storage
   */
  @Override
  @SuppressWarnings("unused")
  public StreamingIngestStorage getStorage(List<List<ChannelData<T>>> blobData) {
    // There's always only one stage for the client in non-iceberg mode
    return targetStage;
  }

  /**
   * Add storage to the manager. Should not be called in non-iceberg mode.
   *
   * @param openChannelResponse this parameter does not affect the instance
   */
  @Override
  public void addStorage(OpenChannelResponse openChannelResponse) {}

  /**
   * Generate a blob path, which is: "YEAR/MONTH/DAY_OF_MONTH/HOUR_OF_DAY/MINUTE/<current utc
   * timestamp + client unique prefix + thread id + counter>.BDEC"
   *
   * @return the generated blob file path
   */
  @Override
  public String generateBlobPath() {
    Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    return getBlobPath(calendar, this.targetStage.getClientPrefix());
  }

  /**
   * Get the unique client prefix generated by the Snowflake server
   *
   * @return the client prefix
   */
  @Override
  public String getClientPrefix() {
    return this.targetStage.getClientPrefix();
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
}
