/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.utils.Constants.BLOB_UPLOAD_MAX_RETRY_COUNT;
import static net.snowflake.ingest.utils.Constants.BLOB_UPLOAD_TIMEOUT_IN_SEC;

import com.codahale.metrics.Timer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import net.snowflake.ingest.utils.Logging;
import net.snowflake.ingest.utils.Pair;
import net.snowflake.ingest.utils.Utils;

/**
 * Register one or more blobs to the targeted Snowflake table, it will be done using the dedicated
 * thread in order to maintain ordering per channel
 */
class RegisterService {

  private static final Logging logger = new Logging(RegisterService.class);

  // Reference to the client that owns this register service
  private final SnowflakeStreamingIngestClientInternal owningClient;

  // Contains one or more blob metadata that will be registered to Snowflake
  // The key is the BlobData object, and the value is the BlobMetadata future that will complete
  // when the blob is uploaded successfully
  private final List<Pair<FlushService.BlobData, CompletableFuture<BlobMetadata>>> blobsList;

  // Lock to protect the read/write access of m_blobsList
  private final Lock blobsListLock;

  // Indicate whether we're under test mode
  private final boolean isTestMode;

  /**
   * Construct a RegisterService object
   *
   * @param client
   */
  RegisterService(SnowflakeStreamingIngestClientInternal client, boolean isTestMode) {
    this.owningClient = client;
    this.blobsList = new ArrayList<>();
    this.blobsListLock = new ReentrantLock();
    this.isTestMode = isTestMode;
  }

  /**
   * Add one or more blob metadata to the list
   *
   * @param blobs
   */
  void addBlobs(List<Pair<FlushService.BlobData, CompletableFuture<BlobMetadata>>> blobs) {
    if (!blobs.isEmpty()) {
      this.blobsListLock.lock();
      try {
        this.blobsList.addAll(blobs);
      } finally {
        this.blobsListLock.unlock();
      }
    }
  }

  /**
   * Register the blobs to Snowflake. This method is called serially from a single thread to ensure
   * the ordering is maintained across independent blobs in the same channel.
   *
   * @param latencyTimerContextMap the map that stores the latency timer for each blob
   * @return a list of blob names that have errors during registration
   */
  List<FlushService.BlobData> registerBlobs(Map<String, Timer.Context> latencyTimerContextMap) {
    List<FlushService.BlobData> errorBlobs = new ArrayList<>();
    if (!this.blobsList.isEmpty()) {
      // Will skip and try again later if someone else is holding the lock
      if (this.blobsListLock.tryLock()) {
        List<Pair<FlushService.BlobData, CompletableFuture<BlobMetadata>>> oldList = null;
        try {
          // Create a copy of the list when we have the lock, and then release the lock to unblock
          // other writers while we work on the old list
          oldList = new ArrayList<>(this.blobsList);
          this.blobsList.clear();
        } finally {
          this.blobsListLock.unlock();
        }

        // In order to guarantee fairness between the time spent on waiting blob uploading VS blob
        // registering and make sure the delay on server side commit is relatively small:
        // 1. If no exception and total time is less than or equal to
        // BLOB_UPLOAD_TIMEOUT_IN_SEC * 2, we will wait for all blobs to be uploaded and then
        // register them
        // 2. If no exception and total time is bigger than BLOB_UPLOAD_TIMEOUT_IN_SEC * 2, we
        // will break the waiting and register the processed blob first
        // 3. If hitting non-timeout exception, we will skip the current blob and continue on
        // processing next blob
        // 4. If hitting timeout exception, we will break the waiting if we haven't reached
        // BLOB_UPLOAD_TIMEOUT_IN_SEC * BLOB_UPLOAD_MAX_RETRY_COUNT and register the
        // processed blob first. Otherwise, we will skip the current blob and continue on processing
        // next blob if time out has been reached.
        int idx = 0;
        int retry = 0;
        while (idx < oldList.size()) {
          List<BlobMetadata> blobs = new ArrayList<>();
          long startTime = System.currentTimeMillis();
          while (idx < oldList.size()
              && System.currentTimeMillis() - startTime <= BLOB_UPLOAD_TIMEOUT_IN_SEC * 2) {
            Pair<FlushService.BlobData, CompletableFuture<BlobMetadata>> futureBlob =
                oldList.get(idx);
            try {
              logger.logDebug(
                  "Start waiting on uploading blob={}", futureBlob.getKey().getFilePath());
              // Wait for uploading to finish
              BlobMetadata blob =
                  futureBlob.getValue().get(BLOB_UPLOAD_TIMEOUT_IN_SEC, TimeUnit.SECONDS);
              logger.logDebug(
                  "Finish waiting on uploading blob={}", futureBlob.getKey().getFilePath());
              if (blob != null) {
                blobs.add(blob);
              }
              retry = 0;
              idx++;
            } catch (Exception e) {
              // Don't throw here if the blob upload encounters exceptions, since we still want to
              // continue register the following blobs after the bad one. Note that it's possible
              // that the following blobs contain invalid data from the invalidated channels if the
              // blobs are generated before these channels got invalidated.

              // Retry logic for timeout exception only
              if (e instanceof TimeoutException && retry < BLOB_UPLOAD_MAX_RETRY_COUNT) {
                logger.logInfo(
                    "Retry on waiting for uploading blob={}", futureBlob.getKey().getFilePath());
                retry++;
                break;
              }
              StringBuilder stackTrace = new StringBuilder();
              if (e.getCause() != null) {
                for (StackTraceElement element : e.getCause().getStackTrace()) {
                  stackTrace.append(System.lineSeparator()).append(element.toString());
                }
              }
              String errorMessage =
                  String.format(
                      "Building or uploading blob failed=%s, exception=%s, detail=%s, cause=%s,"
                          + " detail=%s trace=%s all channels in the blob will be invalidated",
                      futureBlob.getKey().getFilePath(),
                      e.toString(),
                      e.getMessage(),
                      e.getCause(),
                      e.getCause() == null ? null : e.getCause().getMessage(),
                      stackTrace.toString());
              logger.logError(errorMessage);
              if (this.owningClient.getTelemetryService() != null) {
                this.owningClient
                    .getTelemetryService()
                    .reportClientFailure(this.getClass().getSimpleName(), errorMessage);
              }
              this.owningClient
                  .getFlushService()
                  .invalidateAllChannelsInBlob(futureBlob.getKey().getData());
              errorBlobs.add(futureBlob.getKey());
              retry = 0;
              idx++;
            }
          }

          if (blobs.size() > 0 && !isTestMode) {
            logger.logInfo(
                "Start to registering blobs in client={}, totalBlobListSize={},"
                    + " currentBlobListSize={}",
                this.owningClient.getName(),
                oldList.size(),
                blobs.size());
            Timer.Context registerContext =
                Utils.createTimerContext(this.owningClient.registerLatency);

            // Register the blobs, and invalidate any channels that return a failure status code
            this.owningClient.registerBlobs(blobs);

            if (registerContext != null) {
              registerContext.stop();
              blobs.forEach(
                  blob ->
                      latencyTimerContextMap.computeIfPresent(
                          blob.getPath(),
                          (k, v) -> {
                            v.stop();
                            return null;
                          }));
            }
          }
        }
      }
    }
    return errorBlobs;
  }

  /**
   * Get the blobsList, this is for TEST ONLY, no lock protection
   *
   * @return the blobsList
   */
  List<Pair<FlushService.BlobData, CompletableFuture<BlobMetadata>>> getBlobsList() {
    return this.blobsList;
  }
}
