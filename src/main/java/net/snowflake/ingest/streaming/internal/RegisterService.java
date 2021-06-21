/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.utils.Constants.BLOB_UPLOAD_TIMEOUT_IN_SEC;

import com.codahale.metrics.Timer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
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
  private SnowflakeStreamingIngestClientInternal owningClient;

  // Contains one or more blob metadata that will be registered to Snowflake
  // The key is the blob name, and the value is the BlobMetadata future that will complete when the
  // blob is uploaded successfully
  private List<Pair<String, CompletableFuture<BlobMetadata>>> blobsList;

  // Lock to protect the read/write access of m_blobsList
  private Lock blobsListLock;

  // Indicate whether we're under test mode
  private boolean isTestMode;

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
  void addBlobs(List<Pair<String, CompletableFuture<BlobMetadata>>> blobs) {
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
   * @return a list of blob names that has errors during registration
   */
  List<String> registerBlobs(Map<String, Timer.Context> latencyTimerContextMap) {
    List<String> errorBlobs = new ArrayList<>();
    if (!this.blobsList.isEmpty()) {
      // Will skip and try again later if someone else is holding the lock
      if (this.blobsListLock.tryLock()) {
        List<Pair<String, CompletableFuture<BlobMetadata>>> oldList = null;
        try {
          // Create a copy of the list when we have the lock, and then release the lock to unblock
          // other writers while we work on the old list
          oldList = new ArrayList<>(this.blobsList);
          this.blobsList.clear();
        } finally {
          this.blobsListLock.unlock();
        }

        if (!oldList.isEmpty()) {
          List<BlobMetadata> blobs = new ArrayList<>();
          oldList.forEach(
              futureBlob -> {
                try {
                  logger.logDebug("Start waiting on uploading blob={}", futureBlob.getKey());
                  // Wait for uploading to finish, add a timeout in case something bad happens
                  BlobMetadata blob =
                      futureBlob.getValue().get(BLOB_UPLOAD_TIMEOUT_IN_SEC, TimeUnit.SECONDS);
                  logger.logDebug("Finish waiting on uploading blob={}", futureBlob.getKey());
                  if (blob != null) {
                    blobs.add(blob);
                  }
                } catch (InterruptedException | ExecutionException | TimeoutException e) {
                  // Don't throw here if the blob upload times out or encounters other exceptions,
                  // since we still want to continue register the good blobs in the list
                  // In the future we may want to do error-specific handling for unexpected
                  // exceptions
                  // TODO SNOW-348859: ideally we want to invalidate all channels in the blob if
                  // register failed
                  logger.logError("Uploading blob failed={}, exception={}", futureBlob.getKey(), e);
                  errorBlobs.add(futureBlob.getKey());
                }
              });
          if (blobs.size() > 0 && !isTestMode) {
            Timer.Context registerContext =
                Utils.createTimerContext(this.owningClient.registerLatency);

            this.owningClient.registerBlobs(blobs);

            if (registerContext != null) {
              registerContext.stop();
              blobs.forEach(
                  blob ->
                      latencyTimerContextMap.computeIfPresent(
                          blob.getName(),
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
  List<Pair<String, CompletableFuture<BlobMetadata>>> getBlobsList() {
    return this.blobsList;
  }
}
