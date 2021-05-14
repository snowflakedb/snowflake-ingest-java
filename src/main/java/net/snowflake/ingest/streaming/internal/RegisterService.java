/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.streaming.internal.Constants.BLOB_UPLOAD_TIMEOUT_IN_SEC;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.utils.Logging;
import net.snowflake.ingest.utils.Pair;
import net.snowflake.ingest.utils.StreamingUtils;

/**
 * Register one or more blobs to the targeted Snowflake table, it will be done using the dedicated
 * thread in order to maintain ordering per channel
 */
public class RegisterService {

  private static final Logging logger = new Logging(RegisterService.class);

  // Reference to the client that owns this register service
  private SnowflakeStreamingIngestClient owningClient;

  // Contains one or more blob metadata that will be registered to Snowflake
  // The key is the blob name, and the value is the BlobMetadata future that will complete when the
  // blob is uploaded successfully
  private List<Pair<String, CompletableFuture<BlobMetadata>>> blobsList;

  // Lock to protect the read/write access of m_blobsList
  private Lock blobsListLock;

  /**
   * Construct a RegisterService object
   *
   * @param client
   */
  public RegisterService(SnowflakeStreamingIngestClient client) {
    this.owningClient = client;
    this.blobsList = new ArrayList<>();
    this.blobsListLock = new ReentrantLock();
  }

  /**
   * Add one or more blob metadata to the list
   *
   * @param blobs
   */
  public void addBlobs(List<Pair<String, CompletableFuture<BlobMetadata>>> blobs) {
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
   */
  public List<String> registerBlobs() {
    logger.logDebug("Start registering blob task");
    List<String> errorBlobs = new ArrayList<>();
    if (this.blobsList.isEmpty()) {
      logger.logDebug("No blob to register");
    } else {
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
                  logger.logDebug("Start waiting on uploading blob: {}", futureBlob.getKey());
                  // Wait for uploading to finish, add a timeout in case something bad happens
                  BlobMetadata blob =
                      futureBlob.getValue().get(BLOB_UPLOAD_TIMEOUT_IN_SEC, TimeUnit.SECONDS);
                  logger.logDebug("Finish waiting on uploading blob: {}", futureBlob.getKey());
                  StreamingUtils.assertNotNull("uploaded blob", blob);
                  blobs.add(blob);
                } catch (InterruptedException | ExecutionException | TimeoutException e) {
                  // Don't throw here if the blob upload times out or encounters other exceptions,
                  // since we still want to continue register the good blobs in the list
                  // In the future we may want to do error-specific handling for unexpected
                  // exceptions
                  // TODO SNOW-348859: ideally we want to invalidate all channels in the blob if
                  // register failed
                  logger.logError(
                      "Uploading blob failed: {}, exception: {}", futureBlob.getKey(), e);
                  errorBlobs.add(futureBlob.getKey());
                }
              });
          if (blobs.size() > 0) {
            this.owningClient.registerBlobs(blobs);
          }
        }
      }
    }
    logger.logDebug("Finish registering blob task");
    return errorBlobs;
  }

  /**
   * Get the blobsList, this is for test only, no lock protection
   *
   * @return the blobsList
   */
  public List<Pair<String, CompletableFuture<BlobMetadata>>> getBlobsList() {
    return this.blobsList;
  }
}
