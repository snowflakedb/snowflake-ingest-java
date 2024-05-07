/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.utils.Constants.BLOB_EXTENSION_TYPE;
import static net.snowflake.ingest.utils.Constants.DISABLE_BACKGROUND_FLUSH;
import static net.snowflake.ingest.utils.Constants.MAX_BLOB_SIZE_IN_BYTES;
import static net.snowflake.ingest.utils.Constants.MAX_THREAD_COUNT;
import static net.snowflake.ingest.utils.Constants.THREAD_SHUTDOWN_TIMEOUT_IN_SEC;
import static net.snowflake.ingest.utils.Utils.getStackTrace;

import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.jdbc.internal.google.common.util.concurrent.ThreadFactoryBuilder;
import net.snowflake.ingest.utils.Constants;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.Logging;
import net.snowflake.ingest.utils.Pair;
import net.snowflake.ingest.utils.SFException;
import net.snowflake.ingest.utils.Utils;

/**
 * Responsible for flushing data from client to Snowflake tables. When a flush is triggered, it will
 * do the following in order:
 * <li>take the data from all the channel row buffers
 * <li>build the blob using the ChannelData returned from previous step
 * <li>upload the blob to stage
 * <li>register the blob to the targeted Snowflake table
 *
 * @param <T> type of column data ({@link ParquetChunkData})
 */
class FlushService<T> {

  // The max number of upload retry attempts to the stage
  private static final int DEFAULT_MAX_UPLOAD_RETRIES = 5;

  // Static class to save the list of channels that are used to build a blob, which is mainly used
  // to invalidate all the channels when there is a failure
  static class BlobData<T> {
    private final String path;
    private final List<List<ChannelData<T>>> data;

    BlobData(String path, List<List<ChannelData<T>>> data) {
      this.path = path;
      this.data = data;
    }

    String getPath() {
      return path;
    }

    List<List<ChannelData<T>>> getData() {
      return data;
    }
  }

  private static final Logging logger = new Logging(FlushService.class);

  // Increasing counter to generate a unique blob name per client
  private final AtomicLong counter;

  // The client that owns this flush service
  private final SnowflakeStreamingIngestClientInternal<T> owningClient;

  // Thread to schedule the flush job
  @VisibleForTesting ScheduledExecutorService flushWorker;

  // Thread to register the blob
  @VisibleForTesting ExecutorService registerWorker;

  // Threads to build and upload the blob
  @VisibleForTesting ExecutorService buildUploadWorkers;

  // Reference to the channel cache
  private final ChannelCache<T> channelCache;

  // Reference to the Streaming Ingest stage
  private final StreamingIngestStage targetStage;

  // Reference to register service
  private final RegisterService<T> registerService;

  // Indicates whether we need to schedule a flush
  @VisibleForTesting volatile boolean isNeedFlush;

  // Latest flush time
  @VisibleForTesting volatile long lastFlushTime;

  // Indicates whether it's running as part of the test
  private final boolean isTestMode;

  // A map which stores the timer for each blob in order to capture the flush latency
  private final Map<String, Timer.Context> latencyTimerContextMap;

  // blob encoding version
  private final Constants.BdecVersion bdecVersion;

  /**
   * Constructor for TESTING that takes (usually mocked) StreamingIngestStage
   *
   * @param client
   * @param cache
   * @param isTestMode
   */
  FlushService(
      SnowflakeStreamingIngestClientInternal<T> client,
      ChannelCache<T> cache,
      StreamingIngestStage targetStage, // For testing
      boolean isTestMode) {
    this.owningClient = client;
    this.channelCache = cache;
    this.targetStage = targetStage;
    this.counter = new AtomicLong(0);
    this.registerService = new RegisterService<>(client, isTestMode);
    this.isNeedFlush = false;
    this.lastFlushTime = System.currentTimeMillis();
    this.isTestMode = isTestMode;
    this.latencyTimerContextMap = new ConcurrentHashMap<>();
    this.bdecVersion = this.owningClient.getParameterProvider().getBlobFormatVersion();
    createWorkers();
  }

  /**
   * Default constructor
   *
   * @param client
   * @param cache
   * @param isTestMode
   */
  FlushService(
      SnowflakeStreamingIngestClientInternal<T> client, ChannelCache<T> cache, boolean isTestMode) {
    this.owningClient = client;
    this.channelCache = cache;
    try {
      this.targetStage =
          new StreamingIngestStage(
              isTestMode,
              client.getRole(),
              client.getHttpClient(),
              client.getRequestBuilder(),
              client.getName(),
              DEFAULT_MAX_UPLOAD_RETRIES);
    } catch (SnowflakeSQLException | IOException err) {
      throw new SFException(err, ErrorCode.UNABLE_TO_CONNECT_TO_STAGE);
    }

    this.registerService = new RegisterService<>(client, isTestMode);
    this.counter = new AtomicLong(0);
    this.isNeedFlush = false;
    this.lastFlushTime = System.currentTimeMillis();
    this.isTestMode = isTestMode;
    this.latencyTimerContextMap = new ConcurrentHashMap<>();
    this.bdecVersion = this.owningClient.getParameterProvider().getBlobFormatVersion();
    createWorkers();
  }

  /**
   * Updates performance stats enabled
   *
   * @return
   */
  private CompletableFuture<Void> statsFuture() {
    return CompletableFuture.runAsync(
        () -> {
          if (this.owningClient.cpuHistogram != null) {
            double cpuLoad =
                ManagementFactory.getPlatformMXBean(com.sun.management.OperatingSystemMXBean.class)
                    .getProcessCpuLoad();
            this.owningClient.cpuHistogram.update((long) (cpuLoad * 100));
          }
          return;
        },
        this.flushWorker);
  }

  /**
   * @param isForce if true will flush regardless of other conditions
   * @param timeDiffMillis Time in milliseconds since the last flush
   * @return
   */
  private CompletableFuture<Void> distributeFlush(boolean isForce, long timeDiffMillis) {
    return CompletableFuture.runAsync(
        () -> {
          logFlushTask(isForce, timeDiffMillis);
          distributeFlushTasks();
          this.isNeedFlush = false;
          this.lastFlushTime = System.currentTimeMillis();
          return;
        },
        this.flushWorker);
  }

  /** If tracing is enabled, print always else, check if it needs flush or is forceful. */
  private void logFlushTask(boolean isForce, long timeDiffMillis) {
    final String flushTaskLogFormat =
        String.format(
            "Submit forced or ad-hoc flush task on client=%s, isForce=%s,"
                + " isNeedFlush=%s, timeDiffMillis=%s, currentDiffMillis=%s",
            this.owningClient.getName(),
            isForce,
            this.isNeedFlush,
            timeDiffMillis,
            System.currentTimeMillis() - this.lastFlushTime);
    if (logger.isTraceEnabled()) {
      logger.logTrace(flushTaskLogFormat);
    }
    if (!logger.isTraceEnabled() && (this.isNeedFlush || isForce)) {
      logger.logDebug(flushTaskLogFormat);
    }
  }

  /**
   * Registers blobs with Snowflake
   *
   * @return
   */
  private CompletableFuture<Void> registerFuture() {
    return CompletableFuture.runAsync(
        () -> this.registerService.registerBlobs(latencyTimerContextMap), this.registerWorker);
  }

  /**
   * Kick off a flush job and distribute the tasks if one of the following conditions is met:
   * <li>Flush is forced by the users
   * <li>One or more buffers have reached the flush size
   * <li>Periodical background flush when a time interval has reached
   *
   * @param isForce
   * @return Completable future that will return when the blobs are registered successfully, or null
   *     if none of the conditions is met above
   */
  CompletableFuture<Void> flush(boolean isForce) {
    long timeDiffMillis = System.currentTimeMillis() - this.lastFlushTime;

    if (isForce
        || (!DISABLE_BACKGROUND_FLUSH
            && !isTestMode()
            && (this.isNeedFlush
                || timeDiffMillis
                    >= this.owningClient.getParameterProvider().getCachedMaxClientLagInMs()))) {

      return this.statsFuture()
          .thenCompose((v) -> this.distributeFlush(isForce, timeDiffMillis))
          .thenCompose((v) -> this.registerFuture());
    }
    return this.statsFuture();
  }

  /** Create the workers for each specific job */
  private void createWorkers() {
    // Create thread for checking and scheduling flush job
    ThreadFactory flushThreadFactory =
        new ThreadFactoryBuilder().setNameFormat("ingest-flush-thread").build();
    this.flushWorker = Executors.newSingleThreadScheduledExecutor(flushThreadFactory);
    this.flushWorker.scheduleWithFixedDelay(
        () -> {
          try {
            flush(false)
                .exceptionally(
                    e -> {
                      String errorMessage =
                          String.format(
                              "Background flush task failed, client=%s, exception=%s, detail=%s,"
                                  + " trace=%s.",
                              this.owningClient.getName(),
                              e.getCause(),
                              e.getCause().getMessage(),
                              getStackTrace(e.getCause()));
                      logger.logError(errorMessage);
                      if (this.owningClient.getTelemetryService() != null) {
                        this.owningClient
                            .getTelemetryService()
                            .reportClientFailure(this.getClass().getSimpleName(), errorMessage);
                      }
                      return null;
                    });
          } catch (Exception e) {
            String errorMessage =
                String.format(
                    "Failed to schedule a flush task, client=%s, exception=%s, detail=%s,"
                        + " trace=%s.",
                    this.owningClient.getName(),
                    e.getClass().getName(),
                    e.getMessage(),
                    getStackTrace(e));
            logger.logError(errorMessage);
          }
        },
        0,
        this.owningClient.getParameterProvider().getBufferFlushCheckIntervalInMs(),
        TimeUnit.MILLISECONDS);

    // Create thread for registering blobs
    ThreadFactory registerThreadFactory =
        new ThreadFactoryBuilder().setNameFormat("ingest-register-thread").build();
    this.registerWorker = Executors.newSingleThreadExecutor(registerThreadFactory);

    // Create threads for building and uploading blobs
    // Size: number of available processors * (1 + IO time/CPU time)
    ThreadFactory buildUploadThreadFactory =
        new ThreadFactoryBuilder().setNameFormat("ingest-build-upload-thread-%d").build();
    int buildUploadThreadCount =
        Math.min(
            Runtime.getRuntime().availableProcessors()
                * (1 + this.owningClient.getParameterProvider().getIOTimeCpuRatio()),
            MAX_THREAD_COUNT);
    this.buildUploadWorkers =
        Executors.newFixedThreadPool(buildUploadThreadCount, buildUploadThreadFactory);

    logger.logInfo(
        "Create {} threads for build/upload blobs for client={}, total available processors={}",
        buildUploadThreadCount,
        this.owningClient.getName(),
        Runtime.getRuntime().availableProcessors());
  }

  /**
   * Distribute the flush tasks by iterating through all the channels in the channel cache and kick
   * off a build blob work when certain size has reached or we have reached the end
   */
  void distributeFlushTasks() {
    Iterator<
            Map.Entry<
                String, ConcurrentHashMap<String, SnowflakeStreamingIngestChannelInternal<T>>>>
        itr = this.channelCache.iterator();
    List<Pair<BlobData<T>, CompletableFuture<BlobMetadata>>> blobs = new ArrayList<>();
    List<ChannelData<T>> leftoverChannelsDataPerTable = new ArrayList<>();

    while (itr.hasNext() || !leftoverChannelsDataPerTable.isEmpty()) {
      List<List<ChannelData<T>>> blobData = new ArrayList<>();
      float totalBufferSizeInBytes = 0F;
      final String blobPath = getBlobPath(this.targetStage.getClientPrefix());

      // Distribute work at table level, split the blob if reaching the blob size limit or the
      // channel has different encryption key ids
      while (itr.hasNext() || !leftoverChannelsDataPerTable.isEmpty()) {
        List<ChannelData<T>> channelsDataPerTable = Collections.synchronizedList(new ArrayList<>());
        if (!leftoverChannelsDataPerTable.isEmpty()) {
          channelsDataPerTable.addAll(leftoverChannelsDataPerTable);
          leftoverChannelsDataPerTable.clear();
        } else if (blobData.size()
            >= this.owningClient
                .getParameterProvider()
                .getMaxChunksInBlobAndRegistrationRequest()) {
          // Create a new blob if the current one already contains max allowed number of chunks
          logger.logInfo(
              "Max allowed number of chunks in the current blob reached. chunkCount={}"
                  + " maxChunkCount={} currentBlobPath={}",
              blobData.size(),
              this.owningClient.getParameterProvider().getMaxChunksInBlobAndRegistrationRequest(),
              blobPath);
          break;
        } else {
          ConcurrentHashMap<String, SnowflakeStreamingIngestChannelInternal<T>> table =
              itr.next().getValue();
          // Use parallel stream since getData could be the performance bottleneck when we have a
          // high number of channels
          table.values().parallelStream()
              .forEach(
                  channel -> {
                    if (channel.isValid()) {
                      ChannelData<T> data = channel.getData(blobPath);
                      if (data != null) {
                        channelsDataPerTable.add(data);
                      }
                    }
                  });
        }

        if (!channelsDataPerTable.isEmpty()) {
          int idx = 0;
          float totalBufferSizePerTableInBytes = 0F;
          while (idx < channelsDataPerTable.size()) {
            ChannelData<T> channelData = channelsDataPerTable.get(idx);
            // Stop processing the rest of channels when needed
            if (idx > 0
                && shouldStopProcessing(
                    totalBufferSizeInBytes,
                    totalBufferSizePerTableInBytes,
                    channelData,
                    channelsDataPerTable.get(idx - 1))) {
              leftoverChannelsDataPerTable.addAll(
                  channelsDataPerTable.subList(idx, channelsDataPerTable.size()));
              logger.logInfo(
                  "Creation of another blob is needed because of blob/chunk size limit or"
                      + " different encryption ids or different schema, client={}, table={},"
                      + " blobSize={}, chunkSize={}, nextChannelSize={}, encryptionId1={},"
                      + " encryptionId2={}, schema1={}, schema2={}",
                  this.owningClient.getName(),
                  channelData.getChannelContext().getTableName(),
                  totalBufferSizeInBytes,
                  totalBufferSizePerTableInBytes,
                  channelData.getBufferSize(),
                  channelData.getChannelContext().getEncryptionKeyId(),
                  channelsDataPerTable.get(idx - 1).getChannelContext().getEncryptionKeyId(),
                  channelData.getColumnEps().keySet(),
                  channelsDataPerTable.get(idx - 1).getColumnEps().keySet());
              break;
            }
            totalBufferSizeInBytes += channelData.getBufferSize();
            totalBufferSizePerTableInBytes += channelData.getBufferSize();
            idx++;
          }
          // Add processed channels to the current blob, stop if we need to create a new blob
          blobData.add(channelsDataPerTable.subList(0, idx));
          if (idx != channelsDataPerTable.size()) {
            break;
          }
        }
      }

      // Kick off a build job
      if (blobData.isEmpty()) {
        // we decrement the counter so that we do not have gaps in the blob names created by this
        // client. See method getBlobPath() below.
        this.counter.decrementAndGet();
      } else {
        long flushStartMs = System.currentTimeMillis();
        if (this.owningClient.flushLatency != null) {
          latencyTimerContextMap.putIfAbsent(blobPath, this.owningClient.flushLatency.time());
        }
        blobs.add(
            new Pair<>(
                new BlobData<>(blobPath, blobData),
                CompletableFuture.supplyAsync(
                    () -> {
                      try {
                        BlobMetadata blobMetadata = buildAndUpload(blobPath, blobData);
                        blobMetadata.getBlobStats().setFlushStartMs(flushStartMs);
                        return blobMetadata;
                      } catch (Throwable e) {
                        Throwable ex = e.getCause() == null ? e : e.getCause();
                        String errorMessage =
                            String.format(
                                "Building blob failed, client=%s, blob=%s, exception=%s,"
                                    + " detail=%s, trace=%s, all channels in the blob will be"
                                    + " invalidated",
                                this.owningClient.getName(),
                                blobPath,
                                ex,
                                ex.getMessage(),
                                getStackTrace(ex));
                        logger.logError(errorMessage);
                        if (this.owningClient.getTelemetryService() != null) {
                          this.owningClient
                              .getTelemetryService()
                              .reportClientFailure(this.getClass().getSimpleName(), errorMessage);
                        }

                        if (e instanceof IOException) {
                          invalidateAllChannelsInBlob(blobData, errorMessage);
                          return null;
                        } else if (e instanceof NoSuchAlgorithmException) {
                          throw new SFException(e, ErrorCode.MD5_HASHING_NOT_AVAILABLE);
                        } else if (e instanceof InvalidAlgorithmParameterException
                            | e instanceof NoSuchPaddingException
                            | e instanceof IllegalBlockSizeException
                            | e instanceof BadPaddingException
                            | e instanceof InvalidKeyException) {
                          throw new SFException(e, ErrorCode.ENCRYPTION_FAILURE);
                        } else {
                          throw new SFException(e, ErrorCode.INTERNAL_ERROR, e.getMessage());
                        }
                      }
                    },
                    this.buildUploadWorkers)));
        logger.logInfo(
            "buildAndUpload task added for client={}, blob={}, buildUploadWorkers stats={}",
            this.owningClient.getName(),
            blobPath,
            this.buildUploadWorkers.toString());
      }
    }

    // Add the flush task futures to the register service
    this.registerService.addBlobs(blobs);
  }

  /**
   * Check whether we should stop merging more channels into the same chunk, we need to stop in a
   * few cases:
   *
   * <p>When the blob size is larger than a certain threshold
   *
   * <p>When the chunk size is larger than a certain threshold
   *
   * <p>When the encryption key ids are not the same
   *
   * <p>When the schemas are not the same
   */
  private boolean shouldStopProcessing(
      float totalBufferSizeInBytes,
      float totalBufferSizePerTableInBytes,
      ChannelData<T> current,
      ChannelData<T> prev) {
    return totalBufferSizeInBytes + current.getBufferSize() > MAX_BLOB_SIZE_IN_BYTES
        || totalBufferSizePerTableInBytes + current.getBufferSize()
            > this.owningClient.getParameterProvider().getMaxChunkSizeInBytes()
        || !Objects.equals(
            current.getChannelContext().getEncryptionKeyId(),
            prev.getChannelContext().getEncryptionKeyId())
        || !current.getColumnEps().keySet().equals(prev.getColumnEps().keySet());
  }

  /**
   * Builds and uploads blob to cloud storage.
   *
   * @param blobPath Path of the destination blob in cloud storage
   * @param blobData All the data for one blob. Assumes that all ChannelData in the inner List
   *     belongs to the same table. Will error if this is not the case
   * @return BlobMetadata for FlushService.upload
   */
  BlobMetadata buildAndUpload(String blobPath, List<List<ChannelData<T>>> blobData)
      throws IOException, NoSuchAlgorithmException, InvalidAlgorithmParameterException,
          NoSuchPaddingException, IllegalBlockSizeException, BadPaddingException,
          InvalidKeyException {
    Timer.Context buildContext = Utils.createTimerContext(this.owningClient.buildLatency);

    // Construct the blob along with the metadata of the blob
    BlobBuilder.Blob blob = BlobBuilder.constructBlobAndMetadata(blobPath, blobData, bdecVersion);

    blob.blobStats.setBuildDurationMs(buildContext);

    return upload(blobPath, blob.blobBytes, blob.chunksMetadataList, blob.blobStats);
  }

  /**
   * Upload a blob to Streaming Ingest dedicated stage
   *
   * @param blobPath full path of the blob
   * @param blob blob data
   * @param metadata a list of chunk metadata
   * @param blobStats an object to track latencies and other stats of the blob
   * @return BlobMetadata object used to create the register blob request
   */
  BlobMetadata upload(
      String blobPath, byte[] blob, List<ChunkMetadata> metadata, BlobStats blobStats)
      throws NoSuchAlgorithmException {
    logger.logInfo("Start uploading blob={}, size={}", blobPath, blob.length);
    long startTime = System.currentTimeMillis();

    Timer.Context uploadContext = Utils.createTimerContext(this.owningClient.uploadLatency);
    this.targetStage.put(blobPath, blob);

    if (uploadContext != null) {
      blobStats.setUploadDurationMs(uploadContext);
      this.owningClient.uploadThroughput.mark(blob.length);
      this.owningClient.blobSizeHistogram.update(blob.length);
      this.owningClient.blobRowCountHistogram.update(
          metadata.stream().mapToLong(i -> i.getEpInfo().getRowCount()).sum());
    }

    logger.logInfo(
        "Finish uploading blob={}, size={}, timeInMillis={}",
        blobPath,
        blob.length,
        System.currentTimeMillis() - startTime);

    // at this point we know for sure if the BDEC file has data for more than one chunk, i.e.
    // spans mixed tables or not
    return BlobMetadata.createBlobMetadata(
        blobPath,
        BlobBuilder.computeMD5(blob),
        bdecVersion,
        metadata,
        blobStats,
        metadata == null ? false : metadata.size() > 1);
  }

  /**
   * Release all resources
   *
   * @throws InterruptedException
   */
  void shutdown() throws InterruptedException {
    this.flushWorker.shutdown();
    this.registerWorker.shutdown();
    this.buildUploadWorkers.shutdown();

    boolean isTerminated =
        this.flushWorker.awaitTermination(THREAD_SHUTDOWN_TIMEOUT_IN_SEC, TimeUnit.SECONDS)
            && this.registerWorker.awaitTermination(
                THREAD_SHUTDOWN_TIMEOUT_IN_SEC, TimeUnit.SECONDS)
            && this.buildUploadWorkers.awaitTermination(
                THREAD_SHUTDOWN_TIMEOUT_IN_SEC, TimeUnit.SECONDS);

    if (!isTerminated) {
      logger.logWarn("Tasks can't be terminated within the timeout, force shutdown now.");
      this.flushWorker.shutdownNow();
      this.registerWorker.shutdownNow();
      this.buildUploadWorkers.shutdownNow();
    }
  }

  /** Set the flag to indicate that a flush is needed */
  void setNeedFlush() {
    this.isNeedFlush = true;
  }

  /**
   * Generate a blob path, which is: "YEAR/MONTH/DAY_OF_MONTH/HOUR_OF_DAY/MINUTE/<current utc
   * timestamp + client unique prefix + thread id + counter>.BDEC"
   *
   * @return the generated blob file path
   */
  private String getBlobPath(String clientPrefix) {
    Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    return getBlobPath(calendar, clientPrefix);
  }

  /** For TESTING */
  String getBlobPath(Calendar calendar, String clientPrefix) {
    if (isTestMode && clientPrefix == null) {
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
   * Invalidate all the channels in the blob data
   *
   * @param blobData list of channels that belongs to the blob
   */
  <CD> void invalidateAllChannelsInBlob(
      List<List<ChannelData<CD>>> blobData, String invalidationCause) {
    blobData.forEach(
        chunkData ->
            chunkData.forEach(
                channelData -> {
                  this.owningClient
                      .getChannelCache()
                      .invalidateChannelIfSequencersMatch(
                          channelData.getChannelContext().getDbName(),
                          channelData.getChannelContext().getSchemaName(),
                          channelData.getChannelContext().getTableName(),
                          channelData.getChannelContext().getName(),
                          channelData.getChannelContext().getChannelSequencer(),
                          invalidationCause);
                }));
  }

  /** Get the server generated unique prefix for this client */
  String getClientPrefix() {
    return this.targetStage.getClientPrefix();
  }

  /**
   * Throttle if the number of queued buildAndUpload tasks is bigger than the total number of
   * available processors
   */
  boolean throttleDueToQueuedFlushTasks() {
    ThreadPoolExecutor buildAndUpload = (ThreadPoolExecutor) this.buildUploadWorkers;
    boolean throttleOnQueuedTasks =
        buildAndUpload.getQueue().size() > Runtime.getRuntime().availableProcessors();
    if (throttleOnQueuedTasks) {
      logger.logWarn(
          "Throttled due too many queue flush tasks (probably because of slow uploading speed),"
              + " client={}, buildUploadWorkers stats={}",
          this.owningClient.getName(),
          this.buildUploadWorkers.toString());
    }
    return throttleOnQueuedTasks;
  }

  /** Get whether we're running under test mode */
  boolean isTestMode() {
    return this.isTestMode;
  }
}
