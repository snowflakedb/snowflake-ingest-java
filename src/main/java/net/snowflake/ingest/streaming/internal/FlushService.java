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
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
import net.snowflake.client.jdbc.internal.google.api.client.util.DateTime;
import net.snowflake.client.jdbc.internal.google.common.util.concurrent.ThreadFactoryBuilder;
import net.snowflake.ingest.utils.Constants;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.Logging;
import net.snowflake.ingest.utils.Pair;
import net.snowflake.ingest.utils.SFException;
import net.snowflake.ingest.utils.Utils;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * Responsible for flushing data from client to Snowflake tables. When a flush is triggered, it will
 * do the following in order:
 * <li>take the data from all the channel row buffers
 * <li>build the blob using the ChannelData returned from previous step
 * <li>upload the blob to stage
 * <li>register the blob to the targeted Snowflake table
 *
 * @param <T> type of column data (Arrow {@link org.apache.arrow.vector.VectorSchemaRoot} or {@link
 *     ParquetChunkData})
 */
class FlushService<T> {

  // Static class to save the list of channels that are used to build a blob, which is mainly used
  // to invalidate all the channels when there is a failure
  static class BlobData<T> {
    private final String filePath;
    private final List<List<ChannelData<T>>> data;

    BlobData(String filePath, List<List<ChannelData<T>>> data) {
      this.filePath = filePath;
      this.data = data;
    }

    String getFilePath() {
      return filePath;
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

  // blob file version
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
              client.getName());
    } catch (SnowflakeSQLException | IOException err) {
      throw new SFException(err, ErrorCode.UNABLE_TO_CONNECT_TO_STAGE);
    }

    this.registerService = new RegisterService<>(client, isTestMode);
    this.counter = new AtomicLong(0);
    this.isNeedFlush = false;
    this.lastFlushTime = System.currentTimeMillis();
    this.isTestMode = isTestMode;
    this.latencyTimerContextMap = new HashMap<>();
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
          long flush = System.currentTimeMillis();
          distributeFlushTasks();
          System.out.println("sssssss flush " + (System.currentTimeMillis() - flush));
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
            && !this.isTestMode
            && (this.isNeedFlush
                || timeDiffMillis
                    >= this.owningClient.getParameterProvider().getBufferFlushIntervalInMs()))) {
      System.out.println("sssss schedule " + new DateTime(System.currentTimeMillis()));
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

    while (itr.hasNext()) {
      List<List<ChannelData<T>>> blobData = new ArrayList<>();
      float totalBufferSize = 0;

      final String filePath = getFilePath(this.targetStage.getClientPrefix());

      // Distribute work at table level, create a new blob if reaching the blob size limit
      while (itr.hasNext() && totalBufferSize <= MAX_BLOB_SIZE_IN_BYTES) {
        ConcurrentHashMap<String, SnowflakeStreamingIngestChannelInternal<T>> table =
            itr.next().getValue();
        List<ChannelData<T>> channelsDataPerTable = new ArrayList<>();
        // TODO: we could do parallel stream to get the channelData if needed
        for (SnowflakeStreamingIngestChannelInternal<T> channel : table.values()) {
          if (channel.isValid()) {
            ChannelData<T> data = channel.getData(filePath);
            if (data != null) {
              channelsDataPerTable.add(data);
              totalBufferSize += data.getBufferSize();
            }
          }
        }
        if (!channelsDataPerTable.isEmpty()) {
          blobData.add(channelsDataPerTable);
        }
      }

      // Kick off a build job
      if (blobData.isEmpty()) {
        // we decrement the counter so that we do not have gaps in the filenames created by this
        // client. See method getFilePath() below.
        this.counter.decrementAndGet();
      } else {
        if (this.owningClient.flushLatency != null) {
          latencyTimerContextMap.putIfAbsent(filePath, this.owningClient.flushLatency.time());
        }
        blobs.add(
            new Pair<>(
                new BlobData<>(filePath, blobData),
                CompletableFuture.supplyAsync(
                    () -> {
                      try {
                        return buildAndUpload(filePath, blobData);
                      } catch (Throwable e) {
                        Throwable ex = e.getCause() == null ? e : e.getCause();
                        String errorMessage =
                            String.format(
                                "Building blob failed, client=%s, file=%s, exception=%s,"
                                    + " detail=%s, trace=%s, all channels in the blob will be"
                                    + " invalidated",
                                this.owningClient.getName(),
                                filePath,
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
                          invalidateAllChannelsInBlob(blobData);
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
            filePath,
            this.buildUploadWorkers.toString());
      }
    }

    // Add the flush task futures to the register service
    this.registerService.addBlobs(blobs);
  }

  /**
   * Builds and uploads file to cloud storage.
   *
   * @param filePath Path of the destination file in cloud storage
   * @param blobData All the data for one blob. Assumes that all ChannelData in the inner List
   *     belongs to the same table. Will error if this is not the case
   * @return BlobMetadata for FlushService.upload
   */
  BlobMetadata buildAndUpload(String filePath, List<List<ChannelData<T>>> blobData)
      throws IOException, NoSuchAlgorithmException, InvalidAlgorithmParameterException,
          NoSuchPaddingException, IllegalBlockSizeException, BadPaddingException,
          InvalidKeyException {
    Timer.Context buildContext = Utils.createTimerContext(this.owningClient.buildLatency);

    // Construct the blob along with the metadata of the blob
    BlobBuilder.Blob blob =
        BlobBuilder.constructBlobAndMetadata(
            filePath,
            blobData,
            bdecVersion,
            owningClient.getParameterProvider().getEnableParquetInternalBuffering());
    if (buildContext != null) {
      buildContext.stop();
    }
    return upload(filePath, blob.blobBytes, blob.chunksMetadataList);
  }

  /**
   * Upload a blob to Streaming Ingest dedicated stage
   *
   * @param filePath full path of the blob file
   * @param blob blob data
   * @param metadata a list of chunk metadata
   * @return BlobMetadata object used to create the register blob request
   */
  BlobMetadata upload(String filePath, byte[] blob, List<ChunkMetadata> metadata)
      throws NoSuchAlgorithmException {
    logger.logInfo("Start uploading file={}, size={}", filePath, blob.length);
    long startTime = System.currentTimeMillis();

    Timer.Context uploadContext = Utils.createTimerContext(this.owningClient.uploadLatency);

    this.targetStage.put(filePath, blob);

    if (uploadContext != null) {
      uploadContext.stop();
      this.owningClient.uploadThroughput.mark(blob.length);
      this.owningClient.blobSizeHistogram.update(blob.length);
      this.owningClient.blobRowCountHistogram.update(
          metadata.stream().mapToLong(i -> i.getEpInfo().getRowCount()).sum());
    }

    logger.logInfo(
        "Finish uploading file={}, size={}, timeInMillis={}",
        filePath,
        blob.length,
        System.currentTimeMillis() - startTime);

    return BlobMetadata.createBlobMetadata(
        filePath, BlobBuilder.computeMD5(blob), bdecVersion, metadata);
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
   * Generate a blob file path, which is: "YEAR/MONTH/DAY_OF_MONTH/HOUR_OF_DAY/MINUTE/<current utc
   * timestamp + client unique prefix + thread id + counter>.BDEC"
   *
   * @return the generated blob file path
   */
  private String getFilePath(String clientPrefix) {
    Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    return getFilePath(calendar, clientPrefix);
  }

  /** For TESTING */
  String getFilePath(Calendar calendar, String clientPrefix) {
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
    String fileName =
        Long.toString(time, 36)
            + "_"
            + clientPrefix
            + "_"
            + threadId
            + "_"
            + this.counter.getAndIncrement()
            + "."
            + BLOB_EXTENSION_TYPE;
    return year + "/" + month + "/" + day + "/" + hour + "/" + minute + "/" + fileName;
  }

  /**
   * Invalidate all the channels in the blob data
   *
   * @param blobData list of channels that belongs to the blob
   */
  <CD> void invalidateAllChannelsInBlob(List<List<ChannelData<CD>>> blobData) {
    blobData.forEach(
        chunkData ->
            chunkData.forEach(
                channelData -> {
                  if (channelData.getVectors() instanceof VectorSchemaRoot) {
                    ((VectorSchemaRoot) channelData.getVectors()).close();
                  }
                  this.owningClient
                      .getChannelCache()
                      .invalidateChannelIfSequencersMatch(
                          channelData.getChannelContext().getDbName(),
                          channelData.getChannelContext().getSchemaName(),
                          channelData.getChannelContext().getTableName(),
                          channelData.getChannelContext().getName(),
                          channelData.getChannelContext().getChannelSequencer());
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
}
