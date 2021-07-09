/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.utils.Constants.BLOB_EXTENSION_TYPE;
import static net.snowflake.ingest.utils.Constants.BUFFER_FLUSH_CHECK_INTERVAL_IN_MS;
import static net.snowflake.ingest.utils.Constants.BUFFER_FLUSH_INTERVAL_IN_MS;
import static net.snowflake.ingest.utils.Constants.DISABLE_BACKGROUND_FLUSH;
import static net.snowflake.ingest.utils.Constants.MAX_BLOB_SIZE_IN_BYTES;
import static net.snowflake.ingest.utils.Constants.MAX_THREAD_COUNT;
import static net.snowflake.ingest.utils.Constants.THREAD_SHUTDOWN_TIMEOUT_IN_SEC;

import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.Logging;
import net.snowflake.ingest.utils.Pair;
import net.snowflake.ingest.utils.SFException;
import net.snowflake.ingest.utils.Utils;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;

/**
 * Responsible for flushing data from client to Snowflake tables. When a flush is triggered, it will
 * do the following in order:
 * <li>take the data from all the channel row buffers
 * <li>build the blob using the ChannelData returned from previous step
 * <li>upload the blob to stage
 * <li>register the blob to the targeted Snowflake table
 */
class FlushService {

  // Static class to save the list of channels that are used to build a blob, which is mainly used
  // to invalidate all the channels when there is a failure
  static class BlobData {
    private final String fileName;
    private final List<List<ChannelData>> data;

    BlobData(String fileName, List<List<ChannelData>> data) {
      this.fileName = fileName;
      this.data = data;
    }

    String getFileName() {
      return fileName;
    }

    List<List<ChannelData>> getData() {
      return data;
    }
  }

  private static final Logging logger = new Logging(FlushService.class);

  // Increasing counter to generate a unique blob name per client
  private final AtomicLong counter;

  // The client that owns this flush service
  private final SnowflakeStreamingIngestClientInternal owningClient;

  // Thread to schedule the flush job
  @VisibleForTesting ScheduledExecutorService flushWorker;

  // Thread to register the blob
  @VisibleForTesting ExecutorService registerWorker;

  // Threads to build and upload the blob
  @VisibleForTesting ExecutorService buildUploadWorkers;

  // Reference to the channel cache
  private final ChannelCache channelCache;

  // Reference to the Streaming Ingest stage
  private final StreamingIngestStage targetStage;

  // Reference to register service
  private final RegisterService registerService;

  // Indicates whether we need to schedule a flush
  @VisibleForTesting volatile boolean isNeedFlush;

  // Latest flush time
  @VisibleForTesting volatile long lastFlushTime;

  // Indicates whether it's running as part of the test
  private final boolean isTestMode;

  // A map which stores the timer for each blob in order to capture the flush latency
  private final Map<String, Timer.Context> latencyTimerContextMap;

  /**
   * Constructor for TESTING that takes (usually mocked) StreamingIngestStage
   *
   * @param client
   * @param cache
   * @param isTestMode
   */
  FlushService(
      SnowflakeStreamingIngestClientInternal client,
      ChannelCache cache,
      StreamingIngestStage targetStage, // For testing
      boolean isTestMode) {
    this.owningClient = client;
    this.channelCache = cache;
    this.targetStage = targetStage;
    this.counter = new AtomicLong(0);
    this.registerService = new RegisterService(client, isTestMode);
    this.isNeedFlush = false;
    this.lastFlushTime = System.currentTimeMillis();
    this.isTestMode = isTestMode;
    this.latencyTimerContextMap = new HashMap<>();
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
      SnowflakeStreamingIngestClientInternal client, ChannelCache cache, boolean isTestMode) {
    this.owningClient = client;
    this.channelCache = cache;
    try {
      this.targetStage =
          new StreamingIngestStage(
              isTestMode, client.getRole(), client.getHttpClient(), client.getRequestBuilder());
    } catch (SnowflakeSQLException | IOException err) {
      throw new SFException(err, ErrorCode.UNABLE_TO_CONNECT_TO_STAGE);
    }

    this.registerService = new RegisterService(client, isTestMode);
    this.counter = new AtomicLong(0);
    this.isNeedFlush = false;
    this.lastFlushTime = System.currentTimeMillis();
    this.isTestMode = isTestMode;
    this.latencyTimerContextMap = new HashMap<>();
    createWorkers();
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
    if (this.owningClient.cpuHistogram != null) {
      double cpuLoad =
          ManagementFactory.getPlatformMXBean(com.sun.management.OperatingSystemMXBean.class)
              .getProcessCpuLoad();
      this.owningClient.cpuHistogram.update((long) (cpuLoad * 100));
    }
    long timeDiff = System.currentTimeMillis() - this.lastFlushTime;
    if (isForce
        || (!DISABLE_BACKGROUND_FLUSH
            && !this.isTestMode
            && (this.isNeedFlush || timeDiff >= BUFFER_FLUSH_INTERVAL_IN_MS))) {
      logger.logDebug(
          "Submit flush task on client={}, isForce={}, isNeedFlush={}, time diff in ms={}",
          this.owningClient.getName(),
          isForce,
          this.isNeedFlush,
          timeDiff);

      distributeFlushTasks();
      this.isNeedFlush = false;
      this.lastFlushTime = System.currentTimeMillis();
      return CompletableFuture.runAsync(
          () -> {
            this.registerService
                .registerBlobs(latencyTimerContextMap)
                .forEach(
                    errorBlobData -> {
                      invalidateAllChannelsInBlob(errorBlobData.getData());
                    });
          },
          this.registerWorker);
    }

    return CompletableFuture.completedFuture(null);
  }

  /** Create the workers for each specific job */
  private void createWorkers() {
    int threadCount = 0;

    // Create thread for checking and scheduling flush job
    ThreadFactory flushThreadFactory =
        new ThreadFactoryBuilder().setNameFormat("ingest-flush-thread").build();
    this.flushWorker = Executors.newSingleThreadScheduledExecutor(flushThreadFactory);
    this.flushWorker.scheduleWithFixedDelay(
        () -> {
          flush(false);
        },
        0,
        BUFFER_FLUSH_CHECK_INTERVAL_IN_MS,
        TimeUnit.MILLISECONDS);
    threadCount++;

    // Create thread for registering blobs
    ThreadFactory registerThreadFactory =
        new ThreadFactoryBuilder().setNameFormat("ingest-register-thread").build();
    this.registerWorker = Executors.newSingleThreadExecutor(registerThreadFactory);
    threadCount++;

    // Create threads for building and uploading blobs
    // Min: 1
    // Max: number of available processors - thread for scheduling flush - thread for registering
    ThreadFactory buildUploadThreadFactory =
        new ThreadFactoryBuilder().setNameFormat("ingest-build-upload-thread-%d").build();
    int buildUploadThreadCount =
        Math.max(
            1,
            Math.min(Runtime.getRuntime().availableProcessors(), MAX_THREAD_COUNT) - threadCount);
    this.buildUploadWorkers =
        Executors.newFixedThreadPool(buildUploadThreadCount, buildUploadThreadFactory);

    logger.logDebug(
        "Create {} threads for build/upload blobs for client={}",
        buildUploadThreadCount,
        this.owningClient.getName());
  }

  /**
   * Distribute the flush tasks by iterating through all the channels in the channel cache and kick
   * off a build blob work when certain size has reached or we have reached the end
   */
  void distributeFlushTasks() {
    Iterator<Map.Entry<String, ConcurrentHashMap<String, SnowflakeStreamingIngestChannelInternal>>>
        itr = this.channelCache.iterator();
    List<Pair<BlobData, CompletableFuture<BlobMetadata>>> blobs = new ArrayList<>();
    String fileName = null;

    while (itr.hasNext()) {
      List<List<ChannelData>> blobData = new ArrayList<>();
      float totalBufferSize = 0;
      fileName = fileName == null ? getFileName(this.targetStage.getClientPrefix()) : fileName;
      if (this.owningClient.flushLatency != null) {
        latencyTimerContextMap.putIfAbsent(fileName, this.owningClient.flushLatency.time());
      }

      // Distribute work at table level, create a new blob if reaching the blob size limit
      while (itr.hasNext() && totalBufferSize <= MAX_BLOB_SIZE_IN_BYTES) {
        ConcurrentHashMap<String, SnowflakeStreamingIngestChannelInternal> table =
            itr.next().getValue();
        List<ChannelData> channelsDataPerTable = new ArrayList<>();
        // TODO: we could do parallel stream to get the channelData if needed
        for (SnowflakeStreamingIngestChannelInternal channel : table.values()) {
          if (channel.isValid()) {
            ChannelData data = channel.getData();
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
      if (!blobData.isEmpty()) {
        String finalFileName = fileName;
        fileName = null;
        blobs.add(
            new Pair<>(
                new BlobData(finalFileName, blobData),
                CompletableFuture.supplyAsync(
                    () -> {
                      try {
                        return buildAndUpload(finalFileName, blobData);
                      } catch (IOException e) {
                        logger.logError("Building blob failed={}, exception={}", finalFileName, e);
                        invalidateAllChannelsInBlob(blobData);
                        return null;
                      } catch (NoSuchAlgorithmException e) {
                        throw new SFException(e, ErrorCode.MD5_HASHING_NOT_AVAILABLE);
                      }
                    },
                    this.buildUploadWorkers)));
      }
    }

    // Add the flush task futures to the register service
    this.registerService.addBlobs(blobs);
  }

  /**
   * Builds and uploads file to cloud storage.
   *
   * @param fileName Name of the destination file in cloud storage
   * @param blobData All the data for one blob. Assumes that all ChannelData in the inner List
   *     belong to the same table. Will error if this is not the case
   * @return BlobMetadata for FlushService.upload
   * @throws IOException
   */
  BlobMetadata buildAndUpload(String fileName, List<List<ChannelData>> blobData)
      throws IOException, NoSuchAlgorithmException {
    List<ChunkMetadata> chunksMetadataList = new ArrayList<>();
    List<byte[]> chunksDataList = new ArrayList<>();
    long curDataSize = 0L;
    CRC32 crc = new CRC32();
    Timer.Context buildContext = Utils.createTimerContext(this.owningClient.buildLatency);

    // TODO: channels with different schema can't be combined even if they belongs to same table
    for (List<ChannelData> channelsDataPerTable : blobData) {
      List<ChannelMetadata> channelsMetadataList = new ArrayList<>();
      long rowCount = 0L;
      VectorSchemaRoot root = null;
      ArrowStreamWriter streamWriter = null;
      SnowflakeStreamingIngestChannelInternal firstChannel = null;
      ByteArrayOutputStream chunkData = new ByteArrayOutputStream();
      Map<String, RowBufferStats> columnEpStatsMapCombined = null;

      try {
        for (ChannelData data : channelsDataPerTable) {
          // Create channel metadata
          ChannelMetadata channelMetadata =
              ChannelMetadata.builder()
                  .setOwningChannel(data.getChannel())
                  .setRowSequencer(data.getRowSequencer())
                  .setOffsetToken(data.getOffsetToken())
                  .build();
          // Add channel metadata to the metadata list
          channelsMetadataList.add(channelMetadata);

          logger.logDebug(
              "Start building channel={}, rowCount={}, bufferSize={} in blob={}",
              data.getChannel().getFullyQualifiedName(),
              data.getRowCount(),
              data.getBufferSize(),
              fileName);

          if (root == null) {
            columnEpStatsMapCombined = data.getColumnEps();
            root = new VectorSchemaRoot(data.getVectors());
            streamWriter = new ArrowStreamWriter(root, null, chunkData);
            firstChannel = data.getChannel();
            streamWriter.start();
          } else {
            // This method assumes that channelsDataPerTable is grouped by table. We double check
            // here and throw an error if the assumption is violated
            if (!data.getChannel()
                .getFullyQualifiedTableName()
                .equals(firstChannel.getFullyQualifiedTableName())) {
              throw new SFException(ErrorCode.INVALID_DATA_IN_CHUNK);
            }

            columnEpStatsMapCombined =
                ChannelData.getCombinedColumnStatsMap(
                    columnEpStatsMapCombined, data.getColumnEps());

            for (int vectorIdx = 0; vectorIdx < data.getVectors().size(); vectorIdx++) {
              FieldVector sourceVector = data.getVectors().get(vectorIdx);
              ArrowFieldNode node =
                  new ArrowFieldNode(sourceVector.getValueCount(), sourceVector.getNullCount());
              root.getVector(vectorIdx).loadFieldBuffers(node, sourceVector.getFieldBuffers());
              sourceVector.close();
            }
          }

          // Write channel data using the stream writer
          streamWriter.writeBatch();
          rowCount += data.getRowCount();

          logger.logDebug(
              "Finish building channel={}, rowCount={}, bufferSize={} in blob={}",
              data.getChannel().getFullyQualifiedName(),
              data.getRowCount(),
              data.getBufferSize(),
              fileName);
        }
      } finally {
        if (streamWriter != null) {
          streamWriter.close();
          root.close();
        }
      }

      if (!channelsMetadataList.isEmpty()) {
        // Compress the chunk data
        byte[] compressedChunkData = BlobBuilder.compress(fileName, chunkData);
        String md5 = BlobBuilder.computeMD5(compressedChunkData);
        int compressedChunkDataSize = compressedChunkData.length;

        // Create chunk metadata
        ChunkMetadata chunkMetadata =
            ChunkMetadata.builder()
                .setOwningTable(firstChannel)
                // The start offset will be updated later in BlobBuilder#build to include the blob
                // header
                .setChunkStartOffset(curDataSize)
                .setChunkLength(compressedChunkDataSize)
                .setChannelList(channelsMetadataList)
                .setChunkMD5(md5)
                .setEpInfo(ArrowRowBuffer.buildEpInfoFromStats(rowCount, columnEpStatsMapCombined))
                .build();

        // Add chunk metadata and data to the list
        chunksMetadataList.add(chunkMetadata);
        chunksDataList.add(compressedChunkData);
        curDataSize += compressedChunkDataSize;
        crc.update(compressedChunkData, 0, compressedChunkData.length);

        logger.logDebug(
            "Finish building chunk in blob:{}, table:{}, rowCount:{}, uncompressedSize:{},"
                + " compressedSize:{}",
            fileName,
            firstChannel.getFullyQualifiedTableName(),
            rowCount,
            chunkData.size(),
            compressedChunkDataSize);
      }
    }

    // Build blob file, and then upload to streaming ingest dedicated stage
    byte[] blob =
        BlobBuilder.build(chunksMetadataList, chunksDataList, crc.getValue(), curDataSize);
    if (buildContext != null) {
      buildContext.stop();
    }

    return upload(fileName, blob, chunksMetadataList);
  }

  /**
   * Upload a blob to Streaming Ingest dedicated stage
   *
   * @param fileName name of the blob file
   * @param blob blob data
   * @param metadata a list of chunk metadata
   * @return BlobMetadata object used to create the register blob request
   */
  BlobMetadata upload(String fileName, byte[] blob, List<ChunkMetadata> metadata) {
    logger.logDebug("Start uploading file={}, size={}", fileName, blob.length);

    Timer.Context uploadContext = Utils.createTimerContext(this.owningClient.uploadLatency);

    this.targetStage.put(fileName, blob);

    if (uploadContext != null) {
      uploadContext.stop();
      this.owningClient.uploadThroughput.mark(blob.length);
      this.owningClient.blobSizeHistogram.update(blob.length);
    }

    logger.logDebug("Finish uploading file={}", fileName);

    return new BlobMetadata(fileName, fileName /* TODO: SNOW-331093 */, metadata);
  }

  /**
   * Release all resources
   *
   * @return a boolean indicates whether the shutdown is successful or not
   * @throws InterruptedException
   */
  boolean shutdown() throws InterruptedException {
    this.flushWorker.shutdown();
    this.registerWorker.shutdown();
    this.buildUploadWorkers.shutdown();

    return this.flushWorker.awaitTermination(THREAD_SHUTDOWN_TIMEOUT_IN_SEC, TimeUnit.SECONDS)
        && this.registerWorker.awaitTermination(THREAD_SHUTDOWN_TIMEOUT_IN_SEC, TimeUnit.SECONDS)
        && this.buildUploadWorkers.awaitTermination(
            THREAD_SHUTDOWN_TIMEOUT_IN_SEC, TimeUnit.SECONDS);
  }

  /** Set the flag to indicate that a flush is needed */
  void setNeedFlush() {
    this.isNeedFlush = true;
  }

  /**
   * Generate a blob name, which is: "YEAR/MONTH/DAY_OF_MONTH/HOUR_OF_DAY/CLIENT_PREFIX/<current
   * time + thread id + increasing counter>"
   *
   * @return the generated blob name
   */
  private String getFileName(String clientPrefix) {
    Calendar calendar = Calendar.getInstance();
    return getFileName(calendar, clientPrefix);
  }

  /** For TESTING */
  String getFileName(Calendar calendar, String clientPrefix) {
    if (isTestMode && clientPrefix == null) {
      clientPrefix = "testPrefix";
    }
    Utils.assertStringNotNullOrEmpty("client prefix", clientPrefix);
    int year = calendar.get(Calendar.YEAR);
    int month = calendar.get(Calendar.MONTH);
    int day = calendar.get(Calendar.DAY_OF_MONTH);
    int hour = calendar.get(Calendar.HOUR_OF_DAY);
    long time = calendar.getTimeInMillis();
    long threadId = Thread.currentThread().getId();
    String fileName =
        Long.toString(time, 36)
            + "_"
            + threadId
            + "_"
            + this.counter.getAndIncrement()
            + "."
            + BLOB_EXTENSION_TYPE;
    Path filePath =
        Paths.get(
            Integer.toString(year),
            Integer.toString(month),
            Integer.toString(day),
            Integer.toString(hour),
            clientPrefix,
            fileName);
    return filePath.toString();
  }

  /**
   * Invalidate all the channels in the blob data
   *
   * @param blobData list of channels that belongs to the blob
   */
  private void invalidateAllChannelsInBlob(List<List<ChannelData>> blobData) {
    blobData.forEach(
        chunkData ->
            chunkData.forEach(
                channelData ->
                    this.owningClient
                        .getChannelCache()
                        .invalidateAndRemoveChannelIfSequencersMatch(
                            channelData.getChannel().getDBName(),
                            channelData.getChannel().getSchemaName(),
                            channelData.getChannel().getTableName(),
                            channelData.getChannel().getName(),
                            channelData.getChannel().getChannelSequencer())));
  }
}
