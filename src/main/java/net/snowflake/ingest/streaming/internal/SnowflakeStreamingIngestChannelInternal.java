/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.utils.Constants.INSERT_THROTTLE_MAX_RETRY_COUNT;
import static net.snowflake.ingest.utils.Constants.RESPONSE_SUCCESS;
import static net.snowflake.ingest.utils.ParameterProvider.MAX_MEMORY_LIMIT_IN_BYTES_DEFAULT;

import com.google.common.annotations.VisibleForTesting;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.utils.Constants;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.Logging;
import net.snowflake.ingest.utils.ParameterProvider;
import net.snowflake.ingest.utils.SFException;
import net.snowflake.ingest.utils.Utils;

/**
 * The first version of implementation for SnowflakeStreamingIngestChannel
 *
 * @param <T> type of column data {@link ParquetChunkData})
 */
class SnowflakeStreamingIngestChannelInternal<T> implements SnowflakeStreamingIngestChannel {

  private static final Logging logger = new Logging(SnowflakeStreamingIngestChannelInternal.class);

  // this context contains channel immutable identification and encryption attributes
  private final ChannelFlushContext channelFlushContext;

  // Reference to the row buffer
  private final RowBuffer<T> rowBuffer;

  // Indicates whether the channel is closed
  private volatile boolean isClosed;

  // Reference to the client that owns this channel
  private final SnowflakeStreamingIngestClientInternal<T> owningClient;

  // State of the channel that will be shared with its underlying buffer
  private final ChannelRuntimeState channelState;

  /**
   * Constructor for TESTING ONLY which allows us to set the test mode
   *
   * @param name
   * @param dbName
   * @param schemaName
   * @param tableName
   * @param offsetToken
   * @param channelSequencer
   * @param rowSequencer
   * @param client
   */
  SnowflakeStreamingIngestChannelInternal(
      String name,
      String dbName,
      String schemaName,
      String tableName,
      String offsetToken,
      Long channelSequencer,
      Long rowSequencer,
      SnowflakeStreamingIngestClientInternal<T> client,
      String encryptionKey,
      Long encryptionKeyId,
      OpenChannelRequest.OnErrorOption onErrorOption,
      ZoneOffset defaultTimezone) {
    this(
        name,
        dbName,
        schemaName,
        tableName,
        offsetToken,
        channelSequencer,
        rowSequencer,
        client,
        encryptionKey,
        encryptionKeyId,
        onErrorOption,
        defaultTimezone,
        client.getParameterProvider().getBlobFormatVersion());
  }

  /** Default constructor */
  SnowflakeStreamingIngestChannelInternal(
      String name,
      String dbName,
      String schemaName,
      String tableName,
      String offsetToken,
      Long channelSequencer,
      Long rowSequencer,
      SnowflakeStreamingIngestClientInternal<T> client,
      String encryptionKey,
      Long encryptionKeyId,
      OpenChannelRequest.OnErrorOption onErrorOption,
      ZoneId defaultTimezone,
      Constants.BdecVersion bdecVersion) {
    this.isClosed = false;
    this.owningClient = client;
    this.channelFlushContext =
        new ChannelFlushContext(
            name, dbName, schemaName, tableName, channelSequencer, encryptionKey, encryptionKeyId);
    this.channelState = new ChannelRuntimeState(offsetToken, rowSequencer, true);
    this.rowBuffer =
        AbstractRowBuffer.createRowBuffer(
            onErrorOption,
            defaultTimezone,
            bdecVersion,
            getFullyQualifiedName(),
            this::collectRowSize,
            channelState,
            owningClient != null
                ? owningClient.getParameterProvider().getEnableParquetInternalBuffering()
                : ParameterProvider.ENABLE_PARQUET_INTERNAL_BUFFERING_DEFAULT,
            owningClient != null
                ? owningClient.getParameterProvider().getMaxChannelSizeInBytes()
                : ParameterProvider.MAX_CHANNEL_SIZE_IN_BYTES_DEFAULT,
            owningClient != null
                ? owningClient.getParameterProvider().getMaxAllowedRowSizeInBytes()
                : ParameterProvider.MAX_ALLOWED_ROW_SIZE_IN_BYTES_DEFAULT);
    logger.logInfo(
        "Channel={} created for table={}",
        this.channelFlushContext.getName(),
        this.channelFlushContext.getTableName());
  }

  /**
   * Get the fully qualified channel name
   *
   * @return fully qualified name of the channel, in the format of
   *     dbName.schemaName.tableName.channelName
   */
  @Override
  public String getFullyQualifiedName() {
    return channelFlushContext.getFullyQualifiedName();
  }

  /**
   * Get the name of the channel
   *
   * @return name of the channel
   */
  @Override
  public String getName() {
    return this.channelFlushContext.getName();
  }

  @Override
  public String getDBName() {
    return this.channelFlushContext.getDbName();
  }

  @Override
  public String getSchemaName() {
    return this.channelFlushContext.getSchemaName();
  }

  @Override
  public String getTableName() {
    return this.channelFlushContext.getTableName();
  }

  Long getChannelSequencer() {
    return this.channelFlushContext.getChannelSequencer();
  }

  /** @return current state of the channel */
  @VisibleForTesting
  ChannelRuntimeState getChannelState() {
    return this.channelState;
  }

  /**
   * Get the fully qualified table name that the channel belongs to
   *
   * @return fully qualified table name, in the format of dbName.schemaName.tableName
   */
  @Override
  public String getFullyQualifiedTableName() {
    return channelFlushContext.getFullyQualifiedTableName();
  }

  /**
   * Get all the data needed to build the blob during flush
   *
   * @param filePath the name of the file the data will be written in
   * @return a ChannelData object
   */
  ChannelData<T> getData(final String filePath) {
    ChannelData<T> data = this.rowBuffer.flush(filePath);
    if (data != null) {
      data.setChannelContext(channelFlushContext);
    }
    return data;
  }

  /** @return a boolean to indicate whether the channel is valid or not */
  @Override
  public boolean isValid() {
    return this.channelState.isValid();
  }

  /** Mark the channel as invalid, and release resources */
  void invalidate(String message) {
    this.channelState.invalidate();
    this.rowBuffer.close("invalidate");
    logger.logWarn(
        "Channel is invalidated, name={}, channel sequencer={}, row sequencer={}, message={}",
        getFullyQualifiedName(),
        channelFlushContext.getChannelSequencer(),
        channelState.getRowSequencer(),
        message);
  }

  /** @return a boolean to indicate whether the channel is closed or not */
  @Override
  public boolean isClosed() {
    return this.isClosed;
  }

  /** Mark the channel as closed */
  void markClosed() {
    this.isClosed = true;
    logger.logInfo(
        "Channel is marked as closed, name={}, channel sequencer={}, row sequencer={}",
        getFullyQualifiedName(),
        channelFlushContext.getChannelSequencer(),
        channelState.getRowSequencer());
  }

  /**
   * Flush all data in memory to persistent storage and register with a Snowflake table
   *
   * @param closing whether the flush is called as part of channel closing
   * @return future which will be complete when the flush the data is registered
   */
  CompletableFuture<Void> flush(boolean closing) {
    // Skip this check for closing because we need to set the channel to closed first and then flush
    // in case there is any leftover rows
    if (isClosed() && !closing) {
      throw new SFException(ErrorCode.CLOSED_CHANNEL, getFullyQualifiedName());
    }

    // Simply return if there is no data in the channel, this might not work if we support public
    // flush API since there could a concurrent insert at the same time
    if (this.rowBuffer.getSize() == 0) {
      return CompletableFuture.completedFuture(null);
    }

    return this.owningClient.flush(false);
  }

  /**
   * Close the channel (this will flush in-flight buffered data)
   *
   * @return future which will be complete when the channel is closed
   */
  @Override
  public CompletableFuture<Void> close() {
    checkValidation();

    if (isClosed()) {
      return CompletableFuture.completedFuture(null);
    }

    markClosed();
    return flush(true)
        .thenRunAsync(
            () -> {
              List<SnowflakeStreamingIngestChannelInternal<?>> uncommittedChannels =
                  this.owningClient.verifyChannelsAreFullyCommitted(
                      Collections.singletonList(this));

              this.rowBuffer.close("close");
              this.owningClient.removeChannelIfSequencersMatch(this);

              // Throw an exception if the channel is invalid or has any uncommitted rows
              if (!isValid() || !uncommittedChannels.isEmpty()) {
                throw new SFException(
                    ErrorCode.CHANNELS_WITH_UNCOMMITTED_ROWS,
                    uncommittedChannels.stream()
                        .map(SnowflakeStreamingIngestChannelInternal::getFullyQualifiedName)
                        .collect(Collectors.toList()));
              }
            });
  }

  /**
   * Setup the column fields and vectors using the column metadata from the server
   *
   * @param columns
   */
  // TODO: need to verify with the table schema when supporting sub-columns
  void setupSchema(List<ColumnMetadata> columns) {
    logger.logDebug("Setup schema for channel={}, schema={}", getFullyQualifiedName(), columns);
    this.rowBuffer.setupSchema(columns);
  }

  /**
   * --------------------------------------------------------------------------------------------
   * Insert one row into the channel
   * --------------------------------------------------------------------------------------------
   */

  /**
   * The row is represented using Map where the key is column name and the value is a row of data
   *
   * @param row object data to write
   * @param offsetToken offset of given row, used for replay in case of failures
   * @return insert response that possibly contains errors because of insertion failures
   * @throws SFException when the channel is invalid or closed
   */
  @Override
  public InsertValidationResponse insertRow(Map<String, Object> row, String offsetToken) {

    return insertRows(Collections.singletonList(row), offsetToken);
  }

  /**
   * --------------------------------------------------------------------------------------------
   * Insert a batch of rows into the channel
   * --------------------------------------------------------------------------------------------
   */

  /**
   * Each row is represented using Map where the key is column name and the value is a row of data
   *
   * @param rows object data to write
   * @param offsetToken offset of last row in the row-set, used for replay in case of failures
   * @return insert response that possibly contains errors because of insertion failures
   * @throws SFException when the channel is invalid or closed
   */
  @Override
  public InsertValidationResponse insertRows(
      Iterable<Map<String, Object>> rows, String offsetToken)  {
    throttleInsertIfNeeded(new MemoryInfoProviderFromRuntime());
    checkValidation();

    if (isClosed()) {
      throw new SFException(ErrorCode.CLOSED_CHANNEL, getFullyQualifiedName());
    }

    // We create a shallow copy to protect against concurrent addition/removal of columns, which can
    // lead to double counting of null values, for example. Individual mutable values may still be
    // concurrently modified (e.g. byte[]). Before validation and EP calculation, we must make sure
    // that defensive copies of all mutable objects are created.
    final List<Map<String, Object>> rowsCopy = new LinkedList<>();
//    try {
//      TimeUnit.NANOSECONDS.sleep(1);
//    } catch (InterruptedException e) {
//      throw new RuntimeException(e);
//    }

   busySleep(1000);
    rows.forEach(r -> rowsCopy.add(new LinkedHashMap<>(r)));

    InsertValidationResponse response = this.rowBuffer.insertRows(rowsCopy, offsetToken);

    // Start flush task if the chunk size reaches a certain size
    // TODO: Checking table/chunk level size reduces throughput a lot, we may want to check it only
    // if a large number of rows are inserted
    if (this.rowBuffer.getSize()
        >= this.owningClient.getParameterProvider().getMaxChannelSizeInBytes()) {
      this.owningClient.setNeedFlush();
    }

    return response;
  }

  /** Collect the row size from row buffer if required */
  void collectRowSize(float rowSize) {
    if (this.owningClient.inputThroughput != null) {
      this.owningClient.inputThroughput.mark((long) rowSize);
    }
  }

  /**
   * Get the latest committed offset token from Snowflake, an exception will be thrown if the
   * channel becomes invalid due to errors and the channel needs to be reopened in order to return a
   * valid offset token
   *
   * @return the latest committed offset token
   */
  @Override
  public String getLatestCommittedOffsetToken() {
    checkValidation();

    ChannelsStatusResponse.ChannelStatusResponseDTO response =
        this.owningClient.getChannelsStatus(Collections.singletonList(this)).getChannels().get(0);

    if (response.getStatusCode() != RESPONSE_SUCCESS) {
      throw new SFException(ErrorCode.CHANNEL_STATUS_INVALID, getName(), response.getStatusCode());
    }

    return response.getPersistedOffsetToken();
  }

  /** Check whether we need to throttle the insertRows API */
  void throttleInsertIfNeeded(MemoryInfoProvider memoryInfoProvider) {
    int retry = 0;
    long insertThrottleIntervalInMs =
        this.owningClient.getParameterProvider().getInsertThrottleIntervalInMs();
    while ((hasLowRuntimeMemory(memoryInfoProvider)
            || (this.owningClient.getFlushService() != null
                && this.owningClient.getFlushService().throttleDueToQueuedFlushTasks()))
        && retry < INSERT_THROTTLE_MAX_RETRY_COUNT) {
      try {
        Thread.sleep(insertThrottleIntervalInMs);
        retry++;
      } catch (InterruptedException e) {
        throw new SFException(ErrorCode.INTERNAL_ERROR, "Insert throttle get interrupted");
      }
    }
    if (retry > 0) {
      logger.logInfo(
          "Insert throttled for a total of {} milliseconds, retryCount={}, client={}, channel={}",
          retry * insertThrottleIntervalInMs,
          retry,
          this.owningClient.getName(),
          getFullyQualifiedName());
    }
  }

  /** Check whether we have a low runtime memory condition */
  private boolean hasLowRuntimeMemory(MemoryInfoProvider memoryInfoProvider) {
    int insertThrottleThresholdInBytes =
        this.owningClient.getParameterProvider().getInsertThrottleThresholdInBytes();
    int insertThrottleThresholdInPercentage =
        this.owningClient.getParameterProvider().getInsertThrottleThresholdInPercentage();
    long maxMemoryLimitInBytes =
        this.owningClient.getParameterProvider().getMaxMemoryLimitInBytes();
    long maxMemory =
        maxMemoryLimitInBytes == MAX_MEMORY_LIMIT_IN_BYTES_DEFAULT
            ? memoryInfoProvider.getMaxMemory()
            : maxMemoryLimitInBytes;
    long freeMemory =
        memoryInfoProvider.getFreeMemory()
            + (memoryInfoProvider.getMaxMemory() - memoryInfoProvider.getTotalMemory());
    boolean hasLowRuntimeMemory =
        freeMemory < insertThrottleThresholdInBytes
            && freeMemory * 100 / maxMemory < insertThrottleThresholdInPercentage;
    if (hasLowRuntimeMemory) {
      logger.logWarn(
          "Throttled due to memory pressure, client={}, channel={}.",
          this.owningClient.getName(),
          getFullyQualifiedName());
      Utils.showMemory();
    }
    return hasLowRuntimeMemory;
  }

  /** Check whether the channel is still valid, cleanup and throw an error if not */
  private void checkValidation() {
    if (!isValid()) {
      this.owningClient.removeChannelIfSequencersMatch(this);
      this.rowBuffer.close("checkValidation");
      throw new SFException(ErrorCode.INVALID_CHANNEL, getFullyQualifiedName());
    }
  }

  /** Returns underlying channel's row buffer implementation. */
  RowBuffer<T> getRowBuffer() {
    return rowBuffer;
  }

  /** Returns underlying channel's attributes. */
  @VisibleForTesting
  public ChannelFlushContext getChannelContext() {
    return channelFlushContext;
  }
  public static void busySleep(long nanos)
  {
    long elapsed;
    final long startTime = System.nanoTime();
    do {
      elapsed = System.nanoTime() - startTime;
    } while (elapsed < nanos);
  }
}
