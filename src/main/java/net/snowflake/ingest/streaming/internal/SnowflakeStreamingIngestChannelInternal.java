/*
 * Copyright (c) 2021-2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.utils.Constants.INSERT_THROTTLE_MAX_RETRY_COUNT;
import static net.snowflake.ingest.utils.Constants.RESPONSE_SUCCESS;
import static net.snowflake.ingest.utils.ParameterProvider.MAX_MEMORY_LIMIT_IN_BYTES_DEFAULT;

import com.google.common.annotations.VisibleForTesting;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import net.snowflake.ingest.streaming.DropChannelRequest;
import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.OffsetTokenVerificationFunction;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.utils.Constants;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.Logging;
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
  private final long insertThrottleIntervalInMs;
  private final int insertThrottleThresholdInBytes;
  private final int insertThrottleThresholdInPercentage;
  private final long maxMemoryLimitInBytes;

  // Indicates whether the channel is closed
  private volatile boolean isClosed;

  // Reference to the client that owns this channel
  private final SnowflakeStreamingIngestClientInternal<T> owningClient;

  // State of the channel that will be shared with its underlying buffer
  private final ChannelRuntimeState channelState;

  // Internal map of column name -> column properties
  private final Map<String, ColumnProperties> tableColumns;

  // The latest cause of channel invalidation
  private String invalidationCause;

  private final MemoryInfoProvider memoryInfoProvider;
  private volatile long freeMemoryInBytes = 0;

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
        client.getParameterProvider().getBlobFormatVersion(),
        null);
  }

  /** Default constructor */
  SnowflakeStreamingIngestChannelInternal(
      String name,
      String dbName,
      String schemaName,
      String tableName,
      String endOffsetToken,
      Long channelSequencer,
      Long rowSequencer,
      SnowflakeStreamingIngestClientInternal<T> client,
      String encryptionKey,
      Long encryptionKeyId,
      OpenChannelRequest.OnErrorOption onErrorOption,
      ZoneId defaultTimezone,
      Constants.BdecVersion bdecVersion,
      OffsetTokenVerificationFunction offsetTokenVerificationFunction) {
    this.isClosed = false;
    this.owningClient = client;

    this.insertThrottleIntervalInMs =
        this.owningClient.getParameterProvider().getInsertThrottleIntervalInMs();
    this.insertThrottleThresholdInBytes =
        this.owningClient.getParameterProvider().getInsertThrottleThresholdInBytes();
    this.insertThrottleThresholdInPercentage =
        this.owningClient.getParameterProvider().getInsertThrottleThresholdInPercentage();
    this.maxMemoryLimitInBytes =
        this.owningClient.getParameterProvider().getMaxMemoryLimitInBytes();

    this.memoryInfoProvider = MemoryInfoProviderFromRuntime.getInstance();
    this.channelFlushContext =
    new ChannelFlushContext(
            name, dbName, schemaName, tableName, channelSequencer, encryptionKey, encryptionKeyId);
    this.channelState = new ChannelRuntimeState(endOffsetToken, rowSequencer, true);
    this.rowBuffer =
        AbstractRowBuffer.createRowBuffer(
            onErrorOption,
            defaultTimezone,
            bdecVersion,
            getFullyQualifiedName(),
            this::collectRowSize,
            channelState,
            new ClientBufferParameters(owningClient),
            offsetTokenVerificationFunction,
            owningClient == null ? null : owningClient.getTelemetryService());
    this.tableColumns = new HashMap<>();
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
  void invalidate(String message, String invalidationCause) {
    this.channelState.invalidate();
    this.invalidationCause = invalidationCause;
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
    return this.close(false);
  }

  @Override
  public CompletableFuture<Void> close(boolean drop) {
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
              if (drop) {
                DropChannelRequest.DropChannelRequestBuilder builder =
                    DropChannelRequest.builder(this.getChannelContext().getName())
                        .setDBName(this.getDBName())
                        .setTableName(this.getTableName())
                        .setSchemaName(this.getSchemaName());
                this.owningClient.dropChannel(
                    new DropChannelVersionRequest(builder, this.getChannelSequencer()));
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
    columns.forEach(c -> tableColumns.putIfAbsent(c.getName(), new ColumnProperties(c)));
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
    return insertRows(Collections.singletonList(row), offsetToken, offsetToken);
  }

  /**
   * --------------------------------------------------------------------------------------------
   * Insert a batch of rows into the channel
   * --------------------------------------------------------------------------------------------
   */

  /**
   * Insert a batch of rows into the channel, each row is represented using Map where the key is
   * column name and the value is a row of data. See {@link
   * SnowflakeStreamingIngestChannel#insertRow(Map, String)} for more information about accepted
   * values.
   *
   * @param rows object data to write
   * @param startOffsetToken start offset of the batch/row-set
   * @param endOffsetToken end offset of the batch/row-set, used for replay in case of failures, *
   *     It could be null if you don't plan on replaying or can't replay
   * @return insert response that possibly contains errors because of insertion failures
   */
  @Override
  public InsertValidationResponse insertRows(
      Iterable<Map<String, Object>> rows,
      @Nullable String startOffsetToken,
      @Nullable String endOffsetToken) {
    throttleInsertIfNeeded(memoryInfoProvider);
    checkValidation();

    if (isClosed()) {
      throw new SFException(ErrorCode.CLOSED_CHANNEL, getFullyQualifiedName());
    }

    // We create a shallow copy to protect against concurrent addition/removal of columns, which can
    // lead to double counting of null values, for example. Individual mutable values may still be
    // concurrently modified (e.g. byte[]). Before validation and EP calculation, we must make sure
    // that defensive copies of all mutable objects are created.
    final List<Map<String, Object>> rowsCopy = new LinkedList<>();
    rows.forEach(r -> rowsCopy.add(new LinkedHashMap<>(r)));

    InsertValidationResponse response =
        this.rowBuffer.insertRows(rowsCopy, startOffsetToken, endOffsetToken);

    // Start flush task if the chunk size reaches a certain size
    // TODO: Checking table/chunk level size reduces throughput a lot, we may want to check it only
    // if a large number of rows are inserted
    if (this.rowBuffer.getSize()
        >= this.owningClient.getParameterProvider().getMaxChannelSizeInBytes()) {
      this.owningClient.setNeedFlush(this.channelFlushContext.getFullyQualifiedTableName());
    }

    return response;
  }

  /**
   * Insert a batch of rows into the channel with the end offset token only, please see {@link
   * SnowflakeStreamingIngestChannel#insertRows(Iterable, String, String)} for more information.
   */
  @Override
  public InsertValidationResponse insertRows(
      Iterable<Map<String, Object>> rows, String offsetToken) {
    return insertRows(rows, null, offsetToken);
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

  /** Returns a map of column name -> datatype for the table the channel is bound to */
  @Override
  public Map<String, ColumnProperties> getTableSchema() {
    return this.tableColumns;
  }

  /** Check whether we need to throttle the insertRows API */
  void throttleInsertIfNeeded(MemoryInfoProvider memoryInfoProvider) {
    int retry = 0;
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
    long maxMemory =
        maxMemoryLimitInBytes == MAX_MEMORY_LIMIT_IN_BYTES_DEFAULT
            ? memoryInfoProvider.getMaxMemory()
            : maxMemoryLimitInBytes;
    freeMemoryInBytes = memoryInfoProvider.getFreeMemory();
    boolean hasLowRuntimeMemory =
        freeMemoryInBytes < insertThrottleThresholdInBytes
            && freeMemoryInBytes * 100 / maxMemory < insertThrottleThresholdInPercentage;
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
      throw new SFException(
          ErrorCode.INVALID_CHANNEL, getFullyQualifiedName(), this.invalidationCause);
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
}
