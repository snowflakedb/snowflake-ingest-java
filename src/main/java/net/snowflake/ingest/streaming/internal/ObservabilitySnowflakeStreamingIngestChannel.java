package net.snowflake.ingest.streaming.internal;

import com.google.common.annotations.VisibleForTesting;
import net.snowflake.ingest.connection.TelemetryService;
import net.snowflake.ingest.streaming.*;
import net.snowflake.ingest.utils.*;
import org.apache.parquet.column.ParquetProperties;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.ZoneId;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static net.snowflake.ingest.utils.Constants.INSERT_THROTTLE_MAX_RETRY_COUNT;
import static net.snowflake.ingest.utils.Constants.RESPONSE_SUCCESS;
import static net.snowflake.ingest.utils.ParameterProvider.MAX_MEMORY_LIMIT_IN_BYTES_DEFAULT;

class ObservabilitySnowflakeStreamingIngestChannel implements SnowflakeStreamingIngestChannelFlushable<ParquetChunkData> {
    private static final Logging logger = new Logging(ObservabilitySnowflakeStreamingIngestChannel.class);

    // this context contains channel immutable identification and encryption attributes
    private final ChannelFlushContext channelFlushContext;

    // Reference to the row buffer
    private final HashMap<ObservabilityClusteringKey, RowBuffer<ParquetChunkData>> rowBufferMap;
    private final long insertThrottleIntervalInMs;
    private final int insertThrottleThresholdInBytes;
    private final int insertThrottleThresholdInPercentage;
    private final long maxMemoryLimitInBytes;
    private final Function<ObservabilityClusteringKey, RowBuffer<ParquetChunkData>> addRowBuffer;

    // Indicates whether the channel is closed
    private volatile boolean isClosed;

    // Reference to the client that owns this channel
    private final SnowflakeStreamingIngestClientInternal<ParquetChunkData> owningClient;

    // State of the channel that will be shared with its underlying buffer
    private final ObservabilityChannelState channelState;

    // Internal map of column name -> column properties
    private final Map<String, ColumnProperties> tableColumns;

    // The latest cause of channel invalidation
    private String invalidationCause;

    private final MemoryInfoProvider memoryInfoProvider;
    private volatile long freeMemoryInBytes = 0;

    /** Default constructor */
    ObservabilitySnowflakeStreamingIngestChannel(
            String name,
            String dbName,
            String schemaName,
            String tableName,
            String endOffsetToken,
            Long channelSequencer,
            Long rowSequencer,
            @Nonnull SnowflakeStreamingIngestClientInternal<ParquetChunkData> client,
            String encryptionKey,
            Long encryptionKeyId,
            OpenChannelRequest.OnErrorOption onErrorOption,
            ZoneId defaultTimezone,
            OffsetTokenVerificationFunction offsetTokenVerificationFunction,
            ParquetProperties.WriterVersion parquetWriterVersion) {
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

        this.channelState = new ObservabilityChannelState(endOffsetToken, rowSequencer, true);
        this.rowBufferMap = new HashMap<>();
        this.tableColumns = new HashMap<>();

        this.addRowBuffer = (ObservabilityClusteringKey key) -> {
            RowBuffer<ParquetChunkData> buffer = createRowBuffer(
                key,
                onErrorOption,
                defaultTimezone,
                client.getParameterProvider().getBlobFormatVersion(),
                getFullyQualifiedName(),
                this::collectRowSize,
                channelState,
                new ClientBufferParameters(owningClient, parquetWriterVersion),
                offsetTokenVerificationFunction,
                parquetWriterVersion,
                owningClient.getTelemetryService());

            rowBufferMap.put(key, buffer);

            return buffer;
        };

        logger.logInfo(
                "Channel={} created for table={}",
                this.channelFlushContext.getName(),
                this.channelFlushContext.getTableName());
    }


    /**
     * Row buffer factory.
     */
    private static RowBuffer<ParquetChunkData> createRowBuffer(
        ObservabilityClusteringKey key,
        OpenChannelRequest.OnErrorOption onErrorOption,
        ZoneId defaultTimezone,
        Constants.BdecVersion bdecVersion,
        String fullyQualifiedChannelName,
        Consumer<Float> rowSizeMetric,
        ChannelRuntimeState channelRuntimeState,
        ClientBufferParameters clientBufferParameters,
        OffsetTokenVerificationFunction offsetTokenVerificationFunction,
        ParquetProperties.WriterVersion parquetWriterVersion,
        TelemetryService telemetryService) {
        switch (bdecVersion) {
            case THREE:
                //noinspection unchecked
                return
                    new ObservabilityParquetRowBuffer(
                        key,
                        onErrorOption,
                        defaultTimezone,
                        fullyQualifiedChannelName,
                        rowSizeMetric,
                        channelRuntimeState,
                        clientBufferParameters,
                        offsetTokenVerificationFunction,
                        parquetWriterVersion,
                        telemetryService);
            default:
                throw new SFException(
                    ErrorCode.INTERNAL_ERROR, "Unsupported BDEC format version: " + bdecVersion);
        }
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

    @Override
    public Long getChannelSequencer() {
        return this.channelFlushContext.getChannelSequencer();
    }

    /** @return current state of the channel */
    @Override
    public ChannelRuntimeState getChannelState() {
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
     * @return a ChannelData object
     */
    @Override
    public List<ChannelData<ParquetChunkData>> getData() {
        return this.rowBufferMap.values().stream()
            .map(r -> {
                ChannelData<ParquetChunkData> channelData = r.flush();
                if (channelData != null) {
                    channelData.setChannelContext(channelFlushContext);
                }
                return channelData;
            })
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
    }

    /** @return a boolean to indicate whether the channel is valid or not */
    @Override
    public boolean isValid() {
        return this.channelState.isValid();
    }

    /** Mark the channel as invalid, and release resources */
    @Override
    public void invalidate(String message, String invalidationCause) {
        this.channelState.setValid(false);
        this.invalidationCause = invalidationCause;
        this.rowBufferMap.clear();
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
    @Override
    public void markClosed() {
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
    @Override
    public CompletableFuture<Void> flush(boolean closing) {
        // Skip this check for closing because we need to set the channel to closed first and then flush
        // in case there is any leftover rows
        if (isClosed() && !closing) {
            throw new SFException(ErrorCode.CLOSED_CHANNEL, getFullyQualifiedName());
        }

        // Simply return if there is no data in the channel, this might not work if we support public
        // flush API since there could a concurrent insert at the same time
        if (this.rowBufferMap.size() == 0) {
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
                            List<SnowflakeStreamingIngestChannelFlushable<?>> uncommittedChannels =
                                    this.owningClient.verifyChannelsAreFullyCommitted(
                                            Collections.singletonList(this));

                            this.rowBufferMap.clear();
                            this.owningClient.removeChannelIfSequencersMatch(this);

                            // Throw an exception if the channel is invalid or has any uncommitted rows
                            if (!isValid() || !uncommittedChannels.isEmpty()) {
                                throw new SFException(
                                        ErrorCode.CHANNELS_WITH_UNCOMMITTED_ROWS,
                                        uncommittedChannels.stream()
                                                .map(SnowflakeStreamingIngestChannelFlushable::getFullyQualifiedName)
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
    @Override
    public void setupSchema(List<ColumnMetadata> columns) {
        logger.logDebug("Setup schema for channel={}, schema={}", getFullyQualifiedName(), columns);
        tableColumns.clear();
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
        if (!rows.iterator().hasNext()) {
            throw new IllegalArgumentException("No rows to insert");
        }
        Map<String, Object> firstRow = rows.iterator().next();
        ObservabilityClusteringKey key = ObservabilityClusteringKey.createFromRow(firstRow);

        return insertRows(key, rows, startOffsetToken, endOffsetToken);
    }

    private InsertValidationResponse insertRows(ObservabilityClusteringKey clusteringKey, Iterable<Map<String, Object>> rows, @Nullable String startOffsetToken, @Nullable String endOffsetToken) {
        throttleInsertIfNeeded(memoryInfoProvider);
        checkValidation();

        if (isClosed()) {
            throw new SFException(ErrorCode.CLOSED_CHANNEL, getFullyQualifiedName());
        }

        RowBuffer<ParquetChunkData> rowBuffer = getRowBuffer(clusteringKey);
        InsertValidationResponse response =
                rowBuffer.insertRows(rows, startOffsetToken, endOffsetToken);

        // Start flush task if the chunk size reaches a certain size
        // TODO: Checking table/chunk level size reduces throughput a lot, we may want to check it only
        // if a large number of rows are inserted
        if (rowBuffer.getSize()
                >= this.owningClient.getParameterProvider().getMaxChannelSizeInBytes()) {
            this.owningClient.setNeedFlush(this.channelFlushContext.getFullyQualifiedTableName());
        }

        return response;
    }

    private RowBuffer<ParquetChunkData> getRowBuffer(ObservabilityClusteringKey clusteringKey) {
        RowBuffer<ParquetChunkData> buffer = this.rowBufferMap.get(clusteringKey);
        if (buffer == null) {
            synchronized (rowBufferMap) {
                buffer = this.rowBufferMap.get(clusteringKey);
                if (buffer == null) {
                    buffer = this.addRowBuffer.apply(clusteringKey);
                }
            }
        }
        return buffer;
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
    private void collectRowSize(float rowSize) {
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
    private void throttleInsertIfNeeded(MemoryInfoProvider memoryInfoProvider) {
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
            this.rowBufferMap.values().forEach(r -> r.close("checkValidation"));
            throw new SFException(
                    ErrorCode.INVALID_CHANNEL, getFullyQualifiedName(), this.invalidationCause);
        }
    }

    /** Returns underlying channel's attributes. */
    @VisibleForTesting
    public ChannelFlushContext getChannelContext() {
        return channelFlushContext;
    }
}
