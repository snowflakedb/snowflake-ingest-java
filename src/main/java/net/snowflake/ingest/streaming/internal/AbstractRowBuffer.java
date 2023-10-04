/*
 * Copyright (c) 2022 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import com.google.common.annotations.VisibleForTesting;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.utils.Constants;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.Logging;
import net.snowflake.ingest.utils.Pair;
import net.snowflake.ingest.utils.SFException;

/**
 * The abstract implementation of the buffer in the Streaming Ingest channel that holds the
 * un-flushed rows, these rows will be converted to the underlying format implementation for faster
 * processing
 *
 * @param <T> type of column data ({@link ParquetChunkData} for Parquet)
 */
abstract class AbstractRowBuffer<T> implements RowBuffer<T> {
  private static final Logging logger = new Logging(AbstractRowBuffer.class);
  // these types cannot be packed into the data chunk because they are not readable by the server
  // side scanner
  private static final int INVALID_SERVER_SIDE_DATA_TYPE_ORDINAL = -1;

  // The maximum recommended batch size of data passed into a single insertRows() call
  private static final int INSERT_ROWS_RECOMMENDED_MAX_BATCH_SIZE_IN_BYTES = 16 * 1024 * 1024;

  // Snowflake table column logical type
  enum ColumnLogicalType {
    ANY,
    BOOLEAN(1),
    ROWINDEX,
    NULL(15),
    REAL(8),
    FIXED(2),
    TEXT(9),
    CHAR,
    BINARY(10),
    DATE(7),
    TIME(6),
    TIMESTAMP_LTZ(3),
    TIMESTAMP_NTZ(4),
    TIMESTAMP_TZ(5),
    INTERVAL,
    RAW,
    ARRAY(13, true),
    OBJECT(12, true),
    VARIANT(11, true),
    ROW,
    SEQUENCE,
    FUNCTION,
    USER_DEFINED_TYPE,
    ;

    // ordinal should be in sync with the server side scanner
    private final int ordinal;
    // whether it is a composite data type: array, object or variant
    private final boolean object;

    ColumnLogicalType() {
      // no valid server side ordinal by default
      this(INVALID_SERVER_SIDE_DATA_TYPE_ORDINAL);
    }

    ColumnLogicalType(int ordinal) {
      this(ordinal, false);
    }

    ColumnLogicalType(int ordinal, boolean object) {
      this.ordinal = ordinal;
      this.object = object;
    }

    /**
     * Ordinal to encode the data type for the server side scanner
     *
     * <p>currently used for Parquet format
     */
    public int getOrdinal() {
      return ordinal;
    }

    /** Whether the data type is a composite type: OBJECT, VARIANT, ARRAY. */
    public boolean isObject() {
      return object;
    }
  }

  // Snowflake table column physical type
  enum ColumnPhysicalType {
    ROWINDEX(9),
    DOUBLE(7),
    SB1(1),
    SB2(2),
    SB4(3),
    SB8(4),
    SB16(5),
    LOB(8),
    BINARY,
    ROW(10),
    ;

    // ordinal should be in sync with the server side scanner
    private final int ordinal;

    ColumnPhysicalType() {
      // no valid server side ordinal by default
      this(INVALID_SERVER_SIDE_DATA_TYPE_ORDINAL);
    }

    ColumnPhysicalType(int ordinal) {
      this.ordinal = ordinal;
    }

    /**
     * Ordinal to encode the data type for the server side scanner
     *
     * <p>currently used for Parquet format
     */
    public int getOrdinal() {
      return ordinal;
    }
  }

  // Map the column name to the stats
  @VisibleForTesting Map<String, RowBufferStats> statsMap;

  // Temp stats map to use until all the rows are validated
  @VisibleForTesting Map<String, RowBufferStats> tempStatsMap;

  // Lock used to protect the buffers from concurrent read/write
  private final Lock flushLock;

  // Current row count buffered
  @VisibleForTesting volatile int bufferedRowCount;

  // Current buffer size
  private volatile float bufferSize;

  // Names of non-nullable columns
  private final Set<String> nonNullableFieldNames;

  // Buffer's channel fully qualified name with database, schema and table
  final String channelFullyQualifiedName;

  // Metric callback to report size of inserted rows
  private final Consumer<Float> rowSizeMetric;

  // State of the owning channel
  final ChannelRuntimeState channelState;

  // ON_ERROR option for this channel
  final OpenChannelRequest.OnErrorOption onErrorOption;

  final ZoneId defaultTimezone;

  // Buffer parameters that are set at the owning client level
  final ClientBufferParameters clientBufferParameters;

  AbstractRowBuffer(
      OpenChannelRequest.OnErrorOption onErrorOption,
      ZoneId defaultTimezone,
      String fullyQualifiedChannelName,
      Consumer<Float> rowSizeMetric,
      ChannelRuntimeState channelRuntimeState,
      ClientBufferParameters clientBufferParameters) {
    this.onErrorOption = onErrorOption;
    this.defaultTimezone = defaultTimezone;
    this.rowSizeMetric = rowSizeMetric;
    this.channelState = channelRuntimeState;
    this.channelFullyQualifiedName = fullyQualifiedChannelName;
    this.nonNullableFieldNames = new HashSet<>();
    this.flushLock = new ReentrantLock();
    this.bufferedRowCount = 0;
    this.bufferSize = 0F;
    this.clientBufferParameters = clientBufferParameters;

    // Initialize empty stats
    this.statsMap = new HashMap<>();
    this.tempStatsMap = new HashMap<>();
  }

  /**
   * Adds non-nullable field name. It is used to check if all non-nullable fields have been
   * provided.
   *
   * @param nonNullableFieldName non-nullable filed name
   */
  void addNonNullableFieldName(String nonNullableFieldName) {
    nonNullableFieldNames.add(nonNullableFieldName);
  }

  /** Throws an exception if the column has a collation defined. */
  void validateColumnCollation(ColumnMetadata column) {
    if (column.getCollation() != null) {
      throw new SFException(
          ErrorCode.UNSUPPORTED_DATA_TYPE,
          String.format(
              "Column %s with collation %s detected. Ingestion into collated columns is not"
                  + " supported",
              column.getName(), column.getCollation()));
    }
  }

  /**
   * Get the current buffer size
   *
   * @return the current buffer size
   */
  @Override
  public float getSize() {
    return bufferSize;
  }

  /**
   * Verify that the input row columns are all valid.
   *
   * <p>Checks that the columns, specified in the row, are present in the table and values for all
   * non-nullable columns are specified.
   *
   * @param row the input row
   * @param error the insert error that we return to the customer
   * @param rowIndex the index of the current row in the input batch
   * @return the set of input column names
   */
  Set<String> verifyInputColumns(
      Map<String, Object> row, InsertValidationResponse.InsertError error, int rowIndex) {
    // Map of unquoted column name -> original column name
    Map<String, String> inputColNamesMap =
        row.keySet().stream()
            .collect(Collectors.toMap(LiteralQuoteUtils::unquoteColumnName, value -> value));

    // Check for extra columns in the row
    List<String> extraCols = new ArrayList<>();
    for (String columnName : inputColNamesMap.keySet()) {
      if (!hasColumn(columnName)) {
        extraCols.add(inputColNamesMap.get(columnName));
      }
    }

    if (!extraCols.isEmpty()) {
      if (error != null) {
        error.setExtraColNames(extraCols);
      }
      throw new SFException(
          ErrorCode.INVALID_FORMAT_ROW,
          "Extra columns: " + extraCols,
          String.format(
              "Columns not present in the table shouldn't be specified, rowIndex:%d", rowIndex));
    }

    // Check for missing columns in the row
    List<String> missingCols = new ArrayList<>();
    for (String columnName : this.nonNullableFieldNames) {
      if (!inputColNamesMap.containsKey(columnName)) {
        missingCols.add(statsMap.get(columnName).getColumnDisplayName());
      }
    }

    if (!missingCols.isEmpty()) {
      if (error != null) {
        error.setMissingNotNullColNames(missingCols);
      }
      throw new SFException(
          ErrorCode.INVALID_FORMAT_ROW,
          "Missing columns: " + missingCols,
          String.format(
              "Values for all non-nullable columns must be specified, rowIndex:%d", rowIndex));
    }

    return inputColNamesMap.keySet();
  }

  /**
   * Insert a batch of rows into the row buffer
   *
   * @param rows input row
   * @param offsetToken offset token of the latest row in the batch
   * @return insert response that possibly contains errors because of insertion failures
   */
  @Override
  public InsertValidationResponse insertRows(
      Iterable<Map<String, Object>> rows, String offsetToken) {
    float rowsSizeInBytes = 0F;
    if (!hasColumns()) {
      throw new SFException(ErrorCode.INTERNAL_ERROR, "Empty column fields");
    }
    InsertValidationResponse response = new InsertValidationResponse();
    this.flushLock.lock();
    try {
      this.channelState.updateInsertStats(System.currentTimeMillis(), this.bufferedRowCount);
      if (onErrorOption == OpenChannelRequest.OnErrorOption.CONTINUE
          || onErrorOption == OpenChannelRequest.OnErrorOption.SKIP_AT_FIRST_ERROR) {
        // Used to map incoming row(nth row) to InsertError(for nth row) in response
        int rowIndex = 0;
        for (Map<String, Object> row : rows) {
          InsertValidationResponse.InsertError error =
              new InsertValidationResponse.InsertError(row, rowIndex);
          try {
            Set<String> inputColumnNames = verifyInputColumns(row, error, rowIndex);
            rowsSizeInBytes +=
                addRow(row, this.bufferedRowCount, this.statsMap, inputColumnNames, rowIndex);
            this.bufferedRowCount++;
          } catch (SFException e) {
            error.setException(e);
            response.addError(error);
          } catch (Throwable e) {
            logger.logWarn("Unexpected error happens during insertRows: {}", e);
            error.setException(new SFException(e, ErrorCode.INTERNAL_ERROR, e.getMessage()));
            response.addError(error);
          }
          checkBatchSizeEnforcedMaximum(rowsSizeInBytes);
          rowIndex++;
          if (onErrorOption == OpenChannelRequest.OnErrorOption.SKIP_AT_FIRST_ERROR
              && response.hasErrors()) {
            break;
          }
          if (this.bufferedRowCount == Integer.MAX_VALUE) {
            throw new SFException(ErrorCode.INTERNAL_ERROR, "Row count reaches MAX value");
          }
        }
        checkBatchSizeRecommendedMaximum(rowsSizeInBytes);
      } else if (onErrorOption == OpenChannelRequest.OnErrorOption.ABORT) {
        // If the on_error option is ABORT, simply throw the first exception
        float tempRowsSizeInBytes = 0F;
        int tempRowCount = 0;
        for (Map<String, Object> row : rows) {
          Set<String> inputColumnNames = verifyInputColumns(row, null, tempRowCount);
          tempRowsSizeInBytes +=
              addTempRow(row, tempRowCount, this.tempStatsMap, inputColumnNames, tempRowCount);
          checkBatchSizeEnforcedMaximum(tempRowsSizeInBytes);
          tempRowCount++;
        }
        checkBatchSizeRecommendedMaximum(tempRowsSizeInBytes);

        moveTempRowsToActualBuffer(tempRowCount);

        rowsSizeInBytes = tempRowsSizeInBytes;
        if ((long) this.bufferedRowCount + tempRowCount >= Integer.MAX_VALUE) {
          throw new SFException(ErrorCode.INTERNAL_ERROR, "Row count reaches MAX value");
        }
        this.bufferedRowCount += tempRowCount;
        this.statsMap.forEach(
            (colName, stats) ->
                this.statsMap.put(
                    colName,
                    RowBufferStats.getCombinedStats(stats, this.tempStatsMap.get(colName))));
      }

      this.bufferSize += rowsSizeInBytes;
      this.channelState.setOffsetToken(offsetToken);
      this.rowSizeMetric.accept(rowsSizeInBytes);
    } finally {
      this.tempStatsMap.values().forEach(RowBufferStats::reset);
      clearTempRows();
      this.flushLock.unlock();
    }

    return response;
  }

  /**
   * Flush the data in the row buffer by taking the ownership of the old vectors and pass all the
   * required info back to the flush service to build the blob
   *
   * @param filePath the name of the file the data will be written in
   * @return A ChannelData object that contains the info needed by the flush service to build a blob
   */
  @Override
  public ChannelData<T> flush(final String filePath) {
    logger.logDebug("Start get data for channel={}", channelFullyQualifiedName);
    if (this.bufferedRowCount > 0) {
      Optional<T> oldData = Optional.empty();
      int oldRowCount = 0;
      float oldBufferSize = 0F;
      long oldRowSequencer = 0;
      String oldOffsetToken = null;
      Map<String, RowBufferStats> oldColumnEps = null;
      Pair<Long, Long> oldMinMaxInsertTimeInMs = null;

      logger.logDebug("Buffer flush about to take lock on channel={}", channelFullyQualifiedName);

      this.flushLock.lock();
      try {
        if (this.bufferedRowCount > 0) {
          // Transfer the ownership of the vectors
          oldData = getSnapshot(filePath);
          oldRowCount = this.bufferedRowCount;
          oldBufferSize = this.bufferSize;
          oldRowSequencer = this.channelState.incrementAndGetRowSequencer();
          oldOffsetToken = this.channelState.getOffsetToken();
          oldColumnEps = new HashMap<>(this.statsMap);
          oldMinMaxInsertTimeInMs =
              new Pair<>(
                  this.channelState.getFirstInsertInMs(), this.channelState.getLastInsertInMs());
          // Reset everything in the buffer once we save all the info
          reset();
        }
      } finally {
        this.flushLock.unlock();
      }

      logger.logDebug(
          "Buffer flush released lock on channel={}, rowCount={}, bufferSize={}",
          channelFullyQualifiedName,
          oldRowCount,
          oldBufferSize);

      if (oldData.isPresent()) {
        ChannelData<T> data = new ChannelData<>();
        data.setVectors(oldData.get());
        data.setRowCount(oldRowCount);
        data.setBufferSize(oldBufferSize);
        data.setRowSequencer(oldRowSequencer);
        data.setOffsetToken(oldOffsetToken);
        data.setColumnEps(oldColumnEps);
        data.setMinMaxInsertTimeInMs(oldMinMaxInsertTimeInMs);
        data.setFlusherFactory(this::createFlusher);
        return data;
      }
    }
    return null;
  }

  /** Whether row has a column with a given name. */
  abstract boolean hasColumn(String name);

  /**
   * Add an input row to the buffer.
   *
   * @param row input row
   * @param bufferedRowIndex Row number of buffered rows which will eventually by flushed.
   * @param statsMap column stats map
   * @param formattedInputColumnNames list of input column names after formatting
   * @param insertRowIndex Index of the rows given in insertRows API. Not the same as
   *     bufferedRowIndex
   * @return row size
   */
  abstract float addRow(
      Map<String, Object> row,
      int bufferedRowIndex,
      Map<String, RowBufferStats> statsMap,
      Set<String> formattedInputColumnNames,
      final long insertRowIndex);

  /**
   * Add an input row to the temporary row buffer.
   *
   * <p>The temporary row buffer is used to add rows before validating all of them. Once all the
   * rows to add have been validated, the temporary row buffer is moved to the actual one.
   *
   * @param row input row
   * @param curRowIndex current row index to use
   * @param statsMap column stats map
   * @param formattedInputColumnNames list of input column names after formatting
   * @param insertRowIndex index of the row being inserteed from User Input List
   * @return row size
   */
  abstract float addTempRow(
      Map<String, Object> row,
      int curRowIndex,
      Map<String, RowBufferStats> statsMap,
      Set<String> formattedInputColumnNames,
      long insertRowIndex);

  /** Move rows from the temporary buffer to the current row buffer. */
  abstract void moveTempRowsToActualBuffer(int tempRowCount);

  /** Clear the temporary row buffer. */
  abstract void clearTempRows();

  /** If row has any column. */
  abstract boolean hasColumns();

  /** Reset the variables after each flush. Note that the caller needs to handle synchronization. */
  void reset() {
    this.bufferedRowCount = 0;
    this.bufferSize = 0F;
    this.statsMap.replaceAll((key, value) -> value.forkEmpty());
  }

  /**
   * Get buffered data snapshot for later flushing.
   *
   * @param filePath the name of the file the data will be written in
   */
  abstract Optional<T> getSnapshot(final String filePath);

  @VisibleForTesting
  abstract Object getVectorValueAt(String column, int index);

  @VisibleForTesting
  abstract int getTempRowCount();

  /**
   * Close row buffer by releasing its internal resources only. Note that the allocated memory for
   * the channel is released elsewhere (in close).
   */
  abstract void closeInternal();

  /** Close the row buffer and release allocated memory for the channel. */
  @Override
  public synchronized void close(String name) {

    closeInternal();
  }

  /**
   * Given a set of col names to stats, build the right ep info
   *
   * @param rowCount: count of rows in the given buffer
   * @param colStats: map of column name to RowBufferStats
   * @return the EPs built from column stats
   */
  static EpInfo buildEpInfoFromStats(long rowCount, Map<String, RowBufferStats> colStats) {
    EpInfo epInfo = new EpInfo(rowCount, new HashMap<>());
    for (Map.Entry<String, RowBufferStats> colStat : colStats.entrySet()) {
      RowBufferStats stat = colStat.getValue();
      FileColumnProperties dto = new FileColumnProperties(stat);
      String colName = colStat.getValue().getColumnDisplayName();
      epInfo.getColumnEps().put(colName, dto);
    }
    epInfo.verifyEpInfo();
    return epInfo;
  }

  /** Row buffer factory. */
  static <T> AbstractRowBuffer<T> createRowBuffer(
      OpenChannelRequest.OnErrorOption onErrorOption,
      ZoneId defaultTimezone,
      Constants.BdecVersion bdecVersion,
      String fullyQualifiedChannelName,
      Consumer<Float> rowSizeMetric,
      ChannelRuntimeState channelRuntimeState,
      ClientBufferParameters clientBufferParameters) {
    switch (bdecVersion) {
      case THREE:
        //noinspection unchecked
        return (AbstractRowBuffer<T>)
            new ParquetRowBuffer(
                onErrorOption,
                defaultTimezone,
                fullyQualifiedChannelName,
                rowSizeMetric,
                channelRuntimeState,
                clientBufferParameters);
      default:
        throw new SFException(
            ErrorCode.INTERNAL_ERROR, "Unsupported BDEC format version: " + bdecVersion);
    }
  }

  private void checkBatchSizeEnforcedMaximum(float batchSizeInBytes) {
    if (batchSizeInBytes > clientBufferParameters.getMaxChunkSizeInBytes()) {
      throw new SFException(
          ErrorCode.MAX_BATCH_SIZE_EXCEEDED,
          clientBufferParameters.getMaxChunkSizeInBytes(),
          INSERT_ROWS_RECOMMENDED_MAX_BATCH_SIZE_IN_BYTES);
    }
  }

  private void checkBatchSizeRecommendedMaximum(float batchSizeInBytes) {
    if (batchSizeInBytes > INSERT_ROWS_RECOMMENDED_MAX_BATCH_SIZE_IN_BYTES) {
      logger.logWarn(
          "The batch of rows passed to 'insertRows' is over the recommended max batch size. Given"
              + " {} bytes, recommended max batch is {} bytes. For optimal performance and memory"
              + " utilization, we recommend splitting large batches into multiple smaller ones and"
              + " call insertRows for each smaller batch separately.",
          batchSizeInBytes,
          INSERT_ROWS_RECOMMENDED_MAX_BATCH_SIZE_IN_BYTES);
    }
  }
}
