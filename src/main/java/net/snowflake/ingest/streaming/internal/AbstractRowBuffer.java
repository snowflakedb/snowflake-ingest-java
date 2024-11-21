/*
 * Copyright (c) 2022-2024 Snowflake Computing Inc. All rights reserved.
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
import net.snowflake.ingest.connection.TelemetryService;
import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.OffsetTokenVerificationFunction;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.utils.Constants;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.Logging;
import net.snowflake.ingest.utils.Pair;
import net.snowflake.ingest.utils.SFException;
import org.apache.parquet.column.ParquetProperties;

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

  /** Insert rows function strategy for ON_ERROR=CONTINUE */
  public class ContinueIngestionStrategy<T> implements IngestionStrategy<T> {
    @Override
    public InsertValidationResponse insertRows(
        AbstractRowBuffer<T> rowBuffer,
        Iterable<Map<String, Object>> rows,
        String startOffsetToken,
        String endOffsetToken) {
      InsertValidationResponse response = new InsertValidationResponse();
      float rowsSizeInBytes = 0F;
      int rowIndex = 0;
      int prevRowCount = rowBuffer.bufferedRowCount;
      for (Map<String, Object> row : rows) {
        InsertValidationResponse.InsertError error =
            new InsertValidationResponse.InsertError(row, rowIndex);
        try {
          if (rowBuffer.bufferedRowCount == Integer.MAX_VALUE) {
            throw new SFException(ErrorCode.INTERNAL_ERROR, "Row count reaches MAX value");
          }
          Set<String> inputColumnNames = verifyInputColumns(row, error, rowIndex);
          rowsSizeInBytes +=
              addRow(
                  row,
                  rowBuffer.bufferedRowCount,
                  rowBuffer.statsMap,
                  inputColumnNames,
                  rowIndex,
                  error);
          rowBuffer.bufferedRowCount++;
        } catch (SFException e) {
          error.setException(e);
          response.addError(error);
        } catch (Throwable e) {
          logger.logWarn("Unexpected error happens during insertRows: {}", e);
          error.setException(new SFException(e, ErrorCode.INTERNAL_ERROR, e.getMessage()));
          response.addError(error);
        }
        rowIndex++;
      }
      checkBatchSizeRecommendedMaximum(rowsSizeInBytes);
      checkOffsetMismatch(
          rowBuffer.channelState.getEndOffsetToken(), startOffsetToken, endOffsetToken, rowIndex);
      rowBuffer.channelState.updateOffsetToken(startOffsetToken, endOffsetToken, prevRowCount);
      rowBuffer.bufferSize += rowsSizeInBytes;
      rowBuffer.rowSizeMetric.accept(rowsSizeInBytes);
      return response;
    }
  }

  /** Insert rows function strategy for ON_ERROR=ABORT */
  public class AbortIngestionStrategy<T> implements IngestionStrategy<T> {
    @Override
    public InsertValidationResponse insertRows(
        AbstractRowBuffer<T> rowBuffer,
        Iterable<Map<String, Object>> rows,
        String startOffsetToken,
        String endOffsetToken) {
      // If the on_error option is ABORT, simply throw the first exception
      InsertValidationResponse response = new InsertValidationResponse();
      float rowsSizeInBytes = 0F;
      float tempRowsSizeInBytes = 0F;
      int tempRowCount = 0;
      for (Map<String, Object> row : rows) {
        Set<String> inputColumnNames = verifyInputColumns(row, null, tempRowCount);
        tempRowsSizeInBytes +=
            addTempRow(
                row,
                tempRowCount,
                rowBuffer.tempStatsMap,
                inputColumnNames,
                tempRowCount,
                new InsertValidationResponse.InsertError(row, 0) /* dummy error */);
        tempRowCount++;
        if ((long) rowBuffer.bufferedRowCount + tempRowCount >= Integer.MAX_VALUE) {
          throw new SFException(ErrorCode.INTERNAL_ERROR, "Row count reaches MAX value");
        }
      }
      checkBatchSizeRecommendedMaximum(tempRowsSizeInBytes);

      moveTempRowsToActualBuffer(tempRowCount);

      rowsSizeInBytes = tempRowsSizeInBytes;
      rowBuffer.statsMap.forEach(
          (colName, stats) ->
              rowBuffer.statsMap.put(
                  colName,
                  RowBufferStats.getCombinedStats(stats, rowBuffer.tempStatsMap.get(colName))));
      checkOffsetMismatch(
          rowBuffer.channelState.getEndOffsetToken(),
          startOffsetToken,
          endOffsetToken,
          tempRowCount);
      rowBuffer.channelState.updateOffsetToken(
          startOffsetToken, endOffsetToken, rowBuffer.bufferedRowCount);
      rowBuffer.bufferedRowCount += tempRowCount;
      rowBuffer.bufferSize += rowsSizeInBytes;
      rowBuffer.rowSizeMetric.accept(rowsSizeInBytes);
      return response;
    }
  }

  /** Insert rows function strategy for ON_ERROR=SKIP_BATCH */
  public class SkipBatchIngestionStrategy<T> implements IngestionStrategy<T> {
    @Override
    public InsertValidationResponse insertRows(
        AbstractRowBuffer<T> rowBuffer,
        Iterable<Map<String, Object>> rows,
        String startOffsetToken,
        String endOffsetToken) {
      InsertValidationResponse response = new InsertValidationResponse();
      float rowsSizeInBytes = 0F;
      float tempRowsSizeInBytes = 0F;
      int tempRowCount = 0;
      int rowIndex = 0;
      for (Map<String, Object> row : rows) {
        InsertValidationResponse.InsertError error =
            new InsertValidationResponse.InsertError(row, rowIndex);
        try {
          Set<String> inputColumnNames = verifyInputColumns(row, error, rowIndex);
          tempRowsSizeInBytes +=
              addTempRow(
                  row, tempRowCount, rowBuffer.tempStatsMap, inputColumnNames, rowIndex, error);
          tempRowCount++;
        } catch (SFException e) {
          error.setException(e);
          response.addError(error);
        } catch (Throwable e) {
          logger.logWarn("Unexpected error happens during insertRows: {}", e);
          error.setException(new SFException(e, ErrorCode.INTERNAL_ERROR, e.getMessage()));
          response.addError(error);
        }
        rowIndex++;
        if ((long) rowBuffer.bufferedRowCount + rowIndex >= Integer.MAX_VALUE) {
          throw new SFException(ErrorCode.INTERNAL_ERROR, "Row count reaches MAX value");
        }
      }

      if (!response.hasErrors()) {
        checkBatchSizeRecommendedMaximum(tempRowsSizeInBytes);
        moveTempRowsToActualBuffer(tempRowCount);
        rowsSizeInBytes = tempRowsSizeInBytes;
        rowBuffer.statsMap.forEach(
            (colName, stats) ->
                rowBuffer.statsMap.put(
                    colName,
                    RowBufferStats.getCombinedStats(stats, rowBuffer.tempStatsMap.get(colName))));
        checkOffsetMismatch(
            rowBuffer.channelState.getEndOffsetToken(), startOffsetToken, endOffsetToken, rowIndex);
        rowBuffer.channelState.updateOffsetToken(
            startOffsetToken, endOffsetToken, rowBuffer.bufferedRowCount);
        rowBuffer.bufferedRowCount += tempRowCount;
        rowBuffer.bufferSize += rowsSizeInBytes;
        rowBuffer.rowSizeMetric.accept(rowsSizeInBytes);
      }
      return response;
    }
  }

  // Map the column name to the stats
  @VisibleForTesting Map<String, RowBufferStats> statsMap;

  // Temp stats map to use until all the rows are validated
  @VisibleForTesting Map<String, RowBufferStats> tempStatsMap;

  // Map of the column name to the column object, used for null/missing column check
  protected final Map<String, ParquetColumn> fieldIndex;

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

  // Function used to verify offset token logic
  final OffsetTokenVerificationFunction offsetTokenVerificationFunction;

  // Telemetry service use to report telemetry to SF
  final TelemetryService telemetryService;

  AbstractRowBuffer(
      OpenChannelRequest.OnErrorOption onErrorOption,
      ZoneId defaultTimezone,
      String fullyQualifiedChannelName,
      Consumer<Float> rowSizeMetric,
      ChannelRuntimeState channelRuntimeState,
      ClientBufferParameters clientBufferParameters,
      OffsetTokenVerificationFunction offsetTokenVerificationFunction,
      TelemetryService telemetryService) {
    this.onErrorOption = onErrorOption;
    this.defaultTimezone = defaultTimezone;
    this.rowSizeMetric = rowSizeMetric;
    this.channelState = channelRuntimeState;
    this.channelFullyQualifiedName = fullyQualifiedChannelName;
    this.nonNullableFieldNames = new HashSet<>();
    this.flushLock = new ReentrantLock(true);
    this.bufferedRowCount = 0;
    this.bufferSize = 0F;
    this.clientBufferParameters = clientBufferParameters;
    this.offsetTokenVerificationFunction = offsetTokenVerificationFunction;
    this.telemetryService = telemetryService;

    // Initialize empty stats
    this.statsMap = new HashMap<>();
    this.tempStatsMap = new HashMap<>();

    this.fieldIndex = new HashMap<>();
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
   * non-nullable columns are specified and different from null.
   *
   * @param row the input row
   * @param error the insert error that we return to the customer
   * @param rowIndex the index of the current row in the input batch
   * @return the set of input column names
   */
  Set<String> verifyInputColumns(
      Map<String, Object> row, InsertValidationResponse.InsertError error, int rowIndex) {
    // Map of unquoted column name -> original column name
    Set<String> originalKeys = row.keySet();
    Map<String, String> inputColNamesMap = new HashMap<>();
    originalKeys.forEach(
        key -> inputColNamesMap.put(LiteralQuoteUtils.unquoteColumnName(key), key));
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
        missingCols.add(fieldIndex.get(columnName).columnMetadata.getName());
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

    // Check for null values in not-nullable columns in the row
    List<String> nullValueNotNullCols = new ArrayList<>();
    for (String columnName : this.nonNullableFieldNames) {
      if (inputColNamesMap.containsKey(columnName)
          && row.get(inputColNamesMap.get(columnName)) == null) {
        nullValueNotNullCols.add(fieldIndex.get(columnName).columnMetadata.getName());
      }
    }

    if (!nullValueNotNullCols.isEmpty()) {
      if (error != null) {
        error.setNullValueForNotNullColNames(nullValueNotNullCols);
      }
      throw new SFException(
          ErrorCode.INVALID_FORMAT_ROW,
          "Not-nullable columns with null values: " + nullValueNotNullCols,
          String.format(
              "Values for all non-nullable columns must not be null, rowIndex:%d", rowIndex));
    }

    return inputColNamesMap.keySet();
  }

  /**
   * Insert a batch of rows into the row buffer
   *
   * @param rows input row
   * @param startOffsetToken start offset token of the batch
   * @param endOffsetToken offset token of the latest row in the batch
   * @return insert response that possibly contains errors because of insertion failures
   */
  @Override
  public InsertValidationResponse insertRows(
      Iterable<Map<String, Object>> rows, String startOffsetToken, String endOffsetToken) {
    if (!hasColumns()) {
      throw new SFException(ErrorCode.INTERNAL_ERROR, "Empty column fields");
    }
    InsertValidationResponse response = null;
    this.flushLock.lock();
    try {
      this.channelState.updateInsertStats(System.currentTimeMillis(), this.bufferedRowCount);
      IngestionStrategy<T> ingestionStrategy = createIngestionStrategy(onErrorOption);
      response = ingestionStrategy.insertRows(this, rows, startOffsetToken, endOffsetToken);
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
   * @return A ChannelData object that contains the info needed by the flush service to build a blob
   */
  @Override
  public ChannelData<T> flush() {
    logger.logDebug("Start get data for channel={}", channelFullyQualifiedName);
    if (this.bufferedRowCount > 0) {
      Optional<T> oldData = Optional.empty();
      int oldRowCount = 0;
      float oldBufferSize = 0F;
      long oldRowSequencer = 0;
      String oldEndOffsetToken = null;
      String oldStartOffsetToken = null;
      Map<String, RowBufferStats> oldColumnEps = null;
      Pair<Long, Long> oldMinMaxInsertTimeInMs = null;

      logger.logDebug("Buffer flush about to take lock on channel={}", channelFullyQualifiedName);

      this.flushLock.lock();
      try {
        if (this.bufferedRowCount > 0) {
          // Transfer the ownership of the vectors
          oldData = getSnapshot();
          oldRowCount = this.bufferedRowCount;
          oldBufferSize = this.bufferSize;
          oldRowSequencer = this.channelState.incrementAndGetRowSequencer();
          oldEndOffsetToken = this.channelState.getEndOffsetToken();
          oldStartOffsetToken = this.channelState.getStartOffsetToken();
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
        data.setEndOffsetToken(oldEndOffsetToken);
        data.setStartOffsetToken(oldStartOffsetToken);
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
   * @param error Insert error object, used to populate error details when doing structured data
   *     type parsing
   * @return row size
   */
  abstract float addRow(
      Map<String, Object> row,
      int bufferedRowIndex,
      Map<String, RowBufferStats> statsMap,
      Set<String> formattedInputColumnNames,
      final long insertRowIndex,
      InsertValidationResponse.InsertError error);

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
   * @param error Insert error object, used to populate error details when doing structured data
   *     type parsing
   * @return row size
   */
  abstract float addTempRow(
      Map<String, Object> row,
      int curRowIndex,
      Map<String, RowBufferStats> statsMap,
      Set<String> formattedInputColumnNames,
      long insertRowIndex,
      InsertValidationResponse.InsertError error);

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

  /** Get buffered data snapshot for later flushing. */
  abstract Optional<T> getSnapshot();

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
   * @param setAllDefaultValues: whether to set default values for all null min/max field in the EPs
   * @param enableDistinctValuesCount: whether to include valid NDV in the EPs irrespective of the
   *     data type of this column
   * @return the EPs built from column stats
   */
  static EpInfo buildEpInfoFromStats(
      long rowCount,
      Map<String, RowBufferStats> colStats,
      boolean setAllDefaultValues,
      boolean enableDistinctValuesCount) {
    EpInfo epInfo = new EpInfo(rowCount, new HashMap<>(), enableDistinctValuesCount);
    for (Map.Entry<String, RowBufferStats> colStat : colStats.entrySet()) {
      RowBufferStats stat = colStat.getValue();
      FileColumnProperties dto = new FileColumnProperties(stat, setAllDefaultValues);
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
      ClientBufferParameters clientBufferParameters,
      OffsetTokenVerificationFunction offsetTokenVerificationFunction,
      ParquetProperties.WriterVersion parquetWriterVersion,
      TelemetryService telemetryService) {
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
                clientBufferParameters,
                offsetTokenVerificationFunction,
                parquetWriterVersion,
                telemetryService);
      default:
        throw new SFException(
            ErrorCode.INTERNAL_ERROR, "Unsupported BDEC format version: " + bdecVersion);
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

  /**
   * We verify the offset token based on the provided verification logic and report to SF if there
   * is a mismatch.
   */
  private void checkOffsetMismatch(
      String prevEndOffset, String curStartOffset, String curEndOffset, int rowCount) {
    if (offsetTokenVerificationFunction != null
        && !offsetTokenVerificationFunction.verify(
            prevEndOffset, curStartOffset, curEndOffset, rowCount)) {
      logger.logWarn(
          "The channel {} might have an offset token mismatch based on the provided offset token"
              + " verification logic, preEndOffset={}, curStartOffset={}, curEndOffset={},"
              + " rowCount={}.",
          channelFullyQualifiedName,
          prevEndOffset,
          curStartOffset,
          curEndOffset,
          rowCount);
      if (telemetryService != null) {
        telemetryService.reportBatchOffsetMismatch(
            channelFullyQualifiedName, prevEndOffset, curStartOffset, curEndOffset, rowCount);
      }
    }
  }

  /** Create the ingestion strategy based on the channel on_error option */
  IngestionStrategy<T> createIngestionStrategy(OpenChannelRequest.OnErrorOption onErrorOption) {
    switch (onErrorOption) {
      case CONTINUE:
        return new ContinueIngestionStrategy<>();
      case ABORT:
        return new AbortIngestionStrategy<>();
      case SKIP_BATCH:
        return new SkipBatchIngestionStrategy<>();
      default:
        throw new IllegalArgumentException("Unknown on error option: " + onErrorOption);
    }
  }
}
