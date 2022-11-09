/*
 * Copyright (c) 2022 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.utils.Constants;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.Logging;
import net.snowflake.ingest.utils.SFException;
import net.snowflake.ingest.utils.Utils;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.VisibleForTesting;

/**
 * The abstract implementation of the buffer in the Streaming Ingest channel that holds the
 * un-flushed rows, these rows will be converted to the underlying format implementation for faster
 * processing
 *
 * @param <T> type of column data (Arrow {@link org.apache.arrow.vector.VectorSchemaRoot})
 */
abstract class AbstractRowBuffer<T> implements RowBuffer<T> {
  private static final Logging logger = new Logging(AbstractRowBuffer.class);
  // these types cannot be packed into the data chunk because they are not readable by the server
  // side scanner
  private static final int INVALID_SERVER_SIDE_DATA_TYPE_ORDINAL = -1;

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

  // Current row count
  @VisibleForTesting volatile int rowCount;

  // Current buffer size
  private volatile float bufferSize;

  // Names of non-nullable columns
  private final Set<String> nonNullableFieldNames;

  // ON_ERROR option for this channel
  final OpenChannelRequest.OnErrorOption onErrorOption;

  final String channelFullyQualifiedName;

  // Indicates whether the channel's buffer is still valid
  volatile boolean isValid;

  private volatile String offsetToken;

  private final AtomicLong rowSequencer;

  private final Consumer<Float> rowSizeMetric;

  // Allocator used to allocate the buffers
  final BufferAllocator allocator;

  AbstractRowBuffer(BufferConfig bufferConfig) {
    this.allocator = bufferConfig.allocator;
    this.rowSizeMetric = bufferConfig.rowSizeMetric;
    this.isValid = true;
    this.offsetToken = bufferConfig.offsetToken;
    this.rowSequencer = new AtomicLong(bufferConfig.rowSequencer);
    this.onErrorOption = bufferConfig.onErrorOption;
    this.channelFullyQualifiedName = bufferConfig.fullyQualifiedName;
    this.nonNullableFieldNames = new HashSet<>();
    this.flushLock = new ReentrantLock();
    this.rowCount = 0;
    this.bufferSize = 0F;

    // Initialize empty stats
    this.statsMap = new HashMap<>();
    this.tempStatsMap = new HashMap<>();
  }

  @Override
  public boolean isValid() {
    return this.isValid;
  }

  @Override
  public void invalidate() {
    isValid = false;
  }

  @Override
  public String getOffsetToken() {
    return this.offsetToken;
  }

  @Override
  public AtomicLong getRowSequencer() {
    return this.rowSequencer;
  }

  OpenChannelRequest.OnErrorOption getOnErrorOption() {
    return this.onErrorOption;
  }

  /**
   * Adds non-nullable filed name.
   *
   * @param nonNullableFieldName non-nullable filed name
   */
  void addNonNullableFieldName(String nonNullableFieldName) {
    nonNullableFieldNames.add(nonNullableFieldName);
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
   * @return the set of input column names
   */
  Set<String> verifyInputColumns(
      Map<String, Object> row, InsertValidationResponse.InsertError error) {
    Map<String, String> inputColNamesMap =
        row.keySet().stream()
            .collect(Collectors.toMap(AbstractRowBuffer::formatColumnName, value -> value));

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
          ErrorCode.INVALID_ROW,
          "Extra columns: " + extraCols,
          "Columns not present in the table shouldn't be specified.");
    }

    // Check for missing columns in the row
    List<String> missingCols = new ArrayList<>();
    for (String columnName : this.nonNullableFieldNames) {
      if (!inputColNamesMap.containsKey(columnName)) {
        missingCols.add(columnName);
      }
    }

    if (!missingCols.isEmpty()) {
      if (error != null) {
        error.setMissingNotNullColNames(missingCols);
      }
      throw new SFException(
          ErrorCode.INVALID_ROW,
          "Missing columns: " + missingCols,
          "Values for all non-nullable columns must be specified.");
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
    float rowSize = 0F;
    if (!hasColumns()) {
      throw new SFException(ErrorCode.INTERNAL_ERROR, "Empty column fields");
    }
    InsertValidationResponse response = new InsertValidationResponse();
    this.flushLock.lock();
    try {
      if (getOnErrorOption() == OpenChannelRequest.OnErrorOption.CONTINUE) {
        // Used to map incoming row(nth row) to InsertError(for nth row) in response
        long rowIndex = 0;
        for (Map<String, Object> row : rows) {
          InsertValidationResponse.InsertError error =
              new InsertValidationResponse.InsertError(row, rowIndex);
          try {
            Set<String> inputColumnNames = verifyInputColumns(row, error);
            rowSize += addRow(row, this.rowCount, this.statsMap, inputColumnNames);
            this.rowCount++;
            this.bufferSize += rowSize;
          } catch (SFException e) {
            error.setException(e);
            response.addError(error);
          } catch (Throwable e) {
            logger.logWarn("Unexpected error happens during insertRows: {}", e);
            error.setException(new SFException(e, ErrorCode.INTERNAL_ERROR, e.getMessage()));
            response.addError(error);
          }
          rowIndex++;
          if (this.rowCount == Integer.MAX_VALUE) {
            throw new SFException(ErrorCode.INTERNAL_ERROR, "Row count reaches MAX value");
          }
        }
      } else {
        // If the on_error option is ABORT, simply throw the first exception
        float tempRowSize = 0F;
        int tempRowCount = 0;
        for (Map<String, Object> row : rows) {
          Set<String> inputColumnNames = verifyInputColumns(row, null);
          tempRowSize += addTempRow(row, tempRowCount, this.tempStatsMap, inputColumnNames);
          tempRowCount++;
        }

        moveTempRowsToActualBuffer(tempRowCount);

        rowSize = tempRowSize;
        if ((long) this.rowCount + tempRowCount >= Integer.MAX_VALUE) {
          throw new SFException(ErrorCode.INTERNAL_ERROR, "Row count reaches MAX value");
        }
        this.rowCount += tempRowCount;
        this.bufferSize += rowSize;
        this.statsMap.forEach(
            (colName, stats) ->
                this.statsMap.put(
                    colName,
                    RowBufferStats.getCombinedStats(stats, this.tempStatsMap.get(colName))));
      }

      this.offsetToken = offsetToken;
      this.rowSizeMetric.accept(rowSize);
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
    if (this.rowCount > 0) {
      Optional<T> oldData = Optional.empty();
      int oldRowCount = 0;
      float oldBufferSize = 0F;
      long oldRowSequencer = 0;
      String oldOffsetToken = null;
      Map<String, RowBufferStats> oldColumnEps = null;

      logger.logDebug(
          "Arrow buffer flush about to take lock on channel={}", channelFullyQualifiedName);

      this.flushLock.lock();
      try {
        if (this.rowCount > 0) {
          // Transfer the ownership of the vectors
          oldData = getSnapshot();
          oldRowCount = this.rowCount;
          oldBufferSize = this.bufferSize;
          oldRowSequencer = rowSequencer.incrementAndGet();
          oldOffsetToken = offsetToken;
          oldColumnEps = new HashMap<>(this.statsMap);
          // Reset everything in the buffer once we save all the info
          reset();
        }
      } finally {
        this.flushLock.unlock();
      }

      logger.logDebug(
          "Arrow buffer flush released lock on channel={}, rowCount={}, bufferSize={}",
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
   * @param curRowIndex current row index to use
   * @param statsMap column stats map
   * @param formattedInputColumnNames list of input column names after formatting
   * @return row size
   */
  abstract float addRow(
      Map<String, Object> row,
      int curRowIndex,
      Map<String, RowBufferStats> statsMap,
      Set<String> formattedInputColumnNames);

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
   * @return row size
   */
  abstract float addTempRow(
      Map<String, Object> row,
      int curRowIndex,
      Map<String, RowBufferStats> statsMap,
      Set<String> formattedInputColumnNames);

  /** Move rows from the temporary buffer to the current row buffer. */
  abstract void moveTempRowsToActualBuffer(int tempRowCount);

  /** Clear the temporary row buffer. */
  abstract void clearTempRows();

  /** If row has any column. */
  abstract boolean hasColumns();

  /** Reset the variables after each flush. Note that the caller needs to handle synchronization. */
  void reset() {
    this.rowCount = 0;
    this.bufferSize = 0F;
    this.statsMap.replaceAll(
        (key, value) -> new RowBufferStats(value.getCollationDefinitionString()));
  }

  /** Get buffered data snapshot for later flushing. */
  abstract Optional<T> getSnapshot();

  @VisibleForTesting
  abstract Object getVectorValueAt(String column, int index);

  @VisibleForTesting
  abstract int getTempRowCount();

  abstract void closeInternal();

  /**
   * Close the row buffer and release resources. Note that the caller needs to handle
   * synchronization
   */
  @Override
  public void close(String name) {
    long allocatedBeforeRelease = this.allocator.getAllocatedMemory();

    closeInternal();

    long allocatedAfterRelease = this.allocator.getAllocatedMemory();
    logger.logInfo(
        "Trying to close {} for channel={} from function={}, allocatedBeforeRelease={},"
            + " allocatedAfterRelease={}",
        this.getClass().getSimpleName(),
        channelFullyQualifiedName,
        name,
        allocatedBeforeRelease,
        allocatedAfterRelease);
    Utils.closeAllocator(this.allocator);

    // If the channel is valid but still has leftover data, throw an exception because it should be
    // cleaned up already before calling close
    if (allocatedBeforeRelease > 0 && isValid()) {
      throw new SFException(
          ErrorCode.INTERNAL_ERROR,
          String.format(
              "Memory leaked=%d by allocator=%s, channel=%s",
              allocatedBeforeRelease, this.allocator, channelFullyQualifiedName));
    }
  }

  /** Normalize the column name, given with the inserted row, to send to server side. */
  static String formatColumnName(String columnName) {
    Utils.assertStringNotNullOrEmpty("invalid column name", columnName);
    return (columnName.charAt(0) == '"' && columnName.charAt(columnName.length() - 1) == '"')
        ? columnName
        : columnName.toUpperCase();
  }

  /**
   * Given a set of col names to stats, build the right ep info
   *
   * @param rowCount: count of rows in the given arrow buffer
   * @param colStats: map of column name to RowBufferStats
   * @return the EPs built from column stats
   */
  static EpInfo buildEpInfoFromStats(long rowCount, Map<String, RowBufferStats> colStats) {
    EpInfo epInfo = new EpInfo(rowCount, new HashMap<>());
    for (Map.Entry<String, RowBufferStats> colStat : colStats.entrySet()) {
      RowBufferStats stat = colStat.getValue();
      FileColumnProperties dto = new FileColumnProperties(stat);

      String colName = colStat.getKey();
      epInfo.getColumnEps().put(colName, dto);
    }
    return epInfo;
  }

  /** Row buffer factory. */
  static <T> AbstractRowBuffer<T> createRowBuffer(
      BufferConfig bufferConfig, boolean bufferForTests) {
    // TODO: The circular dependency SnowflakeStreamingIngestChannelInternal <-> RowBuffer
    // (SNOW-657667)
    // can be probably reconsidered
    switch (bufferConfig.bdecVersion) {
      case ONE:
        //noinspection unchecked
        return (AbstractRowBuffer<T>) new ArrowRowBuffer(bufferConfig);
      case THREE:
        //noinspection unchecked
        return (AbstractRowBuffer<T>) new ParquetRowBuffer(bufferConfig, bufferForTests);
      default:
        throw new SFException(
            ErrorCode.INTERNAL_ERROR,
            "Unsupported BDEC format version: " + bufferConfig.bdecVersion);
    }
  }

  static class BufferConfig {
    final BufferAllocator allocator;
    final Constants.BdecVersion bdecVersion;
    final OpenChannelRequest.OnErrorOption onErrorOption;
    final String fullyQualifiedName;
    final String offsetToken;
    final Long rowSequencer;
    final Consumer<Float> rowSizeMetric;

    BufferConfig(
        BufferAllocator allocator,
        Constants.BdecVersion bdecVersion,
        OpenChannelRequest.OnErrorOption onErrorOption,
        String fullyQualifiedName,
        String offsetToken,
        Long rowSequencer,
        Consumer<Float> rowSizeMetric) {
      this.allocator = allocator;
      this.bdecVersion = bdecVersion;
      this.onErrorOption = onErrorOption;
      this.fullyQualifiedName = fullyQualifiedName;
      this.offsetToken = offsetToken;
      this.rowSequencer = rowSequencer;
      this.rowSizeMetric = rowSizeMetric;
    }
  }
}
