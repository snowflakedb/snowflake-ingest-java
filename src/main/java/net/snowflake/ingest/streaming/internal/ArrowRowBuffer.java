/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import net.snowflake.client.jdbc.internal.google.common.collect.Sets;
import net.snowflake.client.jdbc.internal.snowflake.common.util.Power10;
import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.Logging;
import net.snowflake.ingest.utils.SFException;
import net.snowflake.ingest.utils.Utils;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.Text;
import org.apache.arrow.vector.util.TransferPair;

/**
 * The buffer in the Streaming Ingest channel that holds the un-flushed rows, these rows will be
 * converted to Arrow format for faster processing
 */
class ArrowRowBuffer {

  // Snowflake table column logical type
  private static enum ColumnLogicalType {
    ANY,
    BOOLEAN,
    ROWINDEX,
    NULL,
    REAL,
    FIXED,
    TEXT,
    CHAR,
    BINARY,
    DATE,
    TIME,
    TIMESTAMP_LTZ,
    TIMESTAMP_NTZ,
    TIMESTAMP_TZ,
    INTERVAL,
    RAW,
    ARRAY,
    OBJECT,
    VARIANT,
    ROW,
    SEQUENCE,
    FUNCTION,
    USER_DEFINED_TYPE,
  }

  // Snowflake table column physical type
  private static enum ColumnPhysicalType {
    ROWINDEX,
    DOUBLE,
    SB1,
    SB2,
    SB4,
    SB8,
    SB16,
    LOB,
    BINARY,
    ROW,
  }

  private static final Logging logger = new Logging(ArrowRowBuffer.class);

  // Constants for column fields
  private static final String FIELD_EPOCH_IN_SECONDS = "epoch"; // seconds since epoch
  private static final String FIELD_TIME_ZONE = "timezone"; // time zone index
  private static final String FIELD_FRACTION_IN_NANOSECONDS = "fraction"; // fraction in nanoseconds

  // Column metadata that will send back to server as part of the blob, and will be used by the
  // Arrow reader
  private static final String COLUMN_PHYSICAL_TYPE = "physicalType";
  private static final String COLUMN_LOGICAL_TYPE = "logicalType";
  private static final String COLUMN_NULLABLE = "nullable";
  static final String COLUMN_SCALE = "scale";
  private static final String COLUMN_PRECISION = "precision";
  private static final String COLUMN_CHAR_LENGTH = "charLength";
  private static final String COLUMN_BYTE_LENGTH = "byteLength";
  @VisibleForTesting static final int DECIMAL_BIT_WIDTH = 128;

  // Holder for a set of the Arrow vectors (buffers)
  @VisibleForTesting VectorSchemaRoot vectorsRoot;

  // For ABORT on_error option, temp vectors are needed to temporarily holding the rows until
  // they're all validated, then the rows will be transferred to the final VectorSchemaRoot
  @VisibleForTesting VectorSchemaRoot tempVectorsRoot;

  // Map the column name to Arrow column field
  private final Map<String, Field> fields;

  // Map the column name to the stats
  @VisibleForTesting Map<String, RowBufferStats> statsMap;

  // Temp stats map to use until all the rows are validated
  @VisibleForTesting Map<String, RowBufferStats> tempStatsMap;

  // Lock used to protect the buffers from concurrent read/write
  private final Lock flushLock;

  // Current row count
  @VisibleForTesting volatile int rowCount;

  // Allocator used to allocate the buffers
  private final BufferAllocator allocator;

  // Current buffer size
  private volatile float bufferSize;

  // Reference to the Streaming Ingest channel that owns this buffer
  @VisibleForTesting final SnowflakeStreamingIngestChannelInternal owningChannel;

  // Names of non-nullable columns
  private final Set<String> nonNullableFieldNames;

  /**
   * Given a set of col names to stats, build the right ep info
   *
   * @param rowCount: count of rows in the given arrow buffer
   * @param colStats: map of column name to RowBufferStats
   * @return
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

  /**
   * Construct a ArrowRowBuffer object
   *
   * @param channel
   */
  ArrowRowBuffer(SnowflakeStreamingIngestChannelInternal channel) {
    this.owningChannel = channel;
    this.allocator = channel.getAllocator();
    this.fields = new HashMap<>();
    this.nonNullableFieldNames = new HashSet<>();
    this.flushLock = new ReentrantLock();
    this.rowCount = 0;
    this.bufferSize = 0F;

    // Initialize empty stats
    this.statsMap = new HashMap<>();
    this.tempStatsMap = new HashMap<>();
  }

  /**
   * Setup the column fields and vectors using the column metadata from the server
   *
   * @param columns list of column metadata
   */
  void setupSchema(List<ColumnMetadata> columns) {
    List<FieldVector> vectors = new ArrayList<>();
    List<FieldVector> tempVectors = new ArrayList<>();

    for (ColumnMetadata column : columns) {
      Field field = buildField(column);
      FieldVector vector = field.createVector(this.allocator);
      if (!field.isNullable()) {
        this.nonNullableFieldNames.add(field.getName());
      }
      this.fields.put(column.getName(), field);
      vectors.add(vector);
      this.statsMap.put(column.getName(), new RowBufferStats(column.getCollation()));

      if (this.owningChannel.getOnErrorOption() == OpenChannelRequest.OnErrorOption.ABORT) {
        FieldVector tempVector = field.createVector(this.allocator);
        tempVectors.add(tempVector);
        this.tempStatsMap.put(column.getName(), new RowBufferStats(column.getCollation()));
      }
    }

    this.vectorsRoot = new VectorSchemaRoot(vectors);
    this.tempVectorsRoot = new VectorSchemaRoot(tempVectors);
  }

  /**
   * Close the row buffer and release resources. Note that the caller needs to handle
   * synchronization
   */
  void close() {
    if (this.vectorsRoot != null) {
      this.vectorsRoot.close();
      this.tempVectorsRoot.close();
    }
    this.fields.clear();
    this.allocator.close();
  }

  /** Reset the variables after each flush. Note that the caller needs to handle synchronization */
  void reset() {
    this.vectorsRoot.clear();
    this.rowCount = 0;
    this.bufferSize = 0F;
    this.statsMap.replaceAll(
        (key, value) -> new RowBufferStats(value.getCollationDefinitionString()));
  }

  /**
   * Get the current buffer size
   *
   * @return the current buffer size
   */
  float getSize() {
    return this.bufferSize;
  }

  /**
   * Insert a batch of rows into the row buffer
   *
   * @param rows input row
   * @param offsetToken offset token of the latest row in the batch
   * @return insert response that possibly contains errors because of insertion failures
   */
  InsertValidationResponse insertRows(Iterable<Map<String, Object>> rows, String offsetToken) {
    float rowSize = 0F;
    if (this.fields.isEmpty()) {
      throw new SFException(ErrorCode.INTERNAL_ERROR, "Empty column fields");
    }

    InsertValidationResponse response = new InsertValidationResponse();
    this.flushLock.lock();
    try {
      if (this.owningChannel.getOnErrorOption() == OpenChannelRequest.OnErrorOption.CONTINUE) {
        // Used to map incoming row(nth row) to InsertError(for nth row) in response
        long rowIndex = 0;
        for (Map<String, Object> row : rows) {
          try {
            rowSize += convertRowToArrow(row, this.vectorsRoot, this.rowCount, this.statsMap);
            this.rowCount++;
            this.bufferSize += rowSize;
          } catch (SFException e) {
            response.addError(new InsertValidationResponse.InsertError(row, e, rowIndex));
          } catch (Throwable e) {
            logger.logWarn("Unexpected error happens during insertRows: {}", e.getMessage());
            response.addError(
                new InsertValidationResponse.InsertError(
                    row, new SFException(e, ErrorCode.INTERNAL_ERROR, e.getMessage()), rowIndex));
          }
          rowIndex++;
        }
      } else {
        // If the on_error option is ABORT, simply throw the first exception
        float tempRowSize = 0F;
        int tempRowCount = 0;
        for (Map<String, Object> row : rows) {
          tempRowSize +=
              convertRowToArrow(row, this.tempVectorsRoot, tempRowCount, this.tempStatsMap);
          tempRowCount++;
        }

        // If all the rows are inserted successfully, transfer the rows from temp vectors to
        // the final vectors and update the row size and row count
        // TODO: switch to VectorSchemaRootAppender once it works for all vector types
        for (Field field : fields.values()) {
          FieldVector from = this.tempVectorsRoot.getVector(field);
          FieldVector to = this.vectorsRoot.getVector(field);
          for (int rowIdx = 0; rowIdx < tempRowCount; rowIdx++) {
            to.copyFromSafe(rowIdx, this.rowCount + rowIdx, from);
          }
        }
        rowSize = tempRowSize;
        this.rowCount += tempRowCount;
        this.bufferSize += rowSize;
        this.statsMap.forEach(
            (colName, stats) -> {
              this.statsMap.put(
                  colName, RowBufferStats.getCombinedStats(stats, this.tempStatsMap.get(colName)));
            });
      }

      this.owningChannel.setOffsetToken(offsetToken);
      this.owningChannel.collectRowSize(rowSize);
    } finally {
      this.tempStatsMap.values().forEach(RowBufferStats::reset);
      this.tempVectorsRoot.clear();
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
  ChannelData flush() {
    logger.logDebug("Start get data for channel={}", this.owningChannel.getFullyQualifiedName());
    if (this.rowCount > 0) {
      List<FieldVector> oldVectors = new ArrayList<>();
      int oldRowCount = 0;
      float oldBufferSize = 0F;
      long oldRowSequencer = 0;
      String oldOffsetToken = null;
      Map<String, RowBufferStats> oldColumnEps = null;

      logger.logDebug(
          "Arrow buffer flush about to take lock on channel={}",
          this.owningChannel.getFullyQualifiedName());

      this.flushLock.lock();
      try {
        if (this.rowCount > 0) {
          // Transfer the ownership of the vectors
          for (FieldVector vector : this.vectorsRoot.getFieldVectors()) {
            vector.setValueCount(this.rowCount);
            if (vector instanceof DecimalVector) {
              // DecimalVectors do not transfer FieldType metadata when using
              // vector.getTransferPair. We need to explicitly create the new vector to transfer to
              // in order to keep the metadata.
              ArrowType arrowType =
                  new ArrowType.Decimal(
                      ((DecimalVector) vector).getPrecision(),
                      ((DecimalVector) vector).getScale(),
                      DECIMAL_BIT_WIDTH);
              FieldType fieldType =
                  new FieldType(
                      vector.getField().isNullable(),
                      arrowType,
                      null,
                      vector.getField().getMetadata());
              Field f = new Field(vector.getName(), fieldType, null);
              DecimalVector newVector = new DecimalVector(f, this.allocator);
              TransferPair t = vector.makeTransferPair(newVector);
              t.transfer();
              oldVectors.add((FieldVector) t.getTo());
            } else {
              TransferPair t = vector.getTransferPair(this.allocator);
              t.transfer();
              oldVectors.add((FieldVector) t.getTo());
            }
          }

          oldRowCount = this.rowCount;
          oldBufferSize = this.bufferSize;
          oldRowSequencer = this.owningChannel.incrementAndGetRowSequencer();
          oldOffsetToken = this.owningChannel.getOffsetToken();
          oldColumnEps = new HashMap<>(this.statsMap);
          // Reset everything in the buffer once we save all the info
          reset();
        }
      } finally {
        this.flushLock.unlock();
      }

      logger.logDebug(
          "Arrow buffer flush released lock on channel={}, rowCount={}, bufferSize={}",
          this.owningChannel.getFullyQualifiedName(),
          rowCount,
          bufferSize);

      if (!oldVectors.isEmpty()) {
        ChannelData data = new ChannelData();
        VectorSchemaRoot vectors = new VectorSchemaRoot(oldVectors);
        vectors.setRowCount(oldRowCount);
        data.setVectors(vectors);
        data.setBufferSize(oldBufferSize);
        data.setChannel(this.owningChannel);
        data.setRowSequencer(oldRowSequencer);
        data.setOffsetToken(oldOffsetToken);
        data.setColumnEps(oldColumnEps);

        return data;
      }
    }
    return null;
  }

  /**
   * Build the column field from the column metadata
   *
   * @param column column metadata
   * @return Column field object
   */
  Field buildField(ColumnMetadata column) {
    ArrowType arrowType;
    FieldType fieldType;
    List<Field> children = null;

    // Put info into the metadata, which will be used by the Arrow reader later
    Map<String, String> metadata = new HashMap<>();
    metadata.put(COLUMN_LOGICAL_TYPE, column.getLogicalType());
    metadata.put(COLUMN_PHYSICAL_TYPE, column.getPhysicalType());
    metadata.put(COLUMN_NULLABLE, String.valueOf(column.getNullable()));

    ColumnPhysicalType physicalType;
    ColumnLogicalType logicalType;
    try {
      physicalType = ColumnPhysicalType.valueOf(column.getPhysicalType());
      logicalType = ColumnLogicalType.valueOf(column.getLogicalType());
    } catch (IllegalArgumentException e) {
      throw new SFException(
          ErrorCode.UNKNOWN_DATA_TYPE, column.getLogicalType(), column.getPhysicalType());
    }

    if (column.getPrecision() != null) {
      metadata.put(COLUMN_PRECISION, column.getPrecision().toString());
    }
    if (column.getScale() != null) {
      metadata.put(COLUMN_SCALE, column.getScale().toString());
    }
    if (column.getByteLength() != null) {
      metadata.put(COLUMN_BYTE_LENGTH, column.getByteLength().toString());
    }
    if (column.getLength() != null) {
      metadata.put(COLUMN_CHAR_LENGTH, column.getLength().toString());
    }

    // Handle differently depends on the column logical and physical types
    switch (logicalType) {
      case FIXED:
        if ((column.getScale() != null && column.getScale() != 0)
            || physicalType == ColumnPhysicalType.SB16) {
          arrowType =
              new ArrowType.Decimal(column.getPrecision(), column.getScale(), DECIMAL_BIT_WIDTH);
        } else {
          switch (physicalType) {
            case SB1:
              arrowType = Types.MinorType.TINYINT.getType();
              break;
            case SB2:
              arrowType = Types.MinorType.SMALLINT.getType();
              break;
            case SB4:
              arrowType = Types.MinorType.INT.getType();
              break;
            case SB8:
              arrowType = Types.MinorType.BIGINT.getType();
              break;
            default:
              throw new SFException(
                  ErrorCode.UNKNOWN_DATA_TYPE, column.getLogicalType(), column.getPhysicalType());
          }
        }
        break;
      case ANY:
      case ARRAY:
      case CHAR:
      case TEXT:
      case OBJECT:
      case VARIANT:
        arrowType = Types.MinorType.VARCHAR.getType();
        break;
      case TIMESTAMP_LTZ:
      case TIMESTAMP_NTZ:
        switch (physicalType) {
          case SB8:
            arrowType = Types.MinorType.BIGINT.getType();
            break;
          case SB16:
            {
              arrowType = Types.MinorType.STRUCT.getType();
              FieldType fieldTypeEpoch =
                  new FieldType(true, Types.MinorType.BIGINT.getType(), null, metadata);
              FieldType fieldTypeFraction =
                  new FieldType(true, Types.MinorType.INT.getType(), null, metadata);
              Field fieldEpoch = new Field(FIELD_EPOCH_IN_SECONDS, fieldTypeEpoch, null);
              Field fieldFraction =
                  new Field(FIELD_FRACTION_IN_NANOSECONDS, fieldTypeFraction, null);
              children = new ArrayList<>();
              children.add(fieldEpoch);
              children.add(fieldFraction);
              break;
            }
          default:
            throw new SFException(
                ErrorCode.UNKNOWN_DATA_TYPE, column.getLogicalType(), column.getPhysicalType());
        }
        break;
      case TIMESTAMP_TZ:
        switch (physicalType) {
          case SB8:
            {
              arrowType = Types.MinorType.STRUCT.getType();
              FieldType fieldTypeEpoch =
                  new FieldType(true, Types.MinorType.BIGINT.getType(), null, metadata);
              FieldType fieldTypeTimezone =
                  new FieldType(true, Types.MinorType.INT.getType(), null, metadata);
              Field fieldEpoch = new Field(FIELD_EPOCH_IN_SECONDS, fieldTypeEpoch, null);
              Field fieldTimezone = new Field(FIELD_TIME_ZONE, fieldTypeTimezone, null);

              children = new ArrayList<>();
              children.add(fieldEpoch);
              children.add(fieldTimezone);
              break;
            }
          case SB16:
            {
              arrowType = Types.MinorType.STRUCT.getType();
              FieldType fieldTypeEpoch =
                  new FieldType(true, Types.MinorType.BIGINT.getType(), null, metadata);
              FieldType fieldTypeFraction =
                  new FieldType(true, Types.MinorType.INT.getType(), null, metadata);
              FieldType fieldTypeTimezone =
                  new FieldType(true, Types.MinorType.INT.getType(), null, metadata);
              Field fieldEpoch = new Field(FIELD_EPOCH_IN_SECONDS, fieldTypeEpoch, null);
              Field fieldFraction =
                  new Field(FIELD_FRACTION_IN_NANOSECONDS, fieldTypeFraction, null);
              Field fieldTimezone = new Field(FIELD_TIME_ZONE, fieldTypeTimezone, null);

              children = new ArrayList<>();
              children.add(fieldEpoch);
              children.add(fieldFraction);
              children.add(fieldTimezone);
              break;
            }
          default:
            throw new SFException(
                ErrorCode.UNKNOWN_DATA_TYPE,
                "Unknown physical type for TIMESTAMP_TZ: " + physicalType);
        }
        break;
      case DATE:
        arrowType = Types.MinorType.DATEDAY.getType();
        break;
      case TIME:
        switch (physicalType) {
          case SB4:
            arrowType = Types.MinorType.INT.getType();
            break;
          case SB8:
            arrowType = Types.MinorType.BIGINT.getType();
            break;
          default:
            throw new SFException(
                ErrorCode.UNKNOWN_DATA_TYPE, column.getLogicalType(), column.getPhysicalType());
        }
        break;
      case BOOLEAN:
        arrowType = Types.MinorType.BIT.getType();
        break;
      case BINARY:
        arrowType = Types.MinorType.VARBINARY.getType();
        break;
      case REAL:
        arrowType = Types.MinorType.FLOAT8.getType();
        break;
      default:
        throw new SFException(
            ErrorCode.UNKNOWN_DATA_TYPE, column.getLogicalType(), column.getPhysicalType());
    }

    // Create the corresponding column field base on the column data type
    fieldType = new FieldType(column.getNullable(), arrowType, null, metadata);
    return new Field(column.getName(), fieldType, children);
  }

  private String formatColumnName(String columnName) {
    Utils.assertStringNotNullOrEmpty("invalid column name", columnName);
    return (columnName.charAt(0) == '"' && columnName.charAt(columnName.length() - 1) == '"')
        ? columnName.substring(1, columnName.length() - 1)
        : columnName.toUpperCase();
  }

  /**
   * Verify that the input row columns are all valid
   *
   * @param row the input row
   * @return the set of input column names
   */
  private Set<String> verifyInputColumns(Map<String, Object> row) {
    Set<String> inputColumns =
        row.keySet().stream().map(this::formatColumnName).collect(Collectors.toSet());

    for (String columnName : this.nonNullableFieldNames) {
      if (!inputColumns.contains(columnName)) {
        throw new SFException(
            ErrorCode.INVALID_ROW,
            "Missing column: " + columnName,
            "Values for all non-nullable columns must be specified.");
      }
    }

    for (String columnName : inputColumns) {
      Field field = this.fields.get(columnName);
      if (field == null) {
        throw new SFException(
            ErrorCode.INVALID_ROW,
            "Extra column: " + columnName,
            "Columns not present in the table shouldn't be specified.");
      }
    }

    return inputColumns;
  }

  /**
   * Convert the input row to the correct Arrow format
   *
   * @param row input row
   * @param sourceVectors vectors (buffers) that hold the row
   * @param curRowIndex current row index to use
   * @param statsMap column stats map
   * @return row size
   */
  private float convertRowToArrow(
      Map<String, Object> row,
      VectorSchemaRoot sourceVectors,
      int curRowIndex,
      Map<String, RowBufferStats> statsMap) {
    // Verify all the input columns are valid
    Set<String> inputColumnNames = verifyInputColumns(row);

    float rowBufferSize = 0F;
    for (Map.Entry<String, Object> entry : row.entrySet()) {
      rowBufferSize += 0.125; // 1/8 for null value bitmap
      String columnName = this.formatColumnName(entry.getKey());
      Object value = entry.getValue();
      Field field = this.fields.get(columnName);
      Utils.assertNotNull("Arrow column field", field);
      FieldVector vector = sourceVectors.getVector(field);
      Utils.assertNotNull("Arrow column vector", vector);
      RowBufferStats stats = statsMap.get(columnName);
      Utils.assertNotNull("Arrow column stats", stats);
      ColumnLogicalType logicalType =
          ColumnLogicalType.valueOf(field.getMetadata().get(COLUMN_LOGICAL_TYPE));
      ColumnPhysicalType physicalType =
          ColumnPhysicalType.valueOf(field.getMetadata().get(COLUMN_PHYSICAL_TYPE));

      if (value == null) {
        if (!field.getFieldType().isNullable()) {
          throw new SFException(
              ErrorCode.INVALID_ROW, columnName, "Passed null to non nullable field");
        }
        insertNull(vector, stats, curRowIndex);
      } else {
        switch (logicalType) {
          case FIXED:
            if (!field.getMetadata().get(COLUMN_SCALE).equals("0")
                || physicalType == ColumnPhysicalType.SB16) {
              int scale =
                  DataValidationUtil.validateAndParseInteger(field.getMetadata().get(COLUMN_SCALE));
              BigDecimal bigDecimalValue = DataValidationUtil.validateAndParseBigDecimal(value);

              // vector.setSafe requires the BigDecimal input scale explicitly match its scale
              bigDecimalValue = bigDecimalValue.setScale(scale);
              ((DecimalVector) vector).setSafe(curRowIndex, bigDecimalValue);
              BigInteger intRep =
                  bigDecimalValue
                      .multiply(BigDecimal.valueOf(Power10.intTable[scale]))
                      .toBigInteger();
              stats.addIntValue(intRep);
              rowBufferSize += 16;
            } else {
              switch (physicalType) {
                case SB1:
                  byte byteValue = DataValidationUtil.validateAndParseByte(value);
                  ((TinyIntVector) vector).setSafe(curRowIndex, byteValue);
                  stats.addIntValue(BigInteger.valueOf(byteValue));
                  rowBufferSize += 1;
                  break;
                case SB2:
                  short shortValue = DataValidationUtil.validateAndParseShort(value);
                  ((SmallIntVector) vector).setSafe(curRowIndex, shortValue);
                  stats.addIntValue(BigInteger.valueOf(shortValue));
                  rowBufferSize += 2;
                  break;
                case SB4:
                  int intVal = DataValidationUtil.validateAndParseInteger(value);
                  ((IntVector) vector).setSafe(curRowIndex, intVal);
                  stats.addIntValue(BigInteger.valueOf(intVal));
                  rowBufferSize += 4;
                  break;
                case SB8:
                  long longValue = DataValidationUtil.validateAndParseLong(value);
                  ((BigIntVector) vector).setSafe(curRowIndex, longValue);
                  stats.addIntValue(BigInteger.valueOf(longValue));
                  rowBufferSize += 8;
                  break;
                default:
                  throw new SFException(ErrorCode.UNKNOWN_DATA_TYPE, logicalType, physicalType);
              }
            }
            break;
          case ANY:
          case CHAR:
          case TEXT:
            {
              String maxLengthString = field.getMetadata().get(COLUMN_CHAR_LENGTH);
              String str =
                  DataValidationUtil.validateAndParseString(
                      value,
                      Optional.ofNullable(maxLengthString)
                          .map(s -> DataValidationUtil.validateAndParseInteger(maxLengthString)));
              Text text = new Text(str);
              ((VarCharVector) vector).setSafe(curRowIndex, text);
              stats.addStrValue(str);
              rowBufferSize += text.getBytes().length;
              break;
            }
          case OBJECT:
            {
              String str = DataValidationUtil.validateAndParseObject(value);
              Text text = new Text(str);
              ((VarCharVector) vector).setSafe(curRowIndex, text);
              rowBufferSize += text.getBytes().length;
              break;
            }
          case ARRAY:
          case VARIANT:
            {
              String str = DataValidationUtil.validateAndParseVariant(value);
              Text text = new Text(str);
              ((VarCharVector) vector).setSafe(curRowIndex, text);
              rowBufferSize += text.getBytes().length;
              break;
            }
          case TIMESTAMP_LTZ:
          case TIMESTAMP_NTZ:
            switch (physicalType) {
              case SB8:
                {
                  BigIntVector bigIntVector = (BigIntVector) vector;
                  BigInteger timeInScale =
                      DataValidationUtil.validateAndParseTime(value, field.getMetadata());
                  bigIntVector.setSafe(curRowIndex, timeInScale.longValue());
                  stats.addIntValue(timeInScale);
                  rowBufferSize += 8;
                  break;
                }
              case SB16:
                {
                  StructVector structVector = (StructVector) vector;
                  BigIntVector epochVector =
                      (BigIntVector) structVector.getChild(FIELD_EPOCH_IN_SECONDS);
                  IntVector fractionVector =
                      (IntVector) structVector.getChild(FIELD_FRACTION_IN_NANOSECONDS);
                  rowBufferSize += 0.25; // for children vector's null value
                  structVector.setIndexDefined(curRowIndex);

                  TimestampWrapper timestampWrapper =
                      DataValidationUtil.validateAndParseTimestampNtzSb16(
                          value, field.getMetadata());
                  epochVector.setSafe(curRowIndex, timestampWrapper.getEpoch());
                  fractionVector.setSafe(curRowIndex, timestampWrapper.getFraction());
                  rowBufferSize += 12;
                  stats.addIntValue(timestampWrapper.getTimeInScale());
                  break;
                }
              default:
                throw new SFException(ErrorCode.UNKNOWN_DATA_TYPE, logicalType, physicalType);
            }
            break;
          case TIMESTAMP_TZ:
            switch (physicalType) {
              case SB8:
                {
                  StructVector structVector = (StructVector) vector;
                  BigIntVector epochVector =
                      (BigIntVector) structVector.getChild(FIELD_EPOCH_IN_SECONDS);
                  IntVector timezoneVector = (IntVector) structVector.getChild(FIELD_TIME_ZONE);

                  rowBufferSize += 0.25; // for children vector's null value
                  structVector.setIndexDefined(curRowIndex);

                  TimestampWrapper timestampWrapper =
                      DataValidationUtil.validateAndParseTimestampTz(value, field.getMetadata());
                  epochVector.setSafe(curRowIndex, timestampWrapper.getTimeInScale().longValue());
                  timezoneVector.setSafe(
                      curRowIndex,
                      timestampWrapper
                          .getTimeZoneIndex()
                          .orElseThrow(
                              () ->
                                  new SFException(
                                      ErrorCode.INVALID_ROW,
                                      value,
                                      "Unable to parse timezone for TIMESTAMP_TZ column")));
                  rowBufferSize += 12;
                  BigInteger timeInBinary =
                      timestampWrapper
                          .getSfTimestamp()
                          .orElseThrow(
                              () ->
                                  new SFException(
                                      ErrorCode.INVALID_ROW,
                                      value,
                                      "Unable to parse timestamp for TIMESTAMP_TZ column"))
                          .toBinary(Integer.parseInt(field.getMetadata().get(COLUMN_SCALE)), true);
                  stats.addIntValue(timeInBinary);
                  break;
                }
              case SB16:
                {
                  StructVector structVector = (StructVector) vector;
                  BigIntVector epochVector =
                      (BigIntVector) structVector.getChild(FIELD_EPOCH_IN_SECONDS);
                  IntVector fractionVector =
                      (IntVector) structVector.getChild(FIELD_FRACTION_IN_NANOSECONDS);
                  IntVector timezoneVector = (IntVector) structVector.getChild(FIELD_TIME_ZONE);

                  rowBufferSize += 0.375; // for children vector's null value
                  structVector.setIndexDefined(curRowIndex);

                  TimestampWrapper timestampWrapper =
                      DataValidationUtil.validateAndParseTimestampTz(value, field.getMetadata());
                  epochVector.setSafe(curRowIndex, timestampWrapper.getEpoch());
                  fractionVector.setSafe(curRowIndex, timestampWrapper.getFraction());
                  timezoneVector.setSafe(
                      curRowIndex,
                      timestampWrapper
                          .getTimeZoneIndex()
                          .orElseThrow(
                              () ->
                                  new SFException(
                                      ErrorCode.INVALID_ROW,
                                      value,
                                      "Unable to parse timezone for TIMESTAMP_TZ column")));
                  rowBufferSize += 16;
                  BigInteger timeInBinary =
                      timestampWrapper
                          .getSfTimestamp()
                          .orElseThrow(
                              () ->
                                  new SFException(
                                      ErrorCode.INVALID_ROW,
                                      value,
                                      "Unable to parse timestamp for TIMESTAMP_TZ column"))
                          .toBinary(Integer.parseInt(field.getMetadata().get(COLUMN_SCALE)), true);
                  stats.addIntValue(timeInBinary);
                  break;
                }
              default:
                throw new SFException(ErrorCode.UNKNOWN_DATA_TYPE, logicalType, physicalType);
            }
            break;
          case DATE:
            {
              DateDayVector dateDayVector = (DateDayVector) vector;
              // Expect days past the epoch
              int intValue = DataValidationUtil.validateAndParseDate(value);
              dateDayVector.setSafe(curRowIndex, intValue);
              stats.addIntValue(BigInteger.valueOf(intValue));
              rowBufferSize += 4;
              break;
            }
          case TIME:
            switch (physicalType) {
              case SB4:
                {
                  BigInteger timeInScale =
                      DataValidationUtil.validateAndParseTime(value, field.getMetadata());
                  stats.addIntValue(timeInScale);
                  ((IntVector) vector).setSafe(curRowIndex, timeInScale.intValue());
                  stats.addIntValue(timeInScale);
                  rowBufferSize += 4;
                  break;
                }
              case SB8:
                {
                  BigInteger timeInScale =
                      DataValidationUtil.validateAndParseTime(value, field.getMetadata());
                  ((BigIntVector) vector).setSafe(curRowIndex, timeInScale.longValue());
                  stats.addIntValue(timeInScale);
                  rowBufferSize += 8;
                  break;
                }
              default:
                throw new SFException(ErrorCode.UNKNOWN_DATA_TYPE, logicalType, physicalType);
            }
            break;
          case BOOLEAN:
            {
              int intValue = DataValidationUtil.validateAndParseBoolean(value);
              ((BitVector) vector).setSafe(curRowIndex, intValue);
              rowBufferSize += 0.125;
              stats.addIntValue(BigInteger.valueOf(intValue));
              break;
            }
          case BINARY:
            byte[] bytes = DataValidationUtil.validateAndParseBinary(value);
            ((VarBinaryVector) vector).setSafe(curRowIndex, bytes);
            stats.addStrValue(new String(bytes, StandardCharsets.UTF_8));
            rowBufferSize += bytes.length;
            break;
          case REAL:
            double doubleValue = DataValidationUtil.validateAndParseReal(value);
            ((Float8Vector) vector).setSafe(curRowIndex, doubleValue);
            stats.addRealValue(doubleValue);
            rowBufferSize += 8;
            break;
          default:
            throw new SFException(ErrorCode.UNKNOWN_DATA_TYPE, logicalType, physicalType);
        }
      }
    }

    // Insert nulls to the columns that doesn't show up in the input
    for (String columnName : Sets.difference(this.fields.keySet(), inputColumnNames)) {
      insertNull(
          sourceVectors.getVector(this.fields.get(columnName)),
          statsMap.get(columnName),
          curRowIndex);
    }

    return rowBufferSize;
  }

  /** Helper function to insert null value to a field vector */
  private void insertNull(FieldVector vector, RowBufferStats stats, int curRowIndex) {
    if (BaseFixedWidthVector.class.isAssignableFrom(vector.getClass())) {
      ((BaseFixedWidthVector) vector).setNull(curRowIndex);
    } else if (BaseVariableWidthVector.class.isAssignableFrom(vector.getClass())) {
      ((BaseVariableWidthVector) vector).setNull(curRowIndex);
    } else if (vector instanceof StructVector) {
      ((StructVector) vector).setNull(curRowIndex);
      ((StructVector) vector)
          .getChildrenFromFields()
          .forEach(
              child -> {
                ((BaseFixedWidthVector) child).setNull(curRowIndex);
              });
    } else {
      throw new SFException(ErrorCode.INTERNAL_ERROR, "Unexpected FieldType");
    }
    stats.incCurrentNullCount();
  }
}
