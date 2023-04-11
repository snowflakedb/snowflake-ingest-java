/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import net.snowflake.client.jdbc.internal.google.common.collect.Sets;
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
class ArrowRowBuffer extends AbstractRowBuffer<VectorSchemaRoot> {
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

  /** Construct a ArrowRowBuffer object. */
  ArrowRowBuffer(
      OpenChannelRequest.OnErrorOption onErrorOption,
      ZoneId defaultTimezone,
      BufferAllocator allocator,
      String fullyQualifiedChannelName,
      Consumer<Float> rowSizeMetric,
      ChannelRuntimeState channelState) {
    super(
        onErrorOption,
        defaultTimezone,
        allocator,
        fullyQualifiedChannelName,
        rowSizeMetric,
        channelState);
    this.fields = new HashMap<>();
  }

  /**
   * Setup the column fields and vectors using the column metadata from the server
   *
   * @param columns list of column metadata
   */
  @Override
  public void setupSchema(List<ColumnMetadata> columns) {
    List<FieldVector> vectors = new ArrayList<>();
    List<FieldVector> tempVectors = new ArrayList<>();

    for (ColumnMetadata column : columns) {
      validateColumnCollation(column);
      Field field = buildField(column);
      FieldVector vector = field.createVector(this.allocator);
      if (!field.isNullable()) {
        addNonNullableFieldName(field.getName());
      }
      this.fields.put(column.getInternalName(), field);
      vectors.add(vector);
      this.statsMap.put(
          column.getInternalName(), new RowBufferStats(column.getName(), column.getCollation()));

      if (onErrorOption == OpenChannelRequest.OnErrorOption.ABORT) {
        FieldVector tempVector = field.createVector(this.allocator);
        tempVectors.add(tempVector);
        this.tempStatsMap.put(
            column.getInternalName(), new RowBufferStats(column.getName(), column.getCollation()));
      }
    }

    this.vectorsRoot = new VectorSchemaRoot(vectors);
    this.tempVectorsRoot = new VectorSchemaRoot(tempVectors);
  }

  /** Close the row buffer by releasing its internal resources. */
  @Override
  public void closeInternal() {
    if (this.vectorsRoot != null) {
      this.vectorsRoot.close();
      this.tempVectorsRoot.close();
    }
    this.fields.clear();
  }

  /** Reset the variables after each flush. Note that the caller needs to handle synchronization */
  @Override
  void reset() {
    super.reset();
    this.vectorsRoot.clear();
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
    return new Field(column.getInternalName(), fieldType, children);
  }

  @Override
  void moveTempRowsToActualBuffer(int tempRowCount) {
    // If all the rows are inserted successfully, transfer the rows from temp vectors to
    // the final vectors and update the row size and row count
    // TODO: switch to VectorSchemaRootAppender once it works for all vector types
    for (Field field : fields.values()) {
      FieldVector from = this.tempVectorsRoot.getVector(field);
      FieldVector to = this.vectorsRoot.getVector(field);
      for (int rowIdx = 0; rowIdx < tempRowCount; rowIdx++) {
        to.copyFromSafe(rowIdx, this.bufferedRowCount + rowIdx, from);
      }
    }
  }

  @Override
  void clearTempRows() {
    tempVectorsRoot.clear();
  }

  @Override
  boolean hasColumns() {
    return !fields.isEmpty();
  }

  @Override
  Optional<VectorSchemaRoot> getSnapshot(final String filePath) {
    List<FieldVector> oldVectors = new ArrayList<>();
    for (FieldVector vector : this.vectorsRoot.getFieldVectors()) {
      vector.setValueCount(this.bufferedRowCount);
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
                vector.getField().isNullable(), arrowType, null, vector.getField().getMetadata());
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
    VectorSchemaRoot root = new VectorSchemaRoot(oldVectors);
    root.setRowCount(this.bufferedRowCount);
    return oldVectors.isEmpty() ? Optional.empty() : Optional.of(root);
  }

  @Override
  boolean hasColumn(String name) {
    return this.fields.get(name) != null;
  }

  @Override
  float addRow(
      Map<String, Object> row,
      int bufferedRowIndex,
      Map<String, RowBufferStats> statsMap,
      Set<String> formattedInputColumnNames,
      final long insertRowIndex) {
    return convertRowToArrow(
        row, vectorsRoot, bufferedRowIndex, statsMap, formattedInputColumnNames, insertRowIndex);
  }

  @Override
  float addTempRow(
      Map<String, Object> row,
      int curRowIndex,
      Map<String, RowBufferStats> statsMap,
      Set<String> formattedInputColumnNames,
      long insertRowIndex) {
    return convertRowToArrow(
        row, tempVectorsRoot, curRowIndex, statsMap, formattedInputColumnNames, insertRowIndex);
  }

  /**
   * Convert the input row to the correct Arrow format
   *
   * @param row input row
   * @param sourceVectors vectors (buffers) that hold the row
   * @param bufferedRowIndex Buffered row index. This is not the same as the input row index
   * @param statsMap column stats map
   * @param inputColumnNames list of input column names after formatting
   * @param insertRowsCurrIndex Row index of the input Rows passed in {@link
   *     net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel#insertRows(Iterable,
   *     String)}
   * @return row size
   */
  private float convertRowToArrow(
      Map<String, Object> row,
      VectorSchemaRoot sourceVectors,
      int bufferedRowIndex,
      Map<String, RowBufferStats> statsMap,
      Set<String> inputColumnNames,
      long insertRowsCurrIndex) {
    // Insert values to the corresponding arrow buffers
    float rowBufferSize = 0F;
    // Create new empty stats just for the current row.
    Map<String, RowBufferStats> forkedStatsMap = new HashMap<>();

    for (Map.Entry<String, Object> entry : row.entrySet()) {
      rowBufferSize += 0.125; // 1/8 for null value bitmap
      String columnName = LiteralQuoteUtils.unquoteColumnName(entry.getKey());
      Object value = entry.getValue();
      Field field = this.fields.get(columnName);
      Utils.assertNotNull("Arrow column field", field);
      FieldVector vector = sourceVectors.getVector(field);
      Utils.assertNotNull("Arrow column vector", vector);
      RowBufferStats forkedStats = statsMap.get(columnName).forkEmpty();
      forkedStatsMap.put(columnName, forkedStats);
      Utils.assertNotNull("Arrow column stats", forkedStats);
      ColumnLogicalType logicalType =
          ColumnLogicalType.valueOf(field.getMetadata().get(COLUMN_LOGICAL_TYPE));
      ColumnPhysicalType physicalType =
          ColumnPhysicalType.valueOf(field.getMetadata().get(COLUMN_PHYSICAL_TYPE));

      boolean isParsedValueNull = false;
      if (value != null) {
        switch (logicalType) {
          case FIXED:
            int columnPrecision = Integer.parseInt(field.getMetadata().get(COLUMN_PRECISION));
            int columnScale = getColumnScale(field.getMetadata());
            BigDecimal inputAsBigDecimal =
                DataValidationUtil.validateAndParseBigDecimal(
                    forkedStats.getColumnDisplayName(), value, insertRowsCurrIndex);
            // vector.setSafe requires the BigDecimal input scale explicitly match its scale
            inputAsBigDecimal = inputAsBigDecimal.setScale(columnScale, RoundingMode.HALF_UP);

            DataValidationUtil.checkValueInRange(
                inputAsBigDecimal, columnScale, columnPrecision, insertRowsCurrIndex);

            if (columnScale != 0 || physicalType == ColumnPhysicalType.SB16) {
              ((DecimalVector) vector).setSafe(bufferedRowIndex, inputAsBigDecimal);
              forkedStats.addIntValue(inputAsBigDecimal.unscaledValue());
              rowBufferSize += 16;
            } else {
              switch (physicalType) {
                case SB1:
                  ((TinyIntVector) vector)
                      .setSafe(bufferedRowIndex, inputAsBigDecimal.byteValueExact());
                  forkedStats.addIntValue(inputAsBigDecimal.toBigInteger());
                  rowBufferSize += 1;
                  break;
                case SB2:
                  ((SmallIntVector) vector)
                      .setSafe(bufferedRowIndex, inputAsBigDecimal.shortValueExact());
                  forkedStats.addIntValue(inputAsBigDecimal.toBigInteger());
                  rowBufferSize += 2;
                  break;
                case SB4:
                  ((IntVector) vector).setSafe(bufferedRowIndex, inputAsBigDecimal.intValueExact());
                  forkedStats.addIntValue(inputAsBigDecimal.toBigInteger());
                  rowBufferSize += 4;
                  break;
                case SB8:
                  ((BigIntVector) vector)
                      .setSafe(bufferedRowIndex, inputAsBigDecimal.longValueExact());
                  forkedStats.addIntValue(inputAsBigDecimal.toBigInteger());
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
                      forkedStats.getColumnDisplayName(),
                      value,
                      Optional.ofNullable(maxLengthString).map(Integer::parseInt),
                      insertRowsCurrIndex);
              Text text = new Text(str);
              ((VarCharVector) vector).setSafe(bufferedRowIndex, text);
              forkedStats.addStrValue(str);
              rowBufferSize += text.getBytes().length;
              break;
            }
          case OBJECT:
            {
              String str =
                  DataValidationUtil.validateAndParseObject(
                      forkedStats.getColumnDisplayName(), value, insertRowsCurrIndex);
              Text text = new Text(str);
              ((VarCharVector) vector).setSafe(bufferedRowIndex, text);
              rowBufferSize += text.getBytes().length;
              break;
            }
          case ARRAY:
            {
              String str =
                  DataValidationUtil.validateAndParseArray(
                      forkedStats.getColumnDisplayName(), value, insertRowsCurrIndex);
              Text text = new Text(str);
              ((VarCharVector) vector).setSafe(bufferedRowIndex, text);
              rowBufferSize += text.getBytes().length;
              break;
            }
          case VARIANT:
            {
              String str =
                  DataValidationUtil.validateAndParseVariant(
                      forkedStats.getColumnDisplayName(), value, insertRowsCurrIndex);
              if (str != null) {
                Text text = new Text(str);
                ((VarCharVector) vector).setSafe(bufferedRowIndex, text);
                rowBufferSize += text.getBytes().length;
              } else {
                isParsedValueNull = true;
              }
              break;
            }
          case TIMESTAMP_LTZ:
          case TIMESTAMP_NTZ:
            boolean trimTimezone = logicalType == ColumnLogicalType.TIMESTAMP_NTZ;

            switch (physicalType) {
              case SB8:
                {
                  BigIntVector bigIntVector = (BigIntVector) vector;
                  TimestampWrapper timestampWrapper =
                      DataValidationUtil.validateAndParseTimestamp(
                          forkedStats.getColumnDisplayName(),
                          value,
                          getColumnScale(field.getMetadata()),
                          defaultTimezone,
                          trimTimezone,
                          insertRowsCurrIndex);
                  BigInteger timestampBinary = timestampWrapper.toBinary(false);
                  bigIntVector.setSafe(bufferedRowIndex, timestampBinary.longValue());
                  forkedStats.addIntValue(timestampBinary);
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
                  structVector.setIndexDefined(bufferedRowIndex);

                  TimestampWrapper timestampWrapper =
                      DataValidationUtil.validateAndParseTimestamp(
                          forkedStats.getColumnDisplayName(),
                          value,
                          getColumnScale(field.getMetadata()),
                          defaultTimezone,
                          trimTimezone,
                          insertRowsCurrIndex);
                  epochVector.setSafe(bufferedRowIndex, timestampWrapper.getEpoch());
                  fractionVector.setSafe(bufferedRowIndex, timestampWrapper.getFraction());
                  rowBufferSize += 12;
                  forkedStats.addIntValue(timestampWrapper.toBinary(false));
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
                  structVector.setIndexDefined(bufferedRowIndex);

                  TimestampWrapper timestampWrapper =
                      DataValidationUtil.validateAndParseTimestamp(
                          forkedStats.getColumnDisplayName(),
                          value,
                          getColumnScale(field.getMetadata()),
                          defaultTimezone,
                          false,
                          insertRowsCurrIndex);
                  epochVector.setSafe(
                      bufferedRowIndex, timestampWrapper.toBinary(false).longValueExact());
                  timezoneVector.setSafe(bufferedRowIndex, timestampWrapper.getTimeZoneIndex());
                  rowBufferSize += 12;
                  forkedStats.addIntValue(timestampWrapper.toBinary(true));
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
                  structVector.setIndexDefined(bufferedRowIndex);

                  TimestampWrapper timestampWrapper =
                      DataValidationUtil.validateAndParseTimestamp(
                          forkedStats.getColumnDisplayName(),
                          value,
                          getColumnScale(field.getMetadata()),
                          defaultTimezone,
                          false,
                          insertRowsCurrIndex);
                  epochVector.setSafe(bufferedRowIndex, timestampWrapper.getEpoch());
                  fractionVector.setSafe(bufferedRowIndex, timestampWrapper.getFraction());
                  timezoneVector.setSafe(bufferedRowIndex, timestampWrapper.getTimeZoneIndex());
                  rowBufferSize += 16;
                  BigInteger timeInBinary = timestampWrapper.toBinary(true);
                  forkedStats.addIntValue(timeInBinary);
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
              int intValue =
                  DataValidationUtil.validateAndParseDate(
                      forkedStats.getColumnDisplayName(), value, insertRowsCurrIndex);
              dateDayVector.setSafe(bufferedRowIndex, intValue);
              forkedStats.addIntValue(BigInteger.valueOf(intValue));
              rowBufferSize += 4;
              break;
            }
          case TIME:
            switch (physicalType) {
              case SB4:
                {
                  BigInteger timeInScale =
                      DataValidationUtil.validateAndParseTime(
                          forkedStats.getColumnDisplayName(),
                          value,
                          getColumnScale(field.getMetadata()),
                          insertRowsCurrIndex);
                  ((IntVector) vector).setSafe(bufferedRowIndex, timeInScale.intValue());
                  forkedStats.addIntValue(timeInScale);
                  rowBufferSize += 4;
                  break;
                }
              case SB8:
                {
                  BigInteger timeInScale =
                      DataValidationUtil.validateAndParseTime(
                          forkedStats.getColumnDisplayName(),
                          value,
                          getColumnScale(field.getMetadata()),
                          insertRowsCurrIndex);
                  ((BigIntVector) vector).setSafe(bufferedRowIndex, timeInScale.longValue());
                  forkedStats.addIntValue(timeInScale);
                  rowBufferSize += 8;
                  break;
                }
              default:
                throw new SFException(ErrorCode.UNKNOWN_DATA_TYPE, logicalType, physicalType);
            }
            break;
          case BOOLEAN:
            {
              int intValue =
                  DataValidationUtil.validateAndParseBoolean(
                      forkedStats.getColumnDisplayName(), value, insertRowsCurrIndex);
              ((BitVector) vector).setSafe(bufferedRowIndex, intValue);
              rowBufferSize += 0.125;
              forkedStats.addIntValue(BigInteger.valueOf(intValue));
              break;
            }
          case BINARY:
            String maxLengthString = field.getMetadata().get(COLUMN_BYTE_LENGTH);
            byte[] bytes =
                DataValidationUtil.validateAndParseBinary(
                    forkedStats.getColumnDisplayName(),
                    value,
                    Optional.ofNullable(maxLengthString).map(Integer::parseInt),
                    insertRowsCurrIndex);
            ((VarBinaryVector) vector).setSafe(bufferedRowIndex, bytes);
            forkedStats.addBinaryValue(bytes);
            rowBufferSize += bytes.length;
            break;
          case REAL:
            double doubleValue =
                DataValidationUtil.validateAndParseReal(
                    forkedStats.getColumnDisplayName(), value, insertRowsCurrIndex);
            ((Float8Vector) vector).setSafe(bufferedRowIndex, doubleValue);
            forkedStats.addRealValue(doubleValue);
            rowBufferSize += 8;
            break;
          default:
            throw new SFException(ErrorCode.UNKNOWN_DATA_TYPE, logicalType, physicalType);
        }
      }

      if (value == null || isParsedValueNull) {
        if (!field.getFieldType().isNullable()) {
          throw new SFException(
              ErrorCode.INVALID_FORMAT_ROW, columnName, "Passed null to non nullable field");
        } else {
          insertNull(vector, forkedStats, bufferedRowIndex);
        }
      }
    }

    // All input values passed validation, iterate over the columns again and combine their existing
    // statistics with the forked statistics for the current row.
    for (Map.Entry<String, RowBufferStats> forkedColStats : forkedStatsMap.entrySet()) {
      String columnName = forkedColStats.getKey();
      statsMap.put(
          columnName,
          RowBufferStats.getCombinedStats(statsMap.get(columnName), forkedColStats.getValue()));
    }

    // Insert nulls to the columns that doesn't show up in the input
    for (String columnName : Sets.difference(this.fields.keySet(), inputColumnNames)) {
      rowBufferSize += 0.125; // 1/8 for null value bitmap
      insertNull(
          sourceVectors.getVector(this.fields.get(columnName)),
          statsMap.get(columnName),
          bufferedRowIndex);
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

  private int getColumnScale(Map<String, String> metadata) {
    return Integer.parseInt(metadata.get(ArrowRowBuffer.COLUMN_SCALE));
  }

  @Override
  public Flusher<VectorSchemaRoot> createFlusher() {
    return new ArrowFlusher();
  }

  @VisibleForTesting
  @Override
  Object getVectorValueAt(String column, int index) {
    Object value = vectorsRoot.getVector(column).getObject(index);
    return (value instanceof Text) ? new String(((Text) value).getBytes()) : value;
  }

  @VisibleForTesting
  int getTempRowCount() {
    return tempVectorsRoot.getRowCount();
  }
}
