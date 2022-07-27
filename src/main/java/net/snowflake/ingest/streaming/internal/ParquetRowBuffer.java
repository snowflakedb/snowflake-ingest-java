/*
 * Copyright (c) 2022 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import net.snowflake.client.jdbc.internal.google.common.collect.Sets;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.utils.Constants;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.Logging;
import net.snowflake.ingest.utils.Pair;
import net.snowflake.ingest.utils.SFException;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

/**
 * The buffer in the Streaming Ingest channel that holds the un-flushed rows, these rows will be
 * converted to Parquet format for faster processing
 */
public class ParquetRowBuffer extends AbstractRowBuffer<ParquetChunkData> {
  private static final Logging logger = new Logging(ParquetRowBuffer.class);

  private static final Set<ColumnPhysicalType> TIME_SUPPORTED_PHYSICAL_TYPES =
      new HashSet<>(Arrays.asList(ColumnPhysicalType.SB4, ColumnPhysicalType.SB8));
  private static final Set<ColumnPhysicalType> TIMESTAMP_SUPPORTED_PHYSICAL_TYPES =
      new HashSet<>(Arrays.asList(ColumnPhysicalType.SB8, ColumnPhysicalType.SB16));
  private static final String PARQUET_MESSAGE_TYPE_NAME = "bdec";

  private final Map<String, Pair<ColumnMetadata, Integer>> fieldIndex;
  private final Map<String, String> metadata;
  private final List<List<Object>> data;
  private final List<List<Object>> tempData;

  private MessageType schema;

  /**
   * Construct a ParquetRowBuffer object
   *
   * @param channel client channel
   */
  ParquetRowBuffer(SnowflakeStreamingIngestChannelInternal<ParquetChunkData> channel) {
    super(channel);
    fieldIndex = new HashMap<>();
    metadata = new HashMap<>();
    data = new ArrayList<>();
    tempData = new ArrayList<>();
  }

  @Override
  public void setupSchema(List<ColumnMetadata> columns) {
    fieldIndex.clear();
    metadata.clear();
    List<Type> parquetTypes = new ArrayList<>();
    // Snowflake column id that corresponds to the order in 'columns' received from server
    // id is required to pack column metadata for the server scanner, e.g. decimal scale and
    // precision
    int id = 1;
    for (ColumnMetadata column : columns) {
      Type type = getColumnParquetType(column, id);
      parquetTypes.add(type);
      fieldIndex.put(column.getName(), new Pair<>(column, parquetTypes.size() - 1));
      if (!column.getNullable()) {
        addNonNullableFieldName(column.getName());
      }
      this.statsMap.put(column.getName(), new RowBufferStats(column.getCollation()));

      if (this.owningChannel.getOnErrorOption() == OpenChannelRequest.OnErrorOption.ABORT) {
        this.tempStatsMap.put(column.getName(), new RowBufferStats(column.getCollation()));
      }

      id++;
    }
    schema = new MessageType(PARQUET_MESSAGE_TYPE_NAME, parquetTypes);
  }

  /**
   * Get the column parquet type from the metadata received from server side.
   *
   * @param column column metadata
   * @return column parquet type
   */
  private Type getColumnParquetType(ColumnMetadata column, int id) {
    Type parquetType;
    String name = column.getName();

    ColumnPhysicalType physicalType;
    ColumnLogicalType logicalType;
    try {
      physicalType = ColumnPhysicalType.valueOf(column.getPhysicalType());
      logicalType = ColumnLogicalType.valueOf(column.getLogicalType());
    } catch (IllegalArgumentException e) {
      throw new SFException(
          ErrorCode.UNKNOWN_DATA_TYPE, column.getLogicalType(), column.getPhysicalType());
    }

    this.metadata.put(
        Integer.toString(id), logicalType.getOrdinal() + "," + physicalType.getOrdinal());

    // Parquet Type.Repetition in general supports repeated values for the same row column, like a
    // list of values.
    // This generator uses only
    // either 0 or 1 value for nullable data type (OPTIONAL: 0 or none value if it is null)
    // or exactly 1 value for non-nullable data type (REQUIRED)
    Type.Repetition repetition =
        column.getNullable() ? Type.Repetition.OPTIONAL : Type.Repetition.REQUIRED;

    // Handle differently depends on the column logical and physical types
    switch (logicalType) {
      case FIXED:
        parquetType = getFixedColumnParquetType(column, id, physicalType, repetition);
        break;
      case ARRAY:
      case OBJECT:
      case VARIANT:
        // mark the column metadata as being an object json for the server side scanner
        this.metadata.put(id + ":obj_enc", "1");
        // parquetType is same as the next one
      case ANY:
      case CHAR:
      case TEXT:
      case BINARY:
        parquetType =
            Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, repetition)
                .as(LogicalTypeAnnotation.stringType())
                .id(id)
                .named(name);
        break;
      case TIMESTAMP_LTZ:
      case TIMESTAMP_NTZ:
      case TIMESTAMP_TZ:
        parquetType =
            getTimeColumnParquetType(
                column.getScale(),
                physicalType,
                logicalType,
                TIMESTAMP_SUPPORTED_PHYSICAL_TYPES,
                repetition,
                id,
                name);
        break;
      case DATE:
        parquetType =
            Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition)
                .as(LogicalTypeAnnotation.dateType())
                .id(id)
                .named(name);
        break;
      case TIME:
        parquetType =
            getTimeColumnParquetType(
                column.getScale(),
                physicalType,
                logicalType,
                TIME_SUPPORTED_PHYSICAL_TYPES,
                repetition,
                id,
                name);
        break;
      case BOOLEAN:
        parquetType =
            Types.primitive(PrimitiveType.PrimitiveTypeName.BOOLEAN, repetition).id(id).named(name);
        break;
      case REAL:
        parquetType =
            Types.primitive(PrimitiveType.PrimitiveTypeName.DOUBLE, repetition).id(id).named(name);
        break;
      default:
        throw new SFException(
            ErrorCode.UNKNOWN_DATA_TYPE, column.getLogicalType(), column.getPhysicalType());
    }

    return parquetType;
  }

  /**
   * Get the parquet type for column of Snowflake FIXED logical type.
   *
   * @param column column metadata
   * @param id column id in Snowflake table schema
   * @param physicalType Snowflake physical type of column
   * @param repetition parquet repetition type of column
   * @return column parquet type
   */
  private Type getFixedColumnParquetType(
      ColumnMetadata column, int id, ColumnPhysicalType physicalType, Type.Repetition repetition) {
    String name = column.getName();
    // the LogicalTypeAnnotation.DecimalLogicalTypeAnnotation is used by server side scanner
    // to discover data type scale and precision
    LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalLogicalTypeAnnotation =
        column.getScale() != null && column.getPrecision() != null
            ? LogicalTypeAnnotation.DecimalLogicalTypeAnnotation.decimalType(
                column.getScale(), column.getPrecision())
            : null;
    Type parquetType;
    if ((column.getScale() != null && column.getScale() != 0)
        || physicalType == ColumnPhysicalType.SB16) {
      parquetType =
          Types.primitive(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, repetition)
              .length(16)
              .as(decimalLogicalTypeAnnotation)
              .id(id)
              .named(name);
    } else {
      switch (physicalType) {
        case SB1:
        case SB2:
        case SB4:
          parquetType =
              Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition)
                  .as(decimalLogicalTypeAnnotation)
                  .id(id)
                  .length(4)
                  .named(name);
          break;
        case SB8:
          parquetType =
              Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, repetition)
                  .as(decimalLogicalTypeAnnotation)
                  .id(id)
                  .length(8)
                  .named(name);
          break;
        default:
          throw new SFException(
              ErrorCode.UNKNOWN_DATA_TYPE, column.getLogicalType(), column.getPhysicalType());
      }
    }
    return parquetType;
  }

  /**
   * Get the parquet type for column of a Snowflake time logical type.
   *
   * @param scale column scale
   * @param physicalType Snowflake physical type of column
   * @param logicalType Snowflake logical type of column
   * @param supportedPhysicalTypes supported Snowflake physical types for the given column
   * @param repetition parquet repetition type of column
   * @param id column id in Snowflake table schema
   * @param name column name
   * @return column parquet type
   */
  private static Type getTimeColumnParquetType(
      Integer scale,
      ColumnPhysicalType physicalType,
      ColumnLogicalType logicalType,
      Set<ColumnPhysicalType> supportedPhysicalTypes,
      Type.Repetition repetition,
      int id,
      String name) {
    if (scale == null || scale > 9 || scale < 0 || !supportedPhysicalTypes.contains(physicalType)) {
      throw new SFException(
          ErrorCode.UNKNOWN_DATA_TYPE,
          "Data type: " + logicalType + ", " + physicalType + ", scale: " + scale);
    }
    LogicalTypeAnnotation.TimeUnit timeUnit = getTimeUnitFromScale(scale);

    PrimitiveType.PrimitiveTypeName type = getTimePrimitiveType(physicalType);
    if (physicalType == ColumnPhysicalType.SB16) {
      LogicalTypeAnnotation typeAnnotation = LogicalTypeAnnotation.decimalType(scale, 38);
      return Types.primitive(type, repetition).as(typeAnnotation).length(16).id(id).named(name);
    } else {
      LogicalTypeAnnotation typeAnnotation =
          logicalType == ColumnLogicalType.TIME
              ? LogicalTypeAnnotation.timeType(false, timeUnit)
              : LogicalTypeAnnotation.timestampType(false, timeUnit);
      return Types.primitive(type, repetition).as(typeAnnotation).id(id).named(name);
    }
  }

  /**
   * Get the parquet primitive type name for column of a Snowflake time logical type.
   *
   * @param physicalType Snowflake physical type of column
   * @return column parquet primitive type name
   */
  private static PrimitiveType.PrimitiveTypeName getTimePrimitiveType(
      ColumnPhysicalType physicalType) {
    PrimitiveType.PrimitiveTypeName type;
    switch (physicalType) {
      case SB4:
        type = PrimitiveType.PrimitiveTypeName.INT32;
        break;
      case SB8:
        type = PrimitiveType.PrimitiveTypeName.INT64;
        break;
      case SB16:
        type = PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
        break;
      default:
        throw new UnsupportedOperationException("Time physical type: " + physicalType);
    }
    return type;
  }

  /**
   * Get the parquet time unit for column of a Snowflake time logical type.
   *
   * @param scale Snowflake column scale
   * @return column parquet time unit
   */
  private static LogicalTypeAnnotation.TimeUnit getTimeUnitFromScale(int scale) {
    LogicalTypeAnnotation.TimeUnit timeUnit;
    if (scale <= 3) {
      timeUnit = LogicalTypeAnnotation.TimeUnit.MILLIS;
    } else if (scale <= 6) {
      timeUnit = LogicalTypeAnnotation.TimeUnit.MICROS;
    } else {
      timeUnit = LogicalTypeAnnotation.TimeUnit.NANOS;
    }
    return timeUnit;
  }

  @Override
  boolean hasColumn(String name) {
    return fieldIndex.containsKey(name);
  }

  @Override
  float addRow(
      Map<String, Object> row,
      int curRowIndex,
      Map<String, RowBufferStats> statsMap,
      Set<String> formattedInputColumnNames) {
    return addRow(row, data, statsMap, formattedInputColumnNames);
  }

  @Override
  float addTempRow(
      Map<String, Object> row,
      int curRowIndex,
      Map<String, RowBufferStats> statsMap,
      Set<String> formattedInputColumnNames) {
    return addRow(row, tempData, statsMap, formattedInputColumnNames);
  }

  /**
   * Adds a row to the parquet buffer.
   *
   * @param row row to add
   * @param out internal buffer to add to
   * @param statsMap column stats map
   * @param inputColumnNames list of input column names after formatting
   * @return row size
   */
  private float addRow(
      Map<String, Object> row,
      List<List<Object>> out,
      Map<String, RowBufferStats> statsMap,
      Set<String> inputColumnNames) {
    Object[] indexedRow = new Object[fieldIndex.size()];
    float size = 0F;
    for (Map.Entry<String, Object> entry : row.entrySet()) {
      String key = entry.getKey();
      Object value = entry.getValue();
      String columnName = formatColumnName(key);
      int colIndex = fieldIndex.get(columnName).getSecond();
      RowBufferStats stats = statsMap.get(columnName);
      ColumnMetadata column = fieldIndex.get(columnName).getFirst();
      ColumnDescriptor columnDescriptor = schema.getColumns().get(colIndex);
      PrimitiveType.PrimitiveTypeName typeName =
          columnDescriptor.getPrimitiveType().getPrimitiveTypeName();
      ParquetValueParser.ParquetBufferValue valueWithSize =
          ParquetValueParser.parseColumnValueToParquet(value, column, typeName, stats);
      indexedRow[colIndex] = valueWithSize.getValue();
      size += valueWithSize.getSize();
    }
    out.add(Arrays.asList(indexedRow));

    for (String columnName : Sets.difference(this.fieldIndex.keySet(), inputColumnNames)) {
      statsMap.get(columnName).incCurrentNullCount();
    }
    return size;
  }

  @Override
  void moveTempRowsToActualBuffer(int tempRowCount) {
    data.addAll(tempData);
  }

  @Override
  void clearTempRows() {
    tempData.clear();
  }

  @Override
  boolean hasColumns() {
    return !fieldIndex.isEmpty();
  }

  @Override
  Optional<ParquetChunkData> getSnapshot() {
    List<List<Object>> oldData = new ArrayList<>();
    data.forEach(r -> oldData.add(new ArrayList<>(r)));
    return oldData.isEmpty()
        ? Optional.empty()
        : Optional.of(new ParquetChunkData(oldData, metadata));
  }

  @Override
  Object getVectorValueAt(String column, int index) {
    int colIndex = fieldIndex.get(column).getSecond();
    Object value = data.get(index).get(colIndex);
    ColumnMetadata columnMetadata = fieldIndex.get(column).getFirst();
    String physicalTypeStr = columnMetadata.getPhysicalType();
    ColumnPhysicalType physicalType = ColumnPhysicalType.valueOf(physicalTypeStr);
    String logicalTypeStr = columnMetadata.getLogicalType();
    ColumnLogicalType logicalType = ColumnLogicalType.valueOf(logicalTypeStr);
    if (logicalType == ColumnLogicalType.FIXED) {
      if (physicalType == ColumnPhysicalType.SB1) {
        value = ((Integer) value).byteValue();
      }
      if (physicalType == ColumnPhysicalType.SB2) {
        value = ((Integer) value).shortValue();
      }
      if (physicalType == ColumnPhysicalType.SB16) {
        value = new BigDecimal(new BigInteger((byte[]) value), columnMetadata.getScale());
      }
    }
    if (logicalType == ColumnLogicalType.BINARY && value != null) {
      value = ((String) value).getBytes(StandardCharsets.UTF_8);
    }
    return value;
  }

  @Override
  int getTempRowCount() {
    return tempData.size();
  }

  @Override
  void reset() {
    super.reset();
    data.clear();
  }

  @Override
  public void close(String name) {
    this.fieldIndex.clear();
    logger.logInfo(
        "Trying to close parquet buffer for channel={} from function={}",
        this.owningChannel.getName(),
        name);
  }

  @Override
  public Flusher<ParquetChunkData> createFlusher(Constants.BdecVersion bdecVerion) {
    return new ParquetFlusher(schema);
  }
}
