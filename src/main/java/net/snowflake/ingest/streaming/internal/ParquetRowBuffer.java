/*
 * Copyright (c) 2022 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import net.snowflake.client.jdbc.internal.google.common.collect.Sets;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.utils.Constants;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.Logging;
import net.snowflake.ingest.utils.Pair;
import net.snowflake.ingest.utils.SFException;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.BdecParquetWriter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

/**
 * The buffer in the Streaming Ingest channel that holds the un-flushed rows, these rows will be
 * converted to Parquet format for faster processing
 */
public class ParquetRowBuffer extends AbstractRowBuffer<ParquetChunkData> {
  private static final Logging logger = new Logging(ParquetRowBuffer.class);

  private static final String PARQUET_MESSAGE_TYPE_NAME = "bdec";

  private final Map<String, Pair<ColumnMetadata, Integer>> fieldIndex;

  /* map that contains metadata like typeinfo for columns and other information needed by the server scanner */
  private final Map<String, String> metadata;

  /* Unflushed rows as Java objects. Needed for the Parquet w/o memory optimization. */
  private final List<List<Object>> data;
  /* BDEC Parquet writer. It is used to buffer unflushed data in Parquet internal buffers instead of using Java objects */
  private BdecParquetWriter bdecParquetWriter;

  private ByteArrayOutputStream fileOutput;
  private final List<List<Object>> tempData;
  private final String channelName;

  private MessageType schema;
  private final boolean bufferForTests;
  private final boolean enableParquetInternalBuffering;
  /** Construct a ParquetRowBuffer object. */
  ParquetRowBuffer(
      OpenChannelRequest.OnErrorOption onErrorOption,
      ZoneId defaultTimezone,
      BufferAllocator allocator,
      String fullyQualifiedChannelName,
      Consumer<Float> rowSizeMetric,
      ChannelRuntimeState channelRuntimeState,
      boolean bufferForTests,
      boolean enableParquetInternalBuffering) {
    super(
        onErrorOption,
        defaultTimezone,
        allocator,
        fullyQualifiedChannelName,
        rowSizeMetric,
        channelRuntimeState);
    fieldIndex = new HashMap<>();
    metadata = new HashMap<>();
    data = new ArrayList<>();
    tempData = new ArrayList<>();
    channelName = fullyQualifiedChannelName;
    this.bufferForTests = bufferForTests;
    this.enableParquetInternalBuffering = enableParquetInternalBuffering;
  }

  @Override
  public void setupSchema(List<ColumnMetadata> columns) {
    fieldIndex.clear();
    metadata.clear();
    metadata.put("sfVer", "1,1");
    List<Type> parquetTypes = new ArrayList<>();
    int id = 1;
    for (ColumnMetadata column : columns) {
      validateColumnCollation(column);
      ParquetTypeGenerator.ParquetTypeInfo typeInfo =
          ParquetTypeGenerator.generateColumnParquetTypeInfo(column, id);
      parquetTypes.add(typeInfo.getParquetType());
      this.metadata.putAll(typeInfo.getMetadata());
      fieldIndex.put(column.getInternalName(), new Pair<>(column, parquetTypes.size() - 1));
      if (!column.getNullable()) {
        addNonNullableFieldName(column.getInternalName());
      }
      this.statsMap.put(
          column.getInternalName(), new RowBufferStats(column.getName(), column.getCollation()));

      if (onErrorOption == OpenChannelRequest.OnErrorOption.ABORT) {
        this.tempStatsMap.put(
            column.getInternalName(), new RowBufferStats(column.getName(), column.getCollation()));
      }

      id++;
    }
    schema = new MessageType(PARQUET_MESSAGE_TYPE_NAME, parquetTypes);
    createFileWriter();
    tempData.clear();
    data.clear();
  }

  /** Create BDEC file writer if Parquet memory optimization is enabled. */
  private void createFileWriter() {
    fileOutput = new ByteArrayOutputStream();
    try {
      if (enableParquetInternalBuffering) {
        bdecParquetWriter = new BdecParquetWriter(fileOutput, schema, metadata, channelName);
      } else {
        this.bdecParquetWriter = null;
      }
      data.clear();
    } catch (IOException e) {
      throw new SFException(ErrorCode.INTERNAL_ERROR, "cannot create parquet writer", e);
    }
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
    return addRow(row, this::writeRow, statsMap, formattedInputColumnNames);
  }

  void writeRow(List<Object> row) {
    if (enableParquetInternalBuffering) {
      bdecParquetWriter.writeRow(row);
    } else {
      data.add(row);
    }
  }

  @Override
  float addTempRow(
      Map<String, Object> row,
      int curRowIndex,
      Map<String, RowBufferStats> statsMap,
      Set<String> formattedInputColumnNames) {
    return addRow(row, this::writeRow, statsMap, formattedInputColumnNames);
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
      Consumer<List<Object>> out,
      Map<String, RowBufferStats> statsMap,
      Set<String> inputColumnNames) {
    Object[] indexedRow = new Object[fieldIndex.size()];
    float size = 0F;

    // Create new empty stats just for the current row.
    Map<String, RowBufferStats> forkedStatsMap = new HashMap<>();

    for (Map.Entry<String, Object> entry : row.entrySet()) {
      String key = entry.getKey();
      Object value = entry.getValue();
      String columnName = LiteralQuoteUtils.unquoteColumnName(key);
      int colIndex = fieldIndex.get(columnName).getSecond();
      RowBufferStats forkedStats = statsMap.get(columnName).forkEmpty();
      forkedStatsMap.put(columnName, forkedStats);
      ColumnMetadata column = fieldIndex.get(columnName).getFirst();
      ColumnDescriptor columnDescriptor = schema.getColumns().get(colIndex);
      PrimitiveType.PrimitiveTypeName typeName =
          columnDescriptor.getPrimitiveType().getPrimitiveTypeName();
      ParquetValueParser.ParquetBufferValue valueWithSize =
          ParquetValueParser.parseColumnValueToParquet(
              value, column, typeName, forkedStats, defaultTimezone);
      indexedRow[colIndex] = valueWithSize.getValue();
      size += valueWithSize.getSize();
    }
    out.accept(Arrays.asList(indexedRow));

    // All input values passed validation, iterate over the columns again and combine their existing
    // statistics with the forked statistics for the current row.
    for (Map.Entry<String, Object> entry : row.entrySet()) {
      String key = entry.getKey();
      String columnName = LiteralQuoteUtils.unquoteColumnName(key);
      RowBufferStats stats = statsMap.get(columnName);
      RowBufferStats forkedStats = forkedStatsMap.get(columnName);
      statsMap.put(columnName, RowBufferStats.getCombinedStats(stats, forkedStats));
    }

    // Increment null count for column missing in the input map
    for (String columnName : Sets.difference(this.fieldIndex.keySet(), inputColumnNames)) {
      statsMap.get(columnName).incCurrentNullCount();
    }
    return size;
  }

  @Override
  void moveTempRowsToActualBuffer(int tempRowCount) {
    tempData.forEach(this::writeRow);
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
  Optional<ParquetChunkData> getSnapshot(final String filePath) {
    // We insert the filename in the file itself as metadata so that streams can work on replicated
    // mixed tables. For a more detailed discussion on the topic see SNOW-561447 and
    // http://go/streams-on-replicated-mixed-tables
    metadata.put(Constants.PRIMARY_FILE_ID_KEY, StreamingIngestUtils.getShortname(filePath));

    List<List<Object>> oldData = new ArrayList<>();
    if (!enableParquetInternalBuffering) {
      data.forEach(r -> oldData.add(new ArrayList<>(r)));
    }
    return rowCount <= 0
        ? Optional.empty()
        : Optional.of(new ParquetChunkData(oldData, bdecParquetWriter, fileOutput, metadata));
  }

  /** Used only for testing. */
  @Override
  Object getVectorValueAt(String column, int index) {
    if (data == null) {
      return null;
    }
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
      value = value instanceof String ? ((String) value).getBytes(StandardCharsets.UTF_8) : value;
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
    createFileWriter();
    data.clear();
  }

  /** Close the row buffer by releasing its internal resources. */
  @Override
  void closeInternal() {
    this.fieldIndex.clear();
    if (bdecParquetWriter != null) {
      try {
        bdecParquetWriter.close();
      } catch (IOException e) {
        throw new SFException(ErrorCode.INTERNAL_ERROR, "Failed to close parquet writer", e);
      }
    }
  }

  @Override
  public Flusher<ParquetChunkData> createFlusher() {
    return new ParquetFlusher(schema, enableParquetInternalBuffering);
  }
}
